package com.telcobright.statewalk.v2.machine;

import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.registry.consumes.TimeoutEvent;
import com.telcobright.statewalk.v2.pool.Poolable;
import com.telcobright.statewalk.v2.state.StateConfig;
import com.telcobright.statewalk.v2.state.StateMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Base class for every state machine in the framework.
 *
 * <p><b>Mechanism vs policy.</b> The framework owns the mechanism: pool reset,
 * IDLE invariant, lifecycle ordering, registry-required execution. Subclasses
 * own the policy: what states exist, what entry/exit actions run, what
 * transitions fire on what events. Policy is supplied as data via
 * {@link #defineStates()} (see {@link StateMap}).
 *
 * <p><b>What is final, and why.</b>
 * <ul>
 *   <li>{@link #start()}, {@link #fire(StatemachineEvent)}, {@link #transitionTo(String)},
 *       {@link #resetForReuse()}: the lifecycle skeleton. Subclass cannot
 *       override or reorder these without breaking the IDLE invariant or
 *       leaking captured references.</li>
 *   <li>{@link #setRegistry(MachineRegistryHandle)}, {@link #setMachineId(String)}:
 *       package-private. Only a registry can attach a machine to itself.</li>
 * </ul>
 *
 * <p><b>What subclasses must provide.</b>
 * <ul>
 *   <li>{@link #defineStates()} — declarative state graph (data, not code).</li>
 *   <li>{@link #createContext()} — fresh context per machine instance.</li>
 *   <li>Optional: override {@link #onResetSubclass()} to clear subclass-specific
 *       fields beyond what IDLE-state reset already does.</li>
 * </ul>
 *
 * <p><b>Refuses to run without a registry.</b> {@link #start()} throws if
 * {@link #setRegistry(MachineRegistryHandle)} has not been called. Machines are
 * not standalone — they only exist as registry-managed resources.
 *
 * @param <C> context type — the single state slot per machine. Set at
 *            dispatch time by the registry; mutated by state actions;
 *            snapshotted on every transition when persistence is configured;
 *            cleared on reset. There is no separate "persisting entity" or
 *            "task" generic — the dispatched value IS the initial context.
 */
public abstract class Machine<C> implements Poolable {

    /**
     * Framework logger inherited by every machine type. Subclasses should not
     * declare their own logger for lifecycle / state-transition events — the
     * base class emits everything required.
     */
    protected static final Logger LOG = LoggerFactory.getLogger(Machine.class);

    /** Set lazily on first {@link #start()} so subclasses don't repay graph build cost per borrow. */
    private StateMap stateMap;

    /** Bound by the registry on borrow; cleared on reset. */
    private MachineRegistryHandle registry;
    private String machineId;
    private String typeName;
    private C context;

    /**
     * Truly volatile context — never persisted to snapshots. Holds config
     * params, transient service handles (resolvers, clients), per-tenant
     * overlays — anything that must be re-attached after rehydration rather
     * than carried inside the snapshot. Populated by {@link #volatileLoader}
     * on both creation and rehydration; cleared on {@link #resetForReuse()}.
     */
    private Object volatileContext;

    /**
     * Caller-supplied loader registered through
     * {@code Statewalk.builder().volatileLoader(...)} and propagated by the
     * registry at borrow time. Invoked from {@link #start()} and
     * {@link #rehydrate} after the persistent {@code context} is in place
     * but before any state action runs.
     */
    private Function<Machine<?>, Object> volatileLoader;

    /** "IDLE" until start; otherwise the current state's name. */
    private volatile String currentState = StateMap.IDLE;
    private volatile boolean started;
    private volatile boolean terminated;

    /** Tracked state-timeout future for the current state, if any. */
    private ScheduledFuture<?> stateTimeoutFuture;

    /**
     * If true, this machine emits DEBUG-level state-transition traces (state +
     * context + event tuple) on every {@link #fire(StatemachineEvent)} and
     * {@link #transitionTo(String)}. Set by the registry at dispatch based on
     * the registry's debug-sample rate; cleared on {@link #resetForReuse()}.
     */
    private volatile boolean debugMode;

    // ─────────────────────────────────────────────────────────────────
    // Subclass extension points
    // ─────────────────────────────────────────────────────────────────

    /**
     * Build the state graph for this machine type. Called once per machine
     * instance, lazily on first start. Implementations should construct via
     * {@link StateMap#builder()} and return the result of {@link StateMap.Builder#build()}.
     *
     * <p>The IDLE state is auto-injected — do not declare it.
     */
    protected abstract StateMap defineStates();

    /**
     * Allocate an initial context when dispatch did NOT provide one. Default
     * returns {@code null}; override only if your machine type supports
     * dispatch-without-context (uncommon — most call sites pass a fully
     * populated context to {@code registry.dispatch(id, ctx)}).
     */
    protected C createContext() { return null; }

    /**
     * Subclass hook for reset. Called from {@link #resetForReuse()} after
     * the framework has cleared common fields. Override to null subclass-
     * specific fields the framework can't see.
     *
     * <p>Default: no-op. Most subclasses won't need to override — they keep
     * state in {@code C}, which gets re-created per borrow.
     */
    protected void onResetSubclass() {}

    // ─────────────────────────────────────────────────────────────────
    // Registry binding (package-private — only Registry can call)
    // ─────────────────────────────────────────────────────────────────

    /** @hidden */
    public final void setRegistry(MachineRegistryHandle handle) {
        this.registry = handle;
    }

    /** Framework-internal: returns the handle bound by the registry. Used by
     * {@code Supervisor} to reach its owning flat Registry; typical user code
     * has no reason to call this. */
    public final MachineRegistryHandle getRegistry() {
        return this.registry;
    }

    /** @hidden */
    public final void setMachineId(String id) {
        this.machineId = id;
    }

    /** @hidden Set by the registry on borrow — names the machine's type within the registry. */
    public final void setTypeName(String name) {
        this.typeName = name;
    }

    /** Machine's type name within its registry — e.g. {@code "CallSupervisor"}, {@code "CallSignaling"}. */
    public final String getTypeName() { return typeName; }

    /**
     * @hidden Seat the initial context. Called by the registry on dispatch
     * before {@link #start()}. If not called, {@link #start()} falls back to
     * {@link #createContext()}.
     */
    public final void setInitialContext(C ctx) {
        this.context = ctx;
    }

    /** @hidden Set by the registry on dispatch — do not call from user code. */
    public final void setDebugMode(boolean v) { this.debugMode = v; }
    public final boolean isDebugMode() { return debugMode; }

    /**
     * @hidden Set by the registry on borrow (dispatch + rehydrate) before the
     * machine starts. The same callback fires on both creation and rehydration.
     */
    public final void setVolatileContextLoader(Function<Machine<?>, Object> loader) {
        this.volatileLoader = loader;
    }

    // ─────────────────────────────────────────────────────────────────
    // Public read-only accessors
    // ─────────────────────────────────────────────────────────────────

    public final String getMachineId() { return machineId; }
    public final C getContext() { return context; }
    public final String getCurrentState() { return currentState; }
    public final boolean isStarted() { return started; }
    public final boolean isTerminated() { return terminated; }
    public final boolean isInState(String name) { return currentState.equals(name); }
    public final boolean isIdle() { return StateMap.IDLE.equals(currentState); }

    /**
     * Build (lazily) and return the state graph without starting the machine.
     * Used by the framework's top-level builder to validate that every
     * registry's states are consistent with the supplied configuration —
     * for example, that persistence is configured when an offline state is
     * declared.
     */
    public final StateMap peekStateMap() {
        if (stateMap == null) stateMap = defineStates();
        return stateMap;
    }

    // ─────────────────────────────────────────────────────────────────
    // Lifecycle (final — skeleton not overridable)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Start the machine. Validates the IDLE invariant, allocates context if
     * needed, builds the state map (lazily), and transitions IDLE → initial state.
     *
     * @throws IllegalStateException if no registry is bound, or if not in IDLE.
     */
    public final synchronized void start() {
        if (registry == null) {
            throw new IllegalStateException(
                "Machine cannot run without a registry. Use registry.dispatch(...) or spawnChild(...).");
        }
        if (started) {
            throw new IllegalStateException("Machine already started: " + machineId);
        }
        if (!StateMap.IDLE.equals(currentState)) {
            throw new IllegalStateException(
                "Machine is not in IDLE at start (was " + currentState + ") — pool reset bug suspected");
        }

        if (stateMap == null) {
            stateMap = defineStates();
            if (!stateMap.has(StateMap.IDLE)) {
                throw new IllegalStateException(
                    "StateMap missing IDLE — should be auto-injected by StateMap.Builder");
            }
        }
        if (context == null) {
            context = createContext();
        }
        populateVolatileContext();

        started = true;
        terminated = false;
        transitionTo(stateMap.initialState());
    }

    /**
     * Run the registered loader against this machine and stash the result.
     * Same callback fires on both creation and rehydration, so state action
     * code reads {@link #getVolatileContext()} the same way in either path.
     * Loader exceptions are logged and swallowed — a missing volatile
     * context surfaces at the call site, not as a poisoned machine.
     */
    private void populateVolatileContext() {
        Function<Machine<?>, Object> ldr = this.volatileLoader;
        if (ldr == null) return;
        try {
            this.volatileContext = ldr.apply(this);
        } catch (RuntimeException e) {
            LOG.warn("[{}] volatile-context loader threw: {} — volatileContext left null",
                machineId, e.toString());
            this.volatileContext = null;
        }
    }

    /**
     * Subclasses (and state actions) read this to get at config / service
     * handles attached by the loader. Returns {@code null} if no loader was
     * registered for this machine's registry.
     */
    public final Object getVolatileContext() {
        return volatileContext;
    }

    /**
     * Emit an internal event that the registry's resolver routes to the right
     * sibling machine in the same domain. Used by state actions in a
     * MultiRegistry — e.g. CallSignalingMachine.Terminated.entry publishes
     * {@code SignalingTerminated(uuid, cause)} so the CallSupervisor for the
     * same uuid receives it.
     */
    public final void publishEvent(com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent event) {
        if (registry == null) {
            throw new IllegalStateException(
                "Cannot publish — machine has no registry handle (call before start or after reset).");
        }
        registry.publish(event);
    }

    /**
     * Fire an event into the machine. Routes through transition table or
     * stay action of the current state. Silently ignored if the current
     * state has no entry for the event class.
     */
    public final synchronized void fire(StatemachineEvent event) {
        if (!started || terminated) return;
        StateConfig cur = stateMap.get(currentState);

        if (debugMode && LOG.isDebugEnabled()) {
            LOG.debug("[{}] fire state={} event={} ctx={}",
                machineId, currentState, event.getClass().getSimpleName(), context);
        }

        BiConsumer<Object, StatemachineEvent> stay = cur.stayActions().get(event.getClass());
        if (stay != null) {
            stay.accept(this, event);
            if (debugMode && LOG.isDebugEnabled()) {
                LOG.debug("[{}] stay state={} ctx={}", machineId, currentState, context);
            }
            return;
        }
        // Walk guarded transition list in declaration order; first match wins.
        // A null guard counts as "always true" (unconditional / fallback).
        var options = cur.transitions().get(event.getClass());
        if (options != null) {
            for (var opt : options) {
                var guard = opt.guard();
                boolean passes;
                if (guard == null) {
                    passes = true;
                } else {
                    try { passes = guard.test(this, event); }
                    catch (RuntimeException e) {
                        if (debugMode && LOG.isDebugEnabled()) {
                            LOG.debug("[{}] guard threw for event={} state={}: {} — treating as false",
                                machineId, event.getClass().getSimpleName(), currentState, e.toString());
                        }
                        passes = false;
                    }
                }
                if (passes) {
                    transitionTo(opt.targetState());
                    return;
                }
            }
            if (debugMode && LOG.isDebugEnabled()) {
                LOG.debug("[{}] event={} state={} — all guards rejected; staying",
                    machineId, event.getClass().getSimpleName(), currentState);
            }
        }
        // else silently ignore (machine doesn't care about this event in this state)
    }

    /**
     * Imperative transition. Called by the framework on start, by entry
     * actions to chain transitions, and by event dispatch when a transition
     * matches.
     *
     * <p>Order:
     * <ol>
     *   <li>Run current state's exit action.</li>
     *   <li>Cancel current state's timeout (if any).</li>
     *   <li>Set the new state.</li>
     *   <li>Schedule new state's timeout (if any).</li>
     *   <li>Run new state's entry action.</li>
     *   <li>If the new state is final, signal the registry to terminate.</li>
     * </ol>
     */
    public final synchronized void transitionTo(String target) {
        if (terminated) return;

        StateConfig cur = currentState != null ? stateMap.get(currentState) : null;

        // 1. exit action
        if (cur != null && cur.onExit() != null) {
            try { cur.onExit().accept(this); } catch (RuntimeException ignored) {}
        }

        // 2. cancel any outstanding state timeout
        if (stateTimeoutFuture != null) {
            stateTimeoutFuture.cancel(false);
            stateTimeoutFuture = null;
        }

        // 3. switch state
        StateConfig next = stateMap.get(target);
        String fromState = currentState;
        currentState = next.name();

        if (debugMode && LOG.isDebugEnabled()) {
            LOG.debug("[{}] transition {} -> {} ctx={}",
                machineId, fromState, currentState, context);
        }

        // 4. schedule new state's timeout
        if (next.timeout() != null) {
            StateConfig.Timeout to = next.timeout();
            stateTimeoutFuture = registry.schedule(
                machineId,
                () -> {
                    synchronized (this) {
                        if (terminated || !currentState.equals(next.name())) return;
                    }
                    fire(new TimeoutEvent(next.name(), to.targetState()));
                    synchronized (this) {
                        if (currentState.equals(next.name())) {
                            transitionTo(to.targetState());
                        }
                    }
                },
                to.duration(),
                to.unit());
        }

        // 5. entry action
        if (next.onEntry() != null) {
            try { next.onEntry().accept(this); } catch (RuntimeException ignored) {}
        }

        // 5b. notify registry — fires on EVERY transition. Persistence (if
        // configured) hangs off of this; so does last-event-time tracking,
        // metrics, etc. The registry decides what to do; the machine just
        // tells it "I moved."
        long deadlineMs = next.timeout() != null
            ? System.currentTimeMillis() + next.timeout().unit().toMillis(next.timeout().duration())
            : 0L;
        String timeoutTarget = next.timeout() != null ? next.timeout().targetState() : null;
        registry.onStateTransitioned(machineId, next.name(), deadlineMs, timeoutTarget);

        // 6a. offline? notify registry to suspend. Per spec, the offline
        // state's exit action is NOT fired during this transition — the
        // machine is being suspended, not leaving the state.
        if (next.offline() && !terminated) {
            registry.onMachineWentOffline(machineId);
            return;
        }

        // 6b. terminal? signal registry to run the 8-step ritual.
        if (next.finalState() && !terminated) {
            terminated = true;
            registry.onMachineReachedTerminal(machineId);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Rehydration (final — entered after a persistence load)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Restore a machine to a previously-persisted state. Called by the
     * registry after the snapshot has been loaded and the context
     * deserialized.
     *
     * <p>The entry action of the saved state is <b>not</b> replayed —
     * it already ran when the state was originally entered. Behaviour
     * depends on whether the saved state's timeout has elapsed since
     * persistence:
     * <ul>
     *   <li><b>Timeout already fired</b> (deadline passed): run the saved
     *       state's exit action, then transition to the timeout target —
     *       which by builder rules is a final state, so the machine
     *       enters the target's entry action and immediately terminates.</li>
     *   <li><b>Timeout still pending</b>: schedule the remaining portion
     *       of the timeout, no transition.</li>
     *   <li><b>No timeout</b>: just sit in the saved state waiting for
     *       events.</li>
     * </ul>
     *
     * @param savedStateName       the state the machine was in when persisted
     * @param deserializedContext  rehydrated context object (may be null)
     * @param timeoutTargetState   target of the saved state's timeout (null if none)
     * @param timeoutDeadlineMs    wall-clock epoch millis when timeout matures
     *                             (0 if no timeout active)
     */
    @SuppressWarnings("unchecked")
    public final synchronized void rehydrate(String savedStateName,
                                              Object deserializedContext,
                                              String timeoutTargetState,
                                              long timeoutDeadlineMs) {
        if (registry == null) {
            throw new IllegalStateException(
                "Machine cannot rehydrate without a registry. Use registry rehydration path.");
        }
        if (started) {
            throw new IllegalStateException("Machine already started: " + machineId);
        }
        if (!StateMap.IDLE.equals(currentState)) {
            throw new IllegalStateException(
                "Machine is not in IDLE at rehydrate (was " + currentState + ")");
        }

        if (stateMap == null) stateMap = defineStates();
        if (!stateMap.has(savedStateName)) {
            throw new IllegalStateException(
                "Saved state '" + savedStateName + "' not found in machine's state graph");
        }

        this.context = (C) deserializedContext;
        populateVolatileContext();
        this.started = true;
        this.terminated = false;
        this.currentState = savedStateName;

        StateConfig saved = stateMap.get(savedStateName);
        long now = System.currentTimeMillis();
        boolean timeoutFired = timeoutDeadlineMs > 0 && now >= timeoutDeadlineMs;

        if (timeoutFired && timeoutTargetState != null) {
            // The timeout matured during downtime. transitionTo will run
            // saved's onExit and then the target final state's onEntry +
            // termination. Per spec: rehydration must NOT replay saved's
            // onEntry (already done before persistence) — we don't, because
            // we set currentState directly above without invoking transitionTo.
            transitionTo(timeoutTargetState);
            return;
        }

        // Schedule remaining timeout (if any).
        if (saved.timeout() != null && timeoutDeadlineMs > 0) {
            long remainingMs = Math.max(0, timeoutDeadlineMs - now);
            StateConfig.Timeout to = saved.timeout();
            stateTimeoutFuture = registry.schedule(
                machineId,
                () -> {
                    synchronized (this) {
                        if (terminated || !currentState.equals(savedStateName)) return;
                    }
                    fire(new TimeoutEvent(savedStateName, to.targetState()));
                    synchronized (this) {
                        if (currentState.equals(savedStateName)) {
                            transitionTo(to.targetState());
                        }
                    }
                },
                remainingMs,
                java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        if (debugMode && LOG.isDebugEnabled()) {
            LOG.debug("[{}] rehydrated to state={} ctx={}", machineId, currentState, context);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Pool reset (final — common reset; subclass extends via onResetSubclass)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Pool reset. Called by the registry after the termination ritual, before
     * returning the machine to its pool. After this returns, the machine is
     * indistinguishable from a freshly allocated instance and is in IDLE.
     */
    @Override
    public final synchronized void resetForReuse() {
        if (stateTimeoutFuture != null) {
            stateTimeoutFuture.cancel(false);
            stateTimeoutFuture = null;
        }

        // Subclass-specific reset first (so it can read state if needed).
        try { onResetSubclass(); } catch (RuntimeException ignored) {}

        registry = null;
        machineId = null;
        typeName = null;
        context = null;
        volatileContext = null;
        volatileLoader = null;
        started = false;
        terminated = false;
        debugMode = false;
        currentState = StateMap.IDLE;
    }

    // ─────────────────────────────────────────────────────────────────
    // Registry handle (lightweight back-reference; framework-owned)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Limited handle a {@link Machine} uses to talk back to its registry —
     * scheduling timeouts, signalling terminal arrival, and notifying state
     * transitions for persistence.
     *
     * <p>Public so registries in other packages can implement it; treat as
     * framework-internal otherwise.
     */
    public interface MachineRegistryHandle {
        ScheduledFuture<?> schedule(String machineId, Runnable r,
                                    long delay, java.util.concurrent.TimeUnit unit);
        void onMachineReachedTerminal(String machineId);

        /**
         * Publish an internal event for the registry's resolver to route.
         * Used by multi-machine registries where machine instances in the
         * same domain communicate via typed events; the resolver maps the
         * event class to a target machine type + id. Default throws — only
         * multi-machine registries implement it.
         */
        default void publish(com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent event) {
            throw new UnsupportedOperationException(
                "publish() is supported only by MultiRegistry-managed machines.");
        }

        /**
         * Called after every successful state transition (entry action ran
         * cleanly). Registry hooks any cross-cutting concern off this
         * notification: persistence save, last-event-time update for hung
         * detection, metrics, custom listeners.
         *
         * @param newState           the state just entered
         * @param timeoutDeadlineMs  wall-clock epoch millis when the state's
         *                           timeout matures, or {@code 0} if no
         *                           timeout active
         * @param timeoutTargetState the timeout's target state, or {@code null}
         *                           if no timeout active
         */
        default void onStateTransitioned(String machineId, String newState,
                                         long timeoutDeadlineMs, String timeoutTargetState) {}

        /**
         * Called by the machine when it enters a state declared
         * {@code .offline()}. The registry persists the snapshot (already
         * saved by {@link #onStateTransitioned}), removes the machine from
         * the active map, and returns it to the pool. The machine is NOT
         * terminated — a later inbound event for the same id rehydrates it
         * via the normal rehydration path.
         */
        default void onMachineWentOffline(String machineId) {}
    }
}
