package com.telcobright.statewalk.machine;

import com.telcobright.statewalk.event.StatemachineEvent;
import com.telcobright.statewalk.event.TimeoutEvent;
import com.telcobright.statewalk.pool.Poolable;
import com.telcobright.statewalk.state.StateConfig;
import com.telcobright.statewalk.state.StateMap;

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
 * <h2>Identity tokens (v3)</h2>
 * Pooled machines are reused across requests, so object identity alone can
 * never prove that deferred work (a queued chain task, a scheduled timeout)
 * still belongs to the borrow that created it. Two monotonic counters close
 * that hole:
 * <ul>
 *   <li><b>{@link #getEpoch() epoch}</b> — bumped on every
 *       {@link #resetForReuse()}. Anything that captures a machine reference
 *       across threads (queued fire/start, cascade cleanup, global timeout)
 *       captures the epoch too and re-checks it at execution; a mismatch means
 *       the machine has since been reset (and possibly re-borrowed by another
 *       session), so the stale work must no-op.</li>
 *   <li><b>stateVersion</b> — bumped on every state switch AND on reset. State
 *       timeouts capture it at scheduling; a mismatch at fire time means the
 *       state was left (or re-entered — a same-name re-entry is a NEW visit)
 *       and the old timer must no-op. This replaces the old name-based guard,
 *       which could not tell two visits to the same state apart.</li>
 * </ul>
 *
 * <p><b>What is final, and why.</b>
 * <ul>
 *   <li>{@link #start()}, {@link #fire(StatemachineEvent)}, {@link #transitionTo(String)},
 *       {@link #resetForReuse()}: the lifecycle skeleton. Subclass cannot
 *       override or reorder these without breaking the IDLE invariant or
 *       leaking captured references.</li>
 *   <li>{@link #setRegistry(MachineRegistryHandle)}, {@link #setMachineId(String)}:
 *       only a registry may attach a machine to itself.</li>
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
 * <p><b>Refuses to run without a registry.</b> {@link #start()} no-ops (with a
 * WARN) if {@link #setRegistry(MachineRegistryHandle)} has not been called.
 * Machines are not standalone — they only exist as registry-managed resources.
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

    /** Bound by the registry on borrow; cleared on reset. volatile for cross-thread visibility. */
    private volatile MachineRegistryHandle registry;
    private volatile String machineId;
    private volatile String typeName;
    private volatile C context;

    /**
     * Borrow-generation token. Bumped on every {@link #resetForReuse()};
     * captured by everything that defers work against this instance.
     */
    private volatile long epoch;

    /**
     * State-visit token. Bumped on every state switch and on reset; captured
     * by state timeouts. Mutated only under the machine monitor.
     */
    private long stateVersion;

    /**
     * Truly volatile context — never persisted to snapshots. Holds config
     * params, transient service handles (resolvers, clients), per-tenant
     * overlays — anything that must be re-attached after rehydration rather
     * than carried inside the snapshot. Populated by {@link #volatileLoader}
     * on both creation and rehydration; cleared on {@link #resetForReuse()}.
     */
    private Object volatileContext;

    /**
     * Caller-supplied loader registered through the registry builder's
     * {@code volatileLoader(...)} and propagated at borrow time. Invoked from
     * {@link #start()} and {@link #rehydrate} after the persistent
     * {@code context} is in place but before any state action runs.
     */
    private Function<Machine<?>, Object> volatileLoader;

    /** "IDLE" until start; otherwise the current state's name. */
    private volatile String currentState = StateMap.IDLE;
    private volatile boolean started;
    private volatile boolean terminated;

    /** Tracked state-timeout future for the current state, if any. */
    private ScheduledFuture<?> stateTimeoutFuture;

    /**
     * Absolute epoch-ms deadline of the current state's timeout (0 if none) and
     * its target state — captured on state entry (and on rehydrate) so a
     * {@code .stay()} can re-persist the mutated context WITHOUT resetting the
     * state timer.
     */
    private volatile long currentDeadlineMs;
    private volatile String currentTimeoutTarget;

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

    /**
     * Hook invoked just before a registry-forced failover drives this machine
     * to its current state's timeout target (shutdown, global timeout,
     * persistence failure). Gives the subclass a chance to record WHY the
     * session is ending (e.g. stamp the end cause the SDR will carry) before
     * the terminal entry action runs. Default: no-op; a throw is logged and
     * the failover proceeds.
     */
    protected void onForcedFailover(String reason) {}

    // ─────────────────────────────────────────────────────────────────
    // Registry binding (only a Registry may call)
    // ─────────────────────────────────────────────────────────────────

    /** @hidden */
    public final void setRegistry(MachineRegistryHandle handle) {
        this.registry = handle;
    }

    /** Framework-internal: returns the handle bound by the registry. Used by
     * {@code Supervisor} to reach its owning Registry; typical user code
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
     * Borrow-generation token — see the class doc. Captured by deferred work
     * (queued chain tasks, timers) and re-checked at execution; a mismatch
     * proves the machine has been reset since the work was created.
     */
    public final long getEpoch() { return epoch; }

    /**
     * Build (lazily) and return the state graph without starting the machine.
     * Used by the registry builder to validate every registered type's states
     * against the supplied configuration — e.g. that persistence is configured
     * when an offline state is declared, and that route targets exist.
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
     * @throws IllegalStateException if already started, or not in IDLE.
     */
    public final synchronized void start() {
        if (registry == null) {
            LOG.warn("Machine start() skipped — no registry handle (already reset?). id={}, type={}, started={}, terminated={}",
                machineId, typeName, started, terminated);
            return;
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
     * Emit an internal event that the registry routes to the right machine of
     * the same request (the supervisor's resolver decides). Used by state
     * actions — e.g. a signaling child's terminal entry publishes
     * {@code SignalingFailed(cause)} so the supervisor of the same id
     * receives it.
     */
    public final void publishEvent(StatemachineEvent event) {
        MachineRegistryHandle h = registry;
        if (h == null) {
            LOG.warn("[{}] publishEvent ignored — no registry handle (call before start or after reset): {}",
                machineId, event);
            return;
        }
        h.publish(event);
    }

    /**
     * Cause hint for the transition-history tap: the simple class name of the
     * event currently being fired, consumed (and cleared) by the next
     * {@link #transitionTo(String)}. An imperative transition chained from an
     * entry action sees {@code null} (recorded as a chained hop). Framework
     * field — set only inside the machine's synchronized fire path.
     */
    private String pendingCauseHint;

    /**
     * Transition-history tap (the session-supervisor SDR history). Called on
     * EVERY state switch, right after the state field changes and BEFORE the
     * new state's entry action runs, so chained transitions record in
     * chronological order. Runs inside the machine's synchronized transition
     * path: implementations must be allocation-light and must not throw (a
     * throw is logged, never fails the transition). Default is a no-op.
     *
     * @param fromState state being left ({@code null} on the IDLE→initial hop)
     * @param toState   state entered
     * @param causeHint simple class name of the driving event, or {@code null}
     *                  for an imperative/chained transition
     */
    protected void onTransitioned(String fromState, String toState, String causeHint) { }

    /**
     * Fire an event into the machine. Routes through the transition table or
     * stay action of the current state. Silently ignored if the current
     * state has no entry for the event class.
     */
    public final synchronized void fire(StatemachineEvent event) {
        if (!started || terminated || registry == null) return;
        StateConfig cur = stateMap.get(currentState);
        pendingCauseHint = event.getClass().getSimpleName();

        if (debugMode && LOG.isDebugEnabled()) {
            LOG.debug("[{}] fire state={} event={} ctx={}",
                machineId, currentState, event.getClass().getSimpleName(), context);
        }

        // 1. Guarded transitions first — walk in declaration order, first
        // passing guard wins. A null guard counts as "always true". A guarded
        // transition that fires takes precedence over any stay handler for the
        // same event; the stay handler (below) is only the no-transition
        // fallback. Declaring both .on(guard) and .stay() for one event class
        // is therefore legal: guard-pass transitions, guard-fail stays.
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
                        LOG.warn("[{}] guard threw for event={} state={}: {} — treating as false",
                            machineId, event.getClass().getSimpleName(), currentState, e.toString());
                        passes = false;
                    }
                }
                if (passes) {
                    // Transition action: the domain's copy-the-payload step.
                    // Runs before the transition; a throw is logged and does
                    // NOT veto the transition (veto is the guard's job).
                    if (opt.action() != null) {
                        try { opt.action().accept(this, event); }
                        catch (RuntimeException e) {
                            LOG.warn("[{}] transition action threw for event={} {}→{}: {}",
                                machineId, event.getClass().getSimpleName(),
                                currentState, opt.targetState(), e.toString());
                        }
                    }
                    transitionTo(opt.targetState());
                    return;
                }
            }
            if (debugMode && LOG.isDebugEnabled()) {
                LOG.debug("[{}] event={} state={} — all guards rejected; trying stay",
                    machineId, event.getClass().getSimpleName(), currentState);
            }
        }

        // 2. No transition fired — run the stay handler if one is declared.
        BiConsumer<Object, StatemachineEvent> stay = cur.stayActions().get(event.getClass());
        if (stay != null) {
            try { stay.accept(this, event); }
            catch (RuntimeException e) {
                LOG.warn("[{}] stay handler threw for event={} state={}: {}",
                    machineId, event.getClass().getSimpleName(), currentState, e.toString());
            }
            if (debugMode && LOG.isDebugEnabled()) {
                LOG.debug("[{}] stay state={} ctx={}", machineId, currentState, context);
            }
            // A stay handler mutated the context without changing state — persist
            // it. Re-emit the CURRENT state with its UNCHANGED deadline so the
            // stay does not reset the snapshot's state timer.
            if (!terminated && registry != null) {
                registry.onStateTransitioned(machineId, currentState, currentDeadlineMs, currentTimeoutTarget);
            }
            return;
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
     *   <li>Set the new state (bumps the state-visit token).</li>
     *   <li>Schedule new state's timeout (if any).</li>
     *   <li>Run new state's entry action.</li>
     *   <li>If the new state is final, signal the registry to terminate.</li>
     * </ol>
     */
    public final synchronized void transitionTo(String target) {
        if (terminated || registry == null) return;

        StateConfig cur = currentState != null ? stateMap.get(currentState) : null;

        // 1. exit action
        if (cur != null && cur.onExit() != null) {
            try { cur.onExit().accept(this); }
            catch (RuntimeException e) {
                LOG.warn("[{}] exit action threw leaving state={}: {}", machineId, currentState, e.toString());
            }
        }

        // 2. cancel any outstanding state timeout
        if (stateTimeoutFuture != null) {
            stateTimeoutFuture.cancel(false);
            stateTimeoutFuture = null;
        }

        // 3. switch state — a new state visit begins (even a same-name
        // re-entry is a NEW visit; the old visit's timer must not fire).
        StateConfig next = stateMap.get(target);
        String fromState = currentState;
        currentState = next.name();
        final long visit = ++stateVersion;
        String causeHint = pendingCauseHint;
        pendingCauseHint = null;
        try { onTransitioned(fromState, currentState, causeHint); }
        catch (RuntimeException e) {
            LOG.warn("[{}] onTransitioned tap threw for {}→{}: {}", machineId, fromState, currentState, e.toString());
        }

        if (debugMode && LOG.isDebugEnabled()) {
            LOG.debug("[{}] transition {} -> {} ctx={}",
                machineId, fromState, currentState, context);
        }

        // 4. schedule new state's timeout. The registry handle routes the
        // fire through the cell's serial chain, so the timeout body never
        // races in-flight events (per-cell serial invariant); the visit token
        // makes a late-queued timer for a left (or re-entered) state a no-op.
        if (next.timeout() != null && registry != null) {
            StateConfig.Timeout to = next.timeout();
            stateTimeoutFuture = registry.schedule(
                machineId,
                () -> {
                    synchronized (this) {
                        if (terminated || registry == null || stateVersion != visit) return;
                    }
                    fire(new TimeoutEvent(next.name(), to.targetState()));
                    synchronized (this) {
                        if (!terminated && registry != null && stateVersion == visit) {
                            transitionTo(to.targetState());
                        }
                    }
                },
                to.duration(),
                to.unit());
        }

        // 5. entry action
        if (next.onEntry() != null) {
            try { next.onEntry().accept(this); }
            catch (RuntimeException e) {
                LOG.warn("[{}] entry action threw entering state={}: {}", machineId, currentState, e.toString());
            }
        }

        // 5b. notify registry — fires on EVERY transition. Persistence (if
        // configured) hangs off of this; so does last-event-time tracking,
        // metrics, etc. The registry decides what to do; the machine just
        // tells it "I moved."
        long deadlineMs = next.timeout() != null
            ? System.currentTimeMillis() + next.timeout().unit().toMillis(next.timeout().duration())
            : 0L;
        String timeoutTarget = next.timeout() != null ? next.timeout().targetState() : null;
        this.currentDeadlineMs = deadlineMs;          // remembered so a later .stay() re-persists with the same deadline
        this.currentTimeoutTarget = timeoutTarget;
        if (registry == null) {
            // Benign concurrent-reset bail: resetForReuse() nulled the registry while this
            // transition was in flight (the machine is being torn down / returned to the pool).
            // The state's onEntry already ran above; we just skip the registry post-transition
            // bookkeeping.
            LOG.debug("transitionTo: registry became null mid-transition for {} state={} (machine reset concurrently — skipping registry notify)", machineId, next.name());
            return;
        }
        registry.onStateTransitioned(machineId, next.name(), deadlineMs, timeoutTarget);

        // 6a. offline? notify registry to suspend. Per spec, the offline
        // state's exit action is NOT fired during this transition — the
        // machine is being suspended, not leaving the state.
        if (next.offline() && !terminated && registry != null) {
            registry.onMachineWentOffline(machineId);
            return;
        }

        // 6b. terminal? signal registry to run the termination ritual.
        if (next.finalState() && !terminated && registry != null) {
            terminated = true;
            registry.onMachineReachedTerminal(machineId);
        }
    }

    /**
     * Framework-internal forced failover: drive a live machine to its current
     * state's declared timeout target (always a final state, per the builder
     * invariant) so that terminal-state work — the session SDR, the teardown
     * backstop — runs on EVERY exit path, not only the graceful ones. Used by
     * the registry for shutdown, global timeout without a configured target,
     * and persistence-failure aborts.
     *
     * <p>No-ops when the machine is not started, already terminated, or reset.
     */
    public final synchronized void forceFailover(String reason) {
        if (!started || terminated || registry == null) return;
        StateConfig cur = stateMap.get(currentState);
        if (cur.finalState()) return;                    // terminal work already ran
        try { onForcedFailover(reason); }
        catch (RuntimeException e) {
            LOG.warn("[{}] onForcedFailover hook threw: {}", machineId, e.toString());
        }
        StateConfig.Timeout to = cur.timeout();
        if (to == null || to.targetState() == null) {
            // Unreachable for user states (mandatory-timeout invariant), but IDLE
            // has no timeout: a started machine is never in IDLE, so just guard.
            LOG.warn("[{}] forceFailover({}) from state={} — no timeout target; hard-terminating",
                machineId, reason, currentState);
            terminated = true;
            return;
        }
        LOG.info("[{}] forced failover from state={} → {} ({})",
            machineId, currentState, to.targetState(), reason);
        transitionTo(to.targetState());
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
        final long visit = ++stateVersion;                // resuming = a fresh visit
        this.currentDeadlineMs = timeoutDeadlineMs;       // so a .stay() after resume re-persists the right deadline
        this.currentTimeoutTarget = timeoutTargetState;

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

        // Schedule remaining timeout (if any) — same chain routing + visit
        // token as a normal state entry.
        if (saved.timeout() != null && timeoutDeadlineMs > 0) {
            long remainingMs = Math.max(0, timeoutDeadlineMs - now);
            StateConfig.Timeout to = saved.timeout();
            stateTimeoutFuture = registry.schedule(
                machineId,
                () -> {
                    synchronized (this) {
                        if (terminated || registry == null || stateVersion != visit) return;
                    }
                    fire(new TimeoutEvent(savedStateName, to.targetState()));
                    synchronized (this) {
                        if (!terminated && registry != null && stateVersion == visit) {
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
     * Bumps both identity tokens, so every piece of deferred work created
     * against the previous borrow becomes a provable no-op.
     */
    @Override
    public final synchronized void resetForReuse() {
        if (stateTimeoutFuture != null) {
            stateTimeoutFuture.cancel(false);
            stateTimeoutFuture = null;
        }

        // Subclass-specific reset first (so it can read state if needed).
        try { onResetSubclass(); }
        catch (RuntimeException e) {
            LOG.warn("[{}] onResetSubclass threw: {}", machineId, e.toString());
        }

        epoch++;
        stateVersion++;
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
        currentDeadlineMs = 0L;
        currentTimeoutTarget = null;
        pendingCauseHint = null;
    }

    // ─────────────────────────────────────────────────────────────────
    // Registry handle (lightweight back-reference; framework-owned)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Limited handle a {@link Machine} uses to talk back to its registry —
     * scheduling timeouts, signalling terminal arrival, and notifying state
     * transitions for persistence.
     *
     * <p><b>Threading contract (v3):</b> {@link #schedule} implementations
     * must route the runnable through the machine's per-cell serial chain, so
     * timer bodies never race queued events.
     *
     * <p>Public so registries in other packages can implement it; treat as
     * framework-internal otherwise.
     */
    public interface MachineRegistryHandle {
        ScheduledFuture<?> schedule(String machineId, Runnable r,
                                    long delay, java.util.concurrent.TimeUnit unit);
        void onMachineReachedTerminal(String machineId);

        /**
         * Publish an internal event for the registry to route (the request's
         * supervisor resolver decides self/forward/drop).
         */
        default void publish(StatemachineEvent event) {
            throw new UnsupportedOperationException(
                "publish() is not supported by this registry handle.");
        }

        /**
         * Called after every successful state transition (entry action ran).
         * Registry hooks any cross-cutting concern off this notification:
         * persistence save, last-event-time update for hung detection,
         * metrics, custom listeners.
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
