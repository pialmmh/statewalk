package com.telcobright.statewalk.v2.registry.api;

import com.telcobright.statewalk.v2.registry.api.DispatchResult;
import com.telcobright.statewalk.v2.registry.internal.QuotaController;
import com.telcobright.statewalk.v2.registry.api.QuotaKeys;
import com.telcobright.statewalk.v2.registry.api.QuotaLimits;
import com.telcobright.statewalk.v2.registry.api.RejectCause;
import com.telcobright.statewalk.v2.channel.Channel;
import com.telcobright.statewalk.v2.registry.consumes.EventTypeRegistry;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.executor.BoundedVirtualThreadExecutor;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.persistence.MachineSnapshot;
import com.telcobright.statewalk.v2.persistence.PersistenceProvider;
import com.telcobright.statewalk.v2.persistence.SnapshotSerializer;
import com.telcobright.statewalk.v2.pool.ObjectPoolManager;
import com.telcobright.statewalk.v2.state.StateMap;
import com.telcobright.statewalk.v2.timeout.TimeoutManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base registry for a single machine type.
 *
 * <p><b>The registry is the only valid path to a machine.</b> Machines are
 * borrowed, started, fed events, and reclaimed exclusively through this class.
 * Subclasses provide protocol-specific bits ({@link #createMachineTemplate()},
 * {@link #handleTermination}, {@link #forceCancel}, name, sizing) and inherit
 * the lifecycle skeleton.
 *
 * <h2>Mechanism (final, framework-owned)</h2>
 * <ul>
 *   <li>{@link #dispatch(String, Object)} — borrow + assert IDLE + register +
 *       start.</li>
 *   <li>{@link #onMachineTerminated(String)} — the 8-step ritual: dedup,
 *       cancel global timeout, remove from active map, subclass hook,
 *       context cleanup, machine reset, return to pool, schedule dedup TTL.</li>
 *   <li>{@link #forceCleanupMachine(String)} — converges on the same ritual.</li>
 *   <li>{@link #shutdown()} — drain in-flight, cancel timeouts, clear pools.</li>
 * </ul>
 *
 * <h2>Channel binding</h2>
 * <p>Subclasses register a {@link Channel} via {@link #setChannel(Channel)}.
 * The framework wires the inbound handler so events arrive at
 * {@link #onInboundEvent(String, StatemachineEvent)}, which routes by id to the active
 * machine.
 *
 * @param <M> machine type produced by this registry
 * @param <C> volatile context type for the machine
 */
public abstract class Registry<M extends Machine<C>, C> implements Machine.MachineRegistryHandle {

    /**
     * Framework logger inherited by every registry type. Subclasses should not
     * declare their own logger for lifecycle events — the base class emits
     * machine-creation, teardown, pool-return, timeout, and force-cleanup logs
     * at INFO level.
     */
    protected static final Logger LOG = LoggerFactory.getLogger(Registry.class);

    // ─── Identity / sizing (subclass) ─────────────────────────────────
    protected abstract String getRegistryName();
    protected abstract int getMaxConcurrent();
    protected abstract long getGlobalTimeoutMs();

    // ─── Machine factory (subclass) ───────────────────────────────────
    protected abstract M createMachineTemplate();

    // ─── Subclass hooks ───────────────────────────────────────────────

    /**
     * Run after the framework has removed the machine from {@code activeMachines}
     * and before context/task cleanup. Settlement, CDR emission, partner-counter
     * decrement go here.
     *
     * <p>Wrapped in try-catch by the framework — exceptions are logged, the
     * ritual proceeds.
     */
    protected void handleTermination(String requestId, M machine, String finalState) {}

    /**
     * Protocol-specific cancel for a hung machine. Default: cancel via primary
     * channel if any. Override for cross-channel logic.
     */
    protected void forceCancel(String requestId) {
        Channel<?, ?> ch = primaryChannel;
        if (ch != null && ch.isConnected()) {
            ch.cancel(requestId);
        }
    }

    /**
     * Called from {@link #shutdown()} after timeouts cancelled, before pool clear.
     * Override for protocol-specific drain (e.g. hangup all active calls).
     */
    protected void onShutdown() {}

    /**
     * Subclass hook: extract quota keys from a task. Default returns
     * {@link QuotaKeys#NONE} (no quota dimensions). Override to return
     * partner / route keys from the task — e.g.
     * {@code QuotaKeys.of(task.partnerId(), task.routeId())}.
     */
    protected QuotaKeys quotaKeysFor(Object task) {
        return QuotaKeys.NONE;
    }

    /**
     * Subclass hook: declare quota limits. Default is unlimited. Override
     * to set per-partner / per-route concurrent and TPS thresholds.
     */
    protected QuotaLimits getQuotaLimits() {
        return QuotaLimits.UNLIMITED;
    }

    /**
     * Subclass hook: name of the state to transition to when the global
     * timeout fires. <b>Must be a final state.</b> The builder may override
     * this via {@code Statewalk.builder().globalTimeout(...)}.
     *
     * <p>Default {@code null} preserves the legacy behaviour
     * ({@link #forceCleanupMachine} on timeout). With a target state set,
     * the registry instead submits a {@link Machine#transitionTo} to the
     * target — which by builder validation is final, so the machine runs
     * its terminal entry action and is reclaimed through the standard
     * 8-step ritual.
     */
    protected String getGlobalTimeoutTargetState() {
        return null;
    }

    /**
     * Subclass hook: build a task object from a first-message event.
     * Called by {@link #onInboundEvent(String, StatemachineEvent)} when an
     * event with {@code isFirst() == true} arrives for an unknown machine id
     * — the framework wants to dispatch a new machine but needs the task
     * payload.
     *
     * <p>Default: throws. Override only if the registry supports inbound-
     * driven creation.
     *
     * @param requestId  the new machine's id (taken from the event's protocol
     *                   id field by upstream code)
     * @param firstEvent the event flagged as {@code isFirst()}
     */
    protected Object createTaskFromFirstEvent(String requestId, StatemachineEvent firstEvent) {
        throw new UnsupportedOperationException(
            "[" + getRegistryName() + "] received first-event " + firstEvent.getClass().getSimpleName()
            + " but createTaskFromFirstEvent is not overridden. Either override it to enable "
            + "inbound-driven creation, or use system.dispatch(name, id, task) explicitly.");
    }

    // ─── Internal state ───────────────────────────────────────────────

    protected ObjectPoolManager<M> machinePool;
    protected TimeoutManager timeoutManager;

    /**
     * Bounded virtual-thread executor for machine work — entry actions,
     * event dispatch, terminal cleanup, persistence saves. Channel threads
     * submit work here and return immediately. Per-machine ordering is
     * preserved by {@code synchronized} on Machine.fire / transitionTo.
     */
    protected BoundedVirtualThreadExecutor machineWork;

    protected final ConcurrentHashMap<String, M> activeMachines = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> startTimeMs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastEventTimeMs = new ConcurrentHashMap<>();
    private final Set<String> terminatedRequests = ConcurrentHashMap.newKeySet();

    /**
     * Per-machine FIFO dispatch chains. Each machineId has a
     * {@link java.util.concurrent.CompletableFuture} chain — every submitted
     * task is appended via {@code thenRunAsync}, guaranteeing in-order
     * processing per machine while different machines run in parallel on
     * the bounded VT executor.
     *
     * <p>Entries are removed when the machine terminates or goes offline —
     * keeps the map bounded by the active-machine count.
     */
    private final ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<Void>> chains =
        new ConcurrentHashMap<>();

    protected volatile boolean initialized = false;
    protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private final AtomicLong totalStarted = new AtomicLong(0);
    private final AtomicLong totalCompleted = new AtomicLong(0);

    private volatile Channel<?, ?> primaryChannel;
    private final Map<String, Channel<?, ?>> channels = new ConcurrentHashMap<>();

    /**
     * Type registry assigned by {@link Statewalk.Builder}. Used to validate
     * inbound event registration and to return poolable events after dispatch.
     */
    private volatile EventTypeRegistry eventTypes;

    /**
     * "1 in N" — every Nth dispatched machine runs in debug mode and emits
     * per-state-transition DEBUG logs. {@code 0} disables sampling entirely.
     * Set by the builder.
     */
    private volatile int debugSampleRate = 0;
    private final AtomicLong dispatchCounter = new AtomicLong(0);

    /**
     * Persistence provider — set by the builder. {@code null} disables both
     * save-on-transition and rehydration. When non-null, every successful
     * state transition writes a snapshot; terminal arrival deletes it.
     */
    private volatile PersistenceProvider persistenceProvider;

    /**
     * If true (and a {@link #persistenceProvider} is set), inbound events
     * for unknown machine ids attempt to rehydrate from the provider.
     * If false, unknown ids without {@code isFirst()} events throw.
     */
    private volatile boolean rehydrateEnabled;

    /** Per-key concurrent + TPS quota tracker. Always present; no-op if limits unset. */
    private final QuotaController quotaController = new QuotaController();

    /** Per-machine quota keys captured at dispatch — needed to release on terminate. */
    private final ConcurrentHashMap<String, QuotaKeys> dispatchQuotaKeys = new ConcurrentHashMap<>();

    /**
     * Effective global-timeout target state — builder override of
     * {@link #getGlobalTimeoutTargetState()}, or that subclass value if no
     * builder override. When non-null, global-timeout fires transition the
     * machine to this state via {@link Machine#transitionTo}; when null,
     * legacy {@link #forceCleanupMachine} is used.
     */
    private volatile String effectiveGlobalTimeoutTargetState;

    /** Effective global timeout (ms). Builder override of {@link #getGlobalTimeoutMs()}. */
    private volatile long effectiveGlobalTimeoutMs;

    /**
     * Caller-supplied loader for the machine's volatile (non-persisted)
     * context. Set once at {@link #initialize}; propagated to every machine
     * at dispatch and at rehydration. {@code null} means no volatile context
     * is loaded — machines see {@code null} from {@code getVolatileContext()}.
     */
    private volatile java.util.function.Function<Machine<?>, Object> volatileContextLoader;

    // ─────────────────────────────────────────────────────────────────
    // Initialisation — package-private; only Statewalk.Builder may call.
    // ─────────────────────────────────────────────────────────────────

    /**
     * Wire dependencies and start the pool / timeout subsystem. Called by
     * {@link Statewalk.Builder#build()}; not visible to user code in other
     * packages — the framework's only public entry point is the builder.
     *
     * @param debugSampleRate {@code 0} disables sampling; {@code N > 0} puts
     *     every Nth dispatched machine in debug mode for full state-transition
     *     trace.
     */
    final void initialize(EventTypeRegistry eventTypes, int poolSize, int timeoutThreads,
                          int debugSampleRate,
                          PersistenceProvider persistenceProvider, boolean rehydrateEnabled,
                          long globalTimeoutMsOverride, String globalTimeoutTargetStateOverride,
                          java.util.function.Function<Machine<?>, Object> volatileContextLoader) {
        validateInit(poolSize, timeoutThreads);
        if (initialized) {
            throw new IllegalStateException(getRegistryName() + " already initialized");
        }
        this.eventTypes = eventTypes;
        this.debugSampleRate = Math.max(0, debugSampleRate);
        this.persistenceProvider = persistenceProvider;
        this.rehydrateEnabled = rehydrateEnabled;
        this.volatileContextLoader = volatileContextLoader;
        this.effectiveGlobalTimeoutMs = globalTimeoutMsOverride > 0
            ? globalTimeoutMsOverride : getGlobalTimeoutMs();
        this.effectiveGlobalTimeoutTargetState = globalTimeoutTargetStateOverride != null
            ? globalTimeoutTargetStateOverride : getGlobalTimeoutTargetState();
        this.timeoutManager = new TimeoutManager(getRegistryName(), Math.max(2, timeoutThreads));
        this.machinePool = new ObjectPoolManager<>(
            getRegistryName() + "-MachinePool",
            this::createMachineTemplate,
            poolSize);
        this.machineWork = new BoundedVirtualThreadExecutor(
            getRegistryName(),
            Math.max(16, getMaxConcurrent() * 4));
        this.initialized = true;
        LOG.info("[{}] initialized poolSize={} timeoutThreads={} debugSampleRate={} persistence={} rehydrate={} globalTimeoutMs={} timeoutTarget={}",
            getRegistryName(), poolSize, timeoutThreads, this.debugSampleRate,
            persistenceProvider != null, rehydrateEnabled,
            this.effectiveGlobalTimeoutMs, this.effectiveGlobalTimeoutTargetState);
    }

    /**
     * Validate subclass-supplied + builder-supplied parameters. Throws on
     * any violation — this fires once at startup, never on the dispatch
     * path.
     */
    private void validateInit(int poolSize, int timeoutThreads) {
        String name = getRegistryName();
        if (name == null || name.isBlank()) {
            throw new IllegalStateException("getRegistryName() returned blank");
        }
        if (poolSize <= 0) {
            throw new IllegalStateException("[" + name + "] poolSize must be > 0 (got " + poolSize + ")");
        }
        if (timeoutThreads <= 0) {
            throw new IllegalStateException("[" + name + "] timeoutThreads must be > 0 (got " + timeoutThreads + ")");
        }
        int maxConc = getMaxConcurrent();
        if (maxConc <= 0) {
            throw new IllegalStateException("[" + name + "] getMaxConcurrent() must be > 0 (got " + maxConc + ")");
        }
        long globalTo = getGlobalTimeoutMs();
        if (globalTo < 0) {
            throw new IllegalStateException("[" + name + "] getGlobalTimeoutMs() must be >= 0 (got " + globalTo + ")");
        }
    }

    /**
     * Bind a channel under a name. Package-private — wired by {@link Statewalk.Builder}.
     * The first channel bound becomes the primary (used by default {@link #forceCancel}).
     */
    final void bindChannel(String name, Channel<?, ?> channel) {
        channels.put(name, channel);
        if (primaryChannel == null) primaryChannel = channel;
    }

    public final Channel<?, ?> getChannel(String name) { return channels.get(name); }

    /**
     * Submit a task to run for {@code requestId} after every previously-
     * submitted task for the same id has completed. Per-machineId FIFO
     * is enforced via a {@link java.util.concurrent.CompletableFuture} chain.
     */
    private void chainSubmit(String requestId, Runnable task) {
        chains.compute(requestId, (k, prev) -> {
            java.util.concurrent.CompletableFuture<Void> base =
                (prev == null) ? java.util.concurrent.CompletableFuture.completedFuture(null) : prev;
            return base.thenRunAsync(() -> {
                try { task.run(); }
                catch (Throwable t) {
                    LOG.warn("[{}] chain task threw for id={}: {}", getRegistryName(), requestId, t.toString());
                }
            }, machineWork.asExecutor());
        });
    }

    /** Drop a machine's chain entry — called on terminal / offline. */
    private void clearChain(String requestId) {
        chains.remove(requestId);
    }

    // ─────────────────────────────────────────────────────────────────
    // Dispatch (final — borrow + assertIDLE + register + start)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Borrow a machine, bind it to {@code requestId} + {@code task}, register
     * it as active, schedule the global timeout, and start it.
     *
     * <p>Convenience overload — returns {@code true} on accept, {@code false}
     * on any rejection. Use {@link #dispatchWithResult(String, Object)} when
     * the rejection cause matters (e.g. to formulate a wire-level response).
     */
    public final boolean dispatch(String requestId, Object task) {
        return dispatchWithResult(requestId, task).accepted();
    }

    /**
     * Same as {@link #dispatch} but returns a {@link DispatchResult} carrying
     * the {@link RejectCause} on rejection. Used by the call handler to
     * translate to protocol-level cause codes when responding to the wire.
     *
     * <p>Quota enforcement happens here, before any pool borrow. Subclass
     * hooks {@link #quotaKeysFor(Object)} and {@link #getQuotaLimits()}
     * configure the per-partner / per-route checks.
     */
    public final DispatchResult dispatchWithResult(String requestId, Object task) {
        if (shuttingDown.get())            return DispatchResult.rejected(RejectCause.SHUTTING_DOWN);
        if (!initialized)                  return DispatchResult.rejected(RejectCause.NOT_INITIALIZED);
        if (activeMachines.size() >= getMaxConcurrent())
                                            return DispatchResult.rejected(RejectCause.CAPACITY_EXCEEDED);

        // Quota check BEFORE borrowing the machine — cheaper to reject early.
        QuotaKeys keys = quotaKeysFor(task);
        QuotaLimits limits = getQuotaLimits();
        RejectCause quotaReject = quotaController.tryAcquire(keys, limits);
        if (quotaReject != null) {
            return DispatchResult.rejected(quotaReject);
        }

        M machine = machinePool.borrow();
        if (!machine.isIdle()) {
            machine = machinePool.borrow();
            if (!machine.isIdle()) {
                quotaController.release(keys);   // roll back quota acquire
                return DispatchResult.rejected(RejectCause.POOL_INTEGRITY_ERROR);
            }
        }

        machine.setRegistry(this);
        machine.setMachineId(requestId);
        machine.setVolatileContextLoader(volatileContextLoader);
        @SuppressWarnings("unchecked")
        Machine<C> typed = (Machine<C>) machine;
        typed.setInitialContext((C) task);

        // Sample-based debug mode: every Nth machine traces every state change.
        boolean debug = false;
        if (debugSampleRate > 0) {
            debug = (dispatchCounter.getAndIncrement() % debugSampleRate) == 0;
            machine.setDebugMode(debug);
        }

        // Race guard: refuse second dispatch on the same id.
        M prior = activeMachines.putIfAbsent(requestId, machine);
        if (prior != null) {
            machine.setDebugMode(false);
            machinePool.returnObject(machine);
            quotaController.release(keys);   // roll back quota acquire
            return DispatchResult.rejected(RejectCause.DUPLICATE_ID);
        }

        // Track quota keys for release on terminate / offline.
        if (keys != QuotaKeys.NONE) dispatchQuotaKeys.put(requestId, keys);

        long now = System.currentTimeMillis();
        startTimeMs.put(requestId, now);
        lastEventTimeMs.put(requestId, now);
        totalStarted.incrementAndGet();

        scheduleGlobalTimeout(requestId);
        LOG.info("[{}] machine created id={} debug={}", getRegistryName(), requestId, debug);

        // Start the machine asynchronously through the per-id FIFO chain.
        final M m = machine;
        chainSubmit(requestId, () -> {
            try {
                m.start();
            } catch (Throwable t) {
                LOG.error("[{}] machine.start threw for id={}: {} — force-cleaning",
                    getRegistryName(), requestId, t.toString());
                forceCleanupMachine(requestId);
            }
        });
        return DispatchResult.ok();
    }

    // ─────────────────────────────────────────────────────────────────
    // Inbound event routing (final — channel feeds in here)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Route an inbound event to the machine bound to {@code requestId}.
     *
     * <p>Validates the event class is registered with the framework's
     * {@link EventTypeRegistry}; an unregistered class throws (catches
     * typos at first dispatch instead of silently dropping).
     *
     * <p>Decision tree when no machine is bound to {@code requestId}:
     * <ul>
     *   <li>Event {@code isFirst() == true} → create a new machine via
     *       {@link #createTaskFromFirstEvent} and dispatch.</li>
     *   <li>Persistence + rehydration enabled → rehydrate from snapshot.</li>
     *   <li>Otherwise → throw {@link IllegalStateException}: a non-first
     *       event arrived for an unknown id with no rehydration path
     *       configured. This is a bug in either the producer (sending
     *       a non-first event without a creation event first) or the
     *       configuration (forgetting to enable persistence + rehydration).</li>
     * </ul>
     *
     * <p>If the event is {@link com.telcobright.statewalk.v2.pool.Poolable},
     * it is returned to its pool after dispatch.
     */
    public final void onInboundEvent(String requestId, StatemachineEvent event) {
        if (shuttingDown.get()) return;
        if (event == null) return;

        // Validate registration — throws on unregistered event classes.
        // (Synchronous — caller wants the typo signal immediately.)
        if (eventTypes != null) eventTypes.requireRegistered(event.getClass());

        // Late-event window — silently drop on the calling thread, no executor work.
        if (terminatedRequests.contains(requestId)) {
            if (eventTypes != null) eventTypes.returnIfPoolable(event);
            return;
        }

        // Resolve the machine: lookup → isFirst auto-create → rehydrate.
        // For the rehydrate path we need a synchronous load (the snapshot
        // determines whether to dispatch, throw, or drop). The actual
        // entry-action work then runs on the executor.
        M m = activeMachines.get(requestId);
        if (m == null) {
            m = resolveMachineForUnknownId(requestId, event);
            if (m == null) {
                if (eventTypes != null) eventTypes.returnIfPoolable(event);
                return;
            }
        }
        if (m.isTerminated()) {
            if (eventTypes != null) eventTypes.returnIfPoolable(event);
            return;
        }
        lastEventTimeMs.put(requestId, System.currentTimeMillis());

        // Hand the event off the channel thread. Per-id FIFO is preserved
        // by the chain — events for the same machine process in submission
        // order; different machines run in parallel.
        final M machine = m;
        chainSubmit(requestId, () -> {
            try {
                machine.fire(event);
            } catch (Throwable t) {
                LOG.warn("[{}] fire threw for id={} event={}: {}",
                    getRegistryName(), requestId, event.getClass().getSimpleName(), t.toString());
            } finally {
                if (eventTypes != null) eventTypes.returnIfPoolable(event);
            }
        });
    }

    /**
     * Decision logic for "no machine in registry for requestId" — see
     * {@link #onInboundEvent} javadoc.
     */
    private M resolveMachineForUnknownId(String requestId, StatemachineEvent event) {
        if (event.isFirst()) {
            Object task = createTaskFromFirstEvent(requestId, event);
            if (!dispatch(requestId, task)) return null;
            return activeMachines.get(requestId);
        }
        if (persistenceProvider != null && rehydrateEnabled) {
            return rehydrateMachine(requestId);
        }
        // Pseudocode rule: not first, no rehydration → throw.
        throw new IllegalStateException(
            "[" + getRegistryName() + "] No machine for requestId=" + requestId
            + " and rehydration is " + (persistenceProvider == null ? "not configured" : "disabled")
            + ". Send a first-event to create, or enable rehydration via "
            + "Statewalk.builder().persistence(...).rehydrate(true).");
    }

    // ─────────────────────────────────────────────────────────────────
    // Rehydration (final)
    // ─────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private M rehydrateMachine(String requestId) {
        var snap = persistenceProvider.load(requestId, getRegistryName()).orElse(null);
        if (snap == null) {
            LOG.debug("[{}] no snapshot for id={} — cannot rehydrate", getRegistryName(), requestId);
            return null;
        }

        M machine = machinePool.borrow();
        if (!machine.isIdle()) {
            machine = machinePool.borrow();
            if (!machine.isIdle()) return null;
        }
        machine.setRegistry(this);
        machine.setMachineId(requestId);
        machine.setVolatileContextLoader(volatileContextLoader);

        Object ctx = SnapshotSerializer.contextFromBase64Json(
            snap.contextJsonBase64(), snap.contextClassName());

        // Race guard — putIfAbsent like dispatch.
        M prior = activeMachines.putIfAbsent(requestId, machine);
        if (prior != null) {
            machinePool.returnObject(machine);
            return prior;
        }

        long now = System.currentTimeMillis();
        startTimeMs.put(requestId, now);
        lastEventTimeMs.put(requestId, now);
        totalStarted.incrementAndGet();

        scheduleGlobalTimeout(requestId);

        Machine<C> typed = (Machine<C>) machine;
        typed.rehydrate(snap.currentState(), ctx, snap.timeoutTargetState(), snap.timeoutDeadlineMs());

        LOG.info("[{}] rehydrated id={} state={} timeoutFired={}",
            getRegistryName(), requestId, snap.currentState(),
            snap.timeoutFiredBy(now));
        return machine;
    }

    /**
     * Standalone rehydrate — restore a machine from its saved snapshot
     * without an inbound event triggering it. Used by {@link MultiRegistry}
     * to preload sibling cells when one cell receives an event and the
     * rest need to come back into memory to participate in the call's
     * lifecycle. Returns {@code true} if a machine was restored.
     *
     * <p>Package-private — only the framework calls this; user code goes
     * through {@code onInboundEvent} which triggers rehydration as a side
     * effect of the event arrival.
     */
    final boolean restoreFromPersistence(String requestId) {
        if (persistenceProvider == null || !rehydrateEnabled) return false;
        if (activeMachines.containsKey(requestId)) return false;
        return rehydrateMachine(requestId) != null;
    }

    // ─────────────────────────────────────────────────────────────────
    // Persistence callback — Machine notifies after every transition
    // ─────────────────────────────────────────────────────────────────

    @Override
    public final void onStateTransitioned(String machineId, String newState,
                                           long timeoutDeadlineMs, String timeoutTargetState) {
        // Always-on bookkeeping — regardless of persistence.
        long now = System.currentTimeMillis();
        if (lastEventTimeMs.containsKey(machineId)) {
            lastEventTimeMs.put(machineId, now);
        }

        // Persistence, if configured.
        if (persistenceProvider == null) return;
        M m = activeMachines.get(machineId);
        if (m == null) return;
        Object ctx = m.getContext();
        try {
            String b64 = SnapshotSerializer.contextToBase64Json(ctx);
            String ctxClass = ctx != null ? ctx.getClass().getName() : null;
            MachineSnapshot snap = new MachineSnapshot(
                machineId,
                getRegistryName(),
                newState,
                ctxClass,
                b64,
                now,
                timeoutTargetState,
                timeoutDeadlineMs);
            persistenceProvider.save(snap);
        } catch (RuntimeException e) {
            LOG.warn("[{}] persistence save failed for id={}: {}",
                getRegistryName(), machineId, e.toString());
        }
    }

    @Override
    public final void onMachineWentOffline(String machineId) {
        // Offline only makes sense with persistence — otherwise a suspended
        // machine has nowhere to go. Treat misconfig as a hard error so it
        // surfaces in tests, not in mysterious lost calls.
        if (persistenceProvider == null) {
            LOG.error("[{}] machine {} entered offline state but persistence is not configured "
                    + "— terminating instead. Add .persistence(...) to the Statewalk builder.",
                getRegistryName(), machineId);
            forceCleanupMachine(machineId);
            return;
        }

        // Snapshot was already saved during onStateTransitioned. Just remove
        // from active map and return to pool — without running the
        // termination ritual (no handleTermination, no snapshot delete).
        cancelGlobalTimeout(machineId);
        Long started = startTimeMs.remove(machineId);
        M machine = activeMachines.remove(machineId);
        lastEventTimeMs.remove(machineId);
        if (machine == null) return;

        long durationMs = started != null ? System.currentTimeMillis() - started : -1L;
        String state = machine.getCurrentState();

        try { machine.resetForReuse(); }
        catch (RuntimeException e) {
            LOG.warn("[{}] reset threw during offline for id={}: {}",
                getRegistryName(), machineId, e.toString());
        }
        if (machine.isIdle()) machinePool.returnObject(machine);

        // Release quota slots (if any) — going offline frees the partner /
        // route concurrent slot. If/when the machine rehydrates, it
        // re-acquires through the standard rehydrate path.
        QuotaKeys keys = dispatchQuotaKeys.remove(machineId);
        if (keys != null) quotaController.release(keys);

        // Drop the FIFO chain — when a future event arrives for this id,
        // a fresh chain will start (rehydrated machine).
        clearChain(machineId);

        LOG.info("[{}] machine offline id={} state={} activeMs={}",
            getRegistryName(), machineId, state, durationMs);
    }

    // ─────────────────────────────────────────────────────────────────
    // Global timeout (final)
    // ─────────────────────────────────────────────────────────────────

    private void scheduleGlobalTimeout(String requestId) {
        long ms = effectiveGlobalTimeoutMs;
        if (ms <= 0) return;
        timeoutManager.scheduleTracked(
            "global:" + requestId,
            () -> {
                final M m = activeMachines.get(requestId);
                if (m == null || m.isTerminated()) return;
                LOG.info("[{}] global timeout fired id={} state={} → {}",
                    getRegistryName(), requestId, m.getCurrentState(),
                    effectiveGlobalTimeoutTargetState != null
                        ? "transition to " + effectiveGlobalTimeoutTargetState
                        : "force cleanup (no target configured)");
                if (effectiveGlobalTimeoutTargetState != null) {
                    // Transition to the configured final state — runs current
                    // state's exit + target's entry; entry of final triggers
                    // the standard 8-step termination ritual.
                    chainSubmit(requestId, () -> {
                        try {
                            if (!m.isTerminated()) m.transitionTo(effectiveGlobalTimeoutTargetState);
                        } catch (Throwable t) {
                            LOG.error("[{}] global-timeout transition threw for id={}: {} — force-cleaning",
                                getRegistryName(), requestId, t.toString());
                            forceCleanupMachine(requestId);
                        }
                    });
                } else {
                    forceCleanupMachine(requestId);
                }
            },
            ms,
            TimeUnit.MILLISECONDS);
    }

    private void cancelGlobalTimeout(String requestId) {
        timeoutManager.cancelTracked("global:" + requestId);
    }

    // ─────────────────────────────────────────────────────────────────
    // Termination ritual (final — 8 steps, fixed order)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Called by the machine when it enters a final state, and by
     * {@link #forceCleanupMachine(String)}. Runs the 8-step ritual.
     */
    @Override
    public final void onMachineReachedTerminal(String requestId) {
        onMachineTerminated(requestId);
    }

    private void onMachineTerminated(String requestId) {
        // 1. dedup
        if (!terminatedRequests.add(requestId)) return;

        // 2. cancel global timeout
        cancelGlobalTimeout(requestId);

        // 3. atomically remove from active map
        Long started = startTimeMs.remove(requestId);
        M machine = activeMachines.remove(requestId);
        lastEventTimeMs.remove(requestId);
        if (machine == null) {
            terminatedRequests.remove(requestId);
            return;
        }

        long durationMs = started != null ? System.currentTimeMillis() - started : -1L;
        String finalState = machine.getCurrentState();

        // 4. subclass hook (settlement, CDR, counter decrement)
        try {
            handleTermination(requestId, machine, finalState);
        } catch (RuntimeException e) {
            LOG.warn("[{}] handleTermination threw for id={}: {}",
                getRegistryName(), requestId, e.toString());
        }

        // 5+6. machine reset (final method on Machine; clears context, IDs, state→IDLE)
        try {
            machine.resetForReuse();
        } catch (RuntimeException e) {
            LOG.warn("[{}] reset threw for id={}: {}",
                getRegistryName(), requestId, e.toString());
        }

        // 7. return to pool (only if reset got us back to IDLE)
        if (machine.isIdle()) {
            machinePool.returnObject(machine);
            LOG.debug("[{}] pool return id={} poolAvailable={}",
                getRegistryName(), requestId, machinePool.getStatistics().available());
        } else {
            LOG.warn("[{}] machine NOT in IDLE after reset id={} state={} — instance dropped",
                getRegistryName(), requestId, machine.getCurrentState());
        }
        totalCompleted.incrementAndGet();

        // Persistence cleanup: terminated machines do not need rehydration.
        if (persistenceProvider != null) {
            try { persistenceProvider.delete(requestId, getRegistryName()); }
            catch (RuntimeException e) {
                LOG.warn("[{}] persistence delete failed for id={}: {}",
                    getRegistryName(), requestId, e.toString());
            }
        }

        // Release quota slots (if any).
        QuotaKeys keys = dispatchQuotaKeys.remove(requestId);
        if (keys != null) quotaController.release(keys);

        // Drop the FIFO chain entry — machine is gone.
        clearChain(requestId);

        LOG.info("[{}] machine terminated id={} finalState={} durationMs={}",
            getRegistryName(), requestId, finalState, durationMs);

        // 8. schedule dedup TTL eviction (60s — covers late event window)
        timeoutManager.schedule(
            () -> terminatedRequests.remove(requestId),
            60, TimeUnit.SECONDS);
    }

    // ─────────────────────────────────────────────────────────────────
    // Force cleanup (final — converges on the same ritual)
    // ─────────────────────────────────────────────────────────────────

    public final boolean forceCleanupMachine(String requestId) {
        M m = activeMachines.get(requestId);
        if (m == null) return false;
        LOG.info("[{}] force cleanup id={} state={}",
            getRegistryName(), requestId, m.getCurrentState());
        try { forceCancel(requestId); } catch (RuntimeException ignored) {}
        onMachineTerminated(requestId);
        return true;
    }

    public final List<String> getHungMachineIds(long thresholdMs) {
        long cutoff = System.currentTimeMillis() - thresholdMs;
        java.util.List<String> hung = new java.util.ArrayList<>();
        for (var e : lastEventTimeMs.entrySet()) {
            if (e.getValue() < cutoff) hung.add(e.getKey());
        }
        return hung;
    }

    // ─────────────────────────────────────────────────────────────────
    // Machine handle: schedule (state timeout) — package-visible to Machine
    // ─────────────────────────────────────────────────────────────────

    @Override
    public final ScheduledFuture<?> schedule(String machineId, Runnable r,
                                             long delay, TimeUnit unit) {
        return timeoutManager.schedule(r, delay, unit);
    }

    // ─────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────

    public final boolean isInitialized() { return initialized; }
    public final int getActiveCount() { return activeMachines.size(); }
    public final long getTotalStarted() { return totalStarted.get(); }
    public final long getTotalCompleted() { return totalCompleted.get(); }
    public final M getMachine(String requestId) { return activeMachines.get(requestId); }

    public final ObjectPoolManager.PoolStatistics getPoolStatistics() {
        return machinePool != null ? machinePool.getStatistics() : null;
    }

    public final Map<String, Object> getStatistics() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("name", getRegistryName());
        s.put("activeCount", getActiveCount());
        s.put("totalStarted", getTotalStarted());
        s.put("totalCompleted", getTotalCompleted());
        s.put("terminatedSetSize", terminatedRequests.size());
        s.put("timeoutsActive", timeoutManager != null ? timeoutManager.activeCount() : 0);
        s.put("pool", getPoolStatistics());
        return Collections.unmodifiableMap(s);
    }

    // ─────────────────────────────────────────────────────────────────
    // Shutdown (final)
    // ─────────────────────────────────────────────────────────────────

    public final void shutdown() {
        if (!initialized) return;
        shuttingDown.set(true);
        initialized = false;

        try { onShutdown(); } catch (RuntimeException ignored) {}

        // Force-cleanup any remaining active machines.
        for (String id : new java.util.ArrayList<>(activeMachines.keySet())) {
            try { forceCleanupMachine(id); } catch (RuntimeException ignored) {}
        }

        // Drain the executor before clearing pools — otherwise in-flight
        // tasks may try to use cleared resources.
        if (machineWork != null) {
            try { machineWork.awaitIdle(5, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            machineWork.close();
        }

        activeMachines.clear();
        terminatedRequests.clear();
        if (machinePool != null) machinePool.clear();
        if (timeoutManager != null) timeoutManager.shutdown();
    }

    // ─────────────────────────────────────────────────────────────────
    // Test-friendly synchronisation: wait for the executor to drain.
    // ─────────────────────────────────────────────────────────────────

    /**
     * Block until all in-flight machine work has completed (entry actions,
     * event dispatches, terminal cleanups, persistence saves). Used by tests
     * after dispatching events to deterministically synchronise with the
     * executor.
     *
     * <p>In production, callers don't need this — the framework's async
     * processing is the desired behaviour.
     *
     * @return true if drained within the timeout
     */
    public final boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNs = System.nanoTime() + unit.toNanos(timeout);

        // Snapshot current chains and join them — captures everything
        // submitted up to this point.
        for (var f : new java.util.ArrayList<>(chains.values())) {
            long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
            try {
                f.get(remainingNs, TimeUnit.NANOSECONDS);
            } catch (java.util.concurrent.ExecutionException e) {
                // Already logged inside chainSubmit; carry on.
            } catch (java.util.concurrent.TimeoutException e) {
                return false;
            }
        }
        if (machineWork == null) return true;
        long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
        return machineWork.awaitIdle(remainingNs, TimeUnit.NANOSECONDS);
    }

    public final BoundedVirtualThreadExecutor getMachineWorkExecutor() { return machineWork; }
}
