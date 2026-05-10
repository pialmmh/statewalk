package com.telcobright.statewalk.v2.registry;

import com.telcobright.statewalk.v2.channel.Channel;
import com.telcobright.statewalk.v2.event.EventTypeRegistry;
import com.telcobright.statewalk.v2.event.StatemachineEvent;
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
public abstract class Registry<M extends Machine<?, C>, C> implements Machine.MachineRegistryHandle {

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

    protected final ConcurrentHashMap<String, M> activeMachines = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> startTimeMs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastEventTimeMs = new ConcurrentHashMap<>();
    private final Set<String> terminatedRequests = ConcurrentHashMap.newKeySet();

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
                          PersistenceProvider persistenceProvider, boolean rehydrateEnabled) {
        if (initialized) {
            throw new IllegalStateException(getRegistryName() + " already initialized");
        }
        this.eventTypes = eventTypes;
        this.debugSampleRate = Math.max(0, debugSampleRate);
        this.persistenceProvider = persistenceProvider;
        this.rehydrateEnabled = rehydrateEnabled;
        this.timeoutManager = new TimeoutManager(getRegistryName(), Math.max(2, timeoutThreads));
        this.machinePool = new ObjectPoolManager<>(
            getRegistryName() + "-MachinePool",
            this::createMachineTemplate,
            poolSize);
        this.initialized = true;
        LOG.info("[{}] initialized poolSize={} timeoutThreads={} debugSampleRate={} persistence={} rehydrate={}",
            getRegistryName(), poolSize, timeoutThreads, this.debugSampleRate,
            persistenceProvider != null, rehydrateEnabled);
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

    // ─────────────────────────────────────────────────────────────────
    // Dispatch (final — borrow + assertIDLE + register + start)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Borrow a machine, bind it to {@code requestId} + {@code task}, register
     * it as active, schedule the global timeout, and start it.
     *
     * @return true on success, false on capacity / shutdown / duplicate id.
     */
    public final boolean dispatch(String requestId, Object task) {
        if (shuttingDown.get() || !initialized) return false;
        if (activeMachines.size() >= getMaxConcurrent()) return false;

        M machine = machinePool.borrow();

        // IDLE invariant assertion — pool entries must be IDLE.
        if (!machine.isIdle()) {
            // Drop the bad instance; allocate fresh and retry once.
            machine = machinePool.borrow();
            if (!machine.isIdle()) {
                return false; // pool integrity broken; bail.
            }
        }

        machine.setRegistry(this);
        machine.setMachineId(requestId);
        @SuppressWarnings("unchecked")
        Machine<Object, C> typed = (Machine<Object, C>) machine;
        typed.setPersistingEntity(task);

        // Sample-based debug mode: every Nth machine traces every state change.
        boolean debug = false;
        if (debugSampleRate > 0) {
            debug = (dispatchCounter.getAndIncrement() % debugSampleRate) == 0;
            machine.setDebugMode(debug);
        }

        // Race guard: refuse second dispatch on the same id.
        M prior = activeMachines.putIfAbsent(requestId, machine);
        if (prior != null) {
            // Another thread won; return our borrow.
            machine.setDebugMode(false);
            machinePool.returnObject(machine);
            return false;
        }

        long now = System.currentTimeMillis();
        startTimeMs.put(requestId, now);
        lastEventTimeMs.put(requestId, now);
        totalStarted.incrementAndGet();

        scheduleGlobalTimeout(requestId);
        machine.start();
        LOG.info("[{}] machine created id={} debug={}", getRegistryName(), requestId, debug);
        return true;
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
        if (eventTypes != null) eventTypes.requireRegistered(event.getClass());

        try {
            M m = activeMachines.get(requestId);
            if (m == null) {
                // Late event window: id was terminated within the last 60s.
                // Silently drop — distinguishable from "unknown id" by the
                // dedup set entry.
                if (terminatedRequests.contains(requestId)) return;
                m = resolveMachineForUnknownId(requestId, event);
                if (m == null) return;   // creation / rehydrate produced nothing
            }
            if (m.isTerminated()) return;
            lastEventTimeMs.put(requestId, System.currentTimeMillis());
            m.fire(event);
        } finally {
            // Return poolable events even if dispatch dropped them.
            if (eventTypes != null) eventTypes.returnIfPoolable(event);
        }
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
        var snap = persistenceProvider.load(requestId).orElse(null);
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

        Machine<Object, C> typed = (Machine<Object, C>) machine;
        typed.rehydrate(snap.currentState(), ctx, snap.timeoutTargetState(), snap.timeoutDeadlineMs());

        LOG.info("[{}] rehydrated id={} state={} timeoutFired={}",
            getRegistryName(), requestId, snap.currentState(),
            snap.timeoutFiredBy(now));
        return machine;
    }

    // ─────────────────────────────────────────────────────────────────
    // Persistence callback — Machine notifies after every transition
    // ─────────────────────────────────────────────────────────────────

    @Override
    public final void onStateTransitioned(String machineId, String newState,
                                           long timeoutDeadlineMs, String timeoutTargetState) {
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
                System.currentTimeMillis(),
                timeoutTargetState,
                timeoutDeadlineMs);
            persistenceProvider.save(snap);
        } catch (RuntimeException e) {
            LOG.warn("[{}] persistence save failed for id={}: {}",
                getRegistryName(), machineId, e.toString());
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Global timeout (final)
    // ─────────────────────────────────────────────────────────────────

    private void scheduleGlobalTimeout(String requestId) {
        long ms = getGlobalTimeoutMs();
        if (ms <= 0) return;
        timeoutManager.scheduleTracked(
            "global:" + requestId,
            () -> {
                M m = activeMachines.get(requestId);
                if (m != null && !m.isTerminated()) {
                    LOG.info("[{}] global timeout fired id={} state={}",
                        getRegistryName(), requestId, m.getCurrentState());
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
            try { persistenceProvider.delete(requestId); }
            catch (RuntimeException e) {
                LOG.warn("[{}] persistence delete failed for id={}: {}",
                    getRegistryName(), requestId, e.toString());
            }
        }

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
        activeMachines.clear();
        terminatedRequests.clear();
        if (machinePool != null) machinePool.clear();
        if (timeoutManager != null) timeoutManager.shutdown();
    }
}
