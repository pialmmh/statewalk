package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.channel.Channel;
import com.telcobright.statewalk.v2.executor.BoundedVirtualThreadExecutor;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.persistence.MachineSnapshot;
import com.telcobright.statewalk.v2.persistence.PersistenceProvider;
import com.telcobright.statewalk.v2.persistence.SnapshotSerializer;
import com.telcobright.statewalk.v2.pool.ObjectPoolManager;
import com.telcobright.statewalk.v2.registry.api.DispatchResult;
import com.telcobright.statewalk.v2.registry.api.QuotaKeys;
import com.telcobright.statewalk.v2.registry.api.QuotaLimits;
import com.telcobright.statewalk.v2.registry.api.RejectCause;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.registry.internal.QuotaController;
import com.telcobright.statewalk.v2.timeout.TimeoutManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Flat registry — one Registry per domain (call / sms / http / etc.). Hosts
 * machines of multiple types for one request id at once: position 0 of each
 * row is the {@link Supervisor}; positions 1+ are children spawned by that
 * supervisor through its resolver.
 *
 * <p>External callers see ONE machine per request id (the supervisor). All
 * child manipulation is package-private and reachable only through
 * {@link InternalEventResolver}, which lives on the supervisor instance.
 *
 * <h2>Data structure</h2>
 * <pre>
 *   active  = Map&lt;parentId, List&lt;Machine&gt;&gt;          // machines[0] = supervisor
 *   pools   = Map&lt;Class, ObjectPoolManager&gt;          // per-type pools
 *   chains  = Map&lt;"parentId#TypeName", CFuture&gt;      // per-instance FIFO
 * </pre>
 *
 * <h2>Machine ids</h2>
 * <ul>
 *   <li>Supervisor: {@code parentId} (the wire UUID).</li>
 *   <li>Child: {@code parentId + "#" + TypeName} (e.g. {@code call-1#CallSignaling}).</li>
 * </ul>
 * Stable, debuggable, friendly to persistence (single-column key).
 *
 * <h2>What gets shared</h2>
 * All lifecycle subsystems — pools, FIFO chains, executor, timeout manager —
 * are owned by this Registry once and used across every machine type it
 * hosts. A bug fix here automatically benefits every domain that extends
 * this base.
 */
public class Registry implements Machine.MachineRegistryHandle {

    private static final Logger LOG = LoggerFactory.getLogger(Registry.class);

    public static final String CHILD_ID_SEPARATOR = "#";

    private final String name;
    private final Class<? extends Supervisor<?>> supervisorType;
    private final Map<Class<? extends Machine<?>>, TypeSpec> types;
    private final Map<Class<? extends Machine<?>>, ObjectPoolManager<? extends Machine<?>>> pools;

    private final ConcurrentHashMap<String, List<Machine<?>>> active = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> chains = new ConcurrentHashMap<>();
    private final Set<String> terminated = ConcurrentHashMap.newKeySet();

    private final BoundedVirtualThreadExecutor work;
    private final TimeoutManager timeouts;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /** Persistence + rehydration config (optional). */
    private final PersistenceProvider persistence;
    private final boolean rehydrateEnabled;

    /**
     * Caller-supplied event → context builder. When an event with
     * {@code isFirst() == true} arrives for an unknown id, this function is
     * called to construct the initial context for the supervisor.
     */
    private final Function<StatemachineEvent, Object> firstEventToContext;

    /** Hard ceiling on concurrent supervisor cells; 0 disables. */
    private final int maxConcurrent;

    /** Wall-clock cap per supervisor cell; 0 disables. */
    private final long globalTimeoutMs;

    /** Terminal state for the supervisor on global-timeout fire; null disables. */
    private final String globalTimeoutTargetState;

    /** 1-in-N debug sampling for dispatched supervisors; 0 disables. */
    private final int debugSampleRate;
    private final AtomicLong dispatchCounter = new AtomicLong(0);

    /** Per-task quota-key extractor + limit thresholds. */
    private final Function<Object, QuotaKeys> quotaKeysExtractor;
    private final QuotaLimits quotaLimits;
    private final QuotaController quotaController = new QuotaController();
    private final ConcurrentHashMap<String, QuotaKeys> dispatchQuotaKeys = new ConcurrentHashMap<>();

    /** Optional protocol channel — state actions reach the wire through this. */
    private final Channel<?, ?> channel;

    Registry(String name,
             Class<? extends Supervisor<?>> supervisorType,
             Map<Class<? extends Machine<?>>, TypeSpec> types,
             int threads,
             PersistenceProvider persistence,
             boolean rehydrateEnabled,
             Function<StatemachineEvent, Object> firstEventToContext,
             int maxConcurrent,
             long globalTimeoutMs,
             String globalTimeoutTargetState,
             int debugSampleRate,
             Function<Object, QuotaKeys> quotaKeysExtractor,
             QuotaLimits quotaLimits,
             Channel<?, ?> channel) {
        this.name = name;
        this.supervisorType = supervisorType;
        this.types = Map.copyOf(types);
        this.timeouts = new TimeoutManager(name, Math.max(2, threads));
        this.work = new BoundedVirtualThreadExecutor(name, Math.max(16, types.size() * 100));
        this.persistence = persistence;
        this.rehydrateEnabled = rehydrateEnabled;
        this.firstEventToContext = firstEventToContext;
        this.maxConcurrent = Math.max(0, maxConcurrent);
        this.globalTimeoutMs = Math.max(0, globalTimeoutMs);
        this.globalTimeoutTargetState = globalTimeoutTargetState;
        this.debugSampleRate = Math.max(0, debugSampleRate);
        this.quotaKeysExtractor = quotaKeysExtractor;
        this.quotaLimits = quotaLimits != null ? quotaLimits : QuotaLimits.UNLIMITED;
        this.channel = channel;

        this.pools = new ConcurrentHashMap<>();
        types.forEach((cls, spec) -> pools.put(cls, makePool(cls, spec)));

        LOG.info("[{}] flat registry initialized — supervisor={}, types={}, persistence={}, rehydrate={}, "
                + "maxConcurrent={}, globalTimeoutMs={}, globalTimeoutTarget={}, debugSampleRate={}, "
                + "quotaEnforced={}, channel={}",
            name, supervisorType.getSimpleName(), types.keySet(),
            persistence != null, rehydrateEnabled,
            this.maxConcurrent, this.globalTimeoutMs, this.globalTimeoutTargetState,
            this.debugSampleRate, this.quotaLimits.enforces(),
            channel != null ? channel.getName() : "<none>");
    }

    /** Exposed for state actions: the wire channel, or {@code null} if not configured. */
    public Channel<?, ?> getChannel() { return channel; }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ObjectPoolManager<? extends Machine<?>> makePool(
            Class<? extends Machine<?>> cls, TypeSpec spec) {
        return new ObjectPoolManager(
            name + "-" + cls.getSimpleName(),
            (Supplier) spec.factory(),
            spec.poolSize());
    }

    public static Builder builder(String name) { return new Builder(name); }

    public String getName() { return name; }

    // ─────────────────────────────────────────────────────────────────
    // Public API — supervisor-only surface; no child operations exposed
    // ─────────────────────────────────────────────────────────────────

    /**
     * Dispatch a new request: borrow the supervisor, run admission gates,
     * register, schedule the global timeout, start. Returns a
     * {@link DispatchResult} so callers can map rejections to wire-level
     * cause codes.
     *
     * <p>Admission order: {@code SHUTTING_DOWN → DUPLICATE_ID → CAPACITY →
     * QUOTA → POOL_INTEGRITY}. Each gate is cheap and short-circuits.
     */
    public DispatchResult dispatch(String parentId, Object task) {
        if (shuttingDown.get()) return DispatchResult.rejected(RejectCause.SHUTTING_DOWN);
        if (active.containsKey(parentId)) {
            LOG.warn("[{}] duplicate dispatch for id={}", name, parentId);
            return DispatchResult.rejected(RejectCause.DUPLICATE_ID);
        }
        if (maxConcurrent > 0 && active.size() >= maxConcurrent) {
            return DispatchResult.rejected(RejectCause.CAPACITY_EXCEEDED);
        }
        // Quota gate — before borrow so a reject doesn't churn the pool.
        QuotaKeys keys = (quotaKeysExtractor != null && task != null)
            ? quotaKeysExtractor.apply(task) : QuotaKeys.NONE;
        if (keys == null) keys = QuotaKeys.NONE;
        RejectCause quotaReject = quotaController.tryAcquire(keys, quotaLimits);
        if (quotaReject != null) {
            return DispatchResult.rejected(quotaReject);
        }
        Machine<?> sup = borrowAndStart(supervisorType, parentId, task);
        if (sup == null) {
            quotaController.release(keys);
            return DispatchResult.rejected(RejectCause.POOL_INTEGRITY_ERROR);
        }
        if (keys != QuotaKeys.NONE) dispatchQuotaKeys.put(parentId, keys);
        scheduleGlobalTimeout(parentId);
        return DispatchResult.ok();
    }

    private void scheduleGlobalTimeout(String parentId) {
        if (globalTimeoutMs <= 0) return;
        timeouts.scheduleTracked(
            "global:" + parentId,
            () -> {
                Machine<?> sup = supervisorOf(parentId);
                if (sup == null || sup.isTerminated()) return;
                LOG.info("[{}] global timeout fired id={} state={} → {}",
                    name, parentId, sup.getCurrentState(),
                    globalTimeoutTargetState != null
                        ? "transition to " + globalTimeoutTargetState
                        : "force cleanup (no target configured)");
                if (globalTimeoutTargetState != null) {
                    final Machine<?> machine = sup;
                    chainSubmit(cellKey(parentId, supervisorType), () -> {
                        try {
                            if (!machine.isTerminated()) machine.transitionTo(globalTimeoutTargetState);
                        } catch (Throwable t) {
                            LOG.error("[{}] global-timeout transition threw for id={}: {} — force-cleaning",
                                name, parentId, t.toString());
                            forceCleanupAll(parentId);
                        }
                    });
                } else {
                    forceCleanupAll(parentId);
                }
            },
            globalTimeoutMs,
            TimeUnit.MILLISECONDS);
    }

    private void cancelGlobalTimeout(String parentId) {
        if (globalTimeoutMs <= 0) return;
        timeouts.cancelTracked("global:" + parentId);
    }

    /** Wire-inbound event arrives for {@code parentId}. Always delivered to
     *  the supervisor at position 0; the supervisor's resolver routes from
     *  there.
     *
     *  <p>On unknown id the framework follows the design-checklist rules:
     *  <ul>
     *    <li>If the event is {@code isFirst()} and a {@code createFromFirstEvent}
     *        hook is configured, build the initial context and dispatch.</li>
     *    <li>Otherwise, if persistence + rehydration are configured, probe
     *        for every cell of this id and restore any with snapshots.</li>
     *    <li>Otherwise, throw {@link IllegalStateException} — the caller
     *        sent an event for an id we don't know about and have no way to
     *        recover (either by creation or by rehydration).</li>
     *  </ul>
     */
    public void onInboundEvent(String parentId, StatemachineEvent event) {
        if (shuttingDown.get()) return;
        Machine<?> sup = supervisorOf(parentId);
        if (sup == null) {
            // First-event auto-creation path.
            if (event.isFirst() && firstEventToContext != null) {
                Object initialCtx = firstEventToContext.apply(event);
                if (initialCtx != null) {
                    dispatch(parentId, initialCtx);
                    // Re-resolve supervisor now that it's been spawned.
                    sup = supervisorOf(parentId);
                }
            }
            // Rehydration path.
            if (sup == null && rehydrateEnabled) {
                int restored = restoreAllCellsFor(parentId);
                if (restored > 0) sup = supervisorOf(parentId);
            }
            if (sup == null) {
                // Per checklist: throw when no recovery path exists.
                throw new IllegalStateException(
                    "[" + name + "] no supervisor for id=" + parentId
                    + " and event " + event.getClass().getSimpleName()
                    + " is not first / no creation hook / no rehydration. "
                    + "Configure .createFromFirstEvent(...) or .persistence(...).rehydrate(true).");
            }
        }
        Supervisor<?> supervisor = (Supervisor<?>) sup;
        chainSubmit(cellKey(parentId, supervisorType), () -> {
            try { supervisor.handleInbound(event); }
            catch (Throwable t) {
                LOG.warn("[{}] supervisor.handleInbound threw for id={}: {}",
                    name, parentId, t.toString());
            }
        });
    }

    public boolean hasAny(String parentId) { return active.containsKey(parentId); }

    public int activeIdCount() { return active.size(); }

    public int activeCellCount() {
        int total = 0;
        for (var list : active.values()) total += list.size();
        return total;
    }

    public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNs = System.nanoTime() + unit.toNanos(timeout);
        // Loop until quiescent — child publishes feed the supervisor's chain
        // and vice versa, so a single drain pass can leave new work pending.
        int prev = -1; int stable = 0;
        for (int pass = 0; pass < 20; pass++) {
            for (var f : new ArrayList<>(chains.values())) {
                long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
                if (remainingNs == 0) return false;
                try { f.get(remainingNs, TimeUnit.NANOSECONDS); }
                catch (ExecutionException ignored) { /* logged in chainSubmit */ }
                catch (TimeoutException e) { return false; }
            }
            long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
            if (!work.awaitIdle(remainingNs, TimeUnit.NANOSECONDS)) return false;
            int cells = activeCellCount();
            if (cells == prev) { stable++; if (stable >= 2) return true; }
            else { stable = 0; prev = cells; }
        }
        return false;
    }

    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) return;
        for (String id : new ArrayList<>(active.keySet())) {
            forceCleanupAll(id);
        }
        try { work.awaitIdle(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        work.close();
        timeouts.shutdown();
        pools.values().forEach(ObjectPoolManager::clear);
    }

    // ─────────────────────────────────────────────────────────────────
    // Package-private — InternalEventResolver is the ONLY caller
    // ─────────────────────────────────────────────────────────────────

    void spawnChildInternal(String parentId, Class<? extends Machine<?>> childType, Object task) {
        if (shuttingDown.get()) return;
        if (childType == supervisorType) {
            throw new IllegalArgumentException("Cannot spawn supervisor as a child");
        }
        if (!types.containsKey(childType)) {
            throw new IllegalArgumentException("Unknown machine type: " + childType.getName());
        }
        Machine<?> existing = findChildInternal(parentId, childType);
        if (existing != null) {
            LOG.debug("[{}] child {} already present for id={}", name, childType.getSimpleName(), parentId);
            return;
        }
        borrowAndStart(childType, childId(parentId, childType), task);
    }

    void cleanupChildInternal(String parentId, Class<? extends Machine<?>> childType) {
        Machine<?> child = findChildInternal(parentId, childType);
        if (child == null) return;
        onCellTerminated(parentId, childType);
    }

    void forwardToChild(String parentId, Class<? extends Machine<?>> childType,
                        StatemachineEvent event) {
        Machine<?> child = findChildInternal(parentId, childType);
        if (child == null) {
            LOG.debug("[{}] no {} for id={}, drop {}",
                name, childType.getSimpleName(), parentId, event.getClass().getSimpleName());
            return;
        }
        final Machine<?> m = child;
        chainSubmit(cellKey(parentId, childType), () -> {
            try { m.fire(event); }
            catch (Throwable t) {
                LOG.warn("[{}] child fire threw for id={}/{}: {}",
                    name, parentId, childType.getSimpleName(), t.toString());
            }
        });
    }

    Machine<?> findChildInternal(String parentId, Class<? extends Machine<?>> childType) {
        return findInternal(parentId, childType);
    }

    /** Package-private introspection — any machine type in the row, supervisor included. */
    Machine<?> findInternal(String parentId, Class<? extends Machine<?>> type) {
        List<Machine<?>> list = active.get(parentId);
        if (list == null) return null;
        for (Machine<?> m : list) {
            if (m.getClass() == type) return m;
        }
        return null;
    }

    void forceCleanupAll(String parentId) {
        List<Machine<?>> list = active.get(parentId);
        if (list == null) return;
        for (Machine<?> m : new ArrayList<>(list)) {
            try { onCellTerminated(parentId, (Class<? extends Machine<?>>) m.getClass()); }
            catch (RuntimeException ignored) {}
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // MachineRegistryHandle — implemented per Registry (handle is per-machine
    // via PerMachineHandle below to track who's calling).
    // ─────────────────────────────────────────────────────────────────

    @Override
    public ScheduledFuture<?> schedule(String machineId, Runnable r, long delay, TimeUnit unit) {
        return timeouts.schedule(r, delay, unit);
    }

    @Override
    public void onMachineReachedTerminal(String machineId) {
        // No-op at this level; per-machine handle dispatches to onCellTerminated.
    }

    @Override
    public void publish(StatemachineEvent event) {
        // No-op at this level; per-machine handle dispatches to publishFrom.
    }

    /** Per-machine handle — knows its parentId + type so callbacks have context. */
    static final class PerMachineHandle implements Machine.MachineRegistryHandle {
        final Registry reg;
        final String parentId;
        final Class<? extends Machine<?>> type;
        PerMachineHandle(Registry reg, String parentId, Class<? extends Machine<?>> type) {
            this.reg = reg; this.parentId = parentId; this.type = type;
        }
        @Override public ScheduledFuture<?> schedule(String mid, Runnable r, long d, TimeUnit u) {
            return reg.timeouts.schedule(r, d, u);
        }
        @Override public void onMachineReachedTerminal(String mid) {
            reg.onCellTerminated(parentId, type);
        }
        @Override public void onStateTransitioned(String mid, String newState,
                                                   long timeoutDeadlineMs, String timeoutTargetState) {
            reg.persistCellSnapshot(mid, type, newState, timeoutDeadlineMs, timeoutTargetState);
        }
        @Override public void publish(StatemachineEvent event) {
            // Always routes back to this row's supervisor.
            reg.onInboundEvent(parentId, event);
        }

        /** Supervisor uses this to reach back into the owning Registry. */
        Registry registry() { return reg; }
    }

    // ─────────────────────────────────────────────────────────────────
    // Persistence + rehydration internals
    // ─────────────────────────────────────────────────────────────────

    /** Called on every successful transition via the per-machine handle. */
    void persistCellSnapshot(String machineId, Class<? extends Machine<?>> type,
                              String newState, long timeoutDeadlineMs,
                              String timeoutTargetState) {
        if (persistence == null) return;
        // Locate the live cell to read its context.
        String parentId = isChildId(machineId)
            ? machineId.substring(0, machineId.indexOf(CHILD_ID_SEPARATOR))
            : machineId;
        Machine<?> m = findInternal(parentId, type);
        if (m == null) return;
        Object ctx = m.getContext();
        try {
            String b64 = SnapshotSerializer.contextToBase64Json(ctx);
            String ctxClass = ctx != null ? ctx.getClass().getName() : null;
            MachineSnapshot snap = new MachineSnapshot(
                machineId, name, newState, ctxClass, b64,
                System.currentTimeMillis(), timeoutTargetState, timeoutDeadlineMs);
            persistence.save(snap);
        } catch (RuntimeException e) {
            LOG.warn("[{}] persistence save failed for {}: {}", name, machineId, e.toString());
        }
    }

    /** Restore every cell with a saved snapshot for the given parentId. */
    private int restoreAllCellsFor(String parentId) {
        if (persistence == null || !rehydrateEnabled) return 0;
        int restored = 0;
        // Supervisor first (its id equals parentId).
        if (restoreOneCell(parentId, parentId, supervisorType)) restored++;
        // Then each child type — id is parentId#TypeName.
        for (Class<? extends Machine<?>> t : types.keySet()) {
            if (t == supervisorType) continue;
            String childId = parentId + CHILD_ID_SEPARATOR + t.getSimpleName();
            if (restoreOneCell(childId, parentId, t)) restored++;
        }
        if (restored > 0) {
            LOG.info("[{}] cross-cell rehydration for id={} restored {} cells", name, parentId, restored);
        }
        return restored;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean restoreOneCell(String machineId, String parentId,
                                    Class<? extends Machine<?>> type) {
        if (findInternal(parentId, type) != null) return false;     // already alive
        var opt = persistence.load(machineId, name);
        if (opt.isEmpty()) return false;
        MachineSnapshot snap = opt.get();
        TypeSpec spec = types.get(type);
        ObjectPoolManager pool = (ObjectPoolManager) pools.get(type);
        Machine m = (Machine) pool.borrow();
        if (!m.isIdle()) { pool.returnObject(m); return false; }

        m.setRegistry(new PerMachineHandle(this, parentId, type));
        m.setMachineId(machineId);
        if (spec.volatileLoader() != null) m.setVolatileContextLoader(spec.volatileLoader());

        Object ctx = SnapshotSerializer.contextFromBase64Json(
            snap.contextJsonBase64(), snap.contextClassName());

        active.computeIfAbsent(parentId, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(m);

        final Machine machine = m;
        final String savedState        = snap.currentState();
        final String savedTargetState  = snap.timeoutTargetState();
        final long   savedDeadline     = snap.timeoutDeadlineMs();
        chainSubmit(cellKey(parentId, type), () -> {
            try {
                machine.rehydrate(savedState, ctx, savedTargetState, savedDeadline);
            } catch (Throwable t) {
                LOG.error("[{}] rehydrate threw for {}: {} — force-cleaning",
                    name, machineId, t.toString());
                forceCleanupAll(parentId);
            }
        });
        return true;
    }

    // ─────────────────────────────────────────────────────────────────
    // Internals — borrow, start, terminate, chain submission
    // ─────────────────────────────────────────────────────────────────

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Machine<?> borrowAndStart(
            Class<? extends Machine<?>> type, String id, Object task) {
        TypeSpec spec = types.get(type);
        if (spec == null) {
            LOG.error("[{}] unknown machine type {}", name, type.getName());
            return null;
        }
        ObjectPoolManager pool = (ObjectPoolManager) pools.get(type);
        Machine m = (Machine) pool.borrow();
        if (!m.isIdle()) {
            pool.returnObject(m);
            m = (Machine) pool.borrow();
            if (!m.isIdle()) {
                LOG.error("[{}] pool integrity error for {}", name, type.getSimpleName());
                return null;
            }
        }
        String parentId = isChildId(id) ? id.substring(0, id.indexOf(CHILD_ID_SEPARATOR)) : id;
        m.setRegistry(new PerMachineHandle(this, parentId, type));
        m.setMachineId(id);
        if (spec.volatileLoader() != null) m.setVolatileContextLoader(spec.volatileLoader());
        if (task != null) ((Machine) m).setInitialContext(task);

        // 1-in-N debug sampling — only sample at the supervisor; children
        // inherit by being part of the same logical request.
        if (type == supervisorType && debugSampleRate > 0) {
            boolean debug = (dispatchCounter.getAndIncrement() % debugSampleRate) == 0;
            m.setDebugMode(debug);
        }

        // Publish the machine into the active map BEFORE scheduling start(),
        // so persistence/transition callbacks fired by start() can resolve
        // the live cell via findInternal(). Submitting start() async with the
        // active.add() after was a race: a fast worker could run start →
        // onStateTransitioned → persistCellSnapshot → findInternal returns
        // null → snapshot silently dropped.
        active.computeIfAbsent(parentId,
            k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(m);

        final Machine machine = m;
        chainSubmit(cellKey(parentId, type), () -> {
            try { machine.start(); }
            catch (Throwable t) {
                LOG.error("[{}] start threw for {}/{}: {} — force-cleaning",
                    name, parentId, type.getSimpleName(), t.toString());
                forceCleanupAll(parentId);
            }
        });
        return m;
    }

    private void onCellTerminated(String parentId, Class<? extends Machine<?>> type) {
        String key = cellKey(parentId, type);
        if (!terminated.add(key)) return;

        List<Machine<?>> list = active.get(parentId);
        if (list == null) { terminated.remove(key); return; }
        Machine<?> machine = null;
        for (Machine<?> m : list) {
            if (m.getClass() == type) { machine = m; break; }
        }
        if (machine == null) { terminated.remove(key); return; }
        list.remove(machine);

        try { machine.resetForReuse(); }
        catch (RuntimeException e) {
            LOG.warn("[{}] reset threw for {}: {}", name, key, e.toString());
        }
        if (machine.isIdle()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            ObjectPoolManager pool = (ObjectPoolManager) pools.get(type);
            pool.returnObject(machine);
        }
        chains.remove(key);

        // Drop the persisted snapshot for this cell — it has reached a final
        // state, the framework ran the termination ritual, and the machine is
        // back in the pool. Crash-rehydration must NOT resurrect it.
        if (persistence != null) {
            String cellMachineId = (type == supervisorType)
                ? parentId
                : parentId + CHILD_ID_SEPARATOR + type.getSimpleName();
            try { persistence.delete(cellMachineId, name); }
            catch (RuntimeException e) {
                LOG.warn("[{}] persistence delete failed for {}: {}", name, cellMachineId, e.toString());
            }
        }

        // Cascade if the supervisor terminated.
        if (type == supervisorType) {
            for (Machine<?> sibling : new ArrayList<>(list)) {
                @SuppressWarnings("unchecked")
                Class<? extends Machine<?>> sibType = (Class<? extends Machine<?>>) sibling.getClass();
                try { onCellTerminated(parentId, sibType); } catch (RuntimeException ignored) {}
            }
            active.remove(parentId);
            // Release quota + cancel global timeout — done once per logical
            // request, on the supervisor's termination.
            cancelGlobalTimeout(parentId);
            QuotaKeys keys = dispatchQuotaKeys.remove(parentId);
            if (keys != null) quotaController.release(keys);
        } else if (list.isEmpty()) {
            active.remove(parentId);
        }

        timeouts.schedule(() -> terminated.remove(key), 60, TimeUnit.SECONDS);
    }

    private Machine<?> supervisorOf(String parentId) {
        List<Machine<?>> list = active.get(parentId);
        if (list == null || list.isEmpty()) return null;
        Machine<?> first = list.get(0);
        return first.getClass() == supervisorType ? first : null;
    }

    private void chainSubmit(String chainKey, Runnable task) {
        chains.compute(chainKey, (k, prev) -> {
            CompletableFuture<Void> base = (prev == null)
                ? CompletableFuture.completedFuture(null) : prev;
            return base.thenRunAsync(() -> {
                try { task.run(); }
                catch (Throwable t) {
                    LOG.warn("[{}] chain task threw for {}: {}", name, chainKey, t.toString());
                }
            }, work.asExecutor());
        });
    }

    private static String cellKey(String parentId, Class<? extends Machine<?>> type) {
        return parentId + CHILD_ID_SEPARATOR + type.getSimpleName();
    }

    private static String childId(String parentId, Class<? extends Machine<?>> childType) {
        return parentId + CHILD_ID_SEPARATOR + childType.getSimpleName();
    }

    private static boolean isChildId(String id) { return id.contains(CHILD_ID_SEPARATOR); }

    // ─────────────────────────────────────────────────────────────────
    // Inner types
    // ─────────────────────────────────────────────────────────────────

    record TypeSpec(
        Supplier<? extends Machine<?>> factory,
        int poolSize,
        Function<Machine<?>, Object> volatileLoader
    ) {}

    // ─────────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────────

    public static final class Builder {
        private final String name;
        private Class<? extends Supervisor<?>> supervisorType;
        private final Map<Class<? extends Machine<?>>, TypeSpec> types = new LinkedHashMap<>();
        private int threads = 2;
        private PersistenceProvider persistence;
        private boolean rehydrateEnabled;
        private Function<StatemachineEvent, Object> firstEventToContext;
        private int maxConcurrent = 0;
        private long globalTimeoutMs = 0;
        private String globalTimeoutTargetState;
        private int debugSampleRate = 0;
        private Function<Object, QuotaKeys> quotaKeysExtractor;
        private QuotaLimits quotaLimits = QuotaLimits.UNLIMITED;
        private Channel<?, ?> channel;

        Builder(String name) { this.name = name; }

        /** First machine declared MUST be a Supervisor — it's machines[0] of every row. */
        public <S extends Supervisor<?>> Builder supervisor(
                Class<S> type, Supplier<S> factory, int poolSize) {
            if (supervisorType != null) {
                throw new IllegalStateException("Supervisor already declared: " + supervisorType.getName());
            }
            supervisorType = type;
            types.put(type, new TypeSpec(factory, poolSize, null));
            return this;
        }

        public <M extends Machine<?>> Builder child(
                Class<M> type, Supplier<M> factory, int poolSize) {
            if (supervisorType == null) {
                throw new IllegalStateException("Declare .supervisor(...) before .child(...)");
            }
            if (types.containsKey(type)) {
                throw new IllegalStateException("Duplicate machine type: " + type.getName());
            }
            types.put(type, new TypeSpec(factory, poolSize, null));
            return this;
        }

        public Builder threads(int n) { this.threads = n; return this; }

        /**
         * Configure persistence. With this set, every state transition of
         * every cell is saved as a {@link MachineSnapshot} keyed by the cell's
         * machineId (which is parentId for supervisors, parentId#TypeName for
         * children). Terminal cells delete their snapshot.
         */
        public Builder persistence(PersistenceProvider provider) {
            this.persistence = provider;
            return this;
        }

        /**
         * Enable rehydration on inbound events for unknown ids. When set, on
         * the first event for an unknown id the framework probes persistence
         * for every cell of that id (supervisor + each declared child type),
         * restores any that have snapshots, then delivers the event.
         *
         * <p>Requires {@link #persistence(PersistenceProvider)}. Without it,
         * the framework throws on an inbound non-first event for an unknown
         * id (per the design checklist).
         */
        public Builder rehydrate(boolean enabled) {
            this.rehydrateEnabled = enabled;
            return this;
        }

        /**
         * Per-machine-type volatile loader. Fires on both creation (in
         * {@link Machine#start()}) and rehydration — same callback, both
         * paths. The returned object is stored on the machine's
         * {@code volatileContext} slot and is NOT persisted.
         */
        public <M extends Machine<?>> Builder volatileLoader(
                Class<M> type, Function<Machine<?>, Object> loader) {
            TypeSpec existing = types.get(type);
            if (existing == null) {
                throw new IllegalStateException(
                    "volatileLoader: machine type not registered: " + type.getName());
            }
            types.put(type, new TypeSpec(existing.factory(), existing.poolSize(), loader));
            return this;
        }

        /**
         * Auto-create the supervisor from an inbound first event (one whose
         * {@code isFirst()} returns true). The function receives the event
         * and returns the supervisor's initial context. Without this hook,
         * unknown-id inbound events are dropped (or throw, depending on
         * rehydration config).
         */
        public Builder createFromFirstEvent(Function<StatemachineEvent, Object> fn) {
            this.firstEventToContext = fn;
            return this;
        }

        /**
         * Hard ceiling on concurrent supervisor cells. Dispatch beyond this
         * returns {@link RejectCause#CAPACITY_EXCEEDED}. {@code 0} disables.
         */
        public Builder maxConcurrent(int n) { this.maxConcurrent = n; return this; }

        /**
         * Wall-clock cap on a single supervisor cell, regardless of state.
         * On fire, transitions the supervisor to {@code targetState} (which
         * must be a final state in the supervisor's graph); the framework
         * then runs the standard termination ritual. {@code 0} disables.
         */
        public Builder globalTimeout(long duration, TimeUnit unit, String targetState) {
            if (duration <= 0) throw new IllegalArgumentException("duration must be > 0");
            if (targetState == null) throw new IllegalArgumentException("targetState required");
            this.globalTimeoutMs = unit.toMillis(duration);
            this.globalTimeoutTargetState = targetState;
            return this;
        }

        /**
         * 1-in-N sampling for debug-flagged supervisors. Every Nth dispatch
         * sets {@code debugMode=true} on the supervisor; state transitions
         * emit DEBUG-level traces. {@code 0} disables.
         */
        public Builder debugSampleRate(int n) { this.debugSampleRate = n; return this; }

        /**
         * Per-task quota-key extractor. The framework calls this on every
         * dispatch with the task (which becomes the supervisor's initial
         * context). Returned keys are checked against {@link #quotaLimits}.
         */
        public Builder quotaKeysExtractor(Function<Object, QuotaKeys> extractor) {
            this.quotaKeysExtractor = extractor;
            return this;
        }

        /** Quota thresholds enforced against the keys returned by {@link #quotaKeysExtractor}. */
        public Builder quotaLimits(QuotaLimits limits) {
            this.quotaLimits = limits != null ? limits : QuotaLimits.UNLIMITED;
            return this;
        }

        /**
         * Wire-protocol channel. State actions can reach it through
         * {@code Registry.getChannel()} when they need to emit outbound
         * commands.
         */
        public Builder channel(Channel<?, ?> channel) { this.channel = channel; return this; }

        public Registry build() {
            if (supervisorType == null) {
                throw new IllegalStateException("No supervisor declared — call .supervisor(...) first");
            }
            if (rehydrateEnabled && persistence == null) {
                throw new IllegalStateException(
                    "rehydrate(true) requires .persistence(...) — none configured");
            }
            if (quotaLimits.enforces() && quotaKeysExtractor == null) {
                throw new IllegalStateException(
                    "quotaLimits enforced but no quotaKeysExtractor — keys cannot be derived");
            }
            return new Registry(name, supervisorType, types, threads,
                persistence, rehydrateEnabled, firstEventToContext,
                maxConcurrent, globalTimeoutMs, globalTimeoutTargetState,
                debugSampleRate, quotaKeysExtractor, quotaLimits, channel);
        }
    }
}
