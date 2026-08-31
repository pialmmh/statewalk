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
 * <h2>Type identity</h2>
 * Every machine type (supervisor and each child) is identified by a
 * {@code String name} declared at registration time (in a
 * {@link SupervisorSpec}, {@link MachineSpec}, or via a raw-factory builder
 * call). Pools, routes, cell ids and persistence keys all use this name.
 * <strong>One Java class can back many distinct registered names</strong> —
 * the framework runs a generic {@link SpecBackedMachine} /
 * {@link SpecBackedSupervisor} for spec-based registrations.
 *
 * <h2>Data structure</h2>
 * <pre>
 *   active  = Map&lt;parentId, List&lt;Machine&gt;&gt;          // machines[0] = supervisor
 *   pools   = Map&lt;typeName, ObjectPoolManager&gt;       // per-type pools
 *   chains  = Map&lt;"parentId#typeName", CFuture&gt;      // per-cell FIFO
 * </pre>
 *
 * <h2>Machine ids</h2>
 * <ul>
 *   <li>Supervisor: {@code parentId} (the wire UUID).</li>
 *   <li>Child: {@code parentId + "#" + typeName} (e.g. {@code call-1#CallSignaling}).</li>
 * </ul>
 * Stable, debuggable, friendly to persistence (single-column key).
 */
public class Registry implements Machine.MachineRegistryHandle {

    private static final Logger LOG = LoggerFactory.getLogger(Registry.class);

    public static final String CHILD_ID_SEPARATOR = "#";

    /**
     * Guard 3 threshold: a debug-sampled cell that terminates with a context
     * Collection/Map larger than this logs a leak-smell WARN. Diagnostics only;
     * does not affect behaviour.
     */
    private static final int CONTEXT_FIELD_WARN_THRESHOLD = 1000;

    private final String name;
    private final String supervisorName;
    private final Map<String, RegistryType> types;                           // typeName → type registration
    private final Map<String, ObjectPoolManager<? extends Machine<?>>> pools; // typeName → pool

    private final ConcurrentHashMap<String, List<Machine<?>>> active = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> chains = new ConcurrentHashMap<>();
    private final Set<String> terminated = ConcurrentHashMap.newKeySet();

    /**
     * Per-cell FIFO for persistence I/O (save then terminal delete), keyed by
     * machineId. Separate from {@link #chains} so the blocking DB write runs
     * OFF the cell's processing chain (H1) while staying ordered per cell.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Void>> saveChains = new ConcurrentHashMap<>();

    private final BoundedVirtualThreadExecutor work;
    /**
     * Dedicated executor for persistence writes (H1) — isolates the blocking DB
     * write from processing so a slow store can't starve event handling.
     * {@code null} when no persistence is configured.
     */
    private final BoundedVirtualThreadExecutor persistWork;
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
    /**
     * Serialises every quota bookkeeping step that touches MORE than one counter
     * as a unit — a rebind (release old + acquire new + remember), the terminal
     * release, the restore re-acquire. Dispatch's single tryAcquire stays lock-
     * free (the controller's counters are atomic on their own).
     */
    private final Object quotaLock = new Object();

    /** Optional protocol channel — state actions reach the wire through this. */
    private final Channel<?, ?> channel;

    Registry(String name,
             String supervisorName,
             Map<String, RegistryType> types,
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
        this.supervisorName = supervisorName;
        this.types = Map.copyOf(types);
        this.timeouts = new TimeoutManager(name, Math.max(2, threads));
        this.work = new BoundedVirtualThreadExecutor(name, Math.max(16, types.size() * 100));
        this.persistence = persistence;
        this.persistWork = persistence != null
            ? new BoundedVirtualThreadExecutor(name + "-persist", Math.max(16, types.size() * 100))
            : null;
        this.rehydrateEnabled = rehydrateEnabled;
        this.firstEventToContext = firstEventToContext;
        this.maxConcurrent = Math.max(0, maxConcurrent);
        this.globalTimeoutMs = Math.max(0, globalTimeoutMs);
        this.globalTimeoutTargetState = globalTimeoutTargetState;
        this.debugSampleRate = Math.max(0, debugSampleRate);
        this.quotaKeysExtractor = quotaKeysExtractor;
        this.quotaLimits = quotaLimits != null ? quotaLimits : QuotaLimits.UNLIMITED;
        this.channel = channel;

        // Lifecycle hardening (Guard 1): reject leak-prone pooled types before
        // any pool is allocated. The throwaway sample per type also fails fast
        // if a factory throws at construction.
        types.forEach((typeName, t) -> {
            Machine<?> sample = t.factory().get();
            PooledFieldValidator.validate(typeName, sample.getClass());
            // Offline states suspend the machine to the store and resume it later
            // — that requires a persistence provider.
            if (persistence == null && sample.peekStateMap().hasOfflineState()) {
                throw new IllegalStateException(
                    "[" + name + "] type '" + typeName + "' declares an offline state but no "
                    + "persistence is configured — offline machines are suspended to (and resumed "
                    + "from) the store. Call .persistence(...).");
            }
        });

        this.pools = new ConcurrentHashMap<>();
        types.forEach((n, t) -> pools.put(n, makePool(n, t)));

        LOG.info("[{}] flat registry initialized — supervisor={}, types={}, persistence={}, rehydrate={}, "
                + "maxConcurrent={}, globalTimeoutMs={}, globalTimeoutTarget={}, debugSampleRate={}, "
                + "quotaEnforced={}, channel={}",
            name, supervisorName, types.keySet(),
            persistence != null, rehydrateEnabled,
            this.maxConcurrent, this.globalTimeoutMs, this.globalTimeoutTargetState,
            this.debugSampleRate, this.quotaLimits.enforces(),
            channel != null ? channel.getName() : "<none>");

        // Startup recovery (failover / restart): resume every unfinished machine
        // from the store so a fresh node continues in-flight work — matured cells
        // settle, the rest keep running. Lazy rehydration (on inbound events) and
        // offline-suspend resume share the same restore path.
        if (persistence != null && rehydrateEnabled) {
            recoverUnfinishedOnStartup();
        }
    }

    /** Exposed for state actions: the wire channel, or {@code null} if not configured. */
    public Channel<?, ?> getChannel() { return channel; }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ObjectPoolManager<? extends Machine<?>> makePool(String typeName, RegistryType t) {
        return new ObjectPoolManager(
            name + "-" + typeName,
            (Supplier) t.factory(),
            t.poolSize());
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
        // Remember the keys BEFORE the cell becomes visible in {@code active}: a
        // rebindQuotaKeys() arriving the instant dispatch returns must see them.
        if (!isNone(keys)) dispatchQuotaKeys.put(parentId, keys);
        Machine<?> sup = borrowAndStart(supervisorName, parentId, task);
        if (sup == null) {
            synchronized (quotaLock) {
                dispatchQuotaKeys.remove(parentId);
                quotaController.release(keys);
            }
            return DispatchResult.rejected(RejectCause.POOL_INTEGRITY_ERROR);
        }
        scheduleGlobalTimeout(parentId);
        return DispatchResult.ok();
    }

    // ─────────────────────────────────────────────────────────────────
    // Quota rebind — the ratified base extension (wifi: a MAC is anonymous
    // at birth; the user = the quota key binds at first login)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Atomically swap the quota keys a live request holds: release the keys it
     * acquired at dispatch (or at its last rebind), then {@code tryAcquire} the
     * new ones against this registry's {@link QuotaLimits}. One decision, both
     * dimensions — either the request ends up holding exactly {@code newKeys},
     * or it keeps exactly its old keys.
     *
     * <ul>
     *   <li>Returns {@code null} on success; the {@link RejectCause} of the first
     *       failing dimension otherwise. On failure every partial acquire is rolled
     *       back and the OLD keys are re-taken, so the counters are exact either way.</li>
     *   <li>Synchronous — the caller renders the reject (e.g. a device-limit page)
     *       from the return value.</li>
     *   <li>Idempotent — rebinding to the keys already held is a no-op success.</li>
     *   <li>Terminal release (when the supervisor reaches a final state) releases
     *       whatever keys are held at that moment — the rebound ones.</li>
     *   <li>Restore fidelity is the caller's: the {@code quotaKeysExtractor} reads the
     *       context, so put the bound identity INTO the context (a {@code .stay()}
     *       re-persists it) and a restart re-acquires the rebound keys.</li>
     * </ul>
     *
     * @param machineId the request id (the supervisor's id, never a child id)
     * @param newKeys   the keys to hold from now on; {@code null} = {@link QuotaKeys#NONE}
     * @return {@code null} when the request now holds {@code newKeys}; the reject cause otherwise
     * @throws IllegalStateException if no live request has this id (terminated, unknown, or a child id)
     */
    public RejectCause rebindQuotaKeys(String machineId, QuotaKeys newKeys) {
        QuotaKeys wanted = newKeys != null ? newKeys : QuotaKeys.NONE;
        synchronized (quotaLock) {
            if (supervisorOf(machineId) == null) {
                throw new IllegalStateException(
                    "[" + name + "] rebindQuotaKeys: no live request with id=" + machineId);
            }
            QuotaKeys old = dispatchQuotaKeys.getOrDefault(machineId, QuotaKeys.NONE);
            if (old.equals(wanted)) return null;                       // idempotent

            quotaController.release(old);                              // NONE → no-op
            RejectCause reject = quotaController.tryAcquire(wanted, quotaLimits);
            if (reject != null) {
                quotaController.acquireUnchecked(old, quotaLimits);    // put the old slots back
                LOG.info("[{}] rebindQuotaKeys rejected id={} {} → {}: {} (old keys kept)",
                    name, machineId, old, wanted, reject);
                return reject;
            }
            if (isNone(wanted)) dispatchQuotaKeys.remove(machineId);
            else dispatchQuotaKeys.put(machineId, wanted);
            LOG.debug("[{}] rebindQuotaKeys id={} {} → {}", name, machineId, old, wanted);
            return null;
        }
    }

    /** The quota keys a live request currently holds ({@link QuotaKeys#NONE} if none / unknown). */
    public QuotaKeys quotaKeysOf(String machineId) {
        return dispatchQuotaKeys.getOrDefault(machineId, QuotaKeys.NONE);
    }

    /** Live concurrent count for a partner key (0 if never seen or the dimension is not enforced). */
    public int quotaPartnerActive(String partnerKey) { return quotaController.partnerActive(partnerKey); }

    /** Live concurrent count for a route key (0 if never seen or the dimension is not enforced). */
    public int quotaRouteActive(String routeKey) { return quotaController.routeActive(routeKey); }

    private static boolean isNone(QuotaKeys k) {
        return k == null || (k.partnerKey() == null && k.routeKey() == null);
    }

    /**
     * Restore-path quota re-acquire (the other half of the ratified extension).
     * A restored supervisor held its slots before the restart; without this the
     * counters restart at zero and the caps under-count until the machine ends.
     * Unchecked on purpose: a live session is never rejected on restore because a
     * cap was lowered in between — the counters must tell the truth.
     */
    private void reacquireQuotaOnRestore(String parentId, Object restoredCtx) {
        if (quotaKeysExtractor == null || restoredCtx == null || !quotaLimits.enforces()) return;
        QuotaKeys keys;
        try { keys = quotaKeysExtractor.apply(restoredCtx); }
        catch (RuntimeException e) {
            LOG.warn("[{}] restore: quotaKeysExtractor threw for id={}: {} — no quota re-acquired",
                name, parentId, e.toString());
            return;
        }
        if (isNone(keys)) return;
        synchronized (quotaLock) {
            if (dispatchQuotaKeys.containsKey(parentId)) return;       // already accounted (defensive)
            quotaController.acquireUnchecked(keys, quotaLimits);
            dispatchQuotaKeys.put(parentId, keys);
        }
        LOG.debug("[{}] restore: quota re-acquired for id={} keys={}", name, parentId, keys);
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
                    chainSubmit(cellKey(parentId, supervisorName), () -> {
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
                    sup = supervisorOf(parentId);
                }
            }
            // Rehydration path.
            if (sup == null && rehydrateEnabled) {
                int restored = restoreAllCellsFor(parentId);
                if (restored > 0) sup = supervisorOf(parentId);
            }
            if (sup == null) {
                throw new IllegalStateException(
                    "[" + name + "] no supervisor for id=" + parentId
                    + " and event " + event.getClass().getSimpleName()
                    + " is not first / no creation hook / no rehydration. "
                    + "Configure .createFromFirstEvent(...) or .persistence(...).rehydrate(true).");
            }
        }
        Supervisor<?> supervisor = (Supervisor<?>) sup;
        chainSubmit(cellKey(parentId, supervisorName), () -> {
            // Drop a late event for a supervisor that was reset/returned-to-pool (or replaced
            // by a new call on the same id) between submit and execution. Events are queued on
            // the cell's serial chain; a terminal event can run resetForReuse() (nulls registry)
            // while a later event is still queued — without this guard that queued handleInbound
            // hits a reset supervisor and getOwningRegistry() throws "not bound to a flat
            // Registry" (benign but fires on ~every concurrent teardown). registry is volatile.
            if (supervisor.getRegistry() == null || supervisorOf(parentId) != supervisor) {
                if (LOG.isDebugEnabled())
                    LOG.debug("[{}] dropping late event {} for reset/replaced supervisor id={}",
                        name, event.getClass().getSimpleName(), parentId);
                return;
            }
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
        int prev = -1; int stable = 0;
        for (int pass = 0; pass < 20; pass++) {
            for (var f : new ArrayList<>(chains.values())) {
                long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
                if (remainingNs == 0) return false;
                try { f.get(remainingNs, TimeUnit.NANOSECONDS); }
                catch (ExecutionException ignored) { /* logged in chainSubmit */ }
                catch (TimeoutException e) { return false; }
            }
            // Drain the persistence save chains too (H1) so post-await assertions
            // about the store / recorder are deterministic.
            for (var f : new ArrayList<>(saveChains.values())) {
                long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
                if (remainingNs == 0) return false;
                try { f.get(remainingNs, TimeUnit.NANOSECONDS); }
                catch (ExecutionException ignored) {}
                catch (TimeoutException e) { return false; }
            }
            long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
            if (!work.awaitIdle(remainingNs, TimeUnit.NANOSECONDS)) return false;
            if (persistWork != null) {
                remainingNs = Math.max(0, deadlineNs - System.nanoTime());
                if (!persistWork.awaitIdle(remainingNs, TimeUnit.NANOSECONDS)) return false;
            }
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
        if (persistWork != null) {
            // Let queued saves/deletes flush before closing the store-facing executor.
            try { persistWork.awaitIdle(5, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            persistWork.close();
        }
        work.close();
        timeouts.shutdown();
        pools.values().forEach(ObjectPoolManager::clear);
    }

    // ─────────────────────────────────────────────────────────────────
    // Package-private — InternalEventResolver is the ONLY caller
    // ─────────────────────────────────────────────────────────────────

    void spawnChildInternal(String parentId, String childTypeName, Object task) {
        if (shuttingDown.get()) return;
        if (childTypeName.equals(supervisorName)) {
            throw new IllegalArgumentException("Cannot spawn supervisor as a child");
        }
        if (!types.containsKey(childTypeName)) {
            throw new IllegalArgumentException("Unknown machine type: " + childTypeName);
        }
        Machine<?> existing = findChildInternal(parentId, childTypeName);
        if (existing != null) {
            LOG.debug("[{}] child {} already present for id={}", name, childTypeName, parentId);
            return;
        }
        borrowAndStart(childTypeName, childId(parentId, childTypeName), task);
    }

    void cleanupChildInternal(String parentId, String childTypeName) {
        Machine<?> child = findChildInternal(parentId, childTypeName);
        if (child == null) return;
        onCellTerminated(parentId, childTypeName);
    }

    void forwardToChild(String parentId, String childTypeName,
                        StatemachineEvent event) {
        Machine<?> child = findChildInternal(parentId, childTypeName);
        if (child == null) {
            LOG.debug("[{}] no {} for id={}, drop {}",
                name, childTypeName, parentId, event.getClass().getSimpleName());
            return;
        }
        final Machine<?> m = child;
        chainSubmit(cellKey(parentId, childTypeName), () -> {
            try { m.fire(event); }
            catch (Throwable t) {
                LOG.warn("[{}] child fire threw for id={}/{}: {}",
                    name, parentId, childTypeName, t.toString());
            }
        });
    }

    Machine<?> findChildInternal(String parentId, String childTypeName) {
        return findInternal(parentId, childTypeName);
    }

    /** Package-private introspection — any machine type in the row, supervisor included. */
    Machine<?> findInternal(String parentId, String typeName) {
        List<Machine<?>> list = active.get(parentId);
        if (list == null) return null;
        for (Machine<?> m : list) {
            if (typeName.equals(m.getTypeName())) return m;
        }
        return null;
    }

    void forceCleanupAll(String parentId) {
        List<Machine<?>> list = active.get(parentId);
        if (list == null) return;
        for (Machine<?> m : new ArrayList<>(list)) {
            try { onCellTerminated(parentId, m.getTypeName()); }
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

    /** Per-machine handle — knows its parentId + typeName so callbacks have context. */
    static final class PerMachineHandle implements Machine.MachineRegistryHandle {
        final Registry reg;
        final String parentId;
        final String typeName;
        PerMachineHandle(Registry reg, String parentId, String typeName) {
            this.reg = reg; this.parentId = parentId; this.typeName = typeName;
        }
        @Override public ScheduledFuture<?> schedule(String mid, Runnable r, long d, TimeUnit u) {
            return reg.timeouts.schedule(r, d, u);
        }
        @Override public void onMachineReachedTerminal(String mid) {
            reg.onCellTerminated(parentId, typeName);
        }
        @Override public void onStateTransitioned(String mid, String newState,
                                                   long timeoutDeadlineMs, String timeoutTargetState) {
            reg.persistCellSnapshot(mid, typeName, newState, timeoutDeadlineMs, timeoutTargetState);
        }
        @Override public void publish(StatemachineEvent event) {
            reg.onInboundEvent(parentId, event);
        }
        @Override public void onMachineWentOffline(String mid) {
            reg.onCellWentOffline(parentId, typeName);
        }

        /** Supervisor uses this to reach back into the owning Registry. */
        Registry registry() { return reg; }
    }

    // ─────────────────────────────────────────────────────────────────
    // Persistence + rehydration internals
    // ─────────────────────────────────────────────────────────────────

    /**
     * Called on every successful transition via the per-machine handle (H1).
     * Builds the snapshot <b>inline</b> — capturing a consistent point-in-time
     * of the context, which may move on before the async write runs — then
     * offloads the blocking {@code save} to the cell's save chain on the
     * dedicated {@link #persistWork} executor. The processing chain never
     * blocks on the DB.
     */
    void persistCellSnapshot(String machineId, String typeName,
                              String newState, long timeoutDeadlineMs,
                              String timeoutTargetState) {
        if (persistence == null) return;
        String parentId = isChildId(machineId)
            ? machineId.substring(0, machineId.indexOf(CHILD_ID_SEPARATOR))
            : machineId;
        Machine<?> m = findInternal(parentId, typeName);
        if (m == null) return;
        Object ctx = m.getContext();
        final MachineSnapshot snap;
        try {
            // Serialize INLINE: the snapshot must reflect the state as of THIS
            // transition, not whenever the async write later runs.
            String b64 = SnapshotSerializer.contextToBase64Json(ctx);
            String ctxClass = ctx != null ? ctx.getClass().getName() : null;
            snap = new MachineSnapshot(
                machineId, name, newState, ctxClass, b64,
                System.currentTimeMillis(), timeoutTargetState, timeoutDeadlineMs);
        } catch (RuntimeException e) {
            // Can't even capture this cell's state → its recovery snapshot would
            // be stale/missing. Fail the request rather than run un-persistably.
            LOG.error("[{}] snapshot serialize failed for {}: {} — failing the request",
                name, machineId, e.toString());
            failRequest(parentId);
            return;
        }
        submitPersist(parentId, machineId, () -> persistence.save(snap), /* failOnError */ true);
    }

    /**
     * Append a persistence op to a cell's FIFO save chain on {@link #persistWork}.
     * Ordered per machineId, so {@code save(A) → save(B) → delete} apply in
     * transition order. On a write failure: {@code failOnError} true (save) fails
     * the whole request; false (terminal delete) logs only — the cell is already
     * gone, a missed delete is benign (it gets re-deleted on next terminate or
     * cleaned by startup recovery).
     */
    private void submitPersist(String parentId, String machineId, Runnable io, boolean failOnError) {
        saveChains.compute(machineId, (k, prev) -> {
            CompletableFuture<Void> base = (prev == null)
                ? CompletableFuture.completedFuture(null) : prev;
            return base.thenRunAsync(() -> {
                try { io.run(); }
                catch (Throwable t) {
                    if (failOnError) {
                        LOG.error("[{}] persistence write failed for {}: {} — failing the request",
                            name, machineId, t.toString());
                        failRequest(parentId);
                    } else {
                        LOG.warn("[{}] persistence delete failed for {}: {}",
                            name, machineId, t.toString());
                    }
                }
            }, persistWork.asExecutor());
        });
    }

    /**
     * Fail a whole request because its state could not be persisted. Runs the
     * teardown on the supervisor's processing chain (mirrors the global-timeout
     * path) so it serializes with in-flight processing rather than racing it
     * from the persist thread. Dedup in {@link #onCellTerminated} makes a
     * redundant call (e.g. an already-terminated request) a no-op.
     */
    private void failRequest(String parentId) {
        chainSubmit(cellKey(parentId, supervisorName), () -> forceCleanupAll(parentId));
    }

    /**
     * A cell entered an {@code .offline()} state — <b>suspend</b> it rather than
     * terminate: keep the persisted snapshot (the resume point), evict the live
     * machine (active map + pool return), and drop its processing chain. An
     * inbound event for the id, or a startup load-all, rehydrates it.
     *
     * <p>Offline REQUIRES persistence — without a store there is nowhere to
     * suspend to, so the cell is terminated instead (a build-time check also
     * rejects offline-without-persistence, so this is just defence in depth).
     *
     * <p>When the SUPERVISOR goes offline the whole request is suspended (every
     * cell evicted, all snapshots kept, the global timeout cancelled); a child
     * going offline suspends only that child.
     */
    void onCellWentOffline(String parentId, String typeName) {
        if (persistence == null) {
            LOG.warn("[{}] {}/{} entered an offline state but no persistence is configured — "
                + "cannot suspend, terminating instead", name, parentId, typeName);
            onCellTerminated(parentId, typeName);
            return;
        }
        if (typeName.equals(supervisorName)) {
            List<Machine<?>> list = active.get(parentId);
            if (list == null) return;
            for (Machine<?> m : new ArrayList<>(list)) suspendCell(parentId, m.getTypeName());
            active.remove(parentId);
            cancelGlobalTimeout(parentId);
            LOG.debug("[{}] request {} suspended (supervisor offline) — snapshots retained", name, parentId);
        } else {
            suspendCell(parentId, typeName);
            LOG.debug("[{}] {}/{} suspended (child offline)", name, parentId, typeName);
        }
    }

    /** Evict one cell from memory WITHOUT deleting its snapshot (offline suspend). */
    private void suspendCell(String parentId, String typeName) {
        List<Machine<?>> list = active.get(parentId);
        if (list == null) return;
        Machine<?> machine = null;
        for (Machine<?> m : list) {
            if (typeName.equals(m.getTypeName())) { machine = m; break; }
        }
        if (machine == null) return;
        list.remove(machine);
        chains.remove(cellKey(parentId, typeName));
        try { machine.resetForReuse(); }
        catch (RuntimeException e) {
            LOG.warn("[{}] reset threw suspending {}/{}: {}", name, parentId, typeName, e.toString());
        }
        if (machine.isIdle()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            ObjectPoolManager pool = (ObjectPoolManager) pools.get(typeName);
            pool.returnObject(machine);
        }
        // Snapshot intentionally KEPT — it is the rehydration source for resume.
    }

    /**
     * Startup recovery for failover / restart. Loads <b>every unfinished</b>
     * machine for this registry from the store and resumes it. After a node
     * dies and an external supervisor launches the registry on a fresh node,
     * this rebuilds all in-flight requests: <em>matured</em> cells settle (their
     * timeout fires on rehydrate → terminal → snapshot deleted), and the rest
     * reschedule their remaining timeout and keep running — so ongoing calls
     * continue as if there had been no disaster.
     *
     * <p>Snapshots exist only for unfinished machines (terminal ones are deleted
     * on terminate), so "all snapshots for this registry" == "all unfinished".
     * Requests are restored whole (supervisor + children) via
     * {@link #restoreAllCellsFor}, deduped by parent id.
     */
    private void recoverUnfinishedOnStartup() {
        List<MachineSnapshot> unfinished;
        try {
            unfinished = persistence.loadAllForRegistry(name);
        } catch (RuntimeException e) {
            LOG.warn("[{}] startup recovery: loadAllForRegistry threw: {} — skipping", name, e.toString());
            return;
        }
        if (unfinished == null || unfinished.isEmpty()) return;

        java.util.LinkedHashSet<String> parentIds = new java.util.LinkedHashSet<>();
        for (MachineSnapshot snap : unfinished) {
            String mid = snap.machineId();
            parentIds.add(isChildId(mid) ? mid.substring(0, mid.indexOf(CHILD_ID_SEPARATOR)) : mid);
        }
        LOG.info("[{}] startup recovery: {} unfinished snapshot(s) across {} request(s) — resuming",
            name, unfinished.size(), parentIds.size());

        int resumed = 0;
        for (String parentId : parentIds) {
            try { resumed += restoreAllCellsFor(parentId); }
            catch (RuntimeException e) {
                LOG.warn("[{}] startup recovery: resume failed for {}: {}", name, parentId, e.toString());
            }
        }
        LOG.info("[{}] startup recovery: {} cell(s) resumed (matured ones settle, the rest keep running)",
            name, resumed);
    }

    /** Restore every cell with a saved snapshot for the given parentId. */
    private int restoreAllCellsFor(String parentId) {
        if (persistence == null || !rehydrateEnabled) return 0;
        int restored = 0;
        if (restoreOneCell(parentId, parentId, supervisorName)) restored++;
        for (String t : types.keySet()) {
            if (t.equals(supervisorName)) continue;
            String childId = parentId + CHILD_ID_SEPARATOR + t;
            if (restoreOneCell(childId, parentId, t)) restored++;
        }
        if (restored > 0) {
            LOG.info("[{}] cross-cell rehydration for id={} restored {} cells", name, parentId, restored);
        }
        return restored;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean restoreOneCell(String machineId, String parentId, String typeName) {
        if (findInternal(parentId, typeName) != null) return false;     // already alive
        var opt = persistence.load(machineId, name);
        if (opt.isEmpty()) return false;
        MachineSnapshot snap = opt.get();
        RegistryType t = types.get(typeName);
        ObjectPoolManager pool = (ObjectPoolManager) pools.get(typeName);
        Machine m = (Machine) pool.borrow();
        if (!m.isIdle()) { pool.returnObject(m); return false; }

        m.setRegistry(new PerMachineHandle(this, parentId, typeName));
        m.setMachineId(machineId);
        m.setTypeName(typeName);
        if (t.volatileLoader() != null) m.setVolatileContextLoader(t.volatileLoader());

        Object ctx = SnapshotSerializer.contextFromBase64Json(
            snap.contextJsonBase64(), snap.contextClassName());

        // Quota slots come back with the supervisor — before the cell is visible,
        // so a terminal release (matured timeout) or a rebind always finds them.
        if (typeName.equals(supervisorName)) reacquireQuotaOnRestore(parentId, ctx);

        active.computeIfAbsent(parentId, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(m);

        final Machine machine = m;
        final String savedState        = snap.currentState();
        final String savedTargetState  = snap.timeoutTargetState();
        final long   savedDeadline     = snap.timeoutDeadlineMs();
        chainSubmit(cellKey(parentId, typeName), () -> {
            try {
                machine.rehydrate(savedState, ctx, savedTargetState, savedDeadline);
            } catch (Throwable t2) {
                LOG.error("[{}] rehydrate threw for {}: {} — force-cleaning",
                    name, machineId, t2.toString());
                forceCleanupAll(parentId);
            }
        });
        return true;
    }

    // ─────────────────────────────────────────────────────────────────
    // Internals — borrow, start, terminate, chain submission
    // ─────────────────────────────────────────────────────────────────

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Machine<?> borrowAndStart(String typeName, String id, Object task) {
        RegistryType t = types.get(typeName);
        if (t == null) {
            LOG.error("[{}] unknown machine type {}", name, typeName);
            return null;
        }
        ObjectPoolManager pool = (ObjectPoolManager) pools.get(typeName);
        Machine m = (Machine) pool.borrow();
        if (!m.isIdle()) {
            pool.returnObject(m);
            m = (Machine) pool.borrow();
            if (!m.isIdle()) {
                LOG.error("[{}] pool integrity error for {}", name, typeName);
                return null;
            }
        }
        String parentId = isChildId(id) ? id.substring(0, id.indexOf(CHILD_ID_SEPARATOR)) : id;
        synchronized (m) {
            m.setRegistry(new PerMachineHandle(this, parentId, typeName));
            m.setMachineId(id);
            m.setTypeName(typeName);
            if (t.volatileLoader() != null) m.setVolatileContextLoader(t.volatileLoader());
            if (task != null) ((Machine) m).setInitialContext(task);
            LOG.debug("[{}] borrowAndStart: id={} type={} registrySet={} idle={}",
                name, id, typeName, m.getRegistry() != null, m.isIdle());
        }

        // 1-in-N debug sampling — only sample at the supervisor; children
        // inherit by being part of the same logical request.
        if (typeName.equals(supervisorName) && debugSampleRate > 0) {
            boolean debug = (dispatchCounter.getAndIncrement() % debugSampleRate) == 0;
            m.setDebugMode(debug);
        }

        // Publish into active BEFORE scheduling start() so persistence /
        // transition callbacks fired by start() resolve the live cell.
        active.computeIfAbsent(parentId,
            k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(m);

        final Machine machine = m;
        chainSubmit(cellKey(parentId, typeName), () -> {
            try { machine.start(); }
            catch (Throwable t2) {
                LOG.error("[{}] start threw for {}/{}: {} — force-cleaning",
                    name, parentId, typeName, t2.toString());
                forceCleanupAll(parentId);
            }
        });
        return m;
    }

    private void onCellTerminated(String parentId, String typeName) {
        String key = cellKey(parentId, typeName);
        if (!terminated.add(key)) return;

        List<Machine<?>> list = active.get(parentId);
        if (list == null) { terminated.remove(key); return; }
        Machine<?> machine = null;
        for (Machine<?> m : list) {
            if (typeName.equals(m.getTypeName())) { machine = m; break; }
        }
        if (machine == null) { terminated.remove(key); return; }
        list.remove(machine);

        // Lifecycle hardening (Guard 3): on debug-sampled cells, flag context
        // collections that grew unbounded within the request — the one leak
        // surface the build-time field validator can't see. Sampled only, so
        // this never touches the hot path.
        if (machine.isDebugMode()) {
            Map<String, Integer> oversized =
                ContextInspector.oversizedFields(machine.getContext(), CONTEXT_FIELD_WARN_THRESHOLD);
            if (!oversized.isEmpty()) {
                LOG.warn("[{}] cell {} terminated with oversized context collection(s) {} "
                    + "(threshold {}). A context collection growing without bound within one "
                    + "request is a leak smell — check the state actions that append to it.",
                    name, key, oversized, CONTEXT_FIELD_WARN_THRESHOLD);
            }
        }

        try { machine.resetForReuse(); }
        catch (RuntimeException e) {
            LOG.warn("[{}] reset threw for {}: {}", name, key, e.toString());
        }
        if (machine.isIdle()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            ObjectPoolManager pool = (ObjectPoolManager) pools.get(typeName);
            pool.returnObject(machine);
        }
        chains.remove(key);

        // Drop the persisted snapshot for this cell — it has reached a final
        // state, the framework ran the termination ritual, and the machine is
        // back in the pool. Crash-rehydration must NOT resurrect it. Routed
        // through the same save chain so it lands AFTER any pending saves for
        // this cell (ordering), then the chain entry is retired.
        if (persistence != null) {
            String cellMachineId = typeName.equals(supervisorName)
                ? parentId
                : parentId + CHILD_ID_SEPARATOR + typeName;
            submitPersist(parentId, cellMachineId,
                () -> persistence.delete(cellMachineId, name), /* failOnError */ false);
            CompletableFuture<Void> chain = saveChains.get(cellMachineId);
            if (chain != null) chain.whenComplete((v, t) -> saveChains.remove(cellMachineId, chain));
        }

        // Cascade if the supervisor terminated.
        if (typeName.equals(supervisorName)) {
            // Queue child cleanup onto each child's OWN chain so resetForReuse()
            // runs AFTER all pending events (fire/start) on that chain complete.
            // This prevents clearing registry while the child still has queued work.
            for (Machine<?> sibling : new ArrayList<>(list)) {
                String sibType = sibling.getTypeName();
                String sibKey = cellKey(parentId, sibType);
                chainSubmit(sibKey, () -> {
                    try { onCellTerminated(parentId, sibType); } catch (RuntimeException ignored) {}
                });
            }
            active.remove(parentId);
            cancelGlobalTimeout(parentId);
            synchronized (quotaLock) {
                QuotaKeys keys = dispatchQuotaKeys.remove(parentId);
                if (keys != null) quotaController.release(keys);
            }
        } else if (list.isEmpty()) {
            active.remove(parentId);
        }

        timeouts.schedule(() -> terminated.remove(key), 60, TimeUnit.SECONDS);
    }

    private Machine<?> supervisorOf(String parentId) {
        List<Machine<?>> list = active.get(parentId);
        if (list == null || list.isEmpty()) return null;
        Machine<?> first = list.get(0);
        return supervisorName.equals(first.getTypeName()) ? first : null;
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

    private static String cellKey(String parentId, String typeName) {
        return parentId + CHILD_ID_SEPARATOR + typeName;
    }

    private static String childId(String parentId, String childTypeName) {
        return parentId + CHILD_ID_SEPARATOR + childTypeName;
    }

    private static boolean isChildId(String id) { return id.contains(CHILD_ID_SEPARATOR); }

    // ─────────────────────────────────────────────────────────────────
    // Inner types
    // ─────────────────────────────────────────────────────────────────

    /** Per-type registration: factory + pool size + optional volatile loader. */
    record RegistryType(
        Supplier<? extends Machine<?>> factory,
        int poolSize,
        Function<Machine<?>, Object> volatileLoader
    ) {}

    // ─────────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────────

    public static final class Builder {
        private final String name;
        private String supervisorName;
        private final Map<String, RegistryType> types = new LinkedHashMap<>();
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

        // ── Spec-based registration (primary API) ─────────────────────

        /** Register the supervisor via a {@link SupervisorSpec}. */
        public <C> Builder supervisor(SupervisorSpec<C> spec, int poolSize) {
            requireUnique(spec.name());
            this.supervisorName = spec.name();
            this.types.put(spec.name(),
                new RegistryType(() -> new SpecBackedSupervisor<>(spec), poolSize, null));
            return this;
        }

        /** Register a child machine type via a {@link MachineSpec}. */
        public <C> Builder child(MachineSpec<C> spec, int poolSize) {
            requireSupervisorFirst();
            requireUnique(spec.name());
            this.types.put(spec.name(),
                new RegistryType(() -> new SpecBackedMachine<>(spec), poolSize, null));
            return this;
        }

        // ── Raw-factory registration (for custom subclasses / tests) ──

        /**
         * Register the supervisor with a raw factory + name. Use when a custom
         * {@code Supervisor} subclass is needed (otherwise prefer {@link #supervisor(SupervisorSpec, int)}).
         */
        public Builder supervisor(String typeName, Supplier<? extends Supervisor<?>> factory, int poolSize) {
            requireUnique(typeName);
            this.supervisorName = typeName;
            this.types.put(typeName, new RegistryType(factory, poolSize, null));
            return this;
        }

        /**
         * Register a child machine type with a raw factory + name. Use when a
         * custom {@code Machine} subclass is needed (otherwise prefer
         * {@link #child(MachineSpec, int)}).
         */
        public Builder child(String typeName, Supplier<? extends Machine<?>> factory, int poolSize) {
            requireSupervisorFirst();
            requireUnique(typeName);
            this.types.put(typeName, new RegistryType(factory, poolSize, null));
            return this;
        }

        private void requireSupervisorFirst() {
            if (supervisorName == null) {
                throw new IllegalStateException("Declare .supervisor(...) before .child(...)");
            }
        }

        private void requireUnique(String typeName) {
            if (typeName == null || typeName.isBlank()) {
                throw new IllegalArgumentException("typeName must be non-blank");
            }
            if (types.containsKey(typeName)) {
                throw new IllegalStateException("Duplicate machine type name: " + typeName);
            }
        }

        public Builder threads(int n) { this.threads = n; return this; }

        public Builder persistence(PersistenceProvider provider) {
            this.persistence = provider;
            return this;
        }

        public Builder rehydrate(boolean enabled) {
            this.rehydrateEnabled = enabled;
            return this;
        }

        /**
         * Per-machine-type volatile loader, addressed by the type's
         * {@code name}. Fires on both creation and rehydration.
         */
        public Builder volatileLoader(String typeName, Function<Machine<?>, Object> loader) {
            RegistryType existing = types.get(typeName);
            if (existing == null) {
                throw new IllegalStateException(
                    "volatileLoader: machine type not registered: " + typeName);
            }
            types.put(typeName, new RegistryType(existing.factory(), existing.poolSize(), loader));
            return this;
        }

        public Builder createFromFirstEvent(Function<StatemachineEvent, Object> fn) {
            this.firstEventToContext = fn;
            return this;
        }

        public Builder maxConcurrent(int n) { this.maxConcurrent = n; return this; }

        public Builder globalTimeout(long duration, TimeUnit unit, String targetState) {
            if (duration <= 0) throw new IllegalArgumentException("duration must be > 0");
            if (targetState == null) throw new IllegalArgumentException("targetState required");
            this.globalTimeoutMs = unit.toMillis(duration);
            this.globalTimeoutTargetState = targetState;
            return this;
        }

        public Builder debugSampleRate(int n) { this.debugSampleRate = n; return this; }

        public Builder quotaKeysExtractor(Function<Object, QuotaKeys> extractor) {
            this.quotaKeysExtractor = extractor;
            return this;
        }

        public Builder quotaLimits(QuotaLimits limits) {
            this.quotaLimits = limits != null ? limits : QuotaLimits.UNLIMITED;
            return this;
        }

        public Builder channel(Channel<?, ?> channel) { this.channel = channel; return this; }

        public Registry build() {
            if (supervisorName == null) {
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
            return new Registry(name, supervisorName, types, threads,
                persistence, rehydrateEnabled, firstEventToContext,
                maxConcurrent, globalTimeoutMs, globalTimeoutTargetState,
                debugSampleRate, quotaKeysExtractor, quotaLimits, channel);
        }
    }
}
