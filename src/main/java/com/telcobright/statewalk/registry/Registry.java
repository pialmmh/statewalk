package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.channel.Channel;
import com.telcobright.statewalk.event.StatemachineEvent;
import com.telcobright.statewalk.executor.BoundedVirtualThreadExecutor;
import com.telcobright.statewalk.machine.Machine;
import com.telcobright.statewalk.persistence.MachineSnapshot;
import com.telcobright.statewalk.persistence.PersistenceProvider;
import com.telcobright.statewalk.persistence.SnapshotSerializer;
import com.telcobright.statewalk.pool.ObjectPoolManager;
import com.telcobright.statewalk.state.StateMap;
import com.telcobright.statewalk.timeout.TimeoutManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * THE statewalk registry — one Registry per domain (call / sms / http / wifi).
 * Hosts machines of multiple types for one request id at once: position 0 of
 * each row is the {@link Supervisor}; positions 1+ are children spawned by
 * that supervisor through its resolver.
 *
 * <p>External callers see ONE machine per request id (the supervisor). All
 * child manipulation is package-private and reachable only through
 * {@link InternalEventResolver}, which lives on the supervisor instance.
 * Construction is builder-only: {@link #builder(String)} is the sole way to
 * obtain an instance.
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
 * <h2>Concurrency model (v3)</h2>
 * <ul>
 *   <li><b>One serial chain per cell</b> ({@code parentId#typeName}): events,
 *       starts, state timeouts, retirement — everything that touches a cell
 *       runs in FIFO order on its chain. Chain continuations are built OUTSIDE
 *       the map mutation and always run on the executor, never inline inside a
 *       {@code compute} (the v2 saturation drop is structurally gone).</li>
 *   <li><b>Atomic lifecycle claims:</b> each cell carries a phase
 *       (LIVE → TERMINATING | SUSPENDING) claimed by CAS. Exactly one owner
 *       runs a cell's retirement or suspension; the 60-second dedup set that
 *       broke retry and id-reuse in v2 does not exist.</li>
 *   <li><b>Epoch identity:</b> every piece of deferred work captures the
 *       machine's borrow epoch and re-checks it at execution, so stale timers
 *       and queued tasks can never touch a re-borrowed machine.</li>
 *   <li><b>Atomic dispatch/restore claims:</b> a request id is claimed with
 *       {@code putIfAbsent}; concurrent dispatches of the same id cannot
 *       double-admit, and restore is single-flight per id.</li>
 *   <li><b>Terminal work is unconditional:</b> every exit path — shutdown,
 *       global timeout, persistence failure — drives a live supervisor to its
 *       declared failover state (always final), so domain terminal work (the
 *       session SDR, teardown) runs on EVERY path, then retires the cell.</li>
 * </ul>
 *
 * <h2>Data structure</h2>
 * <pre>
 *   active  = Map&lt;parentId, CopyOnWriteArrayList&lt;Cell&gt;&gt;   // cells[0] = supervisor
 *   pools   = Map&lt;typeName, ObjectPoolManager&gt;              // per-type pools
 *   chains  = Map&lt;"parentId#typeName", CFuture&gt;             // per-cell FIFO tails
 * </pre>
 *
 * <h2>Machine ids</h2>
 * <ul>
 *   <li>Supervisor: {@code parentId} (the wire UUID).</li>
 *   <li>Child: {@code parentId + "#" + typeName} (e.g. {@code call-1#CallSignaling}).</li>
 * </ul>
 * Stable, debuggable, friendly to persistence (single-column key).
 */
public final class Registry {

    private static final Logger LOG = LoggerFactory.getLogger(Registry.class);

    public static final String CHILD_ID_SEPARATOR = "#";

    /**
     * How long a finished request id keeps a tombstone that (a) drops late
     * events at DEBUG and (b) blocks snapshot resurrection while the terminal
     * delete may still be queued/retrying. A NEW dispatch of the id clears it.
     */
    private static final long FINISHED_TOMBSTONE_MS = 5 * 60_000L;

    /** How long a quarantined id refuses restore retries before probing again. */
    private static final long QUARANTINE_RETRY_MS = 10 * 60_000L;

    /**
     * Guard 3 threshold: a debug-sampled cell that terminates with a context
     * Collection/Map larger than this logs a leak-smell WARN. Diagnostics only;
     * does not affect behaviour.
     */
    private static final int CONTEXT_FIELD_WARN_THRESHOLD = 1000;

    // ─────────────────────────────────────────────────────────────────
    // Cell — one live machine binding, with its lifecycle claim
    // ─────────────────────────────────────────────────────────────────

    enum CellPhase { LIVE, TERMINATING, SUSPENDING }

    static final class Cell {
        final Registry reg;
        final String parentId;
        final String typeName;
        final String machineId;
        final String chainKey;
        final Machine<?> machine;
        final long epoch;
        final CopyOnWriteArrayList<Cell> row;
        final AtomicReference<CellPhase> phase = new AtomicReference<>(CellPhase.LIVE);

        Cell(Registry reg, String parentId, String typeName, String machineId,
             Machine<?> machine, CopyOnWriteArrayList<Cell> row) {
            this.reg = reg;
            this.parentId = parentId;
            this.typeName = typeName;
            this.machineId = machineId;
            this.chainKey = cellKey(parentId, typeName);
            this.machine = machine;
            this.epoch = machine.getEpoch();
            this.row = row;
        }

        boolean isSupervisor()      { return reg.supervisorName.equals(typeName); }
        boolean isLive()            { return phase.get() == CellPhase.LIVE; }
        boolean claimTerminating()  { return phase.compareAndSet(CellPhase.LIVE, CellPhase.TERMINATING); }
        boolean claimSuspending()   { return phase.compareAndSet(CellPhase.LIVE, CellPhase.SUSPENDING); }
        /** The bound machine still belongs to this cell (not reset/re-borrowed). */
        boolean epochValid()        { return machine.getEpoch() == epoch; }
    }

    // ─────────────────────────────────────────────────────────────────
    // Fields
    // ─────────────────────────────────────────────────────────────────

    private final String name;
    private final String supervisorName;
    private final Map<String, RegistryType> types;                            // typeName → type registration
    private final Map<String, StateMap> typeStateMaps;                        // typeName → frozen graph (validation, tombstones)
    private final Map<String, ObjectPoolManager<? extends Machine<?>>> pools; // typeName → pool

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Cell>> active = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> chains = new ConcurrentHashMap<>();

    /**
     * Per-cell FIFO for persistence I/O (saves then the terminal delete),
     * keyed by machineId. Separate from {@link #chains} so the blocking DB
     * write runs OFF the cell's processing chain while staying ordered per
     * cell.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Void>> saveChains = new ConcurrentHashMap<>();

    /** Single-flight gate for restore, per request id. */
    private final ConcurrentHashMap<String, Object> restoreGates = new ConcurrentHashMap<>();

    /** Finished-request tombstones: id → finish stamp. See {@link #FINISHED_TOMBSTONE_MS}. */
    private final ConcurrentHashMap<String, Long> recentlyFinished = new ConcurrentHashMap<>();

    /** Ids whose snapshots failed rehydration — no restore retries until the stamp expires. */
    private final ConcurrentHashMap<String, Long> quarantinedIds = new ConcurrentHashMap<>();

    /** Live global-deadline (epoch ms) per request id; persisted on supervisor snapshots. */
    private final ConcurrentHashMap<String, Long> globalDeadlines = new ConcurrentHashMap<>();

    private final BoundedVirtualThreadExecutor work;
    /**
     * Dedicated executor for persistence writes — isolates the blocking DB
     * write from processing so a slow store can't starve event handling.
     * {@code null} when no persistence is configured.
     */
    private final BoundedVirtualThreadExecutor persistWork;
    private final TimeoutManager timeouts;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /** Entry-point backpressure: bounds wire-inbound events queued but not yet processed. */
    private final Semaphore inboundPermits;
    private final AtomicLong overloadWarnedAtMs = new AtomicLong(0);

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

    /** Terminal state for the supervisor on global-timeout fire; null → forced failover. */
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
     * as a unit — a rebind (acquire new + release old + remember), the terminal
     * release, the restore re-acquire. Dispatch's single tryAcquire stays lock-
     * free (the controller's counters are atomic on their own).
     */
    private final Object quotaLock = new Object();

    /** Optional protocol channel — state actions reach the wire through this. */
    private final Channel<?, ?> channel;

    private Registry(Builder b) {
        this.name = b.name;
        this.supervisorName = b.supervisorName;
        this.types = Map.copyOf(b.types);
        this.timeouts = new TimeoutManager(name, Math.max(2, b.threads));
        this.work = new BoundedVirtualThreadExecutor(name, Math.max(16, types.size() * 100));
        this.persistence = b.persistence;
        this.persistWork = b.persistence != null
            ? new BoundedVirtualThreadExecutor(name + "-persist", Math.max(16, types.size() * 100))
            : null;
        this.rehydrateEnabled = b.rehydrateEnabled;
        this.firstEventToContext = b.firstEventToContext;
        this.maxConcurrent = Math.max(0, b.maxConcurrent);
        this.globalTimeoutMs = Math.max(0, b.globalTimeoutMs);
        this.globalTimeoutTargetState = b.globalTimeoutTargetState;
        this.debugSampleRate = Math.max(0, b.debugSampleRate);
        this.quotaKeysExtractor = b.quotaKeysExtractor;
        this.quotaLimits = b.quotaLimits != null ? b.quotaLimits : QuotaLimits.UNLIMITED;
        this.channel = b.channel;
        this.inboundPermits = new Semaphore(Math.max(64, b.maxPendingInbound));

        // Build-time hardening: reject leak-prone pooled types, freeze each
        // type's state graph, verify offline-needs-persistence, and verify
        // every route target names a registered child (typos die here, not as
        // DEBUG drops in production).
        Map<String, StateMap> graphs = new LinkedHashMap<>();
        types.forEach((typeName, t) -> {
            Machine<?> sample = t.factory().get();
            PooledFieldValidator.validate(typeName, sample.getClass());
            StateMap graph = sample.peekStateMap();
            graphs.put(typeName, graph);
            if (persistence == null && graph.hasOfflineState()) {
                throw new IllegalStateException(
                    "[" + name + "] type '" + typeName + "' declares an offline state but no "
                    + "persistence is configured — offline machines are suspended to (and resumed "
                    + "from) the store. Call .persistence(...).");
            }
            if (sample instanceof Supervisor<?> sup) {
                for (String target : sup.resolver().referencedChildNames()) {
                    if (!types.containsKey(target) || target.equals(supervisorName)) {
                        throw new IllegalStateException(
                            "[" + name + "] supervisor '" + typeName + "' routes to unknown child '"
                            + target + "'. Registered children: "
                            + types.keySet().stream().filter(n -> !n.equals(supervisorName)).toList());
                    }
                }
            }
        });
        this.typeStateMaps = Map.copyOf(graphs);

        this.pools = new ConcurrentHashMap<>();
        types.forEach((n, t) -> pools.put(n, makePool(n, t)));

        LOG.info("[{}] registry initialized — supervisor={}, types={}, persistence={}, rehydrate={}, "
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

        // Wire the channel LAST — events may start flowing the moment start()
        // returns, and everything above must be ready for them.
        if (channel != null) {
            startChannel(b.channelDecoder);
        }

        // Periodic TPS-bucket pruning (concurrency counters prune themselves).
        if (this.quotaLimits.enforces()) {
            scheduleQuotaPrune();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void startChannel(BiFunction<String, Object, StatemachineEvent> decoder) {
        Channel raw = this.channel;
        raw.start((Channel.Inbound) (requestId, event) -> {
            try {
                StatemachineEvent decoded;
                if (decoder != null) {
                    decoded = decoder.apply((String) requestId, event);
                    if (decoded == null) return CompletableFuture.completedFuture(null);   // decoder says: ignore
                } else if (event instanceof StatemachineEvent se) {
                    decoded = se;
                } else {
                    return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "[" + name + "] channel event " + (event == null ? "null" : event.getClass().getName())
                        + " is not a StatemachineEvent and no decoder was configured — use "
                        + "builder.channel(channel, decoder)"));
                }
                return submitInbound((String) requestId, decoded);
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(t);
            }
        });
        LOG.info("[{}] channel '{}' started (inbound wired)", name, channel.getName());
    }

    private void scheduleQuotaPrune() {
        timeouts.schedule(() -> {
            if (shuttingDown.get()) return;
            try { quotaController.pruneStaleTpsBuckets(); }
            catch (RuntimeException e) { LOG.warn("[{}] quota prune threw: {}", name, e.toString()); }
            scheduleQuotaPrune();
        }, 60, TimeUnit.SECONDS);
    }

    /** Exposed for state actions: the wire channel, or {@code null} if not configured. */
    public Channel<?, ?> getChannel() { return channel; }

    /** Typed convenience over {@link #getChannel()} — the caller asserts the parameterisation. */
    @SuppressWarnings("unchecked")
    public <O, I> Channel<O, I> channelAs() { return (Channel<O, I>) channel; }

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
     * Dispatch a new request: claim the id, run admission gates, borrow the
     * supervisor, register, schedule the global timeout, start. Returns a
     * {@link DispatchResult} so callers can map rejections to wire-level
     * cause codes.
     *
     * <p>Admission order: {@code SHUTTING_DOWN → DUPLICATE_ID (atomic claim) →
     * CAPACITY → QUOTA → POOL_INTEGRITY}. Each gate is cheap and
     * short-circuits; every failure path unwinds exactly what it took.
     */
    public DispatchResult dispatch(String parentId, Object task) {
        if (shuttingDown.get()) return DispatchResult.rejected(RejectCause.SHUTTING_DOWN);

        // Atomic id claim — two concurrent dispatches of one id can never both
        // pass (the v2 check-then-act race is structurally gone).
        CopyOnWriteArrayList<Cell> row = new CopyOnWriteArrayList<>();
        if (active.putIfAbsent(parentId, row) != null) {
            LOG.warn("[{}] duplicate dispatch for id={}", name, parentId);
            return DispatchResult.rejected(RejectCause.DUPLICATE_ID);
        }

        boolean committed = false;
        QuotaKeys keys = QuotaKeys.NONE;
        boolean quotaHeld = false;
        try {
            if (maxConcurrent > 0 && active.size() > maxConcurrent) {
                return DispatchResult.rejected(RejectCause.CAPACITY_EXCEEDED);
            }
            // Quota gate — before borrow so a reject doesn't churn the pool.
            keys = (quotaKeysExtractor != null && task != null)
                ? quotaKeysExtractor.apply(task) : QuotaKeys.NONE;
            if (keys == null) keys = QuotaKeys.NONE;
            RejectCause quotaReject = quotaController.tryAcquire(keys, quotaLimits);
            if (quotaReject != null) {
                return DispatchResult.rejected(quotaReject);
            }
            quotaHeld = true;
            // Remember the keys BEFORE the cell becomes reachable: a
            // rebindQuotaKeys() arriving the instant dispatch returns must see them.
            if (!isNone(keys)) dispatchQuotaKeys.put(parentId, keys);

            // A fresh dispatch of a recently-finished id is a NEW session —
            // clear the tombstone so a later suspend/restore isn't blocked.
            recentlyFinished.remove(parentId);

            // Global deadline BEFORE start is queued: the first persisted
            // snapshot must already carry it.
            if (globalTimeoutMs > 0) {
                long deadline = System.currentTimeMillis() + globalTimeoutMs;
                globalDeadlines.put(parentId, deadline);
                scheduleGlobalTimeoutAt(parentId, deadline);
            }

            Cell sup = bindAndStart(row, supervisorName, parentId, parentId, task);
            if (sup == null) {
                return DispatchResult.rejected(RejectCause.POOL_INTEGRITY_ERROR);
            }
            committed = true;
            return DispatchResult.ok();
        } finally {
            if (!committed) {
                active.remove(parentId, row);
                cancelGlobalTimeout(parentId);
                globalDeadlines.remove(parentId);
                if (quotaHeld) {
                    synchronized (quotaLock) {
                        dispatchQuotaKeys.remove(parentId);
                        quotaController.release(keys, quotaLimits);
                    }
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Quota rebind — anonymous-at-birth requests (wifi: a MAC is anonymous
    // at packet #1; the user = the quota key binds at first login)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Atomically swap the quota keys a live request holds: acquire the new
     * keys, then release the ones it acquired at dispatch (or at its last
     * rebind). One decision, both dimensions — either the request ends up
     * holding exactly {@code newKeys}, or it keeps exactly its old keys.
     *
     * <p><b>Exactness (v3):</b> for each dimension that actually changes, the
     * NEW key is acquired (checked) BEFORE the old key is released — there is
     * no instant where the request holds nothing, so a racing dispatch can
     * never push a counter past its cap through the rebind window. A dimension
     * whose key is unchanged is untouched. Rebind burns no TPS tokens — it is
     * a re-identification, not a new transaction.
     *
     * <ul>
     *   <li>Returns {@code null} on success; the {@link RejectCause} of the first
     *       failing dimension otherwise. On failure the old keys were never
     *       released, so the counters are exact either way.</li>
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
            if (supervisorCell(machineId) == null) {
                throw new IllegalStateException(
                    "[" + name + "] rebindQuotaKeys: no live request with id=" + machineId);
            }
            QuotaKeys old = dispatchQuotaKeys.getOrDefault(machineId, QuotaKeys.NONE);
            if (old.equals(wanted)) return null;                       // idempotent

            boolean partnerChanges = !java.util.Objects.equals(old.partnerKey(), wanted.partnerKey());
            boolean routeChanges   = !java.util.Objects.equals(old.routeKey(), wanted.routeKey());

            // Acquire-new-first, per changed dimension.
            if (partnerChanges && wanted.partnerKey() != null) {
                RejectCause rc = quotaController.tryAcquirePartner(wanted.partnerKey(), quotaLimits);
                if (rc != null) {
                    LOG.info("[{}] rebindQuotaKeys rejected id={} {} → {}: {} (old keys kept)",
                        name, machineId, old, wanted, rc);
                    return rc;
                }
            }
            if (routeChanges && wanted.routeKey() != null) {
                RejectCause rc = quotaController.tryAcquireRoute(wanted.routeKey(), quotaLimits);
                if (rc != null) {
                    if (partnerChanges && wanted.partnerKey() != null) {
                        quotaController.releasePartner(wanted.partnerKey(), quotaLimits);
                    }
                    LOG.info("[{}] rebindQuotaKeys rejected id={} {} → {}: {} (old keys kept)",
                        name, machineId, old, wanted, rc);
                    return rc;
                }
            }
            // Release-old-after — only the dimensions that changed.
            if (partnerChanges && old.partnerKey() != null) {
                quotaController.releasePartner(old.partnerKey(), quotaLimits);
            }
            if (routeChanges && old.routeKey() != null) {
                quotaController.releaseRoute(old.routeKey(), quotaLimits);
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
     * Restore-path quota re-acquire. A restored supervisor held its slots
     * before the restart; without this the counters restart at zero and the
     * caps under-count until the machine ends. Unchecked on purpose: a live
     * session is never rejected on restore because a cap was lowered in
     * between — the counters must tell the truth.
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

    // ─────────────────────────────────────────────────────────────────
    // Global (whole-lifetime) timeout
    // ─────────────────────────────────────────────────────────────────

    private void scheduleGlobalTimeoutAt(String parentId, long deadlineMs) {
        long delay = Math.max(0, deadlineMs - System.currentTimeMillis());
        timeouts.scheduleTracked(
            "global:" + parentId,
            () -> {
                Cell cell = supervisorCell(parentId);
                if (cell == null) return;
                LOG.info("[{}] global timeout fired id={} state={} → {}",
                    name, parentId, cell.machine.getCurrentState(),
                    globalTimeoutTargetState != null
                        ? "transition to " + globalTimeoutTargetState
                        : "forced failover");
                chainSubmit(cell.chainKey, () -> {
                    if (!cell.epochValid() || !cell.isLive()) return;
                    if (globalTimeoutTargetState != null) {
                        try {
                            if (!cell.machine.isTerminated()) cell.machine.transitionTo(globalTimeoutTargetState);
                        } catch (Throwable t) {
                            LOG.error("[{}] global-timeout transition threw for id={}: {} — forcing failover",
                                name, parentId, t.toString());
                            abortCellNow(cell, "global-timeout transition threw");
                        }
                    } else {
                        abortCellNow(cell, "global timeout");
                    }
                });
            },
            delay,
            TimeUnit.MILLISECONDS);
    }

    private void cancelGlobalTimeout(String parentId) {
        timeouts.cancelTracked("global:" + parentId);
    }

    // ─────────────────────────────────────────────────────────────────
    // Inbound events
    // ─────────────────────────────────────────────────────────────────

    /**
     * Wire-inbound event for {@code parentId}. Always delivered to the
     * supervisor at position 0; the supervisor's resolver routes from there.
     * Throwing variant of {@link #submitInbound} for direct callers: an
     * unknown id with no recovery path throws {@link IllegalStateException}
     * synchronously; shutdown and overload are silent (logged) drops.
     */
    public void onInboundEvent(String parentId, StatemachineEvent event) {
        CompletableFuture<Void> f = submitInbound(parentId, event);
        if (f.isCompletedExceptionally()) {
            // Surface only the resolution error (unknown id) — matches the
            // documented contract; overload/shutdown were already logged.
            try { f.getNow(null); }
            catch (java.util.concurrent.CompletionException e) {
                if (e.getCause() instanceof IllegalStateException ise) throw ise;
            }
        }
    }

    /**
     * Wire-inbound event with a completion ack: the returned future completes
     * when the supervisor's cell actually processed the event (not merely
     * queued it), or exceptionally when it was rejected:
     * <ul>
     *   <li>{@link IllegalStateException} — unknown id and no recovery path
     *       (not first / no creation hook / no rehydration);</li>
     *   <li>{@link RejectedExecutionException} — registry shutting down, or
     *       inbound backlog over the configured bound (backpressure).</li>
     * </ul>
     * At-least-once consumers (Kafka) commit their offset on completion.
     */
    public CompletableFuture<Void> submitInbound(String parentId, StatemachineEvent event) {
        return submitInboundInternal(parentId, event, false, true);
    }

    private CompletableFuture<Void> submitInboundInternal(String parentId, StatemachineEvent event,
                                                          boolean internal, boolean allowResubmit) {
        if (shuttingDown.get()) {
            return CompletableFuture.failedFuture(new RejectedExecutionException(
                "[" + name + "] shutting down — inbound event dropped for id=" + parentId));
        }
        Cell supCell = supervisorCell(parentId);
        if (supCell == null) {
            // First-event auto-creation path.
            if (event.isFirst() && firstEventToContext != null) {
                Object initialCtx = firstEventToContext.apply(event);
                if (initialCtx != null) {
                    dispatch(parentId, initialCtx);
                    supCell = supervisorCell(parentId);
                }
            }
            // Rehydration path (blocked for recently finished / quarantined ids).
            // Re-fetch unconditionally: a single-flight WAITER gets 0 restored
            // cells back while the winner has already made the row live.
            if (supCell == null && rehydrateEnabled && !recentlyFinished.containsKey(parentId)) {
                restoreAllCellsFor(parentId);
                supCell = supervisorCell(parentId);
            }
            if (supCell == null) {
                if (recentlyFinished.containsKey(parentId)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("[{}] dropping late event {} for finished id={}",
                            name, event.getClass().getSimpleName(), parentId);
                    }
                    return CompletableFuture.completedFuture(null);   // finished — processed-as-dropped
                }
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "[" + name + "] no supervisor for id=" + parentId
                    + " and event " + event.getClass().getSimpleName()
                    + " is not first / no creation hook / no rehydration. "
                    + "Configure .createFromFirstEvent(...) or .persistence(...).rehydrate(true)."));
            }
        }

        // Entry-point backpressure — external submissions only; internal
        // publishes are bounded by the graph and must never be shed.
        if (!internal && !inboundPermits.tryAcquire()) {
            warnOverload();
            return CompletableFuture.failedFuture(new RejectedExecutionException(
                "[" + name + "] inbound backlog over bound — event "
                + event.getClass().getSimpleName() + " for id=" + parentId + " shed"));
        }
        final boolean holdsPermit = !internal;
        final Cell cell = supCell;
        final Supervisor<?> supervisor = (Supervisor<?>) cell.machine;
        final CompletableFuture<Void> ack = new CompletableFuture<>();
        chainSubmit(cell.chainKey, () -> {
            boolean ackDeferred = false;
            try {
                // Identity check: the machine must still be THIS cell's borrow
                // and the cell must still be live. A suspended cell's event is
                // re-submitted once (it arrived while the session logically
                // existed — losing it would strand the suspended session, the
                // v2 over-billing bug); a terminated cell's event is dropped.
                if (!cell.epochValid() || !cell.isLive()) {
                    if (cell.phase.get() == CellPhase.SUSPENDING && allowResubmit && rehydrateEnabled) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("[{}] event {} raced suspend of id={} — re-submitting for rehydrate",
                                name, event.getClass().getSimpleName(), parentId);
                        }
                        ackDeferred = true;    // the resubmission's processing settles the ack
                        submitInboundInternal(parentId, event, true, false)
                            .whenComplete((v, t) -> { if (t != null) ack.completeExceptionally(t); else ack.complete(null); });
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("[{}] dropping late event {} for retired/replaced supervisor id={}",
                            name, event.getClass().getSimpleName(), parentId);
                    }
                    return;
                }
                try { supervisor.handleInbound(event); }
                catch (Throwable t) {
                    LOG.warn("[{}] supervisor.handleInbound threw for id={}: {}",
                        name, parentId, t.toString());
                }
            } finally {
                if (holdsPermit) inboundPermits.release();
                if (!ackDeferred && !ack.isDone()) ack.complete(null);
            }
        });
        return ack;
    }

    private void warnOverload() {
        long now = System.currentTimeMillis();
        long last = overloadWarnedAtMs.get();
        if (now - last > 5_000 && overloadWarnedAtMs.compareAndSet(last, now)) {
            LOG.warn("[{}] inbound backlog over bound ({} permits) — shedding wire events; "
                + "the registry is overloaded or a downstream stall is backing work up",
                name, inboundPermits.availablePermits());
        }
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
        for (int pass = 0; pass < 40; pass++) {
            for (var f : new ArrayList<>(chains.values())) {
                long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
                if (remainingNs == 0) return false;
                try { f.get(remainingNs, TimeUnit.NANOSECONDS); }
                catch (ExecutionException ignored) { /* logged at the task */ }
                catch (TimeoutException e) { return false; }
            }
            // Drain the persistence save chains too so post-await assertions
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

    /**
     * Shutdown: stop the channel (no event is consumed-and-lost after the
     * registry stops accepting), then drive EVERY live request through its
     * failover state — so domain terminal work (session SDRs, teardown) runs —
     * then drain and close. Suspended requests are untouched: their snapshots
     * are their life, and the next start resumes them.
     */
    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) return;
        if (channel != null) {
            try { channel.stop(); }
            catch (RuntimeException e) { LOG.warn("[{}] channel stop threw: {}", name, e.toString()); }
        }
        for (String id : new ArrayList<>(active.keySet())) {
            abortRequest(id, "registry shutdown");
        }
        try { work.awaitIdle(10, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Belt and braces: anything a race left behind is hard-retired so no
        // machine stays bound after shutdown returns.
        for (var row : new ArrayList<>(active.values())) {
            for (Cell cell : row) {
                if (cell.claimTerminating()) {
                    try { doRetire(cell); } catch (RuntimeException e) {
                        LOG.warn("[{}] shutdown hard-retire threw for {}: {}", name, cell.chainKey, e.toString());
                    }
                }
            }
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
        CopyOnWriteArrayList<Cell> row = active.get(parentId);
        if (row == null) {
            LOG.warn("[{}] spawnChild {} for unknown id={} — ignored", name, childTypeName, parentId);
            return;
        }
        if (findLiveCell(parentId, childTypeName) != null) {
            LOG.debug("[{}] child {} already present for id={}", name, childTypeName, parentId);
            return;
        }
        bindAndStart(row, childTypeName, childId(parentId, childTypeName), parentId, task);
    }

    void cleanupChildInternal(String parentId, String childTypeName) {
        Cell cell = findLiveCell(parentId, childTypeName);
        if (cell == null || cell.isSupervisor()) return;
        if (!cell.claimTerminating()) return;
        chainSubmit(cell.chainKey, () -> doRetire(cell));
    }

    /**
     * Retire EVERY live child of the request (never the supervisor). Claims
     * are taken immediately — a respawn of the same child type name can follow
     * this call synchronously (the retry contract) — while the actual
     * retirement runs serialized on each child's own chain.
     */
    void cleanupAllChildrenInternal(String parentId) {
        CopyOnWriteArrayList<Cell> row = active.get(parentId);
        if (row == null) return;
        for (Cell cell : row) {
            if (!cell.isSupervisor() && cell.claimTerminating()) {
                chainSubmit(cell.chainKey, () -> doRetire(cell));
            }
        }
    }

    void forwardToChild(String parentId, String childTypeName, StatemachineEvent event) {
        Cell cell = findLiveCell(parentId, childTypeName);
        if (cell == null) {
            LOG.debug("[{}] no {} for id={}, drop {}",
                name, childTypeName, parentId, event.getClass().getSimpleName());
            return;
        }
        chainSubmit(cell.chainKey, () -> {
            if (!cell.epochValid() || !cell.isLive()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] dropping late event {} for retired child {}/{}",
                        name, event.getClass().getSimpleName(), parentId, childTypeName);
                }
                return;
            }
            try { cell.machine.fire(event); }
            catch (Throwable t) {
                LOG.warn("[{}] child fire threw for id={}/{}: {}",
                    name, parentId, childTypeName, t.toString());
            }
        });
    }

    Machine<?> findChildInternal(String parentId, String childTypeName) {
        return findInternal(parentId, childTypeName);
    }

    /** Package-private test hook: the pool backing a machine type (leak assertions). */
    ObjectPoolManager<? extends Machine<?>> poolOf(String typeName) {
        return pools.get(typeName);
    }

    /** Package-private test hook: live TPS/concurrency counter entries (prune assertions). */
    QuotaController quotaController() { return quotaController; }

    /** Package-private introspection — any live machine type in the row, supervisor included. */
    Machine<?> findInternal(String parentId, String typeName) {
        Cell cell = findLiveCell(parentId, typeName);
        return cell != null ? cell.machine : null;
    }

    private Cell findLiveCell(String parentId, String typeName) {
        List<Cell> row = active.get(parentId);
        if (row == null) return null;
        for (Cell c : row) {
            if (c.isLive() && typeName.equals(c.typeName)) return c;
        }
        return null;
    }

    private Cell supervisorCell(String parentId) {
        List<Cell> row = active.get(parentId);
        if (row == null || row.isEmpty()) return null;
        Cell first;
        try { first = row.get(0); }
        catch (IndexOutOfBoundsException raced) { return null; }
        return first.isSupervisor() && first.isLive() ? first : null;
    }

    // ─────────────────────────────────────────────────────────────────
    // Per-machine registry handle
    // ─────────────────────────────────────────────────────────────────

    /**
     * Per-cell handle — carries the exact {@link Cell}, so callbacks identify
     * the binding, not just a (parentId, typeName) pair that a retry may have
     * re-populated with a NEW cell.
     */
    static final class PerMachineHandle implements Machine.MachineRegistryHandle {
        final Registry reg;
        final Cell cell;
        PerMachineHandle(Registry reg, Cell cell) {
            this.reg = reg; this.cell = cell;
        }
        /**
         * State timers route through the cell's serial chain: the scheduler
         * thread only ever queues; the timeout body runs in FIFO order with
         * the cell's events, restoring the per-cell serial invariant.
         */
        @Override public ScheduledFuture<?> schedule(String mid, Runnable r, long d, TimeUnit u) {
            return reg.timeouts.schedule(() -> reg.chainSubmit(cell.chainKey, () -> {
                if (cell.epochValid()) r.run();
            }), d, u);
        }
        @Override public void onMachineReachedTerminal(String mid) {
            if (cell.claimTerminating()) reg.doRetire(cell);
        }
        @Override public void onStateTransitioned(String mid, String newState,
                                                   long timeoutDeadlineMs, String timeoutTargetState) {
            reg.persistCellSnapshot(cell, newState, timeoutDeadlineMs, timeoutTargetState);
        }
        @Override public void publish(StatemachineEvent event) {
            reg.submitInboundInternal(cell.parentId, event, true, true);
        }
        @Override public void onMachineWentOffline(String mid) {
            reg.onCellWentOffline(cell);
        }

        /** Supervisor uses this to reach back into the owning Registry. */
        Registry registry() { return reg; }
    }

    // ─────────────────────────────────────────────────────────────────
    // Persistence + rehydration internals
    // ─────────────────────────────────────────────────────────────────

    /**
     * Called on every successful transition via the per-machine handle.
     * Builds the snapshot <b>inline</b> — capturing a consistent point-in-time
     * of the context, which may move on before the async write runs — then
     * offloads the blocking {@code save} to the cell's save chain on the
     * dedicated {@link #persistWork} executor. The processing chain never
     * blocks on the DB.
     */
    void persistCellSnapshot(Cell cell, String newState, long timeoutDeadlineMs, String timeoutTargetState) {
        if (persistence == null) return;
        if (!cell.epochValid()) return;
        Object ctx = cell.machine.getContext();
        final MachineSnapshot snap;
        try {
            // Serialize INLINE: the snapshot must reflect the state as of THIS
            // transition, not whenever the async write later runs.
            String b64 = SnapshotSerializer.contextToBase64Json(ctx);
            String ctxClass = ctx != null ? ctx.getClass().getName() : null;
            long globalDeadline = cell.isSupervisor()
                ? globalDeadlines.getOrDefault(cell.parentId, 0L) : 0L;
            snap = new MachineSnapshot(
                cell.machineId, name, newState, ctxClass, b64,
                System.currentTimeMillis(), timeoutTargetState, timeoutDeadlineMs, globalDeadline);
        } catch (RuntimeException e) {
            // Can't even capture this cell's state → its recovery snapshot would
            // be stale/missing. Fail the request THROUGH its failover state so
            // the domain's terminal work (SDR) still runs.
            LOG.error("[{}] snapshot serialize failed for {}: {} — aborting the request",
                name, cell.machineId, e.toString());
            abortRequest(cell.parentId, "snapshot serialization failed");
            return;
        }
        appendSerial(saveChains, cell.machineId, persistWork, () -> {
            try { persistence.save(snap); }
            catch (Throwable t) {
                LOG.error("[{}] persistence write failed for {}: {} — aborting the request",
                    name, cell.machineId, t.toString());
                abortRequest(cell.parentId, "persistence write failed");
            }
        });
    }

    /**
     * Terminal snapshot delete, ordered AFTER any pending saves for the cell
     * (same per-machineId save chain), with bounded retry. A delete that
     * exhausts its retries logs an ERROR and leaves an orphan row — which is
     * safe: the row necessarily holds a FINAL state (the terminal transition's
     * save ran first on the same chain), and restore treats final-state
     * snapshots as tombstones to purge, never to resurrect.
     */
    private void submitDeleteWithRetry(String machineId, int attempt) {
        CompletableFuture<Void> tail = appendSerial(saveChains, machineId, persistWork, () -> {
            try {
                persistence.delete(machineId, name);
            } catch (Throwable t) {
                if (attempt < 3 && !shuttingDown.get()) {
                    LOG.warn("[{}] snapshot delete failed for {} (attempt {}): {} — retrying",
                        name, machineId, attempt, t.toString());
                    timeouts.schedule(() -> submitDeleteWithRetry(machineId, attempt + 1),
                        attempt, TimeUnit.SECONDS);
                } else {
                    LOG.error("[{}] snapshot delete failed permanently for {}: {} — orphan row remains; "
                        + "restore treats its final state as a tombstone", name, machineId, t.toString());
                }
            }
        });
        // Retire the save-chain entry once this tail completes and is still
        // current (a retry or a new session for the same id keeps it alive).
        tail.whenComplete((v, e) -> saveChains.remove(machineId, tail));
    }

    /**
     * A cell entered an {@code .offline()} state — <b>suspend</b> it rather
     * than terminate: keep the persisted snapshot (the resume point), evict
     * the live machine, drop its chain entry. An inbound event for the id, or
     * a startup load-all, rehydrates it. Runs on the going-offline cell's own
     * chain (called from its transition).
     *
     * <p>When the SUPERVISOR goes offline the whole request is suspended
     * (every cell evicted, all snapshots kept, the live global timer
     * cancelled — the deadline itself is persisted on the supervisor snapshot
     * and re-armed on restore); a child going offline suspends only that child.
     */
    void onCellWentOffline(Cell cell) {
        if (persistence == null) {
            LOG.warn("[{}] {}/{} entered an offline state but no persistence is configured — "
                + "cannot suspend, terminating instead", name, cell.parentId, cell.typeName);
            if (cell.claimTerminating()) doRetire(cell);
            return;
        }
        if (cell.isSupervisor()) {
            for (Cell sibling : new ArrayList<>(cell.row)) {
                if (sibling == cell) continue;
                if (sibling.claimSuspending()) {
                    chainSubmit(sibling.chainKey, () -> doSuspend(sibling));
                }
            }
            if (cell.claimSuspending()) doSuspend(cell);
            active.remove(cell.parentId, cell.row);
            cancelGlobalTimeout(cell.parentId);
            globalDeadlines.remove(cell.parentId);   // persisted on the snapshot; re-armed on restore
            LOG.debug("[{}] request {} suspended (supervisor offline) — snapshots retained", name, cell.parentId);
        } else {
            if (cell.claimSuspending()) doSuspend(cell);
            LOG.debug("[{}] {}/{} suspended (child offline)", name, cell.parentId, cell.typeName);
        }
    }

    /** Evict one claimed cell from memory WITHOUT deleting its snapshot (offline suspend). */
    private void doSuspend(Cell cell) {
        cell.row.remove(cell);
        retireChainEntry(cell.chainKey);
        try { cell.machine.resetForReuse(); }
        catch (RuntimeException e) {
            LOG.warn("[{}] reset threw suspending {}: {}", name, cell.chainKey, e.toString());
        }
        if (cell.machine.isIdle()) {
            returnToPool(cell);
        }
        // Snapshot intentionally KEPT — it is the rehydration source for resume.
    }

    /**
     * Startup recovery for failover / restart. Loads <b>every unfinished</b>
     * machine for this registry from the store and resumes it: <em>matured</em>
     * cells settle (their timeout fires on rehydrate → terminal → snapshot
     * deleted), the rest reschedule their remaining timeout and keep running.
     * Final-state supervisor snapshots are tombstones of finished sessions
     * whose delete never landed — purged, never resurrected. Child snapshots
     * without a supervisor snapshot are unroutable orphans — quarantined.
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

        Map<String, Map<String, MachineSnapshot>> byParent = new LinkedHashMap<>();
        for (MachineSnapshot snap : unfinished) {
            String mid = snap.machineId();
            String parentId = isChildId(mid) ? mid.substring(0, mid.indexOf(CHILD_ID_SEPARATOR)) : mid;
            byParent.computeIfAbsent(parentId, k -> new LinkedHashMap<>()).put(mid, snap);
        }
        LOG.info("[{}] startup recovery: {} unfinished snapshot(s) across {} request(s) — resuming",
            name, unfinished.size(), byParent.size());

        int resumed = 0;
        for (var e : byParent.entrySet()) {
            String parentId = e.getKey();
            MachineSnapshot supSnap = e.getValue().get(parentId);
            if (supSnap == null) {
                for (MachineSnapshot orphan : e.getValue().values()) {
                    LOG.warn("[{}] startup recovery: child snapshot {} has no supervisor snapshot — quarantining",
                        name, orphan.machineId());
                    quarantineSnapshot(orphan.machineId(), "orphan child snapshot without supervisor");
                }
                continue;
            }
            try { resumed += restoreAllCellsFor(parentId); }
            catch (RuntimeException ex) {
                LOG.warn("[{}] startup recovery: resume failed for {}: {}", name, parentId, ex.toString());
            }
        }
        LOG.info("[{}] startup recovery: {} cell(s) resumed (matured ones settle, the rest keep running)",
            name, resumed);
    }

    /**
     * Restore every cell with a saved snapshot for the given parentId.
     * Single-flight per id; atomic against a concurrent dispatch of the same
     * id (the row claim decides).
     */
    private int restoreAllCellsFor(String parentId) {
        if (persistence == null || !rehydrateEnabled) return 0;
        Object gate = restoreGates.computeIfAbsent(parentId, k -> new Object());
        synchronized (gate) {
            try {
                if (active.containsKey(parentId)) return 0;              // raced: already live
                if (recentlyFinished.containsKey(parentId)) return 0;
                Long q = quarantinedIds.get(parentId);
                if (q != null) {
                    if (System.currentTimeMillis() - q < QUARANTINE_RETRY_MS) return 0;
                    quarantinedIds.remove(parentId, q);
                }

                // Supervisor snapshot FIRST — no supervisor, no restore.
                Optional<MachineSnapshot> supOpt;
                try { supOpt = persistence.load(parentId, name); }
                catch (RuntimeException e) {
                    LOG.warn("[{}] restore: store load threw for {}: {}", name, parentId, e.toString());
                    return 0;
                }
                if (supOpt.isEmpty()) return 0;
                MachineSnapshot supSnap = supOpt.get();

                // Final-state tombstone: the session FINISHED and only its delete
                // is missing. Purge, never resurrect (no entry-action replay, no
                // zombie quota).
                if (isFinalStateOf(supervisorName, supSnap.currentState())) {
                    LOG.info("[{}] restore: id={} snapshot is terminal ({}) — purging tombstone, not resurrecting",
                        name, parentId, supSnap.currentState());
                    purgeAllSnapshots(parentId);
                    return 0;
                }

                CopyOnWriteArrayList<Cell> row = new CopyOnWriteArrayList<>();
                if (active.putIfAbsent(parentId, row) != null) return 0; // raced with dispatch

                int restored = 0;
                if (restoreOneCell(row, supSnap, supervisorName, parentId)) {
                    restored++;
                } else {
                    active.remove(parentId, row);
                    return 0;                                            // supervisor unrestorable → whole id skipped
                }
                for (String t : types.keySet()) {
                    if (t.equals(supervisorName)) continue;
                    String childId = childId(parentId, t);
                    Optional<MachineSnapshot> childOpt;
                    try { childOpt = persistence.load(childId, name); }
                    catch (RuntimeException e) {
                        LOG.warn("[{}] restore: store load threw for {}: {}", name, childId, e.toString());
                        continue;
                    }
                    if (childOpt.isEmpty()) continue;
                    MachineSnapshot cs = childOpt.get();
                    if (isFinalStateOf(t, cs.currentState())) {
                        try { persistence.delete(childId, name); } catch (RuntimeException ignored) {}
                        continue;
                    }
                    if (restoreOneCell(row, cs, t, parentId)) restored++;
                }

                // Re-arm the request's persisted lifetime cap (v3: a restored
                // session no longer lives forever).
                long gd = supSnap.globalDeadlineMs();
                if (gd > 0) {
                    globalDeadlines.put(parentId, gd);
                    scheduleGlobalTimeoutAt(parentId, gd);   // matured → fires ~immediately
                }

                if (restored > 0) {
                    LOG.info("[{}] cross-cell rehydration for id={} restored {} cells", name, parentId, restored);
                }
                return restored;
            } finally {
                restoreGates.remove(parentId, gate);
            }
        }
    }

    private boolean isFinalStateOf(String typeName, String stateName) {
        StateMap graph = typeStateMaps.get(typeName);
        return graph != null && graph.has(stateName) && graph.get(stateName).finalState();
    }

    private void purgeAllSnapshots(String parentId) {
        try { persistence.delete(parentId, name); } catch (RuntimeException e) {
            LOG.warn("[{}] tombstone purge failed for {}: {}", name, parentId, e.toString());
        }
        for (String t : types.keySet()) {
            if (t.equals(supervisorName)) continue;
            try { persistence.delete(childId(parentId, t), name); } catch (RuntimeException ignored) {}
        }
    }

    private void quarantineSnapshot(String machineId, String reason) {
        String parentId = isChildId(machineId)
            ? machineId.substring(0, machineId.indexOf(CHILD_ID_SEPARATOR)) : machineId;
        quarantinedIds.put(parentId, System.currentTimeMillis());
        try {
            persistence.quarantine(machineId, name, reason);
        } catch (RuntimeException e) {
            LOG.error("[{}] quarantine of {} failed: {} — row left in place; id blocked from restore for {} min",
                name, machineId, e.toString(), QUARANTINE_RETRY_MS / 60_000);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean restoreOneCell(CopyOnWriteArrayList<Cell> row, MachineSnapshot snap,
                                   String typeName, String parentId) {
        String machineId = snap.machineId();

        // 1. Validate + deserialize FIRST — a corrupt snapshot must not leak a
        // borrowed machine per attempt (the v2 bug), and a deploy that renamed
        // a state must quarantine the data for a fixed build, not destroy it.
        StateMap graph = typeStateMaps.get(typeName);
        if (graph == null || !graph.has(snap.currentState())) {
            LOG.error("[{}] restore: snapshot {} holds unknown state '{}' (deploy drift?) — quarantining",
                name, machineId, snap.currentState());
            quarantineSnapshot(machineId, "unknown saved state '" + snap.currentState() + "'");
            return false;
        }
        Object ctx;
        try {
            ctx = SnapshotSerializer.contextFromBase64Json(
                snap.contextJsonBase64(), snap.contextClassName());
        } catch (RuntimeException e) {
            LOG.error("[{}] restore: context deserialization failed for {}: {} — quarantining",
                name, machineId, e.toString());
            quarantineSnapshot(machineId, "context deserialization failed: " + e);
            return false;
        }

        // 2. Borrow + bind.
        RegistryType t = types.get(typeName);
        ObjectPoolManager pool = (ObjectPoolManager) pools.get(typeName);
        Machine m = borrowIdle(pool, typeName);
        if (m == null) return false;

        Cell cell = new Cell(this, parentId, typeName, machineId, m, row);
        synchronized (m) {
            m.setRegistry(new PerMachineHandle(this, cell));
            m.setMachineId(machineId);
            m.setTypeName(typeName);
            if (t.volatileLoader() != null) m.setVolatileContextLoader(t.volatileLoader());
        }

        // 3. Quota slots come back with the supervisor — before the cell is
        // visible, so a terminal release (matured timeout) or a rebind always
        // finds them.
        if (typeName.equals(supervisorName)) reacquireQuotaOnRestore(parentId, ctx);

        row.add(cell);

        final Object restoredCtx = ctx;
        final String savedState        = snap.currentState();
        final String savedTargetState  = snap.timeoutTargetState();
        final long   savedDeadline     = snap.timeoutDeadlineMs();
        chainSubmit(cell.chainKey, () -> {
            if (!cell.epochValid() || !cell.isLive()) return;
            try {
                ((Machine) cell.machine).rehydrate(savedState, restoredCtx, savedTargetState, savedDeadline);
            } catch (Throwable t2) {
                LOG.error("[{}] rehydrate threw for {}: {} — quarantining snapshot and retiring the cell",
                    name, machineId, t2.toString());
                quarantineSnapshot(machineId, "rehydrate threw: " + t2);
                if (cell.claimTerminating()) doRetire(cell);
            }
        });
        return true;
    }

    // ─────────────────────────────────────────────────────────────────
    // Internals — borrow, start, retire, chain submission
    // ─────────────────────────────────────────────────────────────────

    /**
     * Borrow an IDLE machine. A non-idle pooled instance is a corruption
     * artifact: it is DROPPED (never reset — resetting could tear down state a
     * concurrent owner holds; the v2 remedy of returning it to the pool
     * recycled live machines) and a fresh borrow is attempted.
     */
    @SuppressWarnings({"rawtypes"})
    private Machine borrowIdle(ObjectPoolManager pool, String typeName) {
        for (int i = 0; i < 3; i++) {
            Machine m = (Machine) pool.borrow();
            if (m.isIdle()) return m;
            LOG.error("[{}] pool handed a non-IDLE {} instance (state={}) — dropping it",
                name, typeName, m.getCurrentState());
        }
        LOG.error("[{}] pool integrity error for {} — three non-IDLE borrows in a row", name, typeName);
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Cell bindAndStart(CopyOnWriteArrayList<Cell> row, String typeName,
                              String id, String parentId, Object task) {
        RegistryType t = types.get(typeName);
        if (t == null) {
            LOG.error("[{}] unknown machine type {}", name, typeName);
            return null;
        }
        ObjectPoolManager pool = (ObjectPoolManager) pools.get(typeName);
        Machine m = borrowIdle(pool, typeName);
        if (m == null) return null;

        Cell cell = new Cell(this, parentId, typeName, id, m, row);
        synchronized (m) {
            m.setRegistry(new PerMachineHandle(this, cell));
            m.setMachineId(id);
            m.setTypeName(typeName);
            if (t.volatileLoader() != null) m.setVolatileContextLoader(t.volatileLoader());
            if (task != null) ((Machine) m).setInitialContext(task);
            LOG.debug("[{}] bindAndStart: id={} type={} epoch={}", name, id, typeName, cell.epoch);
        }

        // 1-in-N debug sampling — only sample at the supervisor; children
        // inherit by being part of the same logical request.
        if (typeName.equals(supervisorName) && debugSampleRate > 0) {
            boolean debug = (dispatchCounter.getAndIncrement() % debugSampleRate) == 0;
            m.setDebugMode(debug);
        }

        // Publish into the row BEFORE scheduling start() so persistence /
        // transition callbacks fired by start() resolve the live cell.
        row.add(cell);

        chainSubmit(cell.chainKey, () -> {
            if (!cell.epochValid() || !cell.isLive()) return;     // retired before it ever started
            try { cell.machine.start(); }
            catch (Throwable t2) {
                LOG.error("[{}] start threw for {}: {} — aborting the request",
                    name, cell.chainKey, t2.toString());
                abortRequest(parentId, "start threw for " + typeName);
            }
        });
        return cell;
    }

    /**
     * Abort a whole request THROUGH its failover path: the supervisor is
     * driven to its current state's timeout target (always final), so the
     * domain's terminal work — the session SDR, the teardown backstop — runs
     * exactly as it would on a timeout; then the cell retires and the cascade
     * reclaims the children. Runs serialized on the supervisor's chain.
     */
    void abortRequest(String parentId, String reason) {
        Cell cell = supervisorCell(parentId);
        if (cell == null) return;
        chainSubmit(cell.chainKey, () -> abortCellNow(cell, reason));
    }

    /** Chain-context abort: claim, drive to failover, retire. */
    private void abortCellNow(Cell cell, String reason) {
        if (!cell.claimTerminating()) return;
        if (cell.epochValid()) {
            try { cell.machine.forceFailover(reason); }
            catch (Throwable t) {
                LOG.warn("[{}] forceFailover threw for {}: {}", name, cell.chainKey, t.toString());
            }
        }
        doRetire(cell);
    }

    /**
     * The termination ritual for ONE claimed cell. The caller owns the
     * TERMINATING claim; exactly one owner ever gets here per cell.
     * Runs on the cell's chain (terminal transitions and forced aborts both
     * arrive there), so it is serialized with the cell's own event flow.
     */
    private void doRetire(Cell cell) {
        cell.row.remove(cell);

        // Lifecycle hardening (Guard 3): on debug-sampled cells, flag context
        // collections that grew unbounded within the request — the one leak
        // surface the build-time field validator can't see. Sampled only, so
        // this never touches the hot path.
        if (cell.epochValid() && cell.machine.isDebugMode()) {
            Map<String, Integer> oversized =
                ContextInspector.oversizedFields(cell.machine.getContext(), CONTEXT_FIELD_WARN_THRESHOLD);
            if (!oversized.isEmpty()) {
                LOG.warn("[{}] cell {} terminated with oversized context collection(s) {} "
                    + "(threshold {}). A context collection growing without bound within one "
                    + "request is a leak smell — check the state actions that append to it.",
                    name, cell.chainKey, oversized, CONTEXT_FIELD_WARN_THRESHOLD);
            }
        }

        if (cell.epochValid()) {
            try { cell.machine.resetForReuse(); }
            catch (RuntimeException e) {
                LOG.warn("[{}] reset threw for {}: {}", name, cell.chainKey, e.toString());
            }
            if (cell.machine.isIdle()) {
                returnToPool(cell);
            }
        }
        retireChainEntry(cell.chainKey);

        // Drop the persisted snapshot for this cell — it has reached a final
        // state (or was force-retired), the ritual ran, and the machine is
        // back in the pool. Crash-rehydration must NOT resurrect it. Routed
        // through the same save chain so it lands AFTER any pending saves for
        // this cell (ordering), with bounded retry on failure.
        if (persistence != null) {
            submitDeleteWithRetry(cell.machineId, 1);
        }

        if (cell.isSupervisor()) {
            // Cascade: the children are captured HERE, as cell objects — the
            // queued cleanups need no map lookups, so removing the row below
            // cannot orphan them (the v2 cascade bug).
            List<Cell> children = new ArrayList<>(cell.row);
            for (Cell child : children) {
                chainSubmit(child.chainKey, () -> {
                    if (child.claimTerminating()) doRetire(child);
                });
            }
            active.remove(cell.parentId, cell.row);
            cancelGlobalTimeout(cell.parentId);
            globalDeadlines.remove(cell.parentId);

            // Tombstone BEFORE the quota release: from the moment the id is
            // re-dispatchable, a late event must not resurrect the old session.
            final Long stamp = System.currentTimeMillis();
            recentlyFinished.put(cell.parentId, stamp);
            timeouts.schedule(() -> recentlyFinished.remove(cell.parentId, stamp),
                FINISHED_TOMBSTONE_MS, TimeUnit.MILLISECONDS);

            synchronized (quotaLock) {
                QuotaKeys keys = dispatchQuotaKeys.remove(cell.parentId);
                if (keys != null) quotaController.release(keys, quotaLimits);
            }
        } else if (cell.row.isEmpty()) {
            active.remove(cell.parentId, cell.row);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void returnToPool(Cell cell) {
        ObjectPoolManager pool = (ObjectPoolManager) pools.get(cell.typeName);
        pool.returnObject(cell.machine);
    }

    /**
     * Retire a chain map entry safely: only when its current tail completes
     * AND is still the tail. Work queued after this call keeps the entry (and
     * its FIFO order) alive — an immediate respawn under the same cell key
     * (the retry contract) stays serialized with the outgoing cell's cleanup.
     */
    private void retireChainEntry(String chainKey) {
        CompletableFuture<Void> tail = chains.get(chainKey);
        if (tail == null) return;
        tail.whenComplete((v, e) -> chains.remove(chainKey, tail));
    }

    /** Append to the cell's processing chain. */
    private void chainSubmit(String chainKey, Runnable task) {
        appendSerial(chains, chainKey, work, task);
    }

    /**
     * Append a task to a keyed FIFO chain. The map mutation only swaps
     * futures; the continuation is built OUTSIDE the compute and the task
     * always runs on the executor — never inline inside the map (the v2
     * "Recursive update" saturation drop), never on the completing thread.
     */
    private CompletableFuture<Void> appendSerial(ConcurrentHashMap<String, CompletableFuture<Void>> map,
                                                 String key, BoundedVirtualThreadExecutor executor,
                                                 Runnable task) {
        CompletableFuture<Void> tail = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Void>> prevRef = new AtomicReference<>();
        map.compute(key, (k, prev) -> { prevRef.set(prev); return tail; });
        Runnable run = () -> {
            try { task.run(); }
            catch (Throwable t) {
                LOG.warn("[{}] chain task threw for {}: {}", name, key, t.toString());
            } finally {
                tail.complete(null);
            }
        };
        CompletableFuture<Void> prev = prevRef.get();
        if (prev == null || prev.isDone()) {
            executor.submit(run);
        } else {
            prev.whenComplete((v, e) -> executor.submit(run));
        }
        return tail;
    }

    static String cellKey(String parentId, String typeName) {
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
    // Builder — the ONLY way to construct a Registry
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
        private BiFunction<String, Object, StatemachineEvent> channelDecoder;
        private int maxPendingInbound = 10_000;

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

        /**
         * Bind the protocol channel. The registry OWNS its lifecycle: inbound
         * is wired ({@code channel.start}) at build, and {@code channel.stop}
         * runs first in shutdown. Inbound events must implement
         * {@code StatemachineEvent}; if the channel's inbound type is a raw
         * protocol frame, use {@link #channel(Channel, BiFunction)} with a
         * decoder.
         */
        public Builder channel(Channel<?, ?> channel) { this.channel = channel; return this; }

        /**
         * Bind the protocol channel with a decoder mapping the channel's raw
         * inbound frames to {@link StatemachineEvent}s. Return {@code null}
         * from the decoder to ignore a frame.
         */
        @SuppressWarnings("unchecked")
        public <I> Builder channel(Channel<?, I> channel, BiFunction<String, I, StatemachineEvent> decoder) {
            this.channel = channel;
            this.channelDecoder = (BiFunction<String, Object, StatemachineEvent>) (BiFunction<?, ?, ?>) decoder;
            return this;
        }

        /**
         * Bound on wire-inbound events queued but not yet processed; further
         * submissions are shed with a failed ack (backpressure at the entry,
         * never inside the cell chains). Default 10 000.
         */
        public Builder maxPendingInbound(int n) {
            if (n <= 0) throw new IllegalArgumentException("maxPendingInbound must be > 0");
            this.maxPendingInbound = n;
            return this;
        }

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
            return new Registry(this);
        }
    }
}
