package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.executor.BoundedVirtualThreadExecutor;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.pool.ObjectPoolManager;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
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
    private final Class<? extends Supervisor<?, ?>> supervisorType;
    private final Map<Class<? extends Machine<?, ?>>, TypeSpec> types;
    private final Map<Class<? extends Machine<?, ?>>, ObjectPoolManager<? extends Machine<?, ?>>> pools;

    private final ConcurrentHashMap<String, List<Machine<?, ?>>> active = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> chains = new ConcurrentHashMap<>();
    private final Set<String> terminated = ConcurrentHashMap.newKeySet();

    private final BoundedVirtualThreadExecutor work;
    private final TimeoutManager timeouts;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    Registry(String name,
             Class<? extends Supervisor<?, ?>> supervisorType,
             Map<Class<? extends Machine<?, ?>>, TypeSpec> types,
             int threads) {
        this.name = name;
        this.supervisorType = supervisorType;
        this.types = Map.copyOf(types);
        this.timeouts = new TimeoutManager(name, Math.max(2, threads));
        this.work = new BoundedVirtualThreadExecutor(name, Math.max(16, types.size() * 100));

        this.pools = new ConcurrentHashMap<>();
        types.forEach((cls, spec) -> pools.put(cls, makePool(cls, spec)));

        LOG.info("[{}] flat registry initialized — supervisor={}, types={}",
            name, supervisorType.getSimpleName(), types.keySet());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ObjectPoolManager<? extends Machine<?, ?>> makePool(
            Class<? extends Machine<?, ?>> cls, TypeSpec spec) {
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

    /** Dispatch a new request: borrow the supervisor, set it up, start it. */
    public void dispatch(String parentId, Object task) {
        if (shuttingDown.get()) return;
        if (active.containsKey(parentId)) {
            LOG.warn("[{}] duplicate dispatch for id={}", name, parentId);
            return;
        }
        Machine<?, ?> sup = borrowAndStart(supervisorType, parentId, task);
        if (sup == null) return;
        active.computeIfAbsent(parentId, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(sup);
    }

    /** Wire-inbound event arrives for {@code parentId}. Always delivered to
     *  the supervisor at position 0; the supervisor's resolver routes from
     *  there. */
    public void onInboundEvent(String parentId, StatemachineEvent event) {
        if (shuttingDown.get()) return;
        Machine<?, ?> sup = supervisorOf(parentId);
        if (sup == null) {
            LOG.debug("[{}] no supervisor for id={}, event {} dropped",
                name, parentId, event.getClass().getSimpleName());
            return;
        }
        Supervisor<?, ?> supervisor = (Supervisor<?, ?>) sup;
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

    void spawnChildInternal(String parentId, Class<? extends Machine<?, ?>> childType, Object task) {
        if (shuttingDown.get()) return;
        if (childType == supervisorType) {
            throw new IllegalArgumentException("Cannot spawn supervisor as a child");
        }
        if (!types.containsKey(childType)) {
            throw new IllegalArgumentException("Unknown machine type: " + childType.getName());
        }
        Machine<?, ?> existing = findChildInternal(parentId, childType);
        if (existing != null) {
            LOG.debug("[{}] child {} already present for id={}", name, childType.getSimpleName(), parentId);
            return;
        }
        Machine<?, ?> child = borrowAndStart(childType, childId(parentId, childType), task);
        if (child == null) return;
        active.get(parentId).add(child);
    }

    void cleanupChildInternal(String parentId, Class<? extends Machine<?, ?>> childType) {
        Machine<?, ?> child = findChildInternal(parentId, childType);
        if (child == null) return;
        onCellTerminated(parentId, childType);
    }

    void forwardToChild(String parentId, Class<? extends Machine<?, ?>> childType,
                        StatemachineEvent event) {
        Machine<?, ?> child = findChildInternal(parentId, childType);
        if (child == null) {
            LOG.debug("[{}] no {} for id={}, drop {}",
                name, childType.getSimpleName(), parentId, event.getClass().getSimpleName());
            return;
        }
        final Machine<?, ?> m = child;
        chainSubmit(cellKey(parentId, childType), () -> {
            try { m.fire(event); }
            catch (Throwable t) {
                LOG.warn("[{}] child fire threw for id={}/{}: {}",
                    name, parentId, childType.getSimpleName(), t.toString());
            }
        });
    }

    Machine<?, ?> findChildInternal(String parentId, Class<? extends Machine<?, ?>> childType) {
        return findInternal(parentId, childType);
    }

    /** Package-private introspection — any machine type in the row, supervisor included. */
    Machine<?, ?> findInternal(String parentId, Class<? extends Machine<?, ?>> type) {
        List<Machine<?, ?>> list = active.get(parentId);
        if (list == null) return null;
        for (Machine<?, ?> m : list) {
            if (m.getClass() == type) return m;
        }
        return null;
    }

    void forceCleanupAll(String parentId) {
        List<Machine<?, ?>> list = active.get(parentId);
        if (list == null) return;
        for (Machine<?, ?> m : new ArrayList<>(list)) {
            try { onCellTerminated(parentId, (Class<? extends Machine<?, ?>>) m.getClass()); }
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
        final Class<? extends Machine<?, ?>> type;
        PerMachineHandle(Registry reg, String parentId, Class<? extends Machine<?, ?>> type) {
            this.reg = reg; this.parentId = parentId; this.type = type;
        }
        @Override public ScheduledFuture<?> schedule(String mid, Runnable r, long d, TimeUnit u) {
            return reg.timeouts.schedule(r, d, u);
        }
        @Override public void onMachineReachedTerminal(String mid) {
            reg.onCellTerminated(parentId, type);
        }
        @Override public void publish(StatemachineEvent event) {
            // Always routes back to this row's supervisor.
            reg.onInboundEvent(parentId, event);
        }

        /** Supervisor uses this to reach back into the owning Registry. */
        Registry registry() { return reg; }
    }

    // ─────────────────────────────────────────────────────────────────
    // Internals — borrow, start, terminate, chain submission
    // ─────────────────────────────────────────────────────────────────

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Machine<?, ?> borrowAndStart(
            Class<? extends Machine<?, ?>> type, String id, Object task) {
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
        ((Machine) m).setPersistingEntity(task);

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

    private void onCellTerminated(String parentId, Class<? extends Machine<?, ?>> type) {
        String key = cellKey(parentId, type);
        if (!terminated.add(key)) return;

        List<Machine<?, ?>> list = active.get(parentId);
        if (list == null) { terminated.remove(key); return; }
        Machine<?, ?> machine = null;
        for (Machine<?, ?> m : list) {
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

        // Cascade if the supervisor terminated.
        if (type == supervisorType) {
            for (Machine<?, ?> sibling : new ArrayList<>(list)) {
                @SuppressWarnings("unchecked")
                Class<? extends Machine<?, ?>> sibType = (Class<? extends Machine<?, ?>>) sibling.getClass();
                try { onCellTerminated(parentId, sibType); } catch (RuntimeException ignored) {}
            }
            active.remove(parentId);
        } else if (list.isEmpty()) {
            active.remove(parentId);
        }

        timeouts.schedule(() -> terminated.remove(key), 60, TimeUnit.SECONDS);
    }

    private Machine<?, ?> supervisorOf(String parentId) {
        List<Machine<?, ?>> list = active.get(parentId);
        if (list == null || list.isEmpty()) return null;
        Machine<?, ?> first = list.get(0);
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

    private static String cellKey(String parentId, Class<? extends Machine<?, ?>> type) {
        return parentId + CHILD_ID_SEPARATOR + type.getSimpleName();
    }

    private static String childId(String parentId, Class<? extends Machine<?, ?>> childType) {
        return parentId + CHILD_ID_SEPARATOR + childType.getSimpleName();
    }

    private static boolean isChildId(String id) { return id.contains(CHILD_ID_SEPARATOR); }

    // ─────────────────────────────────────────────────────────────────
    // Inner types
    // ─────────────────────────────────────────────────────────────────

    record TypeSpec(Supplier<? extends Machine<?, ?>> factory, int poolSize) {}

    // ─────────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────────

    public static final class Builder {
        private final String name;
        private Class<? extends Supervisor<?, ?>> supervisorType;
        private final Map<Class<? extends Machine<?, ?>>, TypeSpec> types = new LinkedHashMap<>();
        private int threads = 2;

        Builder(String name) { this.name = name; }

        /** First machine declared MUST be a Supervisor — it's machines[0] of every row. */
        public <S extends Supervisor<?, ?>> Builder supervisor(
                Class<S> type, Supplier<S> factory, int poolSize) {
            if (supervisorType != null) {
                throw new IllegalStateException("Supervisor already declared: " + supervisorType.getName());
            }
            supervisorType = type;
            types.put(type, new TypeSpec(factory, poolSize));
            return this;
        }

        public <M extends Machine<?, ?>> Builder child(
                Class<M> type, Supplier<M> factory, int poolSize) {
            if (supervisorType == null) {
                throw new IllegalStateException("Declare .supervisor(...) before .child(...)");
            }
            if (types.containsKey(type)) {
                throw new IllegalStateException("Duplicate machine type: " + type.getName());
            }
            types.put(type, new TypeSpec(factory, poolSize));
            return this;
        }

        public Builder threads(int n) { this.threads = n; return this; }

        public Registry build() {
            if (supervisorType == null) {
                throw new IllegalStateException("No supervisor declared — call .supervisor(...) first");
            }
            return new Registry(name, supervisorType, types, threads);
        }
    }
}
