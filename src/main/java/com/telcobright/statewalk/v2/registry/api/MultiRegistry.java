package com.telcobright.statewalk.v2.registry.api;

import com.telcobright.statewalk.v2.executor.BoundedVirtualThreadExecutor;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.pool.ObjectPoolManager;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.timeout.TimeoutManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Registry that hosts multiple machine TYPES under one roof, all keyed by the
 * same request id. Replacement for the parent/child {@code SupervisorRegistry}
 * pattern: machines for one request id are siblings (a flat group) sharing
 * the id, communicating via {@link InternalEventResolver} routing.
 *
 * <p>Data structure:
 * <pre>
 *   active = { requestId → { machineType → machine instance } }
 *   pools  = { machineType → ObjectPool of idle instances }
 *   chains = { "requestId:machineType" → CompletableFuture FIFO chain }
 * </pre>
 *
 * <p>Communication:
 * <ul>
 *   <li>External wire events arrive via {@link #onInboundEvent}.</li>
 *   <li>Internal events between sibling machines flow through {@link #publish}.</li>
 *   <li>Both go through the resolver → land on a specific cell via {@link #fireOn}.</li>
 * </ul>
 *
 * <p>Cascade cleanup: one machine type may be marked {@code primary}. When the
 * primary cell terminates, the registry force-cleans every other cell for the
 * same id and removes the row.
 */
public final class MultiRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(MultiRegistry.class);

    private final String name;
    private final Map<Class<? extends Machine<?, ?>>, TypeConfig> types;
    private final Map<Class<? extends Machine<?, ?>>, ObjectPoolManager<? extends Machine<?, ?>>> pools;
    private final Class<? extends Machine<?, ?>> primaryType;
    private final InternalEventResolver resolver;
    private final BoundedVirtualThreadExecutor work;
    private final TimeoutManager timeouts;

    private final ConcurrentHashMap<String, MachineSet> active = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> chains = new ConcurrentHashMap<>();
    private final Set<String> terminated = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    MultiRegistry(String name,
                  Map<Class<? extends Machine<?, ?>>, TypeConfig> types,
                  Class<? extends Machine<?, ?>> primaryType,
                  InternalEventResolver resolver,
                  int threads) {
        this.name = name;
        this.types = Map.copyOf(types);
        this.primaryType = primaryType;
        this.resolver = resolver;
        this.timeouts = new TimeoutManager(name, Math.max(2, threads));
        this.work = new BoundedVirtualThreadExecutor(name, Math.max(16, types.size() * 100));

        this.pools = new ConcurrentHashMap<>();
        types.forEach((cls, cfg) -> pools.put(cls, makePool(cls, cfg)));

        LOG.info("[{}] initialized types={} primary={} rules={}",
            name, types.keySet(), primaryType == null ? "none" : primaryType.getSimpleName(),
            resolver.size());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ObjectPoolManager<? extends Machine<?, ?>> makePool(
            Class<? extends Machine<?, ?>> cls, TypeConfig cfg) {
        return new ObjectPoolManager(
            name + "-" + cls.getSimpleName(),
            (Supplier) cfg.factory(),
            cfg.poolSize());
    }

    public static Builder builder(String name) { return new Builder(name); }

    public String getName() { return name; }

    // ─────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────

    /** Borrow a machine of {@code type} for {@code id} and start it. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void spawn(String id, Class<? extends Machine<?, ?>> type, Object task) {
        if (shuttingDown.get()) return;
        TypeConfig cfg = types.get(type);
        if (cfg == null) {
            throw new IllegalArgumentException("Unknown machine type: " + type.getName());
        }
        ObjectPoolManager pool = (ObjectPoolManager) pools.get(type);

        Machine m = (Machine) pool.borrow();
        if (!m.isIdle()) {
            pool.returnObject(m);
            m = (Machine) pool.borrow();
            if (!m.isIdle()) {
                LOG.error("[{}] pool integrity error for type {} — dropping spawn id={}",
                    name, type.getSimpleName(), id);
                return;
            }
        }

        m.setRegistry(new CellHandle(this, id, type));
        m.setMachineId(id);
        if (cfg.volatileLoader() != null) m.setVolatileContextLoader(cfg.volatileLoader());
        ((Machine) m).setPersistingEntity(task);

        final Machine machine = m;
        active.compute(id, (k, set) -> {
            if (set == null) set = new MachineSet();
            set.byType.put(type, machine);
            return set;
        });
        chainSubmit(cellKey(id, type), () -> {
            try {
                machine.start();
            } catch (Throwable t) {
                LOG.error("[{}] start threw for {}:{}: {} — force-cleaning",
                    name, id, type.getSimpleName(), t.toString());
                forceCleanup(id, type);
            }
        });
        LOG.debug("[{}] spawn id={} type={}", name, id, type.getSimpleName());
    }

    /** External entry: an inbound event from a channel. Resolver routes. */
    public void onInboundEvent(String id, StatemachineEvent event) {
        if (shuttingDown.get()) return;
        routeAndFire(event);
    }

    /** Internal entry: a state action publishes an event. Resolver routes. */
    public void publish(StatemachineEvent event) {
        if (shuttingDown.get()) return;
        routeAndFire(event);
    }

    private void routeAndFire(StatemachineEvent event) {
        InternalEventResolver.Rule<?> rule = resolver.lookup(event.getClass());
        if (rule == null) {
            LOG.warn("[{}] unrouted event {} dropped", name, event.getClass().getSimpleName());
            return;
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        Function fn = rule.idExtractor();
        @SuppressWarnings("unchecked")
        String targetId = (String) fn.apply(event);
        if (targetId == null) {
            LOG.warn("[{}] event {} extractor returned null id", name, event.getClass().getSimpleName());
            return;
        }
        fireOn(targetId, rule.targetType(), event);
    }

    /** Fire an event on a specific cell. Used by the resolver after lookup. */
    public void fireOn(String id, Class<? extends Machine<?, ?>> type, StatemachineEvent event) {
        MachineSet set = active.get(id);
        if (set == null) {
            LOG.debug("[{}] fireOn no row for id={} (event {})",
                name, id, event.getClass().getSimpleName());
            return;
        }
        Machine<?, ?> m = set.byType.get(type);
        if (m == null) {
            LOG.debug("[{}] fireOn no {} for id={} (event {})",
                name, type.getSimpleName(), id, event.getClass().getSimpleName());
            return;
        }
        if (m.isTerminated()) return;

        final Machine<?, ?> machine = m;
        chainSubmit(cellKey(id, type), () -> {
            try {
                machine.fire(event);
            } catch (Throwable t) {
                LOG.warn("[{}] fire threw for {}:{}: {}",
                    name, id, type.getSimpleName(), t.toString());
            }
        });
    }

    /** Force-cleanup one cell. */
    public void forceCleanup(String id, Class<? extends Machine<?, ?>> type) {
        onCellTerminated(id, type);
    }

    /** Force-cleanup every machine for an id (cascade). */
    public void forceCleanupAll(String id) {
        MachineSet set = active.get(id);
        if (set == null) return;
        for (var entry : new ArrayList<>(set.byType.entrySet())) {
            forceCleanup(id, entry.getKey());
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Termination ritual (per cell)
    // ─────────────────────────────────────────────────────────────────

    @SuppressWarnings({"unchecked", "rawtypes"})
    void onCellTerminated(String id, Class<? extends Machine<?, ?>> type) {
        String key = cellKey(id, type);
        if (!terminated.add(key)) return;  // dedup

        MachineSet set = active.get(id);
        if (set == null) { terminated.remove(key); return; }
        Machine m = set.byType.remove(type);
        if (m == null) { terminated.remove(key); return; }

        try { m.resetForReuse(); }
        catch (RuntimeException e) {
            LOG.warn("[{}] reset threw for {}: {}", name, key, e.toString());
        }
        if (m.isIdle()) {
            ObjectPoolManager pool = (ObjectPoolManager) pools.get(type);
            pool.returnObject(m);
        }
        chains.remove(key);
        LOG.debug("[{}] cell terminated key={}", name, key);

        // Cascade if this was the primary type for this id.
        if (primaryType != null && primaryType.equals(type)) {
            for (var sibling : new ArrayList<>(set.byType.entrySet())) {
                forceCleanup(id, sibling.getKey());
            }
        }

        if (set.byType.isEmpty()) active.remove(id);

        // Dedup TTL eviction
        timeouts.schedule(() -> terminated.remove(key), 60, TimeUnit.SECONDS);
    }

    // ─────────────────────────────────────────────────────────────────
    // Chain submission (per-cell FIFO)
    // ─────────────────────────────────────────────────────────────────

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

    private static String cellKey(String id, Class<? extends Machine<?, ?>> type) {
        return id + ":" + type.getSimpleName();
    }

    // ─────────────────────────────────────────────────────────────────
    // Test / observation helpers
    // ─────────────────────────────────────────────────────────────────

    public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNs = System.nanoTime() + unit.toNanos(timeout);
        for (var f : new ArrayList<>(chains.values())) {
            long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
            try { f.get(remainingNs, TimeUnit.NANOSECONDS); }
            catch (ExecutionException e) { /* logged in chainSubmit */ }
            catch (TimeoutException e) { return false; }
        }
        long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
        return work.awaitIdle(remainingNs, TimeUnit.NANOSECONDS);
    }

    public int activeIdCount() { return active.size(); }

    public int activeCellCount() {
        int total = 0;
        for (var set : active.values()) total += set.byType.size();
        return total;
    }

    public Machine<?, ?> findMachine(String id, Class<? extends Machine<?, ?>> type) {
        MachineSet set = active.get(id);
        return set == null ? null : set.byType.get(type);
    }

    public boolean hasAny(String id) { return active.containsKey(id); }

    // ─────────────────────────────────────────────────────────────────
    // Shutdown
    // ─────────────────────────────────────────────────────────────────

    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) return;
        for (String id : new ArrayList<>(active.keySet())) {
            try { forceCleanupAll(id); } catch (RuntimeException ignored) {}
        }
        try { work.awaitIdle(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        work.close();
        timeouts.shutdown();
        pools.values().forEach(ObjectPoolManager::clear);
    }

    // ─────────────────────────────────────────────────────────────────
    // Inner types
    // ─────────────────────────────────────────────────────────────────

    record TypeConfig(
        Supplier<? extends Machine<?, ?>> factory,
        int poolSize,
        Function<Machine<?, ?>, Object> volatileLoader
    ) {}

    static class MachineSet {
        final ConcurrentHashMap<Class<? extends Machine<?, ?>>, Machine<?, ?>> byType =
            new ConcurrentHashMap<>();
    }

    /**
     * Per-cell handle. Each Machine instance is wired with one of these on
     * borrow; it carries the id + type so callbacks know which cell is
     * speaking without the Machine class itself needing to know.
     */
    static final class CellHandle implements Machine.MachineRegistryHandle {
        private final MultiRegistry registry;
        private final String id;
        private final Class<? extends Machine<?, ?>> type;

        CellHandle(MultiRegistry registry, String id, Class<? extends Machine<?, ?>> type) {
            this.registry = registry;
            this.id = id;
            this.type = type;
        }

        @Override
        public ScheduledFuture<?> schedule(String machineId, Runnable r,
                                            long delay, TimeUnit unit) {
            return registry.timeouts.schedule(r, delay, unit);
        }

        @Override
        public void onMachineReachedTerminal(String machineId) {
            registry.onCellTerminated(id, type);
        }

        @Override
        public void publish(StatemachineEvent event) {
            registry.publish(event);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────────

    public static final class Builder {
        private final String name;
        private final Map<Class<? extends Machine<?, ?>>, TypeConfig> types = new LinkedHashMap<>();
        private final InternalEventResolver resolver = new InternalEventResolver();
        private Class<? extends Machine<?, ?>> primaryType;
        private int threads = 2;

        Builder(String name) { this.name = name; }

        public <M extends Machine<?, ?>> Builder machine(
                Class<M> type, Supplier<M> factory, int poolSize) {
            return machine(type, factory, poolSize, null);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public <M extends Machine<?, ?>> Builder machine(
                Class<M> type, Supplier<M> factory, int poolSize,
                Function<Machine<?, ?>, Object> volatileLoader) {
            if (types.containsKey(type)) {
                throw new IllegalStateException("Duplicate machine type: " + type.getName());
            }
            types.put(type, new TypeConfig((Supplier) factory, poolSize, volatileLoader));
            return this;
        }

        public Builder primary(Class<? extends Machine<?, ?>> type) {
            if (!types.containsKey(type)) {
                throw new IllegalStateException(
                    "Primary type not registered: " + type.getName());
            }
            this.primaryType = type;
            return this;
        }

        public <E extends StatemachineEvent> Builder route(
                Class<E> eventClass,
                Class<? extends Machine<?, ?>> targetType,
                Function<E, String> idExtractor) {
            if (!types.containsKey(targetType)) {
                throw new IllegalStateException(
                    "Route target type not registered: " + targetType.getName());
            }
            resolver.register(eventClass, targetType, idExtractor);
            return this;
        }

        public Builder threads(int n) { this.threads = n; return this; }

        public MultiRegistry build() {
            if (types.isEmpty()) {
                throw new IllegalStateException("No machine types registered");
            }
            return new MultiRegistry(name, types, primaryType, resolver, threads);
        }
    }
}
