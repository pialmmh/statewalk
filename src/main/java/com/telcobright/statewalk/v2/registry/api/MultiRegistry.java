package com.telcobright.statewalk.v2.registry.api;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.persistence.PersistenceProvider;
import com.telcobright.statewalk.v2.registry.consumes.EventTypeRegistry;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Composes multiple {@link Registry} instances (one per machine type) under a
 * single domain. All per-machine lifecycle — pooling, IDLE invariant, per-id
 * FIFO chains, the 8-step termination ritual, persistence, quota, global
 * timeout — is reused from the base {@code Registry} class. MultiRegistry adds
 * only two things on top:
 *
 * <ol>
 *   <li><b>Cross-type event routing</b> via {@link InternalEventResolver}.
 *       Each event class maps to (targetMachineType, idExtractor); the
 *       resolver hands the event to the right inner Registry.</li>
 *   <li><b>Cascade cleanup on primary terminate.</b> One machine type may be
 *       marked primary; when its instance reaches a final state, every
 *       sibling instance for the same id is force-cleaned.</li>
 * </ol>
 *
 * <p>Lifecycle code does NOT live here. If a memory leak shows up in voice,
 * fixing it in {@link Registry} immediately benefits SMS, HTTP, USSD — every
 * MultiRegistry-backed domain. That was the design goal.
 */
public final class MultiRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(MultiRegistry.class);

    private final String name;
    private final Map<Class<? extends Machine<?, ?>>, Registry<?, ?>> children;
    private final Class<? extends Machine<?, ?>> primaryType;
    private final InternalEventResolver resolver;
    private final PersistenceProvider persistence;
    private final boolean rehydrateEnabled;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /** Per-id set of currently-spawned machine types. Maintained on spawn + child terminate. */
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.Set<Class<? extends Machine<?, ?>>>>
        activeCells = new java.util.concurrent.ConcurrentHashMap<>();

    private MultiRegistry(String name,
                          Map<Class<? extends Machine<?, ?>>, Registry<?, ?>> children,
                          Class<? extends Machine<?, ?>> primaryType,
                          InternalEventResolver resolver,
                          PersistenceProvider persistence,
                          boolean rehydrateEnabled) {
        this.name = name;
        this.children = children;
        this.primaryType = primaryType;
        this.resolver = resolver;
        this.persistence = persistence;
        this.rehydrateEnabled = rehydrateEnabled;
        LOG.info("[{}] composed over {} inner registries (primary={}, routes={}, persistence={}, rehydrate={})",
            name, children.keySet(), primaryType == null ? "none" : primaryType.getSimpleName(),
            resolver.size(), persistence != null, rehydrateEnabled);
    }

    public static Builder builder(String name) { return new Builder(name); }

    public String getName() { return name; }

    // ─────────────────────────────────────────────────────────────────
    // Public API — three primitives, all delegate to inner Registries
    // ─────────────────────────────────────────────────────────────────

    /** Borrow a machine of {@code type} for {@code id} and start it. */
    public void spawn(String id, Class<? extends Machine<?, ?>> type, Object task) {
        if (shuttingDown.get()) return;
        Registry<?, ?> r = children.get(type);
        if (r == null) throw new IllegalArgumentException("Unknown machine type: " + type.getName());
        activeCells.compute(id, (k, set) -> {
            if (set == null) set = java.util.concurrent.ConcurrentHashMap.newKeySet();
            set.add(type);
            return set;
        });
        r.dispatch(id, task);
    }

    /** External entry: wire event arrives, resolver routes to an inner Registry. */
    public void onInboundEvent(String id, StatemachineEvent event) {
        if (shuttingDown.get()) return;
        routeAndFire(event, id);
    }

    /** Internal entry: a machine state action publishes an event. */
    public void publish(StatemachineEvent event) {
        if (shuttingDown.get()) return;
        routeAndFire(event, null);
    }

    /** Force-cleanup one cell. */
    public void forceCleanup(String id, Class<? extends Machine<?, ?>> type) {
        Registry<?, ?> r = children.get(type);
        if (r != null) r.forceCleanupMachine(id);
    }

    /** Force-cleanup every machine for an id (cascade). */
    public void forceCleanupAll(String id) {
        for (var r : children.values()) {
            try { r.forceCleanupMachine(id); } catch (RuntimeException ignored) {}
        }
    }

    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) return;
        for (Registry<?, ?> r : children.values()) {
            try { r.shutdown(); } catch (RuntimeException ignored) {}
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Observation
    // ─────────────────────────────────────────────────────────────────

    public Machine<?, ?> findMachine(String id, Class<? extends Machine<?, ?>> type) {
        Registry<?, ?> r = children.get(type);
        return r == null ? null : r.getMachine(id);
    }

    public boolean hasAny(String id) {
        for (var r : children.values()) if (r.getMachine(id) != null) return true;
        return false;
    }

    public int activeCellCount() {
        int total = 0;
        for (var r : children.values()) total += r.getActiveCount();
        return total;
    }

    /** Number of distinct request ids with at least one active machine. */
    public int activeIdCount() { return activeCells.size(); }

    public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNs = System.nanoTime() + unit.toNanos(timeout);
        // Cross-registry ping-pong (one machine's publish feeds another's chain)
        // means a single pass through children can leave new work pending.
        // Loop until two consecutive passes both find every child idle and
        // the overall row count stable.
        int prevCellCount = -1;
        int stableCount = 0;
        for (int pass = 0; pass < 20; pass++) {
            for (var r : children.values()) {
                long remainingNs = Math.max(0, deadlineNs - System.nanoTime());
                if (remainingNs == 0) return false;
                if (!r.awaitIdle(remainingNs, TimeUnit.NANOSECONDS)) return false;
            }
            int cells = activeCellCount();
            if (cells == prevCellCount) {
                stableCount++;
                if (stableCount >= 2) return true;
            } else {
                stableCount = 0;
                prevCellCount = cells;
            }
        }
        return false;
    }

    // ─────────────────────────────────────────────────────────────────
    // Internals
    // ─────────────────────────────────────────────────────────────────

    private void routeAndFire(StatemachineEvent event, String idHint) {
        InternalEventResolver.Rule<?> rule = resolver.lookup(event.getClass());
        if (rule == null) {
            LOG.warn("[{}] unrouted event {} dropped", name, event.getClass().getSimpleName());
            return;
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        Function fn = rule.idExtractor();
        @SuppressWarnings("unchecked")
        String targetId = (String) fn.apply(event);
        if (targetId == null) targetId = idHint;
        if (targetId == null) {
            LOG.warn("[{}] event {} extractor returned null id", name, event.getClass().getSimpleName());
            return;
        }
        Registry<?, ?> r = children.get(rule.targetType());
        if (r == null) {
            LOG.warn("[{}] no inner registry for target type {}", name, rule.targetType());
            return;
        }
        // Cross-cell rehydration: if the target cell is missing AND persistence
        // is enabled, try to restore EVERY cell that has a saved snapshot for
        // this id. We must restore siblings too, otherwise this event will run
        // but later publishes (e.g. SignalingTerminated → CallSupervisor) will
        // find no recipient and the call's state will silently leak.
        if (r.getMachine(targetId) == null) {
            int restored = restoreAllCellsFor(targetId);
            if (restored == 0) {
                LOG.debug("[{}] no {} for id={} — event {} dropped",
                    name, rule.targetType().getSimpleName(), targetId,
                    event.getClass().getSimpleName());
                return;
            }
            // re-check after rehydration attempt
            if (r.getMachine(targetId) == null) {
                LOG.debug("[{}] rehydrated {} cells for id={} but target {} not among them — event {} dropped",
                    name, restored, targetId, rule.targetType().getSimpleName(),
                    event.getClass().getSimpleName());
                return;
            }
        }
        r.onInboundEvent(targetId, event);
    }

    /**
     * Restore every cell that has a persisted snapshot for {@code id}. Returns
     * the number of cells restored. Called by {@link #routeAndFire} on the
     * arrival of an event whose target cell is not in memory.
     *
     * <p>Restores ALL siblings, not just the target — partial-cell state
     * after rehydration is a known footgun (the supervisor's TearingDown
     * entry expects BalanceTracker to be alive, etc.).
     */
    private int restoreAllCellsFor(String id) {
        if (persistence == null || !rehydrateEnabled) return 0;
        int restored = 0;
        for (Registry<?, ?> reg : children.values()) {
            if (reg.getMachine(id) != null) continue;  // already alive
            try {
                if (reg.restoreFromPersistence(id)) {
                    restored++;
                    activeCells.compute(id, (k, set) -> {
                        if (set == null) set = java.util.concurrent.ConcurrentHashMap.newKeySet();
                        // Find the type whose Registry was just restored
                        for (var e : children.entrySet()) {
                            if (e.getValue() == reg) set.add(e.getKey());
                        }
                        return set;
                    });
                }
            } catch (RuntimeException e) {
                LOG.warn("[{}] cell restore failed for id={} in {}: {}",
                    name, id, reg.getRegistryName(), e.toString());
            }
        }
        if (restored > 0) {
            LOG.info("[{}] cross-cell rehydration for id={} restored {} cells", name, id, restored);
        }
        return restored;
    }

    /** Called from every inner Registry's handleTermination override. */
    private void onChildTerminated(String id, Class<? extends Machine<?, ?>> type) {
        // Track this cell as gone; remove the id from activeCells once empty.
        activeCells.computeIfPresent(id, (k, set) -> {
            set.remove(type);
            return set.isEmpty() ? null : set;
        });
        // Cascade: if the primary just terminated, force-clean every sibling.
        if (primaryType != null && primaryType.equals(type)) {
            for (var e : children.entrySet()) {
                if (e.getKey().equals(type)) continue;
                try { e.getValue().forceCleanupMachine(id); } catch (RuntimeException ignored) {}
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Per-type inner Registry — anonymous subclass with overrides
    // ─────────────────────────────────────────────────────────────────

    /**
     * Builds one inner {@link Registry} for the given machine type. The inner
     * registry IS the proven lifecycle; we only override:
     * <ul>
     *   <li>{@link Registry#handleTermination} — to notify MultiRegistry for cascade.</li>
     *   <li>{@link Machine.MachineRegistryHandle#publish} — so a machine's
     *       {@code publishEvent} call goes up through us to the resolver.</li>
     * </ul>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <M extends Machine<?, ?>> Registry<M, ?> buildInnerRegistry(
            String parentName,
            Class<M> machineType,
            Supplier<M> factory,
            int maxConcurrent,
            long globalTimeoutMs,
            String globalTimeoutTargetState,
            MultiRegistry parent) {

        Registry<M, ?> reg = new Registry() {
            @Override protected String getRegistryName() {
                return parentName + ":" + machineType.getSimpleName();
            }
            @Override protected int getMaxConcurrent() { return maxConcurrent; }
            @Override protected long getGlobalTimeoutMs() { return globalTimeoutMs; }
            @Override protected String getGlobalTimeoutTargetState() { return globalTimeoutTargetState; }
            @Override protected Machine createMachineTemplate() { return factory.get(); }

            @Override
            protected void handleTermination(String requestId, Machine machine, String finalState) {
                parent.onChildTerminated(requestId, machineType);
            }

            @Override
            public void publish(StatemachineEvent event) {
                parent.publish(event);
            }
        };
        return reg;
    }

    // ─────────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────────

    public static final class Builder {

        private record TypeSpec(
            Supplier<? extends Machine<?, ?>> factory,
            int maxConcurrent,
            int poolSize,
            int timeoutThreads,
            Function<Machine<?, ?>, Object> volatileLoader,
            long globalTimeoutMs,
            String globalTimeoutTargetState
        ) {}

        private final String name;
        private final EventTypeRegistry eventTypes = new EventTypeRegistry();
        private final InternalEventResolver resolver = new InternalEventResolver();
        private final Map<Class<? extends Machine<?, ?>>, TypeSpec> typeSpecs = new LinkedHashMap<>();
        private Class<? extends Machine<?, ?>> primaryType;
        private PersistenceProvider persistence;
        private boolean rehydrateEnabled;

        Builder(String name) { this.name = name; }

        /**
         * Configure the snapshot provider. With persistence configured, every
         * state transition of every cell is saved. Snapshots are keyed by
         * (machineId, registryName) so multiple cells of one request id
         * coexist independently.
         */
        public Builder persistence(PersistenceProvider provider) {
            this.persistence = provider;
            return this;
        }

        /**
         * Enable cross-cell rehydration: on the first inbound event for an
         * unknown id, every cell that has a saved snapshot for that id is
         * restored together — not just the cell the event is targeted at.
         * Requires {@link #persistence(PersistenceProvider)}.
         */
        public Builder rehydrate(boolean enabled) {
            this.rehydrateEnabled = enabled;
            return this;
        }

        public <M extends Machine<?, ?>> Builder machine(
                Class<M> type, Supplier<M> factory, int poolSize) {
            return machine(type, factory, poolSize, poolSize, 2, null, 0L, null);
        }

        public <M extends Machine<?, ?>> Builder machine(
                Class<M> type, Supplier<M> factory, int poolSize, int maxConcurrent,
                int timeoutThreads,
                Function<Machine<?, ?>, Object> volatileLoader,
                long globalTimeoutMs, String globalTimeoutTargetState) {
            if (typeSpecs.containsKey(type)) {
                throw new IllegalStateException("Duplicate machine type: " + type.getName());
            }
            typeSpecs.put(type, new TypeSpec(factory, maxConcurrent, poolSize, timeoutThreads,
                volatileLoader, globalTimeoutMs, globalTimeoutTargetState));
            return this;
        }

        public Builder primary(Class<? extends Machine<?, ?>> type) {
            if (!typeSpecs.containsKey(type)) {
                throw new IllegalStateException("Primary type not registered: " + type.getName());
            }
            this.primaryType = type;
            return this;
        }

        public <E extends StatemachineEvent> Builder route(
                Class<E> eventClass,
                Class<? extends Machine<?, ?>> targetType,
                Function<E, String> idExtractor) {
            if (!typeSpecs.containsKey(targetType)) {
                throw new IllegalStateException("Route target type not registered: " + targetType.getName());
            }
            resolver.register(eventClass, targetType, idExtractor);
            eventTypes.register(eventClass);
            return this;
        }

        public MultiRegistry build() {
            if (typeSpecs.isEmpty()) {
                throw new IllegalStateException("No machine types registered");
            }
            if (rehydrateEnabled && persistence == null) {
                throw new IllegalStateException(
                    "rehydrate(true) requires .persistence(...) on the MultiRegistry builder");
            }
            Map<Class<? extends Machine<?, ?>>, Registry<?, ?>> children = new LinkedHashMap<>();
            // Build the MultiRegistry first (so inner registries can refer back).
            // Two-phase: create object, then populate children + initialize.
            MultiRegistry mr = new MultiRegistry(name, children, primaryType, resolver,
                persistence, rehydrateEnabled);
            for (var entry : typeSpecs.entrySet()) {
                var type = entry.getKey();
                var spec = entry.getValue();
                @SuppressWarnings({"unchecked", "rawtypes"})
                Registry<?, ?> reg = buildInnerRegistry(
                    name, (Class) type, (Supplier) spec.factory(),
                    spec.maxConcurrent(),
                    spec.globalTimeoutMs(), spec.globalTimeoutTargetState(),
                    mr);
                children.put(type, reg);
                // Reuse Registry's full initialize() — same pool, chain, persistence,
                // timeout, quota infrastructure that voice already uses. All inner
                // Registries share the SAME persistence provider; snapshots are
                // namespaced by registryName so they don't collide.
                reg.initialize(eventTypes, spec.poolSize(), spec.timeoutThreads(),
                    /* debugSampleRate */ 0,
                    persistence, rehydrateEnabled,
                    /* timeout override */ 0L, null,
                    spec.volatileLoader());
            }
            return mr;
        }
    }
}
