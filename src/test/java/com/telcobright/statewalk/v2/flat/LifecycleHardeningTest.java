package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.v2.persistence.MachineSnapshot;
import com.telcobright.statewalk.v2.persistence.SnapshotSerializer;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lifecycle-hardening guards on {@link Registry} — the reusable
 * memory-leak / GC protections that must hold for <em>every</em> protocol
 * added to the framework, not just the call protocol that exists today.
 *
 * <ol>
 *   <li><b>Guard 1 — build-time field validation:</b> a pooled machine type
 *       that declares a non-final instance field is rejected at build, because
 *       that field captures per-request state which survives pool reuse
 *       ({@link PooledFieldValidator}).</li>
 *   <li><b>Guard 2 — H2 startup recovery:</b> snapshots whose timeout matured
 *       while the process was down are settled on build; unmatured ones are
 *       left for lazy recovery.</li>
 *   <li><b>Guard 3 — context-size smell-test:</b> {@link ContextInspector}
 *       flags context collections that grew unbounded within one request.</li>
 * </ol>
 */
class LifecycleHardeningTest {

    // ── shared events / context ────────────────────────────────────────

    public record Go(String uuid) implements StatemachineEvent {}

    public static class Ctx { public int n; public Ctx() {} }

    private static StateMap twoStateGraph(String initial, String finalState) {
        return StateMap.builder()
            .initialState(initial)
            .state(initial)
                .interim()
                .timeout(1, TimeUnit.SECONDS, finalState)
            .state(finalState)
                .finalState()
                .timeout(1, TimeUnit.SECONDS, finalState)
            .build();
    }

    static final SupervisorSpec<Ctx> CLEAN_SUP = SupervisorSpec.<Ctx>builder()
        .name("Clean")
        .contextFactory(Ctx::new)
        .stateMap(twoStateGraph("A", "DONE"))
        .routes(r -> r.selfHandle(Go.class))
        .build();

    // ─────────────────────────────────────────────────────────────────
    // Guard 1 — build-time pooled-field validation
    // ─────────────────────────────────────────────────────────────────

    /** Custom supervisor subclass with a NON-FINAL instance field → leak vector. */
    public static class LeakySupervisor extends Supervisor<Ctx> {
        private List<String> perCallLog = new ArrayList<>();   // BAD: survives pool reuse
        @Override protected void defineRoutes(InternalEventResolver r) { r.selfHandle(Go.class); }
        @Override protected StateMap defineStates() { return twoStateGraph("A", "DONE"); }
        @Override protected Ctx createContext() { return new Ctx(); }
        // reference the field so the compiler keeps it and javac doesn't warn it unused
        void touch() { perCallLog.add("x"); }
    }

    /** Custom child subclass with a non-final primitive instance field → leak vector. */
    public static class LeakyChild extends Machine<Ctx> {
        private int counter;   // BAD
        @Override protected StateMap defineStates() { return twoStateGraph("W", "DONE"); }
        @Override protected Ctx createContext() { return new Ctx(); }
        void touch() { counter++; }
    }

    /** Custom supervisor with ONLY a final config field → allowed (config, not per-request state). */
    public static class ConfigOkSupervisor extends Supervisor<Ctx> {
        private final String config;
        public ConfigOkSupervisor(String config) { this.config = config; }
        @Override protected void defineRoutes(InternalEventResolver r) { r.selfHandle(Go.class); }
        @Override protected StateMap defineStates() { return twoStateGraph("A", "DONE"); }
        @Override protected Ctx createContext() { return new Ctx(); }
        String config() { return config; }
    }

    @Test
    void build_rejects_supervisor_with_nonfinal_instance_field() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Registry.builder("h-leak-sup")
                .supervisor("LeakySupervisor", LeakySupervisor::new, 2)
                .build());
        assertTrue(ex.getMessage().contains("perCallLog"),
            "error should name the offending field: " + ex.getMessage());
    }

    @Test
    void build_rejects_child_with_nonfinal_instance_field() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Registry.builder("h-leak-child")
                .supervisor(CLEAN_SUP, 2)
                .child("LeakyChild", LeakyChild::new, 2)
                .build());
        assertTrue(ex.getMessage().contains("counter"),
            "error should name the offending field: " + ex.getMessage());
    }

    @Test
    void build_allows_final_config_field_on_custom_subclass() {
        Registry reg = Registry.builder("h-config-ok")
            .supervisor("ConfigOkSupervisor", () -> new ConfigOkSupervisor("cfg"), 2)
            .build();
        try {
            assertNotNull(reg);
        } finally {
            reg.shutdown();
        }
    }

    @Test
    void build_allows_spec_backed_types() {
        Registry reg = Registry.builder("h-spec-ok").supervisor(CLEAN_SUP, 2).build();
        try {
            assertNotNull(reg, "spec-backed types carry only a final spec → always pass");
        } finally {
            reg.shutdown();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Guard 2 — H2 startup recovery
    // ─────────────────────────────────────────────────────────────────

    static final SupervisorSpec<Ctx> RECOVER_SUP = SupervisorSpec.<Ctx>builder()
        .name("Recover")
        .contextFactory(Ctx::new)
        .stateMap(StateMap.builder()
            .initialState("RUNNING")
            .state("RUNNING")
                .interim()
                .timeout(1, TimeUnit.HOURS, "SETTLED")
            .state("SETTLED")
                .finalState()
                .timeout(1, TimeUnit.SECONDS, "SETTLED")
            .build())
        .routes(r -> r.selfHandle(Go.class))
        .build();

    @Test
    void startup_recovery_settles_matured_snapshot() throws InterruptedException {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        long now = System.currentTimeMillis();
        // A crashed JVM left this supervisor in RUNNING; its timeout to the
        // final SETTLED state fell due an hour ago.
        store.save(new MachineSnapshot(
            "crashed-1", "h-recover", "RUNNING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(new Ctx()),
            now - 7_200_000L, "SETTLED", now - 3_600_000L));
        assertEquals(1, store.size());

        Registry reg = Registry.builder("h-recover")
            .supervisor(RECOVER_SUP, 2)
            .persistence(store)
            .rehydrate(true)
            .threads(2)
            .build();
        try {
            assertTrue(reg.awaitIdle(3, TimeUnit.SECONDS), "recovery should drain");
            assertEquals(0, store.size(),
                "matured snapshot rehydrated → SETTLED → terminal ritual purged it");
            assertEquals(0, reg.activeCellCount(), "no live cell left behind");
        } finally {
            reg.shutdown();
        }
    }

    @Test
    void startup_recovery_leaves_unmatured_snapshot_untouched() throws InterruptedException {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        long now = System.currentTimeMillis();
        // Deadline an hour in the FUTURE → not matured → must not be proactively rehydrated.
        store.save(new MachineSnapshot(
            "alive-1", "h-recover2", "RUNNING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(new Ctx()),
            now, "SETTLED", now + 3_600_000L));

        Registry reg = Registry.builder("h-recover2")
            .supervisor(RECOVER_SUP, 2)
            .persistence(store)
            .rehydrate(true)
            .threads(2)
            .build();
        try {
            assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));
            assertEquals(1, store.size(), "unmatured snapshot left for lazy recovery");
            assertEquals(0, reg.activeCellCount(), "not proactively rehydrated at startup");
        } finally {
            reg.shutdown();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Guard 3 — ContextInspector smell-test
    // ─────────────────────────────────────────────────────────────────

    public static class FatCtx {
        public List<String> events = new ArrayList<>();
        public Map<String, String> tags = new HashMap<>();
        public int scalar;
        public FatCtx() {}
    }

    @Test
    void context_inspector_flags_only_oversized_collections() {
        FatCtx c = new FatCtx();
        for (int i = 0; i < 5; i++) c.events.add("e" + i);
        c.tags.put("k", "v");          // size 1, under threshold
        c.scalar = 999;                // not a collection

        Map<String, Integer> over = ContextInspector.oversizedFields(c, 3);
        assertEquals(1, over.size(), "only the oversized collection is reported");
        assertEquals(5, over.get("events"));
        assertFalse(over.containsKey("tags"), "under-threshold collection not flagged");
        assertFalse(over.containsKey("scalar"), "non-collection field not flagged");
    }

    @Test
    void context_inspector_is_null_safe_and_clean_when_empty() {
        assertTrue(ContextInspector.oversizedFields(null, 10).isEmpty());
        assertTrue(ContextInspector.oversizedFields(new FatCtx(), 0).isEmpty(),
            "empty collections never exceed threshold 0");
    }
}
