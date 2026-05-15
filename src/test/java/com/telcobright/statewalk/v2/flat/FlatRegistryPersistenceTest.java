package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.v2.persistence.MachineSnapshot;
import com.telcobright.statewalk.v2.persistence.SnapshotSerializer;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Persistence + rehydration + first-event semantics for flat.Registry.
 *
 * <p>Six scenarios:
 * <ol>
 *   <li>Save on every state transition (cell exists in store after each .on()).</li>
 *   <li>Delete on cell termination (terminal state purges the row).</li>
 *   <li>Volatile loader fires on creation.</li>
 *   <li>Volatile loader fires on rehydration (cross-process restart scenario).</li>
 *   <li>Cross-cell rehydration restores supervisor + child from seeded snapshots.</li>
 *   <li>First-event auto-creation builds context from the inbound event.</li>
 *   <li>Unknown-id non-first event throws when neither hook nor rehydration is configured.</li>
 * </ol>
 *
 * Contexts and machines defined here are {@code public static} so Jackson can
 * round-trip them through the {@link SnapshotSerializer}.
 */
class FlatRegistryPersistenceTest {

    // ── events ────────────────────────────────────────────────────────

    public record Advance(String uuid) implements StatemachineEvent {}
    public record Finish(String uuid)  implements StatemachineEvent {}
    public record InitCall(String uuid, String caller) implements StatemachineEvent {
        @Override public boolean isFirst() { return true; }
    }
    public record UnknownEvent(String uuid) implements StatemachineEvent {}

    // ── persistable context (public static, Jackson-friendly) ─────────

    public static class TestCtx {
        public String caller;
        public int    transitionCount;
        public TestCtx() {}
    }

    // ── supervisor — simple linear state graph ────────────────────────

    public static class TestSupervisor extends Supervisor<TestCtx> {
        @Override
        protected void defineRoutes(InternalEventResolver r) {
            r.selfHandle(Advance.class);
            r.selfHandle(Finish.class);
            r.selfHandle(InitCall.class);
        }

        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("RUNNING")
                .state("RUNNING")
                    .interim()
                    .timeout(10, TimeUnit.SECONDS, "DONE")
                    .onEntry(self -> {
                        TestCtx c = ((TestSupervisor) self).getContext();
                        c.transitionCount++;
                    })
                    .on(Advance.class, "ADVANCED")
                    .on(Finish.class,  "DONE")
                .state("ADVANCED")
                    .interim()
                    .timeout(10, TimeUnit.SECONDS, "DONE")
                    .onEntry(self -> {
                        TestCtx c = ((TestSupervisor) self).getContext();
                        c.transitionCount++;
                    })
                    .on(Finish.class, "DONE")
                .state("DONE")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "DONE")
                .build();
        }

        @Override protected TestCtx createContext() { return new TestCtx(); }
    }

    // ── child machine for cross-cell rehydration test ─────────────────

    public static class ChildCtx {
        public int wakeups;
        public ChildCtx() {}
    }

    public static class TestChild extends Machine<ChildCtx> {
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("WORKING")
                .state("WORKING")
                    .interim()
                    .timeout(10, TimeUnit.SECONDS, "DONE")
                    .on(Finish.class, "DONE")
                .state("DONE")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "DONE")
                .build();
        }
        @Override protected ChildCtx createContext() { return new ChildCtx(); }
    }

    // ── fixtures ──────────────────────────────────────────────────────

    private Registry reg;

    @AfterEach
    void tearDown() {
        if (reg != null) reg.shutdown();
    }

    // ─────────────────────────────────────────────────────────────────
    // (1) save on every transition
    // ─────────────────────────────────────────────────────────────────

    @Test
    void persistence_saves_on_every_state_transition() throws InterruptedException {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        reg = Registry.builder("p-save")
            .supervisor(TestSupervisor.class, TestSupervisor::new, 2)
            .persistence(store)
            .threads(2)
            .build();

        String id = "uuid-1";
        TestCtx ctx = new TestCtx();
        ctx.caller = "1234";
        reg.dispatch(id, ctx);
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        // initial RUNNING transition already persisted
        Optional<MachineSnapshot> afterInit = store.load(id, "p-save");
        assertTrue(afterInit.isPresent(), "snapshot saved on initial transition");
        assertEquals("RUNNING", afterInit.get().currentState());
        assertNotNull(afterInit.get().contextJsonBase64(), "context serialized");

        reg.onInboundEvent(id, new Advance(id));
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        Optional<MachineSnapshot> afterAdvance = store.load(id, "p-save");
        assertTrue(afterAdvance.isPresent());
        assertEquals("ADVANCED", afterAdvance.get().currentState());
    }

    // ─────────────────────────────────────────────────────────────────
    // (2) delete on cell termination
    // ─────────────────────────────────────────────────────────────────

    @Test
    void persistence_deletes_on_cell_termination() throws InterruptedException {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        reg = Registry.builder("p-del")
            .supervisor(TestSupervisor.class, TestSupervisor::new, 2)
            .persistence(store)
            .threads(2)
            .build();

        String id = "uuid-2";
        reg.dispatch(id, new TestCtx());
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(1, store.size(), "row exists while alive");

        reg.onInboundEvent(id, new Finish(id));
        assertTrue(reg.awaitIdle(3, TimeUnit.SECONDS));

        // DONE is final → onCellTerminated → persistence.delete
        assertEquals(0, store.size(), "snapshot deleted on terminal");
        assertTrue(store.load(id, "p-del").isEmpty());
    }

    // ─────────────────────────────────────────────────────────────────
    // (3) volatile loader fires on creation
    // ─────────────────────────────────────────────────────────────────

    @Test
    void volatile_loader_fires_on_creation() throws InterruptedException {
        AtomicInteger creates = new AtomicInteger();
        reg = Registry.builder("p-vol")
            .supervisor(TestSupervisor.class, TestSupervisor::new, 2)
            .volatileLoader(TestSupervisor.class, m -> {
                creates.incrementAndGet();
                return "vol-" + m.getMachineId();
            })
            .threads(2)
            .build();

        reg.dispatch("uuid-vol-a", new TestCtx());
        reg.dispatch("uuid-vol-b", new TestCtx());
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        assertEquals(2, creates.get(), "loader fires once per machine creation");

        Machine<?> a = reg.findInternal("uuid-vol-a", TestSupervisor.class);
        assertNotNull(a);
        assertEquals("vol-uuid-vol-a", a.getVolatileContext());
    }

    // ─────────────────────────────────────────────────────────────────
    // (4) volatile loader fires on rehydration
    //     Seed a snapshot directly into the store, then build a fresh
    //     registry pointing at it, then deliver an inbound event for
    //     that id — registry rehydrates from the snapshot and the
    //     volatile loader fires.
    // ─────────────────────────────────────────────────────────────────

    @Test
    void volatile_loader_fires_on_rehydration() throws InterruptedException {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        String id = "uuid-rehydrate";

        // Pretend a previous JVM left a RUNNING snapshot behind.
        TestCtx priorCtx = new TestCtx();
        priorCtx.caller = "9999";
        priorCtx.transitionCount = 7;
        String b64 = SnapshotSerializer.contextToBase64Json(priorCtx);
        store.save(new MachineSnapshot(
            id, "p-vol-rehy", "RUNNING", TestCtx.class.getName(), b64,
            System.currentTimeMillis(), "DONE",
            System.currentTimeMillis() + 60_000L));

        AtomicInteger loads = new AtomicInteger();
        reg = Registry.builder("p-vol-rehy")
            .supervisor(TestSupervisor.class, TestSupervisor::new, 2)
            .persistence(store)
            .rehydrate(true)
            .volatileLoader(TestSupervisor.class, m -> {
                loads.incrementAndGet();
                return "rehydrated-vol";
            })
            .threads(2)
            .build();

        // Inbound event for an unknown-to-this-registry id; rehydration kicks in.
        reg.onInboundEvent(id, new Advance(id));
        assertTrue(reg.awaitIdle(3, TimeUnit.SECONDS));

        assertEquals(1, loads.get(), "volatile loader fired during rehydration");
        Machine<?> sup = reg.findInternal(id, TestSupervisor.class);
        assertNotNull(sup, "supervisor restored from snapshot");
        assertEquals("rehydrated-vol", sup.getVolatileContext());
    }

    // ─────────────────────────────────────────────────────────────────
    // (5) cross-cell rehydration — supervisor + child both restored
    // ─────────────────────────────────────────────────────────────────

    @Test
    void cross_cell_rehydration_restores_supervisor_and_child() throws InterruptedException {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        String id = "uuid-multi";
        long now = System.currentTimeMillis();

        // Seed supervisor + child snapshots, mimicking what a crashed JVM left.
        TestCtx supCtx = new TestCtx();
        supCtx.caller = "5555";
        store.save(new MachineSnapshot(
            id, "p-multi", "ADVANCED",
            TestCtx.class.getName(), SnapshotSerializer.contextToBase64Json(supCtx),
            now, "DONE", now + 60_000L));

        ChildCtx childCtx = new ChildCtx();
        childCtx.wakeups = 3;
        String childId = id + Registry.CHILD_ID_SEPARATOR + "TestChild";
        store.save(new MachineSnapshot(
            childId, "p-multi", "WORKING",
            ChildCtx.class.getName(), SnapshotSerializer.contextToBase64Json(childCtx),
            now, "DONE", now + 60_000L));

        reg = Registry.builder("p-multi")
            .supervisor(TestSupervisor.class, TestSupervisor::new, 2)
            .child(TestChild.class, TestChild::new, 2)
            .persistence(store)
            .rehydrate(true)
            .threads(2)
            .build();

        // Trigger rehydration via an inbound event.
        reg.onInboundEvent(id, new Advance(id));
        assertTrue(reg.awaitIdle(3, TimeUnit.SECONDS));

        assertEquals(2, reg.activeCellCount(), "supervisor + child restored");
        Machine<?> sup = reg.findInternal(id, TestSupervisor.class);
        Machine<?> child = reg.findInternal(id, TestChild.class);
        assertNotNull(sup);
        assertNotNull(child);
        TestCtx supCtxRestored = (TestCtx) sup.getContext();
        assertEquals("5555", supCtxRestored.caller, "context survived rehydration");
        ChildCtx childCtxRestored = (ChildCtx) child.getContext();
        assertEquals(3, childCtxRestored.wakeups);
    }

    // ─────────────────────────────────────────────────────────────────
    // (6) first-event auto-creation
    // ─────────────────────────────────────────────────────────────────

    @Test
    void first_event_auto_creates_supervisor() throws InterruptedException {
        reg = Registry.builder("p-first")
            .supervisor(TestSupervisor.class, TestSupervisor::new, 2)
            .createFromFirstEvent(ev -> {
                if (ev instanceof InitCall init) {
                    TestCtx c = new TestCtx();
                    c.caller = init.caller();
                    return c;
                }
                return null;
            })
            .threads(2)
            .build();

        String id = "uuid-first";
        // No prior dispatch — the inbound first-event spawns the supervisor.
        reg.onInboundEvent(id, new InitCall(id, "777"));
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        Machine<?> sup = reg.findInternal(id, TestSupervisor.class);
        assertNotNull(sup, "supervisor auto-created from first event");
        TestCtx ctx = (TestCtx) sup.getContext();
        assertEquals("777", ctx.caller, "hook populated context from event");
    }

    // ─────────────────────────────────────────────────────────────────
    // (7) unknown-id non-first event throws with no recovery hooks
    // ─────────────────────────────────────────────────────────────────

    @Test
    void unknown_id_non_first_event_throws() {
        reg = Registry.builder("p-throw")
            .supervisor(TestSupervisor.class, TestSupervisor::new, 2)
            .threads(2)
            .build();

        assertThrows(IllegalStateException.class,
            () -> reg.onInboundEvent("nobody-home", new UnknownEvent("nobody-home")),
            "no supervisor, not first, no rehydrate → throw");
    }
}
