package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.machine.Machine;
import com.telcobright.statewalk.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.event.StatemachineEvent;
import com.telcobright.statewalk.state.StateMap;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Persistence feature behaviours requested for the generic registry:
 * <ul>
 *   <li>persistence optional + builder-configurable (covered by other tests);</li>
 *   <li>context persisted on every transition AND on {@code .stay()};</li>
 *   <li>{@code .offline()} states suspend the machine (evicted from the registry,
 *       snapshot retained) and rehydrate on the next inbound event;</li>
 *   <li>offline requires persistence (build-time rejection);</li>
 *   <li>startup loads ALL unfinished machines and resumes them — node failover;</li>
 *   <li>multiple final states are reachable via the builder.</li>
 * </ul>
 *
 * <p>This test is in the {@code flat} package so it can use the package-private
 * {@code findInternal} to introspect a live cell's context.
 */
@SuppressWarnings("unchecked")
class PersistenceFeaturesTest {

    // ── events ─────────────────────────────────────────────────────
    public record Park(String u)    implements StatemachineEvent {}
    public record Touch(String u)   implements StatemachineEvent {}
    public record Resume(String u)  implements StatemachineEvent {}
    public record Succeed(String u) implements StatemachineEvent {}
    public record Fail(String u)    implements StatemachineEvent {}

    public static class SessCtx { public int touches; public String mark; public SessCtx() {} }

    static final AtomicInteger SUCCESSES = new AtomicInteger();
    static final AtomicInteger FAILURES  = new AtomicInteger();

    /** A session machine exercising stay, offline, and TWO final states. */
    static StateMap sessionStates() {
        return StateMap.builder()
            .initialState("ACTIVE")
            .state("ACTIVE")
                .interim()
                .timeout(1, TimeUnit.HOURS, "EXPIRED")
                .stay(Touch.class, (self, e) -> ((Machine<SessCtx>) self).getContext().touches++)
                .on(Park.class,    "PARKED")
                .on(Succeed.class, "SUCCESS")
                .on(Fail.class,    "FAILED")
            .state("PARKED")
                .interim().offline()
                .timeout(1, TimeUnit.HOURS, "EXPIRED")
                .on(Resume.class, "ACTIVE")
            .state("SUCCESS")
                .finalState().timeout(1, TimeUnit.SECONDS, "SUCCESS")
                .onEntry(self -> SUCCESSES.incrementAndGet())
            .state("FAILED")
                .finalState().timeout(1, TimeUnit.SECONDS, "FAILED")
                .onEntry(self -> FAILURES.incrementAndGet())
            .state("EXPIRED")
                .finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
            .build();
    }

    static SupervisorSpec<SessCtx> sessionSpec() {
        return SupervisorSpec.<SessCtx>builder()
            .name("Session").contextFactory(SessCtx::new).stateMap(sessionStates())
            .routes(r -> {
                r.selfHandle(Park.class);  r.selfHandle(Touch.class);  r.selfHandle(Resume.class);
                r.selfHandle(Succeed.class); r.selfHandle(Fail.class);
            })
            .build();
    }

    private static StatemachineRegistry<SessCtx> build(String name, InMemoryPersistenceProvider store) {
        return StatemachineRegistry.<SessCtx>builder(name)
            .supervisor(sessionSpec(), 16)
            .persistence(store).rehydrate(true).threads(4)
            .build();
    }

    // ── .stay() is persisted, and survives a node failover ─────────────
    @Test
    void stay_event_is_persisted_and_survives_failover() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<SessCtx> a = build("sess", store);
        String id = "s-stay";
        a.dispatch(id, new SessCtx());
        assertTrue(a.awaitIdle(2, TimeUnit.SECONDS));
        a.onInboundEvent(id, new Touch(id));
        a.onInboundEvent(id, new Touch(id));
        a.onInboundEvent(id, new Touch(id));
        assertTrue(a.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(3, ((SessCtx) a.findInternal(id, "Session").getContext()).touches,
            "stays mutated the live context");

        // Node A "dies" (we do NOT shut it down — that would delete snapshots).
        // A fresh node B takes over the same store and must resume the request
        // with the .stay() mutations intact.
        StatemachineRegistry<SessCtx> b = build("sess", store);
        try {
            assertTrue(b.awaitIdle(3, TimeUnit.SECONDS));
            Machine<?> resumed = b.findInternal(id, "Session");
            assertNotNull(resumed, "node B resumed the request from the store");
            assertEquals(3, ((SessCtx) resumed.getContext()).touches,
                "the .stay() mutations were persisted and restored on the new node");
        } finally { b.shutdown(); a.shutdown(); }
    }

    // ── .offline() suspends + rehydrates on the next event ─────────────
    @Test
    void offline_state_suspends_and_rehydrates_on_event() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<SessCtx> reg = build("sess-off", store);
        try {
            String id = "s-off";
            reg.dispatch(id, new SessCtx());
            assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));
            assertEquals(1, reg.activeCellCount());

            reg.onInboundEvent(id, new Park(id));               // ACTIVE → PARKED (offline) → suspend
            assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));
            assertEquals(0, reg.activeCellCount(), "offline state evicted the machine from the registry");
            assertFalse(reg.hasAny(id));
            assertEquals(1, store.size(), "snapshot retained as the resume point");

            reg.onInboundEvent(id, new Resume(id));             // event for suspended id → rehydrate, then Resume
            assertTrue(reg.awaitIdle(3, TimeUnit.SECONDS));
            Machine<?> m = reg.findInternal(id, "Session");
            assertNotNull(m, "rehydrated on inbound event");
            assertEquals("ACTIVE", m.getCurrentState(), "Resume drove it out of the offline state");
        } finally { reg.shutdown(); }
    }

    // ── offline without persistence is rejected at build ───────────────
    @Test
    void offline_state_without_persistence_is_rejected_at_build() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            StatemachineRegistry.<SessCtx>builder("sess-nopersist").supervisor(sessionSpec(), 4).build());
        assertTrue(ex.getMessage().toLowerCase().contains("offline"), ex.getMessage());
    }

    // ── node failover: a fresh node resumes ALL unfinished requests ────
    @Test
    void failover_resumes_all_unfinished_on_a_fresh_node() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<SessCtx> nodeA = build("calls", store);
        final int N = 25;
        for (int i = 0; i < N; i++) nodeA.dispatch("call-" + i, new SessCtx());
        assertTrue(nodeA.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(N, nodeA.activeCellCount(), "all in-flight calls active on node A");
        assertEquals(N, store.size(), "every in-flight call persisted to the shared store");

        // Node A crashes (NOT shut down). External supervisor launches node B on the same store.
        StatemachineRegistry<SessCtx> nodeB = build("calls", store);
        try {
            assertTrue(nodeB.awaitIdle(5, TimeUnit.SECONDS));
            assertEquals(N, nodeB.activeCellCount(), "fresh node resumed every in-flight call from the store");
            for (int i = 0; i < N; i++) assertTrue(nodeB.hasAny("call-" + i), "call-" + i + " resumed on node B");

            // and node B can keep driving a resumed call to completion
            nodeB.onInboundEvent("call-0", new Succeed("call-0"));
            assertTrue(nodeB.awaitIdle(3, TimeUnit.SECONDS));
            assertFalse(nodeB.hasAny("call-0"), "resumed call completed on the new node");
        } finally { nodeB.shutdown(); nodeA.shutdown(); }
    }

    // ── multiple final states are both reachable via the builder ───────
    @Test
    void multiple_final_states_both_reachable() throws Exception {
        SUCCESSES.set(0); FAILURES.set(0);
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<SessCtx> reg = build("sess-final", store);
        try {
            reg.dispatch("ok", new SessCtx());
            reg.dispatch("bad", new SessCtx());
            assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));
            reg.onInboundEvent("ok",  new Succeed("ok"));
            reg.onInboundEvent("bad", new Fail("bad"));
            assertTrue(reg.awaitIdle(3, TimeUnit.SECONDS));

            assertFalse(reg.hasAny("ok"));
            assertFalse(reg.hasAny("bad"));
            assertEquals(1, SUCCESSES.get(), "SUCCESS final reached");
            assertEquals(1, FAILURES.get(),  "FAILED final reached");
            assertEquals(0, reg.activeCellCount());
            assertEquals(0, store.size(), "both terminals purged their snapshots");
        } finally { reg.shutdown(); }
    }
}
