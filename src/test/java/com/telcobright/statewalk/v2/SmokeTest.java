package com.telcobright.statewalk.v2;

import com.telcobright.statewalk.v2.channel.TestChannel;
import com.telcobright.statewalk.v2.event.StatemachineEvent;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.registry.Registry;
import com.telcobright.statewalk.v2.registry.Statewalk;
import com.telcobright.statewalk.v2.registry.StatewalkSystem;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke test verifying the v2 framework end-to-end through the
 * {@link Statewalk} builder — the only sanctioned consumption path.
 *
 * <p>Asserts the v1 invariants (machine cannot start without registry,
 * dispatch transitions to initial state, terminal triggers ritual + pool
 * return, late events dropped, duplicate dispatch rejected) plus the new
 * v2 rules:
 * <ul>
 *   <li>Every event class must be registered with the event-type registry.</li>
 *   <li>Every state must declare a timeout — build-time enforcement.</li>
 *   <li>Builder is the only entry point: registries / channels / events
 *       wire through it.</li>
 * </ul>
 */
class SmokeTest {

    // ─── domain ───────────────────────────────────────────────────────

    record Hello() implements StatemachineEvent {}
    record Goodbye() implements StatemachineEvent {}
    record Unregistered() implements StatemachineEvent {}
    record DemoTask(String name) {}

    static class DemoContext {
        int helloCount;
        boolean goodbyeSeen;
    }

    // ─── machine ──────────────────────────────────────────────────────

    static class DemoMachine extends Machine<DemoTask, DemoContext> {
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("ACTIVE")
                .state("ACTIVE")
                    .timeout(60, TimeUnit.SECONDS, "CLOSED")
                    .stay(Hello.class, (self, e) -> {
                        DemoMachine m = (DemoMachine) self;
                        m.getContext().helloCount++;
                    })
                    .on(Goodbye.class, "CLOSED")
                .state("CLOSED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "CLOSED")  // mandatory; never fires
                    .onEntry(self -> ((DemoMachine) self).getContext().goodbyeSeen = true)
                .build();
        }
        @Override protected DemoContext createContext() { return new DemoContext(); }
    }

    // ─── registry ─────────────────────────────────────────────────────

    static class DemoRegistry extends Registry<DemoMachine, DemoContext> {
        @Override protected String getRegistryName() { return "demo"; }
        @Override protected int getMaxConcurrent() { return 100; }
        @Override protected long getGlobalTimeoutMs() { return 60_000L; }
        @Override protected DemoMachine createMachineTemplate() { return new DemoMachine(); }
    }

    // ─────────────────────────────────────────────────────────────────

    private StatewalkSystem system;
    private DemoRegistry demo;
    private TestChannel<Object, StatemachineEvent> channel;

    @BeforeEach
    void setUp() {
        demo = new DemoRegistry();
        channel = new TestChannel<>("demo-channel");
        system = Statewalk.builder()
            .registerEvent(Hello.class)
            .registerEvent(Goodbye.class)
            .registry("demo", demo, 8, 2)
            .channel("demo", channel)
            .build();
    }

    @AfterEach
    void tearDown() {
        if (system != null) system.shutdown();
    }

    // ─── tests ────────────────────────────────────────────────────────

    @Test
    void machine_cannot_run_without_registry() {
        DemoMachine m = new DemoMachine();
        IllegalStateException ex = assertThrows(IllegalStateException.class, m::start);
        assertTrue(ex.getMessage().contains("registry"));
    }

    @Test
    void dispatch_transitions_to_initial_state() {
        assertTrue(system.dispatch("demo", "req-1", new DemoTask("hello")));
        DemoMachine m = demo.getMachine("req-1");
        assertNotNull(m);
        assertEquals("ACTIVE", m.getCurrentState());
        assertFalse(m.isIdle());
        assertTrue(m.isStarted());
    }

    @Test
    void inbound_event_drives_stay_action() {
        system.dispatch("demo", "req-2", new DemoTask("loop"));
        channel.inject("req-2", new Hello());
        channel.inject("req-2", new Hello());
        channel.inject("req-2", new Hello());

        DemoMachine m = demo.getMachine("req-2");
        assertEquals(3, m.getContext().helloCount);
        assertEquals("ACTIVE", m.getCurrentState());
    }

    @Test
    void terminal_state_triggers_ritual_and_pool_return() {
        var before = demo.getPoolStatistics();
        system.dispatch("demo", "req-3", new DemoTask("done"));
        DemoMachine m = demo.getMachine("req-3");
        DemoContext ctx = m.getContext();

        channel.inject("req-3", new Goodbye());

        assertNull(demo.getMachine("req-3"));
        assertEquals(0, demo.getActiveCount());
        assertTrue(ctx.goodbyeSeen);
        assertEquals(1, demo.getTotalCompleted());

        var after = demo.getPoolStatistics();
        assertEquals(before.totalBorrowed() + 1, after.totalBorrowed());
        assertEquals(before.totalReturned() + 1, after.totalReturned());
    }

    @Test
    void late_event_after_terminal_is_silently_dropped() {
        system.dispatch("demo", "req-4", new DemoTask("late"));
        channel.inject("req-4", new Goodbye());
        assertDoesNotThrow(() -> channel.inject("req-4", new Hello()));
        assertNull(demo.getMachine("req-4"));
    }

    @Test
    void duplicate_dispatch_on_same_id_is_rejected() {
        assertTrue(system.dispatch("demo", "req-5", new DemoTask("first")));
        assertFalse(system.dispatch("demo", "req-5", new DemoTask("second")));
        assertEquals(1, demo.getActiveCount());
    }

    @Test
    void unregistered_event_throws_on_dispatch() {
        system.dispatch("demo", "req-6", new DemoTask("typed"));
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> channel.inject("req-6", new Unregistered()));
        assertTrue(ex.getMessage().contains("not registered"));
    }

    @Test
    void state_without_timeout_is_rejected_at_build_time() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            StateMap.builder()
                .initialState("X")
                .state("X")
                    .on(Hello.class, "Y")
                .state("Y")
                    .timeout(1, TimeUnit.SECONDS, "X")
                    .finalState()
                .build()
        );
        assertTrue(ex.getMessage().contains("missing a mandatory timeout"));
    }

    @Test
    void building_with_unknown_registry_for_channel_fails() {
        assertThrows(IllegalStateException.class, () ->
            Statewalk.builder()
                .registerEvent(Hello.class)
                .channel("does-not-exist", new TestChannel<>())
                .build());
    }

    @Test
    void debug_sample_rate_marks_every_nth_machine_in_debug_mode() {
        DemoRegistry r = new DemoRegistry();
        TestChannel<Object, StatemachineEvent> ch = new TestChannel<>();
        StatewalkSystem sys = Statewalk.builder()
            .registerEvent(Hello.class)
            .registerEvent(Goodbye.class)
            .registry("d", r, 16, 2, 3)   // 1 in 3 machines in debug mode
            .channel("d", ch)
            .build();
        try {
            // Dispatch counter starts at 0; counter % 3 == 0 → debug. So the
            // 1st, 4th, 7th machines run in debug; 2nd, 3rd, 5th, 6th do not.
            for (int i = 0; i < 9; i++) {
                sys.dispatch("d", "m-" + i, new DemoTask("t" + i));
            }
            assertTrue(r.getMachine("m-0").isDebugMode());
            assertFalse(r.getMachine("m-1").isDebugMode());
            assertFalse(r.getMachine("m-2").isDebugMode());
            assertTrue(r.getMachine("m-3").isDebugMode());
            assertFalse(r.getMachine("m-4").isDebugMode());
            assertFalse(r.getMachine("m-5").isDebugMode());
            assertTrue(r.getMachine("m-6").isDebugMode());
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void no_debug_sampling_when_rate_is_zero() {
        DemoRegistry r = new DemoRegistry();
        TestChannel<Object, StatemachineEvent> ch = new TestChannel<>();
        StatewalkSystem sys = Statewalk.builder()
            .registerEvent(Hello.class)
            .registerEvent(Goodbye.class)
            .registry("d", r, 16, 2)   // no debug rate → 0 → none in debug
            .channel("d", ch)
            .build();
        try {
            for (int i = 0; i < 5; i++) {
                sys.dispatch("d", "z-" + i, new DemoTask("t" + i));
                assertFalse(r.getMachine("z-" + i).isDebugMode());
            }
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void poolable_event_is_returned_to_pool_after_dispatch() {
        // Build a separate system with a poolable event registered.
        DemoRegistry r = new DemoRegistry();
        TestChannel<Object, StatemachineEvent> ch = new TestChannel<>();
        StatewalkSystem sys = Statewalk.builder()
            .registerEvent(Goodbye.class)
            .registerPoolableEvent(PoolableHello.class, PoolableHello::new, 32)
            .registry("d", r, 4, 2)
            .channel("d", ch)
            .build();
        try {
            sys.dispatch("d", "p-1", new DemoTask("p"));

            var stats0 = sys.getEventTypes().poolStatistics().get(PoolableHello.class);
            assertNotNull(stats0);
            int returnedBefore = stats0.totalReturned();

            // Borrow and fire — registry should return the event after dispatch.
            PoolableHello evt = sys.getEventTypes().borrow(PoolableHello.class);
            evt.payload = "x";
            ch.inject("p-1", evt);

            var stats1 = sys.getEventTypes().poolStatistics().get(PoolableHello.class);
            assertEquals(returnedBefore + 1, stats1.totalReturned());
        } finally {
            sys.shutdown();
        }
    }

    /** Mutable, poolable variant of Hello — exercises the event pool path. */
    static class PoolableHello implements StatemachineEvent, com.telcobright.statewalk.v2.pool.Poolable {
        String payload;
        @Override public void resetForReuse() { payload = null; }
    }
}
