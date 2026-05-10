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

import java.time.Duration;
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

    private static final Duration AWAIT = Duration.ofSeconds(2);

    private StatewalkSystem system;
    private DemoRegistry demo;
    private TestChannel<Object, StatemachineEvent> channel;

    /** Drain the framework's executor so test assertions see post-dispatch state. */
    private void await() throws InterruptedException {
        if (system != null) assertTrue(system.awaitIdle(AWAIT), "executor did not drain in " + AWAIT);
    }

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
    void dispatch_transitions_to_initial_state() throws InterruptedException {
        assertTrue(system.dispatch("demo", "req-1", new DemoTask("hello")));
        await();
        DemoMachine m = demo.getMachine("req-1");
        assertNotNull(m);
        assertEquals("ACTIVE", m.getCurrentState());
        assertFalse(m.isIdle());
        assertTrue(m.isStarted());
    }

    @Test
    void inbound_event_drives_stay_action() throws InterruptedException {
        system.dispatch("demo", "req-2", new DemoTask("loop"));
        channel.inject("req-2", new Hello());
        channel.inject("req-2", new Hello());
        channel.inject("req-2", new Hello());
        await();

        DemoMachine m = demo.getMachine("req-2");
        assertEquals(3, m.getContext().helloCount);
        assertEquals("ACTIVE", m.getCurrentState());
    }

    @Test
    void terminal_state_triggers_ritual_and_pool_return() throws InterruptedException {
        var before = demo.getPoolStatistics();
        system.dispatch("demo", "req-3", new DemoTask("done"));
        await();
        DemoMachine m = demo.getMachine("req-3");
        DemoContext ctx = m.getContext();

        channel.inject("req-3", new Goodbye());
        await();

        assertNull(demo.getMachine("req-3"));
        assertEquals(0, demo.getActiveCount());
        assertTrue(ctx.goodbyeSeen);
        assertEquals(1, demo.getTotalCompleted());

        var after = demo.getPoolStatistics();
        assertEquals(before.totalBorrowed() + 1, after.totalBorrowed());
        assertEquals(before.totalReturned() + 1, after.totalReturned());
    }

    @Test
    void late_event_after_terminal_is_silently_dropped() throws InterruptedException {
        system.dispatch("demo", "req-4", new DemoTask("late"));
        channel.inject("req-4", new Goodbye());
        await();
        assertDoesNotThrow(() -> channel.inject("req-4", new Hello()));
        await();
        assertNull(demo.getMachine("req-4"));
    }

    @Test
    void duplicate_dispatch_on_same_id_is_rejected() throws InterruptedException {
        assertTrue(system.dispatch("demo", "req-5", new DemoTask("first")));
        assertFalse(system.dispatch("demo", "req-5", new DemoTask("second")));
        await();
        assertEquals(1, demo.getActiveCount());
    }

    @Test
    void unregistered_event_throws_on_dispatch() throws InterruptedException {
        system.dispatch("demo", "req-6", new DemoTask("typed"));
        await();
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
    void debug_sample_rate_marks_every_nth_machine_in_debug_mode() throws InterruptedException {
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
            sys.awaitIdle(AWAIT);
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
    void no_debug_sampling_when_rate_is_zero() throws InterruptedException {
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
            }
            sys.awaitIdle(AWAIT);
            for (int i = 0; i < 5; i++) {
                assertFalse(r.getMachine("z-" + i).isDebugMode());
            }
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void poolable_event_is_returned_to_pool_after_dispatch() throws InterruptedException {
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
            sys.awaitIdle(AWAIT);

            var stats0 = sys.getEventTypes().poolStatistics().get(PoolableHello.class);
            assertNotNull(stats0);
            int returnedBefore = stats0.totalReturned();

            // Borrow and fire — registry should return the event after dispatch.
            PoolableHello evt = sys.getEventTypes().borrow(PoolableHello.class);
            evt.payload = "x";
            ch.inject("p-1", evt);
            sys.awaitIdle(AWAIT);

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

    // ── new feature tests (offline + validation) ──────────────────────

    @Test
    void offline_state_and_final_state_are_mutually_exclusive_at_build_time() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            StateMap.builder()
                .initialState("A")
                .state("A")
                    .timeout(1, TimeUnit.SECONDS, "Z")
                    .on(Hello.class, "Z")
                .state("Z")
                    .finalState()
                    .offline()                    // illegal combo
                    .timeout(1, TimeUnit.SECONDS, "Z")
                .build());
        assertTrue(ex.getMessage().contains("cannot be both final and offline"));
    }

    @Test
    void offline_state_without_persistence_is_rejected_by_builder() {
        // A registry whose machine has an offline state — but no persistence configured.
        class OfflineMachine extends Machine<DemoTask, DemoContext> {
            @Override protected StateMap defineStates() {
                return StateMap.builder()
                    .initialState("WAIT")
                    .state("WAIT")
                        .timeout(1, TimeUnit.SECONDS, "DONE")
                        .offline()
                        .on(Hello.class, "DONE")
                    .state("DONE")
                        .finalState()
                        .timeout(1, TimeUnit.SECONDS, "DONE")
                    .build();
            }
            @Override protected DemoContext createContext() { return new DemoContext(); }
        }
        class OfflineRegistry extends Registry<OfflineMachine, DemoContext> {
            @Override protected String getRegistryName()    { return "off"; }
            @Override protected int    getMaxConcurrent()   { return 16; }
            @Override protected long   getGlobalTimeoutMs() { return 60_000L; }
            @Override protected OfflineMachine createMachineTemplate() { return new OfflineMachine(); }
        }

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Statewalk.builder()
                .registerEvent(Hello.class)
                .registerEvent(Goodbye.class)
                .registry("off", new OfflineRegistry(), 8, 2)
                .build());
        assertTrue(ex.getMessage().contains("offline"));
        assertTrue(ex.getMessage().contains("persistence"));
    }

    @Test
    void global_timeout_transitions_to_configured_final_state() throws InterruptedException {
        // A demo machine whose ACTIVE state has a 30s state-timeout to CLOSED
        // (which is final). We configure a SHORTER (200 ms) GLOBAL timeout
        // through the builder, also targeting CLOSED. The global timeout
        // should fire first and transition the machine through CLOSED → reset.
        DemoRegistry r = new DemoRegistry();
        TestChannel<Object, StatemachineEvent> ch = new TestChannel<>();
        StatewalkSystem sys = Statewalk.builder()
            .registerEvent(Hello.class)
            .registerEvent(Goodbye.class)
            .registry("d", r, 8, 2)
            .channel("d", ch)
            .globalTimeout("d", Duration.ofMillis(200), "CLOSED")
            .build();
        try {
            sys.dispatch("d", "gt-1", new DemoTask("timed-out"));
            sys.awaitIdle(AWAIT);
            assertNotNull(r.getMachine("gt-1"));

            // Wait past the global timeout
            Thread.sleep(400);
            sys.awaitIdle(AWAIT);

            // Machine transitioned to CLOSED (final) → ritual ran → gone.
            assertNull(r.getMachine("gt-1"),
                "machine should have terminated via global timeout → CLOSED");
            assertEquals(1, r.getTotalCompleted());
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void global_timeout_with_non_final_target_is_rejected_at_build() {
        DemoRegistry r = new DemoRegistry();
        // ACTIVE is NOT a final state in DemoMachine — using it as the
        // global-timeout target must fail validation at build().
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Statewalk.builder()
                .registerEvent(Hello.class)
                .registerEvent(Goodbye.class)
                .registry("d", r, 8, 2)
                .globalTimeout("d", Duration.ofSeconds(1), "ACTIVE")
                .channel("d", new TestChannel<>())
                .build());
        assertTrue(ex.getMessage().contains("not a final state"));
    }

    @Test
    void global_timeout_with_unknown_target_is_rejected_at_build() {
        DemoRegistry r = new DemoRegistry();
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Statewalk.builder()
                .registerEvent(Hello.class)
                .registerEvent(Goodbye.class)
                .registry("d", r, 8, 2)
                .globalTimeout("d", Duration.ofSeconds(1), "NOPE")
                .channel("d", new TestChannel<>())
                .build());
        assertTrue(ex.getMessage().contains("not declared"));
    }

    @Test
    void registry_init_validates_pool_size_and_concurrent() {
        class BadRegistry extends Registry<DemoMachine, DemoContext> {
            @Override protected String getRegistryName()    { return "bad"; }
            @Override protected int    getMaxConcurrent()   { return 0; }   // illegal
            @Override protected long   getGlobalTimeoutMs() { return 60_000L; }
            @Override protected DemoMachine createMachineTemplate() { return new DemoMachine(); }
        }
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Statewalk.builder()
                .registerEvent(Hello.class)
                .registerEvent(Goodbye.class)
                .registry("bad", new BadRegistry(), 8, 2)
                .build());
        assertTrue(ex.getMessage().contains("getMaxConcurrent"));
    }
}
