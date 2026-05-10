package com.telcobright.statewalk.v2;

import com.telcobright.statewalk.v2.channel.TestChannel;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.v2.persistence.MachineSnapshot;
import com.telcobright.statewalk.v2.registry.Registry;
import com.telcobright.statewalk.v2.registry.Statewalk;
import com.telcobright.statewalk.v2.registry.StatewalkSystem;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Persistence + rehydration behaviour. Covers:
 * <ul>
 *   <li>Save on every transition while persistence is enabled.</li>
 *   <li>Snapshot delete on terminal.</li>
 *   <li>Rehydration into saved state without replaying entry action.</li>
 *   <li>Rehydration with elapsed timeout — runs exit, then transitions to
 *       timeout target (final state), entry of target runs, terminates.</li>
 *   <li>Throw when no provider + non-first event for unknown id.</li>
 *   <li>{@code isFirst()} event creates a fresh machine.</li>
 *   <li>Builder rejects {@code rehydrate(true)} without persistence.</li>
 *   <li>{@code StateMap.builder()} rejects timeout target that isn't final.</li>
 * </ul>
 */
class PersistenceTest {

    // ── domain ────────────────────────────────────────────────────────

    public record StartCall(String calledNumber) implements StatemachineEvent {
        @Override public boolean isFirst() { return true; }
    }
    public record Answered() implements StatemachineEvent {}
    public record Hangup() implements StatemachineEvent {}

    /** Public so Jackson can construct it via no-arg ctor + setters. */
    public static class CallContext {
        public String calledNumber;
        public int answerCount;
        public boolean hangupSeen;
        public CallContext() {}
    }

    public record CallTask(String calledNumber) {}

    // ── machine ───────────────────────────────────────────────────────

    static final AtomicInteger entryActionRuns = new AtomicInteger(0);
    static final AtomicInteger exitActionRuns = new AtomicInteger(0);

    static class CallMachine extends Machine<CallTask, CallContext> {
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("RINGING")
                .state("RINGING")
                    .interim()
                    .onEntry(self -> {
                        entryActionRuns.incrementAndGet();
                        CallMachine m = (CallMachine) self;
                        if (m.getPersistingEntity() != null) {
                            m.getContext().calledNumber = m.getPersistingEntity().calledNumber();
                        }
                    })
                    .onExit(self -> exitActionRuns.incrementAndGet())
                    .timeout(60, TimeUnit.SECONDS, "FAILED")
                    .on(Answered.class, "ANSWERED")
                    .on(Hangup.class, "FAILED")
                .state("ANSWERED")
                    .interim()
                    .onEntry(self -> {
                        entryActionRuns.incrementAndGet();
                        ((CallMachine) self).getContext().answerCount++;
                    })
                    .onExit(self -> exitActionRuns.incrementAndGet())
                    .timeout(2, TimeUnit.HOURS, "FAILED")
                    .on(Hangup.class, "COMPLETED")
                .state("COMPLETED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "COMPLETED")  // mandatory; never fires
                    .onEntry(self -> {
                        entryActionRuns.incrementAndGet();
                        ((CallMachine) self).getContext().hangupSeen = true;
                    })
                .state("FAILED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "FAILED")
                    .onEntry(self -> entryActionRuns.incrementAndGet())
                .build();
        }

        @Override protected CallContext createContext() { return new CallContext(); }
    }

    static class CallRegistry extends Registry<CallMachine, CallContext> {
        @Override protected String getRegistryName() { return "call"; }
        @Override protected int getMaxConcurrent() { return 100; }
        @Override protected long getGlobalTimeoutMs() { return 60_000L; }
        @Override protected CallMachine createMachineTemplate() { return new CallMachine(); }
        @Override
        protected Object createTaskFromFirstEvent(String requestId, StatemachineEvent firstEvent) {
            if (firstEvent instanceof StartCall sc) return new CallTask(sc.calledNumber());
            return super.createTaskFromFirstEvent(requestId, firstEvent);
        }
    }

    // ── fixture ───────────────────────────────────────────────────────

    private InMemoryPersistenceProvider provider;
    private CallRegistry callReg;
    private TestChannel<Object, StatemachineEvent> channel;

    @BeforeEach
    void setUp() {
        provider = new InMemoryPersistenceProvider();
        callReg = new CallRegistry();
        channel = new TestChannel<>("call-channel");
        entryActionRuns.set(0);
        exitActionRuns.set(0);
    }

    private StatewalkSystem buildSystem(boolean rehydrate) {
        return Statewalk.builder()
            .registerEvent(StartCall.class)
            .registerEvent(Answered.class)
            .registerEvent(Hangup.class)
            .persistence(provider)
            .rehydrate(rehydrate)
            .registry("call", callReg, 16, 2)
            .channel("call", channel)
            .build();
    }

    @AfterEach
    void tearDown() {
        // Each test makes its own system; nothing to do here.
    }

    // ── tests ─────────────────────────────────────────────────────────

    private static final Duration AWAIT = Duration.ofSeconds(2);

    @Test
    void persistence_saves_on_every_transition() throws InterruptedException {
        var sys = buildSystem(false);
        try {
            sys.dispatch("call", "c-1", new CallTask("+880x"));
            sys.awaitIdle(AWAIT);
            // After dispatch: machine in RINGING. Snapshot saved.
            assertEquals(1, provider.size());
            MachineSnapshot s1 = provider.load("c-1").orElseThrow();
            assertEquals("RINGING", s1.currentState());
            assertEquals("FAILED", s1.timeoutTargetState());
            assertNotNull(s1.contextJsonBase64());

            channel.inject("c-1", new Answered());
            sys.awaitIdle(AWAIT);
            // Now in ANSWERED. New snapshot saved.
            MachineSnapshot s2 = provider.load("c-1").orElseThrow();
            assertEquals("ANSWERED", s2.currentState());
            assertTrue(s2.savedAtMs() >= s1.savedAtMs());
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void terminal_state_deletes_the_snapshot() throws InterruptedException {
        var sys = buildSystem(false);
        try {
            sys.dispatch("call", "c-2", new CallTask("+880x"));
            channel.inject("c-2", new Answered());
            sys.awaitIdle(AWAIT);
            assertTrue(provider.load("c-2").isPresent());

            channel.inject("c-2", new Hangup());
            sys.awaitIdle(AWAIT);
            // COMPLETED is final → ritual deletes the snapshot.
            assertTrue(provider.load("c-2").isEmpty());
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void rehydration_skips_entry_action_for_saved_state() throws InterruptedException {
        long now = System.currentTimeMillis();
        long deadlineMs = now + 60 * 60_000;
        CallContext ctx = new CallContext();
        ctx.calledNumber = "+880x";
        ctx.answerCount = 1;
        String b64 = com.telcobright.statewalk.v2.persistence.SnapshotSerializer.contextToBase64Json(ctx);
        provider.save(new MachineSnapshot(
            "c-3", "call", "ANSWERED", CallContext.class.getName(), b64,
            now, "FAILED", deadlineMs));

        var sys = buildSystem(true);
        try {
            entryActionRuns.set(0);
            exitActionRuns.set(0);

            channel.inject("c-3", new Hangup());
            sys.awaitIdle(AWAIT);

            assertNull(callReg.getMachine("c-3"));
            assertTrue(provider.load("c-3").isEmpty());
            assertEquals(1, entryActionRuns.get(),
                "Only COMPLETED.onEntry should run; ANSWERED.onEntry must be skipped on rehydrate");
            assertEquals(1, exitActionRuns.get(),
                "ANSWERED.onExit runs once when transitioning to COMPLETED");
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void rehydration_with_elapsed_timeout_runs_exit_then_target() throws InterruptedException {
        long now = System.currentTimeMillis();
        long elapsedDeadlineMs = now - 1_000;
        CallContext ctx = new CallContext();
        ctx.calledNumber = "+880x";
        String b64 = com.telcobright.statewalk.v2.persistence.SnapshotSerializer.contextToBase64Json(ctx);
        provider.save(new MachineSnapshot(
            "c-4", "call", "RINGING", CallContext.class.getName(), b64,
            now - 60_000, "FAILED", elapsedDeadlineMs));

        var sys = buildSystem(true);
        try {
            entryActionRuns.set(0);
            exitActionRuns.set(0);

            channel.inject("c-4", new Hangup());
            sys.awaitIdle(AWAIT);

            assertNull(callReg.getMachine("c-4"));
            assertTrue(provider.load("c-4").isEmpty());
            assertEquals(1, exitActionRuns.get(), "RINGING.onExit during fired-timeout transition");
            assertEquals(1, entryActionRuns.get(), "FAILED.onEntry on terminal arrival");
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void unknown_id_with_no_persistence_throws() {
        var sys = Statewalk.builder()
            .registerEvent(StartCall.class)
            .registerEvent(Answered.class)
            .registerEvent(Hangup.class)
            // no persistence, no rehydrate
            .registry("call", callReg, 16, 2)
            .channel("call", channel)
            .build();
        try {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> channel.inject("c-5", new Answered()));
            assertTrue(ex.getMessage().contains("rehydration is not configured"));
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void unknown_id_with_persistence_but_no_rehydrate_throws() {
        var sys = buildSystem(false);  // persistence on, rehydrate off
        try {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> channel.inject("c-6", new Answered()));
            assertTrue(ex.getMessage().contains("rehydration is disabled"));
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void snapshot_class_cache_auto_warms_on_save() throws InterruptedException {
        // Clear cache so we can observe the warm-up cleanly.
        com.telcobright.statewalk.v2.persistence.SnapshotSerializer.clearClassCache();
        int sizeBefore = com.telcobright.statewalk.v2.persistence.SnapshotSerializer.classCacheSize();
        assertEquals(0, sizeBefore);

        var sys = buildSystem(false);
        try {
            sys.dispatch("call", "warm-1", new CallTask("+880x"));
            sys.awaitIdle(AWAIT);
            // First save populated the cache as a side effect.
            assertTrue(com.telcobright.statewalk.v2.persistence.SnapshotSerializer.classCacheSize() >= 1,
                "save must auto-warm the class cache");
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void prewarm_context_class_populates_cache() {
        com.telcobright.statewalk.v2.persistence.SnapshotSerializer.clearClassCache();
        Statewalk.builder()
            .registerEvent(StartCall.class)
            .registerEvent(Answered.class)
            .registerEvent(Hangup.class)
            .preWarmContextClass(CallContext.class)
            .persistence(provider)
            .registry("call", callReg, 8, 2)
            .channel("call", channel)
            .build()
            .shutdown();
        assertTrue(com.telcobright.statewalk.v2.persistence.SnapshotSerializer.classCacheSize() >= 1,
            "preWarmContextClass must populate the cache before any save runs");
    }

    @Test
    void isFirst_event_creates_a_new_machine() throws InterruptedException {
        var sys = buildSystem(true);
        try {
            // No prior dispatch — but the event is isFirst, so framework creates.
            channel.inject("c-7", new StartCall("+880abc"));
            sys.awaitIdle(AWAIT);

            var m = callReg.getMachine("c-7");
            assertNotNull(m);
            assertEquals("RINGING", m.getCurrentState());
            assertEquals("+880abc", m.getContext().calledNumber);
            // Snapshot persisted on entering RINGING.
            assertTrue(provider.load("c-7").isPresent());
        } finally {
            sys.shutdown();
        }
    }

    @Test
    void rehydrate_without_persistence_is_misconfiguration() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Statewalk.builder()
                .registerEvent(StartCall.class)
                .registerEvent(Answered.class)
                .registerEvent(Hangup.class)
                .rehydrate(true)              // no .persistence(...) call
                .registry("call", new CallRegistry(), 16, 2)
                .channel("call", new TestChannel<>())
                .build());
        assertTrue(ex.getMessage().contains("rehydrate(true) requires a persistence provider"));
    }

    @Test
    void timeout_target_must_be_a_final_state() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            StateMap.builder()
                .initialState("X")
                .state("X")
                    .interim()
                    .timeout(1, TimeUnit.SECONDS, "Y")    // Y is not final
                    .on(Answered.class, "Y")
                .state("Y")
                    .interim()
                    .timeout(1, TimeUnit.SECONDS, "Z")
                .state("Z")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "Z")
                .build());
        assertTrue(ex.getMessage().contains("not a final state"));
    }
}
