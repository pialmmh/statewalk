package com.telcobright.statewalk.v2;

import com.telcobright.statewalk.v2.channel.TestChannel;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.v2.registry.api.Registry;
import com.telcobright.statewalk.v2.registry.api.Statewalk;
import com.telcobright.statewalk.v2.registry.api.StatewalkSystem;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OfflineRehydrationTest {

    public record OpenSession(String userId) implements StatemachineEvent {
        @Override public boolean isFirst() { return true; }
    }
    public record Heartbeat()  implements StatemachineEvent {}
    public record CloseSession() implements StatemachineEvent {}

    public static class SessionContext {
        public String userId;
        public int    heartbeatCount;
        public SessionContext() {}
    }
    public record SessionTask(String userId) {}

    static final AtomicInteger suspendedEntryRuns      = new AtomicInteger(0);
    static final AtomicInteger suspendedExitRuns       = new AtomicInteger(0);
    static final AtomicInteger closedEntryRuns         = new AtomicInteger(0);
    static final AtomicInteger expiredEntryRuns        = new AtomicInteger(0);

    static class SessionMachine extends Machine<SessionTask, SessionContext> {
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("SUSPENDED")
                .state("SUSPENDED")
                    .interim()
                    .offline()
                    .timeout(500, TimeUnit.MILLISECONDS, "EXPIRED")
                    .onEntry(self -> {
                        suspendedEntryRuns.incrementAndGet();
                        SessionMachine m = (SessionMachine) self;
                        if (m.getPersistingEntity() != null) {
                            m.getContext().userId = m.getPersistingEntity().userId();
                        }
                    })
                    .onExit(self -> suspendedExitRuns.incrementAndGet())
                    .on(Heartbeat.class,    "SUSPENDED")
                    .on(CloseSession.class, "CLOSED")
                .state("CLOSED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "CLOSED")
                    .onEntry(self -> closedEntryRuns.incrementAndGet())
                .state("EXPIRED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "EXPIRED")
                    .onEntry(self -> expiredEntryRuns.incrementAndGet())
                .build();
        }
        @Override protected SessionContext createContext() { return new SessionContext(); }
    }

    static class SessionRegistry extends Registry<SessionMachine, SessionContext> {
        @Override protected String getRegistryName()    { return "sessions"; }
        @Override protected int    getMaxConcurrent()   { return 16; }
        @Override protected long   getGlobalTimeoutMs() { return 60_000L; }
        @Override protected SessionMachine createMachineTemplate() { return new SessionMachine(); }
        @Override
        protected Object createTaskFromFirstEvent(String requestId, StatemachineEvent firstEvent) {
            if (firstEvent instanceof OpenSession o) return new SessionTask(o.userId());
            return super.createTaskFromFirstEvent(requestId, firstEvent);
        }
    }

    private InMemoryPersistenceProvider persistence;
    private SessionRegistry             registry;
    private TestChannel                 channel;
    private StatewalkSystem             system;

    @BeforeEach
    void setUp() {
        suspendedEntryRuns.set(0);
        suspendedExitRuns.set(0);
        closedEntryRuns.set(0);
        expiredEntryRuns.set(0);

        persistence = new InMemoryPersistenceProvider();
        registry    = new SessionRegistry();
        channel     = new TestChannel("session-ch");

        system = Statewalk.builder()
            .registerEvent(OpenSession.class)
            .registerEvent(Heartbeat.class)
            .registerEvent(CloseSession.class)
            .persistence(persistence)
            .rehydrate(true)
            .registry("sessions", registry, 8, 2)
            .channel("sessions", channel)
            .build();
    }

    @AfterEach
    void tearDown() {
        if (system != null) system.shutdown();
    }

    @Test
    void rehydrate_after_offline_does_not_replay_entry_action() throws InterruptedException {
        String id = "session-1";
        channel.inject(id, new OpenSession("alice"));
        assertTrue(system.awaitIdle(java.time.Duration.ofSeconds(2)));

        assertEquals(1, suspendedEntryRuns.get(), "entry runs once on initial creation");
        assertNull(registry.getMachine(id), "offline machine is removed from active map");
        assertTrue(persistence.load(id, "sessions").isPresent(), "snapshot persisted while offline");
        assertEquals("SUSPENDED", persistence.load(id, "sessions").get().currentState());

        // Trigger rehydration by sending a Heartbeat that re-enters SUSPENDED.
        // (Self-transition: exit + entry would both run for a normal transition.
        // We expect entry to run exactly once more for the re-entry — proving
        // the rehydrate itself did NOT replay entry; only the explicit
        // SUSPENDED → SUSPENDED transition runs entry.)
        int entryBefore = suspendedEntryRuns.get();
        channel.inject(id, new Heartbeat());
        assertTrue(system.awaitIdle(java.time.Duration.ofSeconds(2)));

        // Rehydrate restored state silently (no entry replay), then the
        // Heartbeat re-entered SUSPENDED which runs entry once more.
        assertEquals(entryBefore + 1, suspendedEntryRuns.get(),
            "entry replayed by rehydrate path would push count to entryBefore+2; should be +1");
    }

    @Test
    void rehydrate_after_offline_with_only_terminal_event_does_not_replay_entry() throws InterruptedException {
        String id = "session-2";
        channel.inject(id, new OpenSession("bob"));
        assertTrue(system.awaitIdle(java.time.Duration.ofSeconds(2)));
        assertEquals(1, suspendedEntryRuns.get());
        assertNull(registry.getMachine(id));

        // Send a terminal event — rehydrates, then transitions SUSPENDED→CLOSED.
        // SUSPENDED's entry must remain at 1 (no replay).
        channel.inject(id, new CloseSession());
        assertTrue(system.awaitIdle(java.time.Duration.ofSeconds(2)));

        assertEquals(1, suspendedEntryRuns.get(), "rehydrate path must not replay entry");
        assertEquals(1, closedEntryRuns.get(),    "terminal entry runs once on arrival");
        assertEquals(1, suspendedExitRuns.get(),  "exit fires on transition out of SUSPENDED");
        assertNull(registry.getMachine(id), "terminated machine cleared from active map");
        assertFalse(persistence.load(id, "sessions").isPresent(), "snapshot deleted on terminal");
    }

    @Test
    void timeout_after_rehydration_transitions_to_target_state() throws InterruptedException {
        String id = "session-3";
        channel.inject(id, new OpenSession("carol"));
        assertTrue(system.awaitIdle(java.time.Duration.ofSeconds(2)));

        // Machine entered SUSPENDED with a 500ms timeout → EXPIRED.
        // It's now offline. Sleep past the timeout deadline, then trigger
        // rehydration with any event. Framework should see timeout matured
        // and transition straight to EXPIRED (final).
        assertEquals(1, suspendedEntryRuns.get());
        assertNull(registry.getMachine(id));
        long deadline = persistence.load(id, "sessions").get().timeoutDeadlineMs();
        assertTrue(deadline > 0, "snapshot carries timeout deadline");

        Thread.sleep(600);
        assertTrue(System.currentTimeMillis() > deadline, "deadline has passed");

        // Any event for the id triggers rehydration; the timeout matured →
        // transitionTo("EXPIRED") fires automatically inside Machine.rehydrate.
        channel.inject(id, new Heartbeat());
        assertTrue(system.awaitIdle(java.time.Duration.ofSeconds(2)));

        assertEquals(1, expiredEntryRuns.get(), "EXPIRED entry runs once after matured-timeout rehydrate");
        assertEquals(0, closedEntryRuns.get(),  "CloseSession was NOT what closed it");
        assertEquals(1, suspendedExitRuns.get(),"SUSPENDED exit ran on transition to EXPIRED");
        assertEquals(1, suspendedEntryRuns.get(),"SUSPENDED entry NOT replayed during rehydrate");
        assertNull(registry.getMachine(id),     "machine terminated and reclaimed");
        assertFalse(persistence.load(id, "sessions").isPresent(), "snapshot deleted on terminal");
    }
}
