package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.event.StatemachineEvent;
import com.telcobright.statewalk.event.TimeoutEvent;
import com.telcobright.statewalk.machine.Machine;
import com.telcobright.statewalk.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.persistence.MachineSnapshot;
import com.telcobright.statewalk.persistence.SnapshotSerializer;
import com.telcobright.statewalk.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The timeout + rehydration contract:
 *
 * <ol>
 *   <li>Every state has a mandatory timeout — target-mode (fallback to a
 *       final state) or the new STAY-mode (checkpoint: run the per-period
 *       action, re-persist the context with the refreshed deadline, re-arm).</li>
 *   <li>Rehydration seats the machine in the last saved state WITHOUT
 *       replaying its entry action, and honours elapsed time: a target-mode
 *       deadline that matured during downtime transitions immediately; a
 *       stay-mode deadline that matured checkpoints immediately and re-arms;
 *       an unmatured deadline is armed for the remaining slice only.</li>
 * </ol>
 */
class StayTimeoutAndRehydrationTest {

    public record Stop(String u)  implements StatemachineEvent {}
    public record Nudge(String u) implements StatemachineEvent {}
    public record Touch(String u) implements StatemachineEvent {}

    public static class Ctx { public int beats; public String mark; public Ctx() {} }

    static final AtomicInteger WAITING_ENTRIES = new AtomicInteger();
    static final AtomicInteger EXPIRED_ENTRIES = new AtomicInteger();

    private final List<StatemachineRegistry<Ctx>> open = new ArrayList<>();

    @AfterEach
    void tearDown() { for (StatemachineRegistry<Ctx> r : open) r.shutdown(); }

    private StatemachineRegistry<Ctx> track(StatemachineRegistry<Ctx> r) { open.add(r); return r; }

    /** WAITING has a STAY-mode 150ms heartbeat that counts beats into the context. */
    private static SupervisorSpec<Ctx> staySpec() {
        return SupervisorSpec.<Ctx>builder()
            .name("Sup").contextFactory(Ctx::new)
            .stateMap(StateMap.builder()
                .initialState("WAITING")
                .state("WAITING").interim()
                    .timeoutStay(150, TimeUnit.MILLISECONDS,
                        self -> ((Machine<Ctx>) self).getContext().beats++)
                    .onEntry(self -> WAITING_ENTRIES.incrementAndGet())
                    .on(Stop.class, "DONE")
                .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
                .build())
            .routes(r -> r.selfHandle(Stop.class))
            .build();
    }

    /** WAITING has a classic target-mode timeout to the final EXPIRED. */
    private static SupervisorSpec<Ctx> targetSpec(long timeoutMs) {
        return SupervisorSpec.<Ctx>builder()
            .name("Sup").contextFactory(Ctx::new)
            .stateMap(StateMap.builder()
                .initialState("WAITING")
                .state("WAITING").interim()
                    .timeout(timeoutMs, TimeUnit.MILLISECONDS, "EXPIRED")
                    .onEntry(self -> WAITING_ENTRIES.incrementAndGet())
                    .on(Stop.class, "DONE")
                .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
                .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
                    .onEntry(self -> EXPIRED_ENTRIES.incrementAndGet())
                .build())
            .routes(r -> r.selfHandle(Stop.class))
            .build();
    }

    // ─────────────────────────────────────────────────────────────
    // Stay-mode: periodic checkpoint, machine stays, store refreshed
    // ─────────────────────────────────────────────────────────────

    @Test
    void stay_timeout_checkpoints_periodically_and_machine_stays() throws Exception {
        WAITING_ENTRIES.set(0);
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("stay-beat")
            .supervisor(staySpec(), 2)
            .persistence(store).rehydrate(true)
            .threads(2)
            .build());

        String id = "hb-1";
        assertTrue(reg.dispatch(id, new Ctx()).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        long firstDeadline = store.load(id, "stay-beat").orElseThrow().timeoutDeadlineMs();
        assertTrue(firstDeadline > 0, "stay-mode still persists a deadline");

        Thread.sleep(560);   // ≥3 heartbeat periods
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        Machine<?> m = reg.findInternal(id, "Sup");
        assertNotNull(m, "machine STAYS on timeout — it never fell to a target");
        assertEquals("WAITING", m.getCurrentState());
        int beats = ((Ctx) m.getContext()).beats;
        assertTrue(beats >= 2, "heartbeat action ran every period (beats=" + beats + ")");
        assertEquals(1, WAITING_ENTRIES.get(), "entry action ran ONCE — checkpoints don't re-enter");

        MachineSnapshot snap = store.load(id, "stay-beat").orElseThrow();
        assertTrue(snap.timeoutDeadlineMs() > firstDeadline,
            "each checkpoint re-persisted the context with a REFRESHED deadline");
        Ctx persisted = (Ctx) SnapshotSerializer.contextFromBase64Json(
            snap.contextJsonBase64(), snap.contextClassName());
        assertTrue(persisted.beats >= 2, "the beat count is IN the store, not just in memory");

        reg.onInboundEvent(id, new Stop(id));
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertFalse(reg.hasAny(id), "still terminates cleanly on a real event");
        assertEquals(0, store.size());
    }

    // ─────────────────────────────────────────────────────────────
    // Builder validations
    // ─────────────────────────────────────────────────────────────

    @Test
    void builder_rejects_missing_both_modes_and_final_stay() {
        // no timeout at all
        IllegalStateException none = assertThrows(IllegalStateException.class, () ->
            StateMap.builder().initialState("A")
                .state("A").interim()
                .build());
        assertTrue(none.getMessage().contains("timeoutStay"), "message names both modes: " + none.getMessage());

        // both modes on one state
        assertThrows(IllegalStateException.class, () ->
            StateMap.builder().initialState("A")
                .state("A").interim()
                    .timeout(1, TimeUnit.SECONDS, "B")
                    .timeoutStay(1, TimeUnit.SECONDS)
                .state("B").finalState().timeout(1, TimeUnit.SECONDS, "B")
                .build());

        // stay on a final state
        assertThrows(IllegalStateException.class, () ->
            StateMap.builder().initialState("A")
                .state("A").interim().timeout(1, TimeUnit.SECONDS, "B")
                .state("B").finalState().timeoutStay(1, TimeUnit.SECONDS)
                .build());
    }

    // ─────────────────────────────────────────────────────────────
    // Rehydration: no entry replay + elapsed time honoured
    // ─────────────────────────────────────────────────────────────

    @Test
    void rehydration_seats_saved_state_without_replaying_entry_and_arms_remaining_slice() throws Exception {
        WAITING_ENTRIES.set(0); EXPIRED_ENTRIES.set(0);
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        Ctx prior = new Ctx(); prior.mark = "kept";
        // Saved in WAITING with 60s still to go on its timeout.
        store.save(new MachineSnapshot("r-live", "rehy-live", "WAITING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(prior),
            System.currentTimeMillis(), "EXPIRED", System.currentTimeMillis() + 60_000L));

        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("rehy-live")
            .supervisor(targetSpec(60_000), 2)
            .persistence(store).rehydrate(true)
            .threads(2)
            .build());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        Machine<?> m = reg.findInternal("r-live", "Sup");
        assertNotNull(m, "resumed at startup");
        assertEquals("WAITING", m.getCurrentState(), "seated DIRECTLY in the saved state");
        assertEquals("kept", ((Ctx) m.getContext()).mark, "context restored from the store");
        assertEquals(0, WAITING_ENTRIES.get(),
            "the saved state's ENTRY ACTION did not replay on rehydration");
        assertEquals(0, EXPIRED_ENTRIES.get(), "unmatured deadline → no transition");
    }

    @Test
    void rehydration_with_matured_target_deadline_transitions_immediately() throws Exception {
        WAITING_ENTRIES.set(0); EXPIRED_ENTRIES.set(0);
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        // Saved in WAITING; its deadline fell due 5s ago while the node was down.
        store.save(new MachineSnapshot("r-late", "rehy-late", "WAITING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(new Ctx()),
            System.currentTimeMillis() - 10_000L, "EXPIRED", System.currentTimeMillis() - 5_000L));

        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("rehy-late")
            .supervisor(targetSpec(8_000), 2)
            .persistence(store).rehydrate(true)
            .threads(2)
            .build());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        assertFalse(reg.hasAny("r-late"),
            "elapsed downtime counts: matured deadline → transitioned to EXPIRED → terminal ritual");
        assertEquals(0, WAITING_ENTRIES.get(), "WAITING's entry did NOT replay on the way through");
        assertEquals(1, EXPIRED_ENTRIES.get(), "the TARGET state's entry ran exactly once");
        assertEquals(0, store.size(), "snapshot purged by the terminal ritual");
    }

    @Test
    void rehydration_with_matured_stay_deadline_checkpoints_immediately_and_rearms() throws Exception {
        WAITING_ENTRIES.set(0);
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        Ctx prior = new Ctx(); prior.beats = 7;
        // Stay-mode state saved with a deadline that matured during downtime.
        store.save(new MachineSnapshot("r-stay", "rehy-stay", "WAITING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(prior),
            System.currentTimeMillis() - 1_000L, null, System.currentTimeMillis() - 500L));

        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("rehy-stay")
            .supervisor(staySpec(), 2)
            .persistence(store).rehydrate(true)
            .threads(2)
            .build());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        Machine<?> m = reg.findInternal("r-stay", "Sup");
        assertNotNull(m, "stay-mode machine RESUMES (matured stay never terminates it)");
        assertEquals("WAITING", m.getCurrentState());
        assertEquals(0, WAITING_ENTRIES.get(), "no entry replay");
        assertTrue(((Ctx) m.getContext()).beats >= 8,
            "the missed checkpoint ran immediately on restore (7 saved + ≥1 now)");
        MachineSnapshot refreshed = store.load("r-stay", "rehy-stay").orElseThrow();
        assertTrue(refreshed.timeoutDeadlineMs() > System.currentTimeMillis() - 50,
            "checkpoint re-persisted a FUTURE deadline — the heartbeat is re-armed");

        // and the heartbeat keeps ticking after restore
        int beatsNow = ((Ctx) m.getContext()).beats;
        Thread.sleep(400);
        assertTrue(((Ctx) m.getContext()).beats > beatsNow, "re-armed timer keeps checkpointing");
    }

    // ─────────────────────────────────────────────────────────────
    // Hibernation (.offline) at startup: db-only unless matured
    // ─────────────────────────────────────────────────────────────

    /** Offline graph: ACTIVE —Park→ PARKED(.offline, window to EXPIRED) —Stop→ DONE. */
    private static SupervisorSpec<Ctx> hibernatingSpec(long windowSec) {
        return SupervisorSpec.<Ctx>builder()
            .name("Sup").contextFactory(Ctx::new)
            .stateMap(StateMap.builder()
                .initialState("ACTIVE")
                .state("ACTIVE").interim().timeout(1, TimeUnit.HOURS, "EXPIRED")
                    .on(Park.class, "PARKED")
                .state("PARKED").interim().offline()
                    .timeout(windowSec, TimeUnit.SECONDS, "EXPIRED")
                    .on(Stop.class, "DONE")
                    .stay(Touch.class, (self, e) -> ((Machine<Ctx>) self).getContext().beats++)
                .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
                .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
                    .onEntry(self -> EXPIRED_ENTRIES.incrementAndGet())
                .build())
            .routes(r -> { r.selfHandle(Park.class); r.selfHandle(Stop.class); r.selfHandle(Touch.class); })
            .build();
    }

    public record Park(String u) implements StatemachineEvent {}

    @Test
    void startup_leaves_unmatured_hibernated_sessions_db_only_and_settles_matured_ones() throws Exception {
        EXPIRED_ENTRIES.set(0);
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<Ctx> a = track(StatemachineRegistry.<Ctx>builder("hib-start")
            .supervisor(hibernatingSpec(3600), 4)
            .persistence(store).rehydrate(true).threads(2)
            .build());
        // two sessions parked with a LONG window…
        for (String id : java.util.List.of("h-1", "h-2")) {
            assertTrue(a.dispatch(id, new Ctx()).accepted());
            a.onInboundEvent(id, new Park(id));
        }
        assertTrue(a.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(0, a.activeIdCount(), "both hibernated on node A");
        a.shutdown(); open.remove(a);

        // …and one whose window MATURED during downtime (seeded snapshot).
        store.save(new MachineSnapshot("h-old", "hib-start", "PARKED",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(new Ctx()),
            System.currentTimeMillis() - 10_000, "EXPIRED", System.currentTimeMillis() - 5_000));
        assertEquals(3, store.size());

        StatemachineRegistry<Ctx> b = track(StatemachineRegistry.<Ctx>builder("hib-start")
            .supervisor(hibernatingSpec(3600), 4)
            .persistence(store).rehydrate(true).threads(2)
            .build());
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));

        assertEquals(0, b.activeIdCount(),
            "startup must NOT flood memory with hibernated sessions — they stay db-only");
        assertEquals(1, EXPIRED_ENTRIES.get(), "…but the MATURED one was woken and settled");
        assertEquals(2, store.size(), "h-old settled+purged; h-1/h-2 still hibernating");

        // a hibernated one still wakes lazily on its next event
        b.onInboundEvent("h-1", new Touch("h-1"));
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(b.hasAny("h-1"), "lazy rehydration woke it");
        assertEquals(1, ((Ctx) b.findInternal("h-1", "Sup").getContext()).beats);
    }

    @Test
    void offline_graph_without_rehydrate_is_rejected_at_build() {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            StatemachineRegistry.<Ctx>builder("hib-norehydrate")
                .supervisor(hibernatingSpec(3600), 2)
                .persistence(store)          // rehydrate deliberately OFF
                .build());
        assertTrue(ex.getMessage().contains("rehydrate"),
            "a hibernating graph with no wake path must die at build: " + ex.getMessage());
    }

    // ─────────────────────────────────────────────────────────────
    // Domain rule on TimeoutEvent outranks the stay checkpoint
    // ─────────────────────────────────────────────────────────────

    @Test
    void domain_transition_on_timeout_event_wins_over_stay_checkpoint() throws Exception {
        AtomicInteger closed = new AtomicInteger();
        SupervisorSpec<Ctx> spec = SupervisorSpec.<Ctx>builder()
            .name("Sup").contextFactory(Ctx::new)
            .stateMap(StateMap.builder()
                .initialState("WAITING")
                .state("WAITING").interim()
                    .timeoutStay(120, TimeUnit.MILLISECONDS)
                    // After 2 beats the domain decides the timeout should end it.
                    .on(TimeoutEvent.class, "DONE",
                        (self, e) -> ((Machine<Ctx>) self).getContext().beats >= 2,
                        (self, e) -> ((Machine<Ctx>) self).getContext().beats++)
                    .stay(TimeoutEvent.class, (self, e) -> ((Machine<Ctx>) self).getContext().beats++)
                    .on(Stop.class, "DONE")
                .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
                    .onEntry(self -> closed.incrementAndGet())
                .build())
            .routes(r -> r.selfHandle(Stop.class))
            .build();
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("stay-override")
            .supervisor(spec, 2).threads(2).build());

        assertTrue(reg.dispatch("o-1", new Ctx()).accepted());
        Thread.sleep(900);
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertFalse(reg.hasAny("o-1"), "the domain's own TimeoutEvent rule ended the session");
        assertEquals(1, closed.get());
    }
}
