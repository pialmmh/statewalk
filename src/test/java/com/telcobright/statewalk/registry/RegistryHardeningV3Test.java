package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.channel.TestChannel;
import com.telcobright.statewalk.event.StatemachineEvent;
import com.telcobright.statewalk.machine.Machine;
import com.telcobright.statewalk.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.persistence.MachineSnapshot;
import com.telcobright.statewalk.persistence.PersistenceProvider;
import com.telcobright.statewalk.persistence.SnapshotSerializer;
import com.telcobright.statewalk.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v3 hardening regression suite — every scenario here is a bug class the v2
 * audit confirmed and the rewrite closed:
 *
 * <ol>
 *   <li>Supervisor-terminal cascade reclaims LIVE children exactly (pool
 *       return counts, snapshot deletes) — the v2 cascade orphaned them.</li>
 *   <li>Request-id reuse immediately after finish — the v2 60-second dedup
 *       set silently skipped the second session's cleanup.</li>
 *   <li>Late events for a finished id are dropped, never resurrected from a
 *       stale snapshot.</li>
 *   <li>A state timer scheduled by session A can never fire into session B
 *       after the machine is re-borrowed (epoch + visit tokens).</li>
 *   <li>An event racing an offline suspend is re-submitted (rehydrates the
 *       session) instead of being dropped — the v2 over-billing hole.</li>
 *   <li>Concurrent rehydration of one id is single-flight (one row, quota
 *       re-acquired once).</li>
 *   <li>The global lifetime timeout survives a restart (persisted deadline).</li>
 *   <li>A finished session whose terminal delete failed is a TOMBSTONE at
 *       restore — purged, not resurrected.</li>
 *   <li>A dispatch rejected on a concurrency dimension does not burn a TPS
 *       token (exact quota rollback).</li>
 *   <li>A forwardTo route naming an unknown child dies at build.</li>
 *   <li>The channel's inbound side is actually wired: injects reach cells,
 *       stop() cuts intake at shutdown.</li>
 * </ol>
 */
class RegistryHardeningV3Test {

    // ── events / contexts ───────────────────────────────────────────

    public record Stop(String u)   implements StatemachineEvent {}
    public record Park(String u)   implements StatemachineEvent {}
    public record Touch(String u)  implements StatemachineEvent {}
    public record Ping(String u)   implements StatemachineEvent {}

    public static class Ctx { public String partner; public int touches; public Ctx() {} }

    private final List<StatemachineRegistry<Ctx>> open = new ArrayList<>();

    @AfterEach
    void tearDown() { for (StatemachineRegistry<Ctx> r : open) r.shutdown(); }

    private StatemachineRegistry<Ctx> track(StatemachineRegistry<Ctx> r) { open.add(r); return r; }

    // ── graph helpers ───────────────────────────────────────────────

    private static StateMap runningGraph() {
        return StateMap.builder()
            .initialState("RUNNING")
            .state("RUNNING").interim().timeout(1, TimeUnit.HOURS, "EXPIRED")
                .on(Stop.class, "DONE")
                .stay(Touch.class, (self, e) -> ((Machine<Ctx>) self).getContext().touches++)
                .stay(Ping.class, (self, e) -> { })
            .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
            .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
            .build();
    }

    private static StateMap childGraph() {
        return StateMap.builder()
            .initialState("WORKING")
            .state("WORKING").interim().timeout(1, TimeUnit.HOURS, "CLOSED")
                .stay(Ping.class, (self, e) -> { })
            .state("CLOSED").finalState().timeout(1, TimeUnit.SECONDS, "CLOSED")
            .build();
    }

    private static SupervisorSpec<Ctx> spawningSupervisor() {
        return SupervisorSpec.<Ctx>builder()
            .name("Sup").contextFactory(Ctx::new)
            .stateMap(StateMap.builder()
                .initialState("RUNNING")
                .state("RUNNING").interim().timeout(1, TimeUnit.HOURS, "EXPIRED")
                    .onEntry(self -> {
                        Supervisor<Ctx> s = (Supervisor<Ctx>) self;
                        s.resolver().spawnChild("A", null);
                        s.resolver().spawnChild("B", null);
                    })
                    .on(Stop.class, "DONE")
                .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
                .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
                .build())
            .routes(r -> {
                r.selfHandle(Stop.class);
                r.forwardToAll(List.of("A", "B"), Ping.class);
            })
            .build();
    }

    // ─────────────────────────────────────────────────────────────
    // (1) cascade reclaims live children EXACTLY
    // ─────────────────────────────────────────────────────────────

    @Test
    void supervisor_terminal_cascade_reclaims_live_children_exactly() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-cascade")
            .supervisor(spawningSupervisor(), 4)
            .child(MachineSpec.<Ctx>builder().name("A").contextFactory(Ctx::new).stateMap(childGraph()).build(), 4)
            .child(MachineSpec.<Ctx>builder().name("B").contextFactory(Ctx::new).stateMap(childGraph()).build(), 4)
            .persistence(store).rehydrate(true)
            .threads(2)
            .build());

        final int N = 10;
        for (int i = 0; i < N; i++) assertTrue(reg.dispatch("c-" + i, new Ctx()).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(3 * N, reg.activeCellCount(), "supervisor + 2 live children per id");
        assertEquals(3 * N, store.size());

        // Terminate every supervisor while its children are LIVE.
        for (int i = 0; i < N; i++) reg.onInboundEvent("c-" + i, new Stop("c-" + i));
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        assertEquals(0, reg.activeCellCount(), "every cell reclaimed by the cascade");
        assertEquals(0, reg.activeIdCount());
        assertEquals(0, store.size(), "every snapshot deleted — children included");

        // Pool exactness: every borrow of every type came back (v2 leaked ALL
        // children of every multi-cell teardown).
        for (String type : List.of("Sup", "A", "B")) {
            var st = reg.poolOf(type).getStatistics();
            assertEquals(st.totalBorrowed(), st.reclaimed(),
                type + " pool must reclaim every borrow (returned or dropped-at-cap): " + st);
            assertEquals(0, st.doubleReturns(), type + " no double returns: " + st);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // (2) id reuse immediately after finish
    // ─────────────────────────────────────────────────────────────

    @Test
    void request_id_reuse_immediately_after_finish_is_a_clean_new_session() throws Exception {
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-reuse")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Touch.class); r.selfHandle(Ping.class); })
                .build(), 2)
            .threads(2)
            .build());

        for (int round = 0; round < 5; round++) {
            String id = "same-id";
            assertTrue(reg.dispatch(id, new Ctx()).accepted(), "round " + round + " dispatch accepted");
            assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
            reg.onInboundEvent(id, new Touch(id));
            assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
            Machine<?> m = reg.findInternal(id, "Sup");
            assertNotNull(m, "round " + round + " session live");
            assertEquals(1, ((Ctx) m.getContext()).touches, "round " + round + " context is FRESH");
            reg.onInboundEvent(id, new Stop(id));
            assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
            assertFalse(reg.hasAny(id), "round " + round + " cleanly finished — v2's 60s set broke this");
        }
        var st = reg.poolOf("Sup").getStatistics();
        assertEquals(st.totalBorrowed(), st.reclaimed(), "no leaked machine across reuse rounds: " + st);
    }

    // ─────────────────────────────────────────────────────────────
    // (3) late event for a finished id: dropped, never resurrected
    // ─────────────────────────────────────────────────────────────

    @Test
    void late_event_for_finished_id_is_dropped_not_resurrected() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-late")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Ping.class); r.selfHandle(Touch.class); })
                .build(), 2)
            .persistence(store).rehydrate(true)
            .threads(2)
            .build());

        String id = "late-1";
        assertTrue(reg.dispatch(id, new Ctx()).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        reg.onInboundEvent(id, new Stop(id));
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertFalse(reg.hasAny(id));

        // The late event: v2 would find a stale snapshot (if the delete was
        // still queued) and resurrect a zombie. v3 tombstones the id.
        reg.onInboundEvent(id, new Ping(id));            // must NOT throw, must NOT resurrect
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertFalse(reg.hasAny(id), "finished session must stay finished");
        assertEquals(0, store.size());
    }

    // ─────────────────────────────────────────────────────────────
    // (4) stale state timer can not fire into the next session
    // ─────────────────────────────────────────────────────────────

    public record Advance(String u) implements StatemachineEvent {}

    @Test
    void stale_state_timer_never_fires_into_the_next_session() throws Exception {
        // SHORT state timeout; pool size 1 forces the next session onto the
        // SAME machine instance. v2's name-based timer guard shot session B
        // dead at ~0ms; the v3 visit token makes the old timer a no-op.
        AtomicInteger expiredCount = new AtomicInteger();
        StateMap counted = StateMap.builder()
            .initialState("WAITING")
            .state("WAITING").interim().timeout(400, TimeUnit.MILLISECONDS, "EXPIRED")
                .on(Stop.class, "DONE")
            .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
            .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
                .onEntry(self -> expiredCount.incrementAndGet())
            .build();

        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-staletimer")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(counted)
                .routes(r -> r.selfHandle(Stop.class))
                .build(), 1)
            .threads(2)
            .build());

        // Session A: stop it well before its 400ms deadline.
        assertTrue(reg.dispatch("t-1", new Ctx()).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        Thread.sleep(150);
        reg.onInboundEvent("t-1", new Stop("t-1"));
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertFalse(reg.hasAny("t-1"));
        assertEquals(0, expiredCount.get(), "session A stopped cleanly, no expiry");

        // Session B on the same id (and, with pool size 1, the same machine).
        assertTrue(reg.dispatch("t-1", new Ctx()).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        // Cross the OLD (session A) deadline; B's own window is still open.
        Thread.sleep(280);
        Machine<?> m = reg.findInternal("t-1", "Sup");
        assertNotNull(m, "session B must still be alive past A's deadline");
        assertEquals("WAITING", m.getCurrentState(),
            "session A's matured timer must not have fired into session B");
        assertEquals(0, expiredCount.get(), "nobody expired yet");

        // And B's OWN timer still works (the fallback discipline is intact).
        Thread.sleep(600);
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertFalse(reg.hasAny("t-1"), "session B expired by its OWN timer");
        assertEquals(1, expiredCount.get(), "exactly one expiry — B's");
    }

    // ─────────────────────────────────────────────────────────────
    // (5) event racing an offline suspend is re-submitted, not lost
    // ─────────────────────────────────────────────────────────────

    @Test
    void event_racing_offline_suspend_is_resubmitted_and_rehydrates() throws Exception {
        StateMap suspendGraph = StateMap.builder()
            .initialState("ACTIVE")
            .state("ACTIVE").interim().timeout(1, TimeUnit.HOURS, "EXPIRED")
                .on(Park.class, "PARKED")
                .stay(Touch.class, (self, e) -> ((Machine<Ctx>) self).getContext().touches++)
            .state("PARKED").interim().offline().timeout(1, TimeUnit.HOURS, "EXPIRED")
                .stay(Touch.class, (self, e) -> ((Machine<Ctx>) self).getContext().touches++)
                .on(Stop.class, "DONE")
            .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
            .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
            .build();
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-suspendrace")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(suspendGraph)
                .routes(r -> { r.selfHandle(Park.class); r.selfHandle(Touch.class); r.selfHandle(Stop.class); })
                .build(), 4)
            .persistence(store).rehydrate(true)
            .threads(2)
            .build());

        String id = "s-race";
        assertTrue(reg.dispatch(id, new Ctx()).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        // Park (suspend) with a Touch queued RIGHT behind it on the same chain.
        // v2 dropped the Touch at DEBUG — a paying event lost while the session
        // logically existed. v3 re-submits it, which rehydrates the session.
        reg.onInboundEvent(id, new Park(id));
        reg.onInboundEvent(id, new Touch(id));
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        assertTrue(reg.hasAny(id), "the raced Touch rehydrated the suspended session");
        Machine<?> m = reg.findInternal(id, "Sup");
        assertNotNull(m);
        assertEquals("PARKED", m.getCurrentState(), "resumed in the suspended state");
        assertEquals(1, ((Ctx) m.getContext()).touches, "the raced event was APPLIED, not lost");
    }

    // ─────────────────────────────────────────────────────────────
    // (6) restore is single-flight per id
    // ─────────────────────────────────────────────────────────────

    @Test
    void concurrent_rehydration_of_one_id_is_single_flight() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        String id = "sf-1";
        Ctx prior = new Ctx();
        prior.partner = "u1";
        store.save(new MachineSnapshot(id, "v3-singleflight", "RUNNING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(prior),
            System.currentTimeMillis(), "EXPIRED", System.currentTimeMillis() + 3_600_000L));

        // Startup recovery is off (lazy-only store shim) so the concurrent
        // inbound events below are what triggers the restore.
        PersistenceProvider lazyOnly = new PersistenceProvider() {
            @Override public void save(MachineSnapshot s) { store.save(s); }
            @Override public Optional<MachineSnapshot> load(String mid, String r) { return store.load(mid, r); }
            @Override public List<MachineSnapshot> loadAll(String mid) { return store.loadAll(mid); }
            @Override public void delete(String mid, String r) { store.delete(mid, r); }
        };
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-singleflight")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Ping.class); r.selfHandle(Stop.class); r.selfHandle(Touch.class); })
                .build(), 8)
            .persistence(lazyOnly).rehydrate(true)
            .quotaKeysExtractor(t -> {
                Ctx c = (Ctx) t;
                return c.partner != null ? QuotaKeys.ofPartner(c.partner) : QuotaKeys.NONE;
            })
            .quotaLimits(new QuotaLimits(10, 0, 0, 0))
            .threads(4)
            .build());

        final int THREADS = 8;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            Thread t = new Thread(() -> {
                try { go.await(); } catch (InterruptedException e) { return; }
                reg.onInboundEvent(id, new Ping(id));
            });
            ts.add(t); t.start();
        }
        go.countDown();
        for (Thread t : ts) t.join(10_000);
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        assertTrue(reg.hasAny(id));
        assertEquals(1, reg.activeIdCount(), "exactly ONE row for the id");
        assertEquals(1, reg.activeCellCount(), "exactly ONE supervisor cell");
        assertEquals(1, reg.quotaPartnerActive("u1"),
            "quota re-acquired exactly once — v2 double-restored under this race");
    }

    // ─────────────────────────────────────────────────────────────
    // (7) global lifetime timeout survives a restart
    // ─────────────────────────────────────────────────────────────

    @Test
    void global_timeout_survives_restart_via_persisted_deadline() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        StatemachineRegistry<Ctx> a = track(StatemachineRegistry.<Ctx>builder("v3-gto")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Ping.class); r.selfHandle(Touch.class); })
                .build(), 2)
            .persistence(store).rehydrate(true)
            .globalTimeout(700, TimeUnit.MILLISECONDS, "EXPIRED")
            .threads(2)
            .build());

        assertTrue(a.dispatch("g-1", new Ctx()).accepted());
        assertTrue(a.awaitIdle(5, TimeUnit.SECONDS));
        Optional<MachineSnapshot> snap = store.load("g-1", "v3-gto");
        assertTrue(snap.isPresent());
        assertTrue(snap.get().globalDeadlineMs() > 0, "the lifetime deadline is persisted");

        // Node A "crashes" (no shutdown). The deadline matures during downtime.
        Thread.sleep(900);

        // Node B restores — v2 gave restored sessions eternal life; v3 re-arms
        // the persisted deadline, which has matured → the session is ended.
        StatemachineRegistry<Ctx> b = track(StatemachineRegistry.<Ctx>builder("v3-gto")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Ping.class); r.selfHandle(Touch.class); })
                .build(), 2)
            .persistence(store).rehydrate(true)
            .globalTimeout(700, TimeUnit.MILLISECONDS, "EXPIRED")
            .threads(2)
            .build());
        // give the near-immediate re-armed timer room to fire
        Thread.sleep(300);
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));

        assertFalse(b.hasAny("g-1"), "restored session was ended by its persisted lifetime cap");
        assertEquals(0, store.size(), "terminal ritual purged the snapshot");
    }

    // ─────────────────────────────────────────────────────────────
    // (8) finished session with a failed delete = tombstone at restore
    // ─────────────────────────────────────────────────────────────

    @Test
    void finished_session_with_failed_delete_is_purged_not_resurrected() throws Exception {
        InMemoryPersistenceProvider inner = new InMemoryPersistenceProvider();
        AtomicInteger deleteCalls = new AtomicInteger();
        // Provider whose delete ALWAYS fails while the session runs → the
        // final-state snapshot is stranded in the store (the crash window).
        PersistenceProvider failingDelete = new PersistenceProvider() {
            volatile boolean failDeletes = true;
            @Override public void save(MachineSnapshot s) { inner.save(s); }
            @Override public Optional<MachineSnapshot> load(String mid, String r) { return inner.load(mid, r); }
            @Override public List<MachineSnapshot> loadAll(String mid) { return inner.loadAll(mid); }
            @Override public void delete(String mid, String r) {
                deleteCalls.incrementAndGet();
                throw new RuntimeException("simulated delete outage");
            }
        };
        StatemachineRegistry<Ctx> a = track(StatemachineRegistry.<Ctx>builder("v3-tomb")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Ping.class); r.selfHandle(Touch.class); })
                .build(), 2)
            .persistence(failingDelete).rehydrate(true)
            .threads(2)
            .build());

        String id = "tomb-1";
        assertTrue(a.dispatch(id, new Ctx()).accepted());
        assertTrue(a.awaitIdle(5, TimeUnit.SECONDS));
        a.onInboundEvent(id, new Stop(id));
        assertTrue(a.awaitIdle(5, TimeUnit.SECONDS));
        assertFalse(a.hasAny(id), "session finished in memory");
        assertTrue(deleteCalls.get() >= 1, "delete was attempted");
        Optional<MachineSnapshot> stranded = inner.load(id, "v3-tomb");
        assertTrue(stranded.isPresent(), "final-state snapshot stranded by the delete outage");
        assertEquals("DONE", stranded.get().currentState());

        // "Restart" on the same store (deletes work again): the stranded FINAL
        // snapshot must be recognised as a tombstone — purged, never resurrected
        // (v2 re-ran the final state's entry action and re-took quota).
        StatemachineRegistry<Ctx> b = track(StatemachineRegistry.<Ctx>builder("v3-tomb")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Ping.class); r.selfHandle(Touch.class); })
                .build(), 2)
            .persistence(inner).rehydrate(true)
            .threads(2)
            .build());
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));
        assertFalse(b.hasAny(id), "tombstone must not resurrect");
        assertEquals(0, inner.size(), "tombstone purged at startup recovery");
    }

    // ─────────────────────────────────────────────────────────────
    // (9) a concurrency-rejected dispatch burns no TPS token
    // ─────────────────────────────────────────────────────────────

    @Test
    void concurrency_rejected_dispatch_burns_no_tps_token() {
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-tpsburn")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Ping.class); r.selfHandle(Touch.class); })
                .build(), 8)
            .quotaKeysExtractor(t -> QuotaKeys.of("P", ((Ctx) t).partner))
            // partner TPS 2/s; ONE concurrent slot per route
            .quotaLimits(new QuotaLimits(0, 1, 2, 0))
            .threads(2)
            .build());

        Ctx routeA1 = new Ctx(); routeA1.partner = "route-A";
        Ctx routeA2 = new Ctx(); routeA2.partner = "route-A";
        Ctx routeB  = new Ctx(); routeB.partner  = "route-B";

        // Align to a fresh TPS second so the window cannot roll mid-sequence.
        try {
            while (System.currentTimeMillis() % 1000 > 600) Thread.sleep(25);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertTrue(reg.dispatch("x-1", routeA1).accepted());                     // TPS 1/2 used
        assertEquals(RejectCause.ROUTE_CONCURRENCY_EXCEEDED,
            reg.dispatch("x-2", routeA2).rejectCause(), "route full");
        // v2 burned a partner TPS token for the rejected x-2 — starving x-3.
        assertTrue(reg.dispatch("x-3", routeB).accepted(),
            "the concurrency-rejected dispatch must not have consumed the last TPS token");
        assertEquals(RejectCause.PARTNER_TPS_EXCEEDED,
            reg.dispatch("x-4", new Ctx() {{ partner = "route-C"; }}).rejectCause(),
            "the third ADMITTED attempt this second exceeds tps=2");
    }

    // ─────────────────────────────────────────────────────────────
    // (10) forwardTo typo dies at build
    // ─────────────────────────────────────────────────────────────

    @Test
    void route_to_unknown_child_is_rejected_at_build() {
        SupervisorSpec<Ctx> typo = SupervisorSpec.<Ctx>builder()
            .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
            .routes(r -> {
                r.selfHandle(Stop.class);
                r.forwardTo("SignallingChild" /* typo — registered name is Signaling */, Ping.class);
            })
            .build();
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            StatemachineRegistry.<Ctx>builder("v3-route-typo")
                .supervisor(typo, 2)
                .child(MachineSpec.<Ctx>builder().name("Signaling").contextFactory(Ctx::new)
                    .stateMap(childGraph()).build(), 2)
                .build());
        assertTrue(ex.getMessage().contains("SignallingChild"), ex.getMessage());
    }

    // ─────────────────────────────────────────────────────────────
    // (11) channel inbound is actually wired
    // ─────────────────────────────────────────────────────────────

    @Test
    void channel_inbound_is_wired_and_stops_at_shutdown() throws Exception {
        TestChannel<String, StatemachineEvent> ch = new TestChannel<>("wire");
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-channel")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Touch.class); r.selfHandle(Ping.class); })
                .build(), 2)
            .channel(ch)
            .threads(2)
            .build());

        assertTrue(ch.isStarted(), "registry must start the channel at build — v2 never wired inbound");

        assertTrue(reg.dispatch("ch-1", new Ctx()).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        // Inject through the CHANNEL; join the ack (completes when processed).
        ch.inject("ch-1", new Touch("ch-1")).toCompletableFuture().get(5, TimeUnit.SECONDS);
        Machine<?> m = reg.findInternal("ch-1", "Sup");
        assertNotNull(m);
        assertEquals(1, ((Ctx) m.getContext()).touches, "channel-injected event reached the cell");

        reg.shutdown();
        assertFalse(ch.isStarted(), "shutdown stops the channel FIRST");
        var post = ch.inject("ch-1", new Touch("ch-1")).toCompletableFuture();
        assertTrue(post.isCompletedExceptionally(), "post-shutdown injects fail loudly, not silently");
    }

    // ─────────────────────────────────────────────────────────────
    // (11b) a SLOW store never blocks the hot path
    // ─────────────────────────────────────────────────────────────

    @Test
    void slow_store_never_blocks_event_processing() throws Exception {
        // Every WRITE to this store takes 800ms. Six writes will be queued
        // (initial transition + 5 stays) — ~4.8s of store time — while the
        // events themselves must land in a small fraction of ONE write.
        InMemoryPersistenceProvider inner = new InMemoryPersistenceProvider();
        PersistenceProvider slow = new PersistenceProvider() {
            private void crawl() {
                try { Thread.sleep(800); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            @Override public void save(MachineSnapshot s) { crawl(); inner.save(s); }
            @Override public Optional<MachineSnapshot> load(String id, String r) { return inner.load(id, r); }
            @Override public List<MachineSnapshot> loadAll(String id) { return inner.loadAll(id); }
            @Override public void delete(String id, String r) { crawl(); inner.delete(id, r); }
        };
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-slowstore")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Touch.class); r.selfHandle(Ping.class); })
                .build(), 2)
            .persistence(slow)
            .threads(2)
            .build());

        assertTrue(reg.dispatch("slow-1", new Ctx()).accepted());
        long t0 = System.nanoTime();
        for (int i = 0; i < 5; i++) reg.onInboundEvent("slow-1", new Touch("slow-1"));

        // The hot path: all 5 events applied while the FIRST 800ms write is
        // still in flight.
        long budgetMs = 700;
        long deadline = System.currentTimeMillis() + budgetMs;
        Machine<?> m = null;
        while (System.currentTimeMillis() < deadline) {
            m = reg.findInternal("slow-1", "Sup");
            if (m != null && ((Ctx) m.getContext()).touches == 5) break;
            Thread.sleep(10);
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertNotNull(m);
        assertEquals(5, ((Ctx) m.getContext()).touches,
            "all events processed while the store crawls (elapsed " + elapsedMs + "ms)");
        assertTrue(elapsedMs < budgetMs,
            "event processing must not wait on disk: took " + elapsedMs + "ms against 800ms/write");

        // And the writes DO all land, in order, once the store catches up.
        reg.onInboundEvent("slow-1", new Stop("slow-1"));
        assertTrue(reg.awaitIdle(20, TimeUnit.SECONDS), "persist chains drain eventually");
        assertEquals(0, inner.size(), "terminal delete landed after the queued saves");
    }

    // ─────────────────────────────────────────────────────────────
    // (12) saturation soak: heavy concurrent dispatch+finish, no losses
    // ─────────────────────────────────────────────────────────────

    @Test
    void saturation_soak_loses_no_sessions_and_leaks_no_cells() throws Exception {
        StatemachineRegistry<Ctx> reg = track(StatemachineRegistry.<Ctx>builder("v3-soak")
            .supervisor(SupervisorSpec.<Ctx>builder()
                .name("Sup").contextFactory(Ctx::new).stateMap(runningGraph())
                .routes(r -> { r.selfHandle(Stop.class); r.selfHandle(Touch.class); r.selfHandle(Ping.class); })
                .build(), 64)
            .threads(4)
            .build());

        final int N = 400;
        AtomicInteger accepted = new AtomicInteger();
        IntStream.range(0, N).parallel().forEach(i -> {
            String id = "soak-" + i;
            if (reg.dispatch(id, new Ctx()).accepted()) {
                accepted.incrementAndGet();
                reg.onInboundEvent(id, new Touch(id));
                reg.onInboundEvent(id, new Stop(id));
            }
        });
        assertEquals(N, accepted.get(), "zero dropped admissions under parallel load");
        assertTrue(reg.awaitIdle(20, TimeUnit.SECONDS), "registry drains after the soak");
        assertEquals(0, reg.activeCellCount(), "no leaked cells");
        assertEquals(0, reg.activeIdCount(), "no leaked ids");
        var st = reg.poolOf("Sup").getStatistics();
        assertEquals(st.totalBorrowed(), st.reclaimed(), "pool exact after the soak: " + st);
        assertEquals(0, st.doubleReturns());
    }
}
