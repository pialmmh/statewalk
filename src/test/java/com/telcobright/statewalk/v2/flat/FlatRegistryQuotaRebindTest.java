package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.v2.persistence.MachineSnapshot;
import com.telcobright.statewalk.v2.persistence.PersistenceProvider;
import com.telcobright.statewalk.v2.registry.api.DispatchResult;
import com.telcobright.statewalk.v2.registry.api.QuotaKeys;
import com.telcobright.statewalk.v2.registry.api.QuotaLimits;
import com.telcobright.statewalk.v2.registry.api.RejectCause;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The ratified base extension for anonymous-at-birth requests (wifi: a MAC is
 * seen at packet #1, the user — the quota key — arrives at first login):
 *
 * <ol>
 *   <li>{@link Registry#rebindQuotaKeys} — atomic release-old + acquire-new,
 *       reject with rollback, counters exact under races;</li>
 *   <li>restore-path quota re-acquire — a restart (startup recovery) and a lazy
 *       rehydration both bring the slots back from the restored context, so the
 *       caps never under-count after a restart.</li>
 * </ol>
 */
class FlatRegistryQuotaRebindTest {

    // ── events ──────────────────────────────────────────────────────

    public record Stop() implements StatemachineEvent {}
    /** Stays in RUNNING; writes the bound identity into the context (re-persisted by the stay). */
    public record Bind(String partner, String route) implements StatemachineEvent {}
    public record Ping() implements StatemachineEvent {}

    /** Persisted context — the quota keys are derived from it (extractor below). */
    public static class Task {
        public String partner;
        public String route;
        public Task() {}
        public Task(String p, String r) { partner = p; route = r; }
    }

    public static class RebindSupervisor extends Supervisor<Task> {
        @Override
        protected void defineRoutes(InternalEventResolver r) {
            r.selfHandle(Stop.class);
            r.selfHandle(Bind.class);
            r.selfHandle(Ping.class);
        }
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("RUNNING")
                .state("RUNNING")
                    .interim()
                    .timeout(1, TimeUnit.HOURS, "DONE")
                    .on(Stop.class, "DONE")
                    .stay(Bind.class, (self, e) -> {
                        Task t = ((RebindSupervisor) self).getContext();
                        t.partner = ((Bind) e).partner();
                        t.route   = ((Bind) e).route();
                    })
                    .stay(Ping.class, (self, e) -> { })
                .state("DONE")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "DONE")
                .build();
        }
        @Override protected Task createContext() { return new Task(); }
    }

    /** The wifi-style lambda: no partner yet → NONE (anonymous); else both dimensions. */
    static QuotaKeys keysOf(Object task) {
        Task t = (Task) task;
        if (t.partner == null) return QuotaKeys.NONE;
        return QuotaKeys.of(t.partner, t.route);
    }

    private final List<Registry> open = new ArrayList<>();

    @AfterEach
    void tearDown() { for (Registry r : open) r.shutdown(); }

    private Registry.Builder builder(String name, QuotaLimits limits) {
        return Registry.builder(name)
            .supervisor("RebindSupervisor", RebindSupervisor::new, 128)
            .threads(4)
            .quotaKeysExtractor(FlatRegistryQuotaRebindTest::keysOf)
            .quotaLimits(limits);
    }

    private Registry build(String name, QuotaLimits limits) {
        Registry r = builder(name, limits).build();
        open.add(r);
        return r;
    }

    private static void dispatchAnon(Registry reg, String id) {
        DispatchResult d = reg.dispatch(id, new Task());
        assertTrue(d.accepted(), "anonymous dispatch must be accepted: " + d.rejectCause());
    }

    private static void stop(Registry reg, String id) throws InterruptedException {
        reg.onInboundEvent(id, new Stop());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
    }

    // ─────────────────────────────────────────────────────────────
    // rebind — single-threaded semantics
    // ─────────────────────────────────────────────────────────────

    @Test
    void anonymous_machine_binds_at_first_login_and_terminal_release_frees_the_rebound_keys()
            throws InterruptedException {
        Registry reg = build("rb-bind", new QuotaLimits(3, 2, 0, 0));
        dispatchAnon(reg, "mac-1");
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(QuotaKeys.NONE, reg.quotaKeysOf("mac-1"));
        assertEquals(0, reg.quotaPartnerActive("u1"));

        assertNull(reg.rebindQuotaKeys("mac-1", QuotaKeys.of("u1", "u1@zoneA")));

        assertEquals(QuotaKeys.of("u1", "u1@zoneA"), reg.quotaKeysOf("mac-1"));
        assertEquals(1, reg.quotaPartnerActive("u1"));
        assertEquals(1, reg.quotaRouteActive("u1@zoneA"));

        stop(reg, "mac-1");
        assertFalse(reg.hasAny("mac-1"));
        assertEquals(0, reg.quotaPartnerActive("u1"), "terminal release frees the REBOUND keys");
        assertEquals(0, reg.quotaRouteActive("u1@zoneA"));
        assertEquals(QuotaKeys.NONE, reg.quotaKeysOf("mac-1"));
    }

    @Test
    void rebind_at_partner_cap_is_rejected_and_the_old_binding_survives_with_exact_counters()
            throws InterruptedException {
        Registry reg = build("rb-cap", new QuotaLimits(2, 0, 0, 0));
        for (String id : List.of("a", "b", "c")) dispatchAnon(reg, id);
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        assertNull(reg.rebindQuotaKeys("a", QuotaKeys.ofPartner("u1")));
        assertNull(reg.rebindQuotaKeys("b", QuotaKeys.ofPartner("u1")));
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED,
            reg.rebindQuotaKeys("c", QuotaKeys.ofPartner("u1")), "third device of u1 is over the cap");

        assertEquals(2, reg.quotaPartnerActive("u1"), "the failed acquire must not leak a slot");
        assertEquals(QuotaKeys.NONE, reg.quotaKeysOf("c"), "c stays anonymous");

        // c re-targets an already-bound identity: old (u2) must come back exactly on reject.
        assertNull(reg.rebindQuotaKeys("c", QuotaKeys.ofPartner("u2")));
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED, reg.rebindQuotaKeys("c", QuotaKeys.ofPartner("u1")));
        assertEquals(1, reg.quotaPartnerActive("u2"), "old keys re-taken after the reject");
        assertEquals(2, reg.quotaPartnerActive("u1"));
        assertEquals(QuotaKeys.ofPartner("u2"), reg.quotaKeysOf("c"));

        // freeing one u1 slot lets c in; counters stay exact throughout.
        stop(reg, "a");
        assertNull(reg.rebindQuotaKeys("c", QuotaKeys.ofPartner("u1")));
        assertEquals(2, reg.quotaPartnerActive("u1"));
        assertEquals(0, reg.quotaPartnerActive("u2"), "moving c off u2 released u2's slot");
    }

    @Test
    void reject_on_the_route_dimension_rolls_back_the_partner_acquire() throws InterruptedException {
        // partner cap wide open, one device per (user, zone)
        Registry reg = build("rb-route", new QuotaLimits(10, 1, 0, 0));
        dispatchAnon(reg, "d1");
        dispatchAnon(reg, "d2");
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        assertNull(reg.rebindQuotaKeys("d1", QuotaKeys.of("u1", "u1@z1")));
        assertEquals(RejectCause.ROUTE_CONCURRENCY_EXCEEDED,
            reg.rebindQuotaKeys("d2", QuotaKeys.of("u1", "u1@z1")));

        assertEquals(1, reg.quotaPartnerActive("u1"), "partner slot taken for d2 was rolled back");
        assertEquals(1, reg.quotaRouteActive("u1@z1"));
        assertEquals(QuotaKeys.NONE, reg.quotaKeysOf("d2"));

        // same user, another zone → fine (route dimension is per user@zone)
        assertNull(reg.rebindQuotaKeys("d2", QuotaKeys.of("u1", "u1@z2")));
        assertEquals(2, reg.quotaPartnerActive("u1"));
        assertEquals(1, reg.quotaRouteActive("u1@z2"));
    }

    @Test
    void rebind_between_partners_moves_the_slot_and_same_keys_is_idempotent() throws InterruptedException {
        Registry reg = build("rb-move", new QuotaLimits(1, 0, 0, 0));
        dispatchAnon(reg, "m");
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        assertNull(reg.rebindQuotaKeys("m", QuotaKeys.ofPartner("p1")));
        assertNull(reg.rebindQuotaKeys("m", QuotaKeys.ofPartner("p1")), "same keys → no-op success");
        assertEquals(1, reg.quotaPartnerActive("p1"), "idempotent rebind does not double count");

        assertNull(reg.rebindQuotaKeys("m", QuotaKeys.ofPartner("p2")));
        assertEquals(0, reg.quotaPartnerActive("p1"));
        assertEquals(1, reg.quotaPartnerActive("p2"));

        assertNull(reg.rebindQuotaKeys("m", QuotaKeys.NONE), "back to anonymous releases everything");
        assertEquals(0, reg.quotaPartnerActive("p2"));
        assertEquals(QuotaKeys.NONE, reg.quotaKeysOf("m"));
    }

    @Test
    void rebind_of_an_unknown_or_terminated_request_throws() throws InterruptedException {
        Registry reg = build("rb-unknown", new QuotaLimits(1, 0, 0, 0));
        assertThrows(IllegalStateException.class, () -> reg.rebindQuotaKeys("nope", QuotaKeys.ofPartner("p")));

        dispatchAnon(reg, "gone");
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        stop(reg, "gone");
        assertThrows(IllegalStateException.class, () -> reg.rebindQuotaKeys("gone", QuotaKeys.ofPartner("p")));
        assertEquals(0, reg.quotaPartnerActive("p"));
    }

    @Test
    void dispatch_time_keys_are_the_old_keys_a_rebind_releases() throws InterruptedException {
        // A returning device (known MAC) binds at dispatch through the stock gate;
        // a later rebind (e.g. identity correction) must release THOSE keys.
        Registry reg = build("rb-known", new QuotaLimits(1, 0, 0, 0));
        assertTrue(reg.dispatch("known", new Task("u9", null)).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(1, reg.quotaPartnerActive("u9"));
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED,
            reg.dispatch("known-2", new Task("u9", null)).rejectCause());

        assertNull(reg.rebindQuotaKeys("known", QuotaKeys.ofPartner("u10")));
        assertEquals(0, reg.quotaPartnerActive("u9"));
        assertEquals(1, reg.quotaPartnerActive("u10"));
        assertTrue(reg.dispatch("known-2", new Task("u9", null)).accepted(), "u9's slot is free again");
    }

    // ─────────────────────────────────────────────────────────────
    // rebind — concurrency
    // ─────────────────────────────────────────────────────────────

    @Test
    void concurrent_rebind_race_admits_exactly_the_cap() throws Exception {
        final int N = 64, CAP = 3;
        Registry reg = build("rb-race", new QuotaLimits(CAP, 0, 0, 0));
        for (int i = 0; i < N; i++) dispatchAnon(reg, "r-" + i);
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));

        CountDownLatch go = new CountDownLatch(1);
        ConcurrentHashMap<String, RejectCause> results = new ConcurrentHashMap<>();
        AtomicInteger admitted = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final String id = "r-" + i;
            Thread t = new Thread(() -> {
                try { go.await(); } catch (InterruptedException e) { return; }
                RejectCause rc = reg.rebindQuotaKeys(id, QuotaKeys.ofPartner("hot-user"));
                if (rc == null) admitted.incrementAndGet(); else results.put(id, rc);
            });
            threads.add(t); t.start();
        }
        go.countDown();
        for (Thread t : threads) t.join(10_000);

        assertEquals(CAP, admitted.get(), "exactly the cap wins the race");
        assertEquals(N - CAP, results.size());
        results.values().forEach(rc -> assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED, rc));
        assertEquals(CAP, reg.quotaPartnerActive("hot-user"), "counter exact after the race");

        // the winners end → the slots free → the same number can bind again
        List<String> winners = new ArrayList<>();
        for (int i = 0; i < N; i++) if (!results.containsKey("r-" + i)) winners.add("r-" + i);
        for (String w : winners) reg.onInboundEvent(w, new Stop());
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals(0, reg.quotaPartnerActive("hot-user"));

        int again = 0;
        for (String id : results.keySet()) if (reg.rebindQuotaKeys(id, QuotaKeys.ofPartner("hot-user")) == null) again++;
        assertEquals(CAP, again);
        assertEquals(CAP, reg.quotaPartnerActive("hot-user"));
    }

    @Test
    void concurrent_flapping_rebinds_keep_the_counters_exact() throws Exception {
        final int N = 16, FLIPS = 200;
        Registry reg = build("rb-flap", new QuotaLimits(100, 100, 0, 0));
        for (int i = 0; i < N; i++) dispatchAnon(reg, "f-" + i);
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));

        IntStream.range(0, N).parallel().forEach(i -> {
            String id = "f-" + i;
            for (int k = 0; k < FLIPS; k++) {
                QuotaKeys target = (k % 2 == 0) ? QuotaKeys.of("A", "A@z") : QuotaKeys.of("B", "B@z");
                assertNull(reg.rebindQuotaKeys(id, target));
            }
        });
        // every machine ended on B (FLIPS is even → last k = FLIPS-1 is odd)
        assertEquals(0, reg.quotaPartnerActive("A"));
        assertEquals(N, reg.quotaPartnerActive("B"));
        assertEquals(0, reg.quotaRouteActive("A@z"));
        assertEquals(N, reg.quotaRouteActive("B@z"));

        for (int i = 0; i < N; i++) reg.onInboundEvent("f-" + i, new Stop());
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals(0, reg.quotaPartnerActive("B"));
        assertEquals(0, reg.quotaRouteActive("B@z"));
    }

    @Test
    void rebind_racing_a_terminal_release_never_leaks_a_slot() throws Exception {
        final int ROUNDS = 200;
        Registry reg = build("rb-term-race", new QuotaLimits(1000, 0, 0, 0));
        for (int i = 0; i < ROUNDS; i++) dispatchAnon(reg, "t-" + i);
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));

        IntStream.range(0, ROUNDS).parallel().forEach(i -> {
            String id = "t-" + i;
            reg.onInboundEvent(id, new Stop());
            try { reg.rebindQuotaKeys(id, QuotaKeys.ofPartner("late")); }
            catch (IllegalStateException alreadyGone) { /* lost the race — fine */ }
        });
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals(0, reg.activeIdCount());
        assertEquals(0, reg.quotaPartnerActive("late"), "whichever won, the slot is released at terminal");
    }

    // ─────────────────────────────────────────────────────────────
    // restore-path re-acquire
    // ─────────────────────────────────────────────────────────────

    @Test
    void startup_recovery_reacquires_quota_from_restored_contexts() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        Registry a = builder("rb-restore", new QuotaLimits(2, 0, 0, 0))
            .persistence(store).rehydrate(true).build();
        open.add(a);

        // one device bound at dispatch (known MAC), one bound later via rebind + Bind (identity
        // written into the context so the extractor recovers it after the restart)
        assertTrue(a.dispatch("s-1", new Task("u1", null)).accepted());
        dispatchAnon(a, "s-2");
        assertTrue(a.awaitIdle(5, TimeUnit.SECONDS));
        assertNull(a.rebindQuotaKeys("s-2", QuotaKeys.ofPartner("u1")));
        a.onInboundEvent("s-2", new Bind("u1", null));
        assertTrue(a.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(2, a.quotaPartnerActive("u1"));
        assertEquals(2, store.loadAllForRegistry("rb-restore").size(), "both live snapshots in the store");

        // "crash": a fresh registry on the same store (no shutdown → snapshots survive)
        Registry b = builder("rb-restore", new QuotaLimits(2, 0, 0, 0))
            .persistence(store).rehydrate(true).build();
        open.add(b);
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(b.hasAny("s-1") && b.hasAny("s-2"), "both sessions restored");

        assertEquals(2, b.quotaPartnerActive("u1"), "REGRESSION: counters must not restart at zero");
        assertEquals(QuotaKeys.ofPartner("u1"), b.quotaKeysOf("s-1"));
        assertEquals(QuotaKeys.ofPartner("u1"), b.quotaKeysOf("s-2"), "the REBOUND identity was recovered");
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED,
            b.dispatch("s-3", new Task("u1", null)).rejectCause(), "cap still enforced after restart");

        stop(b, "s-1");
        assertEquals(1, b.quotaPartnerActive("u1"));
        assertTrue(b.dispatch("s-3", new Task("u1", null)).accepted());
        stop(b, "s-2");
        stop(b, "s-3");
        assertEquals(0, b.quotaPartnerActive("u1"), "no phantom slot left behind by the re-acquire");
    }

    @Test
    void lazy_rehydration_reacquires_quota_too() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        Registry a = builder("rb-lazy", new QuotaLimits(1, 0, 0, 0))
            .persistence(store).rehydrate(true).build();
        open.add(a);
        assertTrue(a.dispatch("l-1", new Task("u1", null)).accepted());
        assertTrue(a.awaitIdle(5, TimeUnit.SECONDS));

        // a store that hides startup load-all: only the lazy (inbound-event) path can restore
        PersistenceProvider lazyOnly = new PersistenceProvider() {
            @Override public void save(MachineSnapshot s) { store.save(s); }
            @Override public Optional<MachineSnapshot> load(String id, String reg) { return store.load(id, reg); }
            @Override public List<MachineSnapshot> loadAll(String id) { return store.loadAll(id); }
            @Override public void delete(String id, String reg) { store.delete(id, reg); }
        };
        Registry b = builder("rb-lazy", new QuotaLimits(1, 0, 0, 0))
            .persistence(lazyOnly).rehydrate(true).build();
        open.add(b);
        assertFalse(b.hasAny("l-1"), "nothing restored at startup on purpose");
        assertEquals(0, b.quotaPartnerActive("u1"));

        b.onInboundEvent("l-1", new Ping());          // unknown id → lazy rehydrate → stay
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(b.hasAny("l-1"));
        assertEquals(1, b.quotaPartnerActive("u1"), "lazy restore re-acquired the slot");
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED,
            b.dispatch("l-2", new Task("u1", null)).rejectCause());

        stop(b, "l-1");
        assertEquals(0, b.quotaPartnerActive("u1"));
    }

    @Test
    void restored_machine_whose_timeout_matured_settles_and_releases_its_reacquired_slot() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        // hand-craft a matured snapshot (deadline in the past) for a bound identity
        String ctxB64 = com.telcobright.statewalk.v2.persistence.SnapshotSerializer
            .contextToBase64Json(new Task("u7", "u7@z"));
        store.save(new MachineSnapshot("old-1", "rb-matured", "RUNNING", Task.class.getName(), ctxB64,
            System.currentTimeMillis() - 10_000, "DONE", System.currentTimeMillis() - 5_000));

        Registry b = builder("rb-matured", new QuotaLimits(3, 3, 0, 0))
            .persistence(store).rehydrate(true).build();
        open.add(b);
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));

        assertFalse(b.hasAny("old-1"), "matured on restore → terminal → reclaimed");
        assertEquals(0, b.quotaPartnerActive("u7"), "re-acquired on restore, released at terminal — net zero");
        assertEquals(0, b.quotaRouteActive("u7@z"));
        assertEquals(0, store.size(), "terminal delete removed the snapshot");
    }

    @Test
    void restore_without_enforced_limits_keeps_counters_untouched() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        String ctxB64 = com.telcobright.statewalk.v2.persistence.SnapshotSerializer
            .contextToBase64Json(new Task("u8", null));
        store.save(new MachineSnapshot("n-1", "rb-nolimit", "RUNNING", Task.class.getName(), ctxB64,
            System.currentTimeMillis(), "DONE", System.currentTimeMillis() + 3_600_000L));

        Registry b = builder("rb-nolimit", QuotaLimits.UNLIMITED)
            .persistence(store).rehydrate(true).build();
        open.add(b);
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(b.hasAny("n-1"));
        assertEquals(0, b.quotaPartnerActive("u8"), "no enforcement → nothing is counted (dispatch parity)");
        stop(b, "n-1");
    }
}
