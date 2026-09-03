package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.event.StatemachineEvent;
import com.telcobright.statewalk.machine.Machine;
import com.telcobright.statewalk.persistence.MachineSnapshot;
import com.telcobright.statewalk.persistence.SnapshotSerializer;
import com.telcobright.statewalk.persistence.jdbc.JdbcPersistenceProvider;
import com.telcobright.statewalk.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * REAL-MySQL persistence + rehydration suite — runs the whole registry against
 * the MySQL in the local LXC (127.0.0.1:3306, db {@code statewalk_test}).
 * SELF-SKIPS when the server is unreachable, so CI without MySQL stays green.
 *
 * <p>Every "restart" here is the real thing: a SECOND registry built on the
 * same JDBC store while the first was never shut down (a crash, from the
 * store's point of view). Corner cases covered:
 *
 * <ol>
 *   <li>Snapshot on every transition AND on .stay() mutations — verified by
 *       reading the row back from MySQL.</li>
 *   <li>Crash → startup recovery: context (incl. stay mutations) intact,
 *       supervisor + child both restored, quota re-acquired, session
 *       completes on the new node, terminal delete lands in MySQL.</li>
 *   <li>Target-mode deadline matured during downtime → settles to the final
 *       target at restore; the saved state's entry is NOT replayed.</li>
 *   <li>Stay-mode deadline matured during downtime → immediate checkpoint,
 *       refreshed deadline written back to MySQL.</li>
 *   <li>.offline() suspend → row kept, machine evicted → lazy rehydration on
 *       the next inbound event, from MySQL.</li>
 *   <li>Stranded FINAL-state row (terminal delete lost in a crash) → treated
 *       as a tombstone at restore: purged, never resurrected.</li>
 *   <li>Corrupt context payload AND unknown saved state (deploy drift) →
 *       quarantined into the {@code _dead} table, live row gone, id blocked.</li>
 *   <li>Orphan child row without a supervisor row → quarantined at startup.</li>
 *   <li>Global lifetime deadline persisted in MySQL and enforced across the
 *       restart (matured during downtime → session ended).</li>
 * </ol>
 */
class MySqlRehydrationTest {

    private static final String TABLE = "sw_rehydration_test";
    private static MysqlDataSource ds;

    // ── events / contexts ───────────────────────────────────────────

    public record Advance(String u) implements StatemachineEvent {}
    public record Touch(String u)   implements StatemachineEvent {}
    public record Park(String u)    implements StatemachineEvent {}
    public record Resume(String u)  implements StatemachineEvent {}
    public record Stop(String u)    implements StatemachineEvent {}

    public static class Ctx {
        public String partner;
        public int touches;
        public int beats;
        public Ctx() {}
    }

    public static class ChildCtx { public int marks; public ChildCtx() {} }

    static final AtomicInteger RUNNING_ENTRIES = new AtomicInteger();
    static final AtomicInteger EXPIRED_ENTRIES = new AtomicInteger();

    private final List<StatemachineRegistry<Ctx>> open = new ArrayList<>();

    @BeforeAll
    static void connect() {
        try {
            MysqlDataSource d = new MysqlDataSource();
            d.setUrl("jdbc:mysql://127.0.0.1:3306/statewalk_test"
                + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=2000"
                + "&serverTimezone=UTC");
            d.setUser("root");
            d.setPassword("123456");
            try (Connection c = d.getConnection(); Statement s = c.createStatement()) {
                s.execute("SELECT 1");
                s.execute("DROP TABLE IF EXISTS " + TABLE);
                s.execute("DROP TABLE IF EXISTS " + TABLE + "_dead");
            }
            ds = d;
        } catch (Exception e) {
            ds = null;
        }
        assumeTrue(ds != null, "no MySQL on 127.0.0.1:3306 — skipping MySQL rehydration tests");
    }

    @AfterEach
    void tearDown() {
        for (var r : open) r.shutdown();
        open.clear();
    }

    private JdbcPersistenceProvider provider() {
        return new JdbcPersistenceProvider(ds, TABLE);
    }

    private StatemachineRegistry<Ctx> track(StatemachineRegistry<Ctx> r) { open.add(r); return r; }

    // ── graphs ──────────────────────────────────────────────────────

    private static StateMap sessionGraph() {
        return StateMap.builder()
            .initialState("RUNNING")
            .state("RUNNING").interim()
                .timeout(1, TimeUnit.HOURS, "EXPIRED")
                .onEntry(self -> RUNNING_ENTRIES.incrementAndGet())
                .stay(Touch.class, (self, e) -> ((Machine<Ctx>) self).getContext().touches++)
                .on(Advance.class, "PHASE2")
                .on(Park.class, "PARKED")
                .on(Stop.class, "DONE")
            .state("PHASE2").interim()
                .timeout(1, TimeUnit.HOURS, "EXPIRED")
                .stay(Touch.class, (self, e) -> ((Machine<Ctx>) self).getContext().touches++)
                .on(Stop.class, "DONE")
            .state("PARKED").interim().offline()
                .timeout(1, TimeUnit.HOURS, "EXPIRED")
                .on(Resume.class, "RUNNING")
                .stay(Touch.class, (self, e) -> ((Machine<Ctx>) self).getContext().touches++)
            .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
            .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
                .onEntry(self -> EXPIRED_ENTRIES.incrementAndGet())
            .build();
    }

    private static SupervisorSpec<Ctx> sessionSpec() {
        return SupervisorSpec.<Ctx>builder()
            .name("Sup").contextFactory(Ctx::new).stateMap(sessionGraph())
            .routes(r -> {
                r.selfHandle(Advance.class); r.selfHandle(Touch.class);
                r.selfHandle(Park.class); r.selfHandle(Resume.class); r.selfHandle(Stop.class);
            })
            .build();
    }

    private static MachineSpec<ChildCtx> childSpec() {
        return MachineSpec.<ChildCtx>builder()
            .name("Child").contextFactory(ChildCtx::new)
            .stateMap(StateMap.builder()
                .initialState("WORKING")
                .state("WORKING").interim().timeout(1, TimeUnit.HOURS, "CLOSED")
                .state("CLOSED").finalState().timeout(1, TimeUnit.SECONDS, "CLOSED")
                .build())
            .build();
    }

    private StatemachineRegistry<Ctx> buildNode(String regName, boolean withChild) {
        var b = StatemachineRegistry.<Ctx>builder(regName)
            .supervisor(sessionSpec(), 8);
        if (withChild) b.child(childSpec(), 8);
        return track(b
            .persistence(provider()).rehydrate(true)
            .preWarmContextClass(Ctx.class)
            .quotaKeysExtractor(t -> t.partner != null ? QuotaKeys.ofPartner(t.partner) : QuotaKeys.NONE)
            .quotaLimits(new QuotaLimits(10, 0, 0, 0))
            .threads(2)
            .build());
    }

    // ─────────────────────────────────────────────────────────────
    // (1) every transition + every .stay() lands in MySQL
    // ─────────────────────────────────────────────────────────────

    @Test
    void every_transition_and_stay_mutation_is_persisted_to_mysql() throws Exception {
        JdbcPersistenceProvider p = provider();
        var reg = buildNode("my-save", false);

        String id = "row-1";
        Ctx task = new Ctx(); task.partner = "acme";
        assertTrue(reg.dispatch(id, task).accepted());
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));

        MachineSnapshot afterStart = p.load(id, "my-save").orElseThrow();
        assertEquals("RUNNING", afterStart.currentState(), "initial transition persisted");

        reg.onInboundEvent(id, new Touch(id));
        reg.onInboundEvent(id, new Touch(id));
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        MachineSnapshot afterStays = p.load(id, "my-save").orElseThrow();
        Ctx persisted = (Ctx) SnapshotSerializer.contextFromBase64Json(
            afterStays.contextJsonBase64(), afterStays.contextClassName());
        assertEquals(2, persisted.touches, ".stay() mutations reached the MySQL row");
        assertEquals("acme", persisted.partner, "context JSON round-trips through MySQL");

        reg.onInboundEvent(id, new Advance(id));
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals("PHASE2", p.load(id, "my-save").orElseThrow().currentState());

        reg.onInboundEvent(id, new Stop(id));
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        assertTrue(p.load(id, "my-save").isEmpty(), "terminal delete removed the MySQL row");
    }

    // ─────────────────────────────────────────────────────────────
    // (2) crash → node B resumes: context + child + quota, then completes
    // ─────────────────────────────────────────────────────────────

    @Test
    void crash_failover_resumes_context_child_and_quota_then_completes() throws Exception {
        var nodeA = buildNode("my-failover", true);
        String id = "call-9";
        Ctx task = new Ctx(); task.partner = "u1";
        assertTrue(nodeA.dispatch(id, task).accepted());
        assertTrue(nodeA.awaitIdle(10, TimeUnit.SECONDS));
        // spawn a child cell + mutate the supervisor context in place
        ((Supervisor<?>) nodeA.findInternal(id, "Sup")).resolver().spawnChild("Child", new ChildCtx());
        nodeA.onInboundEvent(id, new Touch(id));
        nodeA.onInboundEvent(id, new Touch(id));
        nodeA.onInboundEvent(id, new Touch(id));
        assertTrue(nodeA.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals(2, nodeA.activeCellCount(), "supervisor + child live on node A");
        assertEquals(1, nodeA.quotaPartnerActive("u1"));

        // Node A CRASHES — no shutdown, its rows stay in MySQL. Node B takes over.
        open.remove(nodeA);   // deliberately NOT shut down (that would delete rows)
        var nodeB = buildNode("my-failover", true);
        assertTrue(nodeB.awaitIdle(10, TimeUnit.SECONDS));

        assertTrue(nodeB.hasAny(id), "node B resumed the session from MySQL at startup");
        assertEquals(2, nodeB.activeCellCount(), "supervisor AND child restored");
        Machine<?> sup = nodeB.findInternal(id, "Sup");
        assertEquals("RUNNING", sup.getCurrentState());
        assertEquals(3, ((Ctx) sup.getContext()).touches, "stay mutations survived the crash");
        assertEquals(1, nodeB.quotaPartnerActive("u1"),
            "quota slot re-acquired from the restored context — caps stay truthful");

        // The resumed session still works and finishes cleanly on node B.
        nodeB.onInboundEvent(id, new Touch(id));
        nodeB.onInboundEvent(id, new Stop(id));
        assertTrue(nodeB.awaitIdle(10, TimeUnit.SECONDS));
        assertFalse(nodeB.hasAny(id));
        assertEquals(0, nodeB.quotaPartnerActive("u1"), "slot released at terminal");
        assertEquals(0, provider().size(), "all rows (child included) deleted in MySQL");

        // silence the crashed node's executors
        nodeA.shutdown();
    }

    // ─────────────────────────────────────────────────────────────
    // (3) target-mode deadline matured during downtime
    // ─────────────────────────────────────────────────────────────

    @Test
    void matured_target_deadline_settles_at_restore_without_entry_replay() throws Exception {
        RUNNING_ENTRIES.set(0); EXPIRED_ENTRIES.set(0);
        JdbcPersistenceProvider p = provider();
        // A crashed node left this session in RUNNING; its 1h fallback fell due
        // 5 seconds ago (simulated long outage).
        Ctx prior = new Ctx(); prior.touches = 42;
        p.save(new MachineSnapshot("late-1", "my-matured", "RUNNING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(prior),
            System.currentTimeMillis() - 60_000L, "EXPIRED", System.currentTimeMillis() - 5_000L));

        var nodeB = buildNode("my-matured", false);
        assertTrue(nodeB.awaitIdle(10, TimeUnit.SECONDS));

        assertFalse(nodeB.hasAny("late-1"),
            "elapsed downtime counted: RUNNING's matured fallback drove it to EXPIRED → retired");
        assertEquals(0, RUNNING_ENTRIES.get(), "saved state's entry NOT replayed");
        assertEquals(1, EXPIRED_ENTRIES.get(), "target state's entry ran exactly once");
        assertTrue(p.load("late-1", "my-matured").isEmpty(), "row purged after settling");
    }

    // ─────────────────────────────────────────────────────────────
    // (4) stay-mode deadline matured during downtime
    // ─────────────────────────────────────────────────────────────

    @Test
    void matured_stay_deadline_checkpoints_at_restore_and_refreshes_mysql_row() throws Exception {
        JdbcPersistenceProvider p = provider();
        SupervisorSpec<Ctx> staySpec = SupervisorSpec.<Ctx>builder()
            .name("Sup").contextFactory(Ctx::new)
            .stateMap(StateMap.builder()
                .initialState("HOLDING")
                .state("HOLDING").interim()
                    .timeoutStay(60, TimeUnit.SECONDS, self -> ((Machine<Ctx>) self).getContext().beats++)
                    .on(Stop.class, "DONE")
                .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
                .build())
            .routes(r -> r.selfHandle(Stop.class))
            .build();
        // Stay-state row whose heartbeat deadline matured 10s ago.
        Ctx prior = new Ctx(); prior.beats = 3;
        p.save(new MachineSnapshot("hb-1", "my-stay", "HOLDING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(prior),
            System.currentTimeMillis() - 70_000L, null, System.currentTimeMillis() - 10_000L));

        var nodeB = track(StatemachineRegistry.<Ctx>builder("my-stay")
            .supervisor(staySpec, 4)
            .persistence(p).rehydrate(true)
            .threads(2)
            .build());
        assertTrue(nodeB.awaitIdle(10, TimeUnit.SECONDS));

        Machine<?> m = nodeB.findInternal("hb-1", "Sup");
        assertNotNull(m, "stay-mode session RESUMES — a matured heartbeat never kills it");
        assertEquals("HOLDING", m.getCurrentState());
        assertEquals(4, ((Ctx) m.getContext()).beats, "the missed checkpoint ran once at restore (3+1)");
        MachineSnapshot refreshed = p.load("hb-1", "my-stay").orElseThrow();
        assertTrue(refreshed.timeoutDeadlineMs() > System.currentTimeMillis(),
            "MySQL row now carries the RE-ARMED future deadline");
        Ctx persisted = (Ctx) SnapshotSerializer.contextFromBase64Json(
            refreshed.contextJsonBase64(), refreshed.contextClassName());
        assertEquals(4, persisted.beats, "checkpoint wrote the beat back to MySQL");
    }

    // ─────────────────────────────────────────────────────────────
    // (5) offline suspend → lazy rehydration on the next event
    // ─────────────────────────────────────────────────────────────

    @Test
    void offline_suspend_keeps_row_and_lazy_rehydrates_on_next_event() throws Exception {
        JdbcPersistenceProvider p = provider();
        var reg = buildNode("my-offline", false);

        String id = "park-1";
        assertTrue(reg.dispatch(id, new Ctx()).accepted());
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        reg.onInboundEvent(id, new Touch(id));
        reg.onInboundEvent(id, new Park(id));            // RUNNING → PARKED (.offline) → suspend
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));

        assertFalse(reg.hasAny(id), "machine evicted from memory on offline suspend");
        MachineSnapshot parked = p.load(id, "my-offline").orElseThrow();
        assertEquals("PARKED", parked.currentState(), "the MySQL row IS the suspended session");

        reg.onInboundEvent(id, new Resume(id));          // lazy rehydrate from MySQL, then Resume
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        Machine<?> m = reg.findInternal(id, "Sup");
        assertNotNull(m, "rehydrated on the inbound event");
        assertEquals("RUNNING", m.getCurrentState(), "Resume drove it out of the offline state");
        assertEquals(1, ((Ctx) m.getContext()).touches, "pre-suspend context intact");

        reg.onInboundEvent(id, new Stop(id));
        assertTrue(reg.awaitIdle(10, TimeUnit.SECONDS));
        assertTrue(p.load(id, "my-offline").isEmpty());
    }

    // ─────────────────────────────────────────────────────────────
    // (6) stranded FINAL row = tombstone
    // ─────────────────────────────────────────────────────────────

    @Test
    void stranded_final_state_row_is_purged_never_resurrected() throws Exception {
        RUNNING_ENTRIES.set(0); EXPIRED_ENTRIES.set(0);
        JdbcPersistenceProvider p = provider();
        // A crash between the terminal save and its delete stranded this row.
        p.save(new MachineSnapshot("dead-1", "my-tomb", "DONE",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(new Ctx()),
            System.currentTimeMillis(), "DONE", System.currentTimeMillis() + 1_000L));

        var nodeB = buildNode("my-tomb", false);
        assertTrue(nodeB.awaitIdle(10, TimeUnit.SECONDS));

        assertFalse(nodeB.hasAny("dead-1"), "finished session stays finished");
        assertEquals(0, RUNNING_ENTRIES.get() + EXPIRED_ENTRIES.get(),
            "no entry action of any state ran — no side-effect replay from a tombstone");
        assertTrue(p.load("dead-1", "my-tomb").isEmpty(), "tombstone purged from MySQL");
        assertEquals(0, nodeB.quotaPartnerActive("u1"), "no zombie quota");
    }

    // ─────────────────────────────────────────────────────────────
    // (7) corrupt payload + deploy drift → _dead table
    // ─────────────────────────────────────────────────────────────

    @Test
    void corrupt_and_drifted_rows_are_quarantined_into_dead_table() throws Exception {
        JdbcPersistenceProvider p = provider();
        // (a) corrupt: context class that does not exist on this classpath.
        p.save(new MachineSnapshot("bad-ctx", "my-dead", "RUNNING",
            "com.nonexistent.RenamedContext", "AAAA",
            System.currentTimeMillis(), "EXPIRED", System.currentTimeMillis() + 3_600_000L));
        // (b) drift: a state name this build's graph does not declare.
        p.save(new MachineSnapshot("bad-state", "my-dead", "STATE_FROM_OLD_BUILD",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(new Ctx()),
            System.currentTimeMillis(), "EXPIRED", System.currentTimeMillis() + 3_600_000L));

        var nodeB = buildNode("my-dead", false);
        assertTrue(nodeB.awaitIdle(10, TimeUnit.SECONDS));

        assertFalse(nodeB.hasAny("bad-ctx"));
        assertFalse(nodeB.hasAny("bad-state"));
        assertEquals(0, p.size(), "both live rows moved OUT of the live table");
        // count only THIS registry's dead rows — the _dead table is shared by the class
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT COUNT(*) FROM " + TABLE + "_dead WHERE registry_name=?")) {
            ps.setString(1, "my-dead");
            try (var rs = ps.executeQuery()) {
                rs.next();
                assertEquals(2, rs.getInt(1),
                    "…and INTO " + TABLE + "_dead with their reasons — data preserved for a fixed build");
            }
        }

        // The dead rows carry the failure reason for forensics.
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT dead_reason FROM " + TABLE + "_dead WHERE machine_id=?")) {
            ps.setString(1, "bad-state");
            try (var rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getString(1).contains("STATE_FROM_OLD_BUILD"),
                    "reason names the unknown state: " + rs.getString(1));
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    // (7b) quarantine is idempotent — a repeat never destroys the dead row
    // ─────────────────────────────────────────────────────────────

    @Test
    void repeated_quarantine_keeps_the_dead_row() throws Exception {
        JdbcPersistenceProvider p = provider();
        p.save(new MachineSnapshot("rq-1", "my-requarantine", "RUNNING",
            Ctx.class.getName(), SnapshotSerializer.contextToBase64Json(new Ctx()),
            System.currentTimeMillis(), "EXPIRED", System.currentTimeMillis() + 3_600_000L));
        p.quarantine("rq-1", "my-requarantine", "first");
        p.quarantine("rq-1", "my-requarantine", "again");     // live row already gone
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT dead_reason FROM " + TABLE + "_dead WHERE machine_id=? AND registry_name=?")) {
            ps.setString(1, "rq-1");
            ps.setString(2, "my-requarantine");
            try (var rs = ps.executeQuery()) {
                assertTrue(rs.next(), "the dead row must survive a repeated quarantine (it did NOT before the fix)");
                assertEquals("first", rs.getString(1), "and keep the ORIGINAL reason");
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    // (8) orphan child row without a supervisor row
    // ─────────────────────────────────────────────────────────────

    @Test
    void orphan_child_row_without_supervisor_is_quarantined_at_startup() throws Exception {
        JdbcPersistenceProvider p = provider();
        p.save(new MachineSnapshot("lost-9#Child", "my-orphan", "WORKING",
            ChildCtx.class.getName(), SnapshotSerializer.contextToBase64Json(new ChildCtx()),
            System.currentTimeMillis(), "CLOSED", System.currentTimeMillis() + 3_600_000L));

        var nodeB = buildNode("my-orphan", true);
        assertTrue(nodeB.awaitIdle(10, TimeUnit.SECONDS));

        assertFalse(nodeB.hasAny("lost-9"), "an unroutable child alone is never restored");
        assertTrue(p.load("lost-9#Child", "my-orphan").isEmpty(), "orphan out of the live table");
        assertTrue(p.deadSize() >= 1, "orphan preserved in the dead-letter table");
    }

    // ─────────────────────────────────────────────────────────────
    // (9) global lifetime cap persisted + enforced across restart
    // ─────────────────────────────────────────────────────────────

    @Test
    void global_deadline_is_persisted_in_mysql_and_enforced_after_restart() throws Exception {
        JdbcPersistenceProvider p = provider();
        var nodeA = track(StatemachineRegistry.<Ctx>builder("my-gto")
            .supervisor(sessionSpec(), 4)
            .persistence(p).rehydrate(true)
            .globalTimeout(800, TimeUnit.MILLISECONDS, "EXPIRED")
            .threads(2)
            .build());
        assertTrue(nodeA.dispatch("g-9", new Ctx()).accepted());
        assertTrue(nodeA.awaitIdle(10, TimeUnit.SECONDS));
        assertTrue(p.load("g-9", "my-gto").orElseThrow().globalDeadlineMs() > 0,
            "the lifetime cap is IN the MySQL row");

        // crash; the cap matures during downtime
        open.remove(nodeA);
        Thread.sleep(1_000);

        var nodeB = track(StatemachineRegistry.<Ctx>builder("my-gto")
            .supervisor(sessionSpec(), 4)
            .persistence(p).rehydrate(true)
            .globalTimeout(800, TimeUnit.MILLISECONDS, "EXPIRED")
            .threads(2)
            .build());
        Thread.sleep(300);   // room for the re-armed (already matured) timer
        assertTrue(nodeB.awaitIdle(10, TimeUnit.SECONDS));

        assertFalse(nodeB.hasAny("g-9"),
            "restored session was ended by its PERSISTED lifetime cap — no eternal sessions");
        assertTrue(p.load("g-9", "my-gto").isEmpty());
        nodeA.shutdown();
    }
}
