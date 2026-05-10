package com.telcobright.statewalk.v2;

import com.telcobright.statewalk.v2.admission.DispatchResult;
import com.telcobright.statewalk.v2.admission.QuotaKeys;
import com.telcobright.statewalk.v2.admission.QuotaLimits;
import com.telcobright.statewalk.v2.admission.RejectCause;
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
 * Per-partner and per-route quota — concurrent + TPS dimensions. Verifies
 * dispatch returns {@link DispatchResult} with the correct {@link RejectCause}
 * when a quota is exceeded, and that releasing a machine restores capacity.
 */
class QuotaTest {

    public record QTask(String partner, String route) {}

    public static class QCtx {
        public QCtx() {}
    }

    public record GoOffline() implements StatemachineEvent {}

    static class QMachine extends Machine<QTask, QCtx> {
        @Override protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("ACTIVE")
                .state("ACTIVE")
                    .interim()
                    .timeout(60, TimeUnit.SECONDS, "DONE")
                    .on(GoOffline.class, "DONE")
                .state("DONE")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "DONE")
                .build();
        }
        @Override protected QCtx createContext() { return new QCtx(); }
    }

    static class QRegistry extends Registry<QMachine, QCtx> {
        public QuotaLimits limits = QuotaLimits.UNLIMITED;
        @Override protected String getRegistryName()   { return "q"; }
        @Override protected int getMaxConcurrent()     { return 100; }
        @Override protected long getGlobalTimeoutMs()  { return 60_000L; }
        @Override protected QMachine createMachineTemplate() { return new QMachine(); }

        @Override protected QuotaKeys quotaKeysFor(Object task) {
            QTask t = (QTask) task;
            return QuotaKeys.of(t.partner(), t.route());
        }
        @Override protected QuotaLimits getQuotaLimits() { return limits; }
    }

    private static final Duration AWAIT = Duration.ofSeconds(2);
    private QRegistry reg;
    private TestChannel<Object, StatemachineEvent> channel;
    private StatewalkSystem system;

    @BeforeEach
    void setUp() {
        reg = new QRegistry();
        channel = new TestChannel<>("q-channel");
    }

    private StatewalkSystem build(QuotaLimits limits) {
        reg.limits = limits;
        return Statewalk.builder()
            .registerEvent(GoOffline.class)
            .registry("q", reg, 32, 2)
            .channel("q", channel)
            .build();
    }

    @AfterEach
    void tearDown() {
        if (system != null) system.shutdown();
    }

    // ── tests ────────────────────────────────────────────────────────

    @Test
    void no_limits_means_no_rejections() {
        system = build(QuotaLimits.UNLIMITED);
        for (int i = 0; i < 20; i++) {
            DispatchResult r = reg.dispatchWithResult("a-" + i, new QTask("pA", "r1"));
            assertTrue(r.accepted());
            assertNull(r.rejectCause());
        }
    }

    @Test
    void partner_concurrency_limit_rejects_excess() throws InterruptedException {
        system = build(new QuotaLimits(/*partnerConc*/ 3, 0, 0, 0));

        for (int i = 0; i < 3; i++) {
            assertTrue(reg.dispatchWithResult("p-" + i, new QTask("pA", "r1")).accepted());
        }
        // 4th from same partner is rejected
        DispatchResult r = reg.dispatchWithResult("p-3", new QTask("pA", "r1"));
        assertFalse(r.accepted());
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED, r.rejectCause());

        // Different partner — accepted
        assertTrue(reg.dispatchWithResult("p-4", new QTask("pB", "r1")).accepted());

        // Terminate one of pA's machines → frees a slot.
        channel.inject("p-0", new GoOffline());
        system.awaitIdle(AWAIT);
        // Now pA can dispatch again.
        assertTrue(reg.dispatchWithResult("p-5", new QTask("pA", "r1")).accepted());
    }

    @Test
    void route_concurrency_limit_rejects_excess() {
        system = build(new QuotaLimits(0, /*routeConc*/ 2, 0, 0));

        assertTrue(reg.dispatchWithResult("r-1", new QTask("p1", "rA")).accepted());
        assertTrue(reg.dispatchWithResult("r-2", new QTask("p2", "rA")).accepted());
        DispatchResult r = reg.dispatchWithResult("r-3", new QTask("p3", "rA"));
        assertFalse(r.accepted());
        assertEquals(RejectCause.ROUTE_CONCURRENCY_EXCEEDED, r.rejectCause());

        // Different route — accepted
        assertTrue(reg.dispatchWithResult("r-4", new QTask("p1", "rB")).accepted());
    }

    @Test
    void partner_tps_limit_rejects_burst() {
        system = build(new QuotaLimits(0, 0, /*partnerTps*/ 5, 0));

        for (int i = 0; i < 5; i++) {
            DispatchResult r = reg.dispatchWithResult("t-" + i, new QTask("pA", "r1"));
            assertTrue(r.accepted(), "first 5 within partner TPS should accept (i=" + i + ")");
        }
        DispatchResult r = reg.dispatchWithResult("t-overflow", new QTask("pA", "r1"));
        assertFalse(r.accepted());
        assertEquals(RejectCause.PARTNER_TPS_EXCEEDED, r.rejectCause());

        // Different partner is independent
        assertTrue(reg.dispatchWithResult("t-other", new QTask("pB", "r1")).accepted());
    }

    @Test
    void route_tps_limit_rejects_burst() {
        system = build(new QuotaLimits(0, 0, 0, /*routeTps*/ 3));

        for (int i = 0; i < 3; i++) {
            assertTrue(reg.dispatchWithResult("u-" + i, new QTask("p" + i, "rA")).accepted());
        }
        DispatchResult r = reg.dispatchWithResult("u-overflow", new QTask("pX", "rA"));
        assertFalse(r.accepted());
        assertEquals(RejectCause.ROUTE_TPS_EXCEEDED, r.rejectCause());
    }

    @Test
    void rejection_does_not_leak_quota_slots() throws InterruptedException {
        // Limits: partner=2, route=10. Two partner slots used → rejection on 3rd
        // from the same partner. The rejection MUST roll back any partial
        // acquires (e.g., the partner concurrent counter increment that ran
        // before the route check noticed there's no problem).
        system = build(new QuotaLimits(2, 10, 0, 0));

        assertTrue(reg.dispatchWithResult("x-1", new QTask("pA", "rA")).accepted());
        assertTrue(reg.dispatchWithResult("x-2", new QTask("pA", "rA")).accepted());

        // Reject many times — each must release.
        for (int i = 0; i < 5; i++) {
            DispatchResult r = reg.dispatchWithResult("x-rej-" + i, new QTask("pA", "rA"));
            assertFalse(r.accepted());
            assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED, r.rejectCause());
        }

        // Free one pA slot → can take one more.
        channel.inject("x-1", new GoOffline());
        system.awaitIdle(AWAIT);
        assertTrue(reg.dispatchWithResult("x-3", new QTask("pA", "rA")).accepted(),
            "after release, the next dispatch must be admitted; if not, quota was leaked by rejections");
    }

    @Test
    void duplicate_id_rejection_returns_proper_cause() {
        system = build(QuotaLimits.UNLIMITED);
        assertTrue(reg.dispatchWithResult("dup", new QTask("p", "r")).accepted());
        DispatchResult r = reg.dispatchWithResult("dup", new QTask("p", "r"));
        assertFalse(r.accepted());
        assertEquals(RejectCause.DUPLICATE_ID, r.rejectCause());
    }

    @Test
    void capacity_rejection_when_max_concurrent_reached() {
        // Slim registry: maxConcurrent=2 (override via subclass).
        QRegistry slim = new QRegistry() {
            @Override protected int getMaxConcurrent() { return 2; }
        };
        system = Statewalk.builder()
            .registerEvent(GoOffline.class)
            .registry("q", slim, 8, 2)
            .channel("q", channel)
            .build();
        assertTrue(slim.dispatchWithResult("c-1", new QTask("p", "r")).accepted());
        assertTrue(slim.dispatchWithResult("c-2", new QTask("p", "r")).accepted());
        DispatchResult r = slim.dispatchWithResult("c-3", new QTask("p", "r"));
        assertFalse(r.accepted());
        assertEquals(RejectCause.CAPACITY_EXCEEDED, r.rejectCause());
    }
}
