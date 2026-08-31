package com.telcobright.statewalk.v2.session;

import com.telcobright.statewalk.v2.flat.InternalEventResolver;
import com.telcobright.statewalk.v2.flat.Registry;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.session.events.ServiceEnd;
import com.telcobright.statewalk.v2.session.events.SettleRequest;
import com.telcobright.statewalk.v2.session.events.Settled;
import com.telcobright.statewalk.v2.session.events.SignalingDone;
import com.telcobright.statewalk.v2.session.events.SignalingFailed;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The generic base contract, driven by a fake protocol: one SDR from BOTH
 * terminals, full-cell history, silent sessions ejected as FAILED, sync and
 * async settlement.
 */
class SessionSupervisorBaseTest {

    // ── fake wire events ────────────────────────────────────────────
    public record GoOnline() implements StatemachineEvent {}
    public record FailNow() implements StatemachineEvent {}

    // ── fake domain context ─────────────────────────────────────────
    public static class TestCtx extends SessionContext {
        public boolean admitReject;
        public boolean noBudget;
        public Object grant;
        public Object totals;
    }

    public static class SigCtx implements HistoryCarrier {
        public final SessionHistory history;
        public SigCtx(SessionHistory h) { this.history = h; }
        @Override public SessionHistory history() { return history; }
        @Override public String historyName() { return "signaling"; }
    }

    public static class BudCtx implements HistoryCarrier {
        public final SessionHistory history;
        public BudCtx(SessionHistory h) { this.history = h; }
        @Override public SessionHistory history() { return history; }
        @Override public String historyName() { return "budget"; }
    }

    /** Fake signaling child: GoOnline → GRANTED (publishes SignalingDone); FailNow → NO_AUTH (publishes SignalingFailed). */
    public static class Sig extends RecordingMachine<SigCtx> {
        @Override protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("RUN")
                .state("RUN").interim().timeout(60, TimeUnit.SECONDS, "NO_AUTH")
                    .on(GoOnline.class, "GRANTED")
                    .on(FailNow.class, "NO_AUTH")
                .state("GRANTED").finalState().timeout(1, TimeUnit.SECONDS, "GRANTED")
                    .onEntry(self -> ((Sig) self).publishEvent(new SignalingDone("grant-1")))
                .state("NO_AUTH").finalState().timeout(1, TimeUnit.SECONDS, "NO_AUTH")
                    .onEntry(self -> ((Sig) self).publishEvent(new SignalingFailed("sig-fail")))
                .build();
        }
        @Override protected SigCtx createContext() { return null; }
    }

    /** Fake budget child: answers SettleRequest with Settled totals. */
    public static class Budget extends RecordingMachine<BudCtx> {
        @Override protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("METERING")
                .state("METERING").interim().timeout(60, TimeUnit.SECONDS, "CLOSED")
                    .on(SettleRequest.class, "CLOSED")
                .state("CLOSED").finalState().timeout(1, TimeUnit.SECONDS, "CLOSED")
                    .onEntry(self -> ((Budget) self).publishEvent(new Settled("totals-1")))
                .build();
        }
        @Override protected BudCtx createContext() { return null; }
    }

    /** The fake protocol supervisor on the base. */
    public static class TestSupervisor extends SessionSupervisor<TestCtx> {
        private final SdrSink sink;
        public TestSupervisor(SdrSink sink) { this.sink = sink; }

        @Override protected SessionTimings timings() { return new SessionTimings(5, 2, 60, 5); }

        @Override protected AdmissionVerdict runAdmission(TestCtx ctx) {
            ctx.history.note("supervisor", "admission ran");
            return ctx.admitReject ? AdmissionVerdict.reject("no-balance") : AdmissionVerdict.accept("ok");
        }

        @Override protected void spawnChildren(InternalEventResolver r, TestCtx ctx) {
            r.spawnChild("Sig", new SigCtx(ctx.history));
            if (!ctx.noBudget) r.spawnChild("Budget", new BudCtx(ctx.history));
        }

        @Override protected void onActive(TestCtx ctx) { ctx.history.note("supervisor", "service delivered"); }
        @Override protected void onTeardown(TestCtx ctx) { ctx.history.note("supervisor", "service stopped"); }
        @Override protected void onSignalingDone(TestCtx ctx, Object grant) { ctx.grant = grant; }
        @Override protected void onSettled(TestCtx ctx, Object totals) { ctx.totals = totals; }
        @Override protected boolean settlesAsync() { return !getContext().noBudget; }
        @Override protected Object buildSdr(TestCtx ctx, String outcome) { return "domain:" + outcome; }
        @Override protected SdrSink sdrSink() { return sink; }

        @Override protected void defineDomainRoutes(InternalEventResolver r) {
            r.forwardTo("Sig", GoOnline.class);
            r.forwardTo("Sig", FailNow.class);
            r.forwardTo("Budget", SettleRequest.class);
        }
    }

    // ── harness ─────────────────────────────────────────────────────

    private final List<Registry> open = new ArrayList<>();
    private final List<SdrRecord> sdrs = new CopyOnWriteArrayList<>();

    private Registry build() {
        Registry r = Registry.builder("session-base-test")
            .supervisor("TestSupervisor", () -> new TestSupervisor(sdrs::add), 16)
            .child("Sig", Sig::new, 16)
            .child("Budget", Budget::new, 16)
            .threads(2)
            .build();
        open.add(r);
        return r;
    }

    @AfterEach
    void tearDown() { for (Registry r : open) r.shutdown(); }

    private SdrRecord awaitSdr(long timeoutMs) throws InterruptedException {
        long until = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < until) {
            if (!sdrs.isEmpty()) return sdrs.get(0);
            Thread.sleep(50);
        }
        fail("no SDR written within " + timeoutMs + " ms");
        return null;
    }

    private static List<String> supervisorHops(SdrRecord sdr) {
        List<String> hops = new ArrayList<>();
        for (TransitionRecord t : sdr.history()) {
            if (!t.isNote() && "supervisor".equals(t.machine())) hops.add(t.fromState() + ">" + t.toState());
        }
        return hops;
    }

    // ── tests ───────────────────────────────────────────────────────

    @Test
    void full_success_path_writes_one_sdr_with_full_cell_history() throws Exception {
        Registry reg = build();
        TestCtx ctx = new TestCtx();
        ctx.sessionKey = "k-1";
        assertTrue(reg.dispatch("k-1", ctx).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        reg.onInboundEvent("k-1", new GoOnline());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        reg.onInboundEvent("k-1", new ServiceEnd("deauth"));

        SdrRecord sdr = awaitSdr(5000);
        assertEquals(1, sdrs.size());
        assertEquals("SUCCEEDED", sdr.outcome());
        assertEquals("deauth", sdr.endCause());
        assertTrue(sdr.activatedAtMs() > 0);
        assertEquals(1, sdr.attempts());
        assertEquals("domain:SUCCEEDED", sdr.domain());
        assertEquals("totals-1", ctx.totals);
        assertEquals("grant-1", ctx.grant);

        List<String> hops = supervisorHops(sdr);
        assertTrue(hops.contains("ADMITTING>ADMITTED"), hops.toString());
        assertTrue(hops.contains("ADMITTED>ACTIVE"), hops.toString());
        assertTrue(hops.contains("ACTIVE>TEARING_DOWN"), hops.toString());
        assertTrue(hops.contains("TEARING_DOWN>SUCCEEDED"), hops.toString());
        assertTrue(sdr.history().stream().anyMatch(t -> "signaling".equals(t.machine()) && !t.isNote()),
            "signaling child transitions must be in the one history");
        assertTrue(sdr.history().stream().anyMatch(t -> "budget".equals(t.machine()) && !t.isNote()),
            "budget child transitions must be in the one history");
    }

    @Test
    void admission_reject_ends_failed_with_sdr() throws Exception {
        Registry reg = build();
        TestCtx ctx = new TestCtx();
        ctx.sessionKey = "k-2";
        ctx.admitReject = true;
        assertTrue(reg.dispatch("k-2", ctx).accepted());

        SdrRecord sdr = awaitSdr(5000);
        assertEquals(1, sdrs.size());
        assertEquals("FAILED", sdr.outcome());
        assertEquals("no-balance", sdr.endCause());
        assertEquals(0, sdr.activatedAtMs());
    }

    @Test
    void silent_session_is_ejected_as_failed_call_with_sdr() throws Exception {
        Registry reg = build();
        TestCtx ctx = new TestCtx();
        ctx.sessionKey = "k-3";
        assertTrue(reg.dispatch("k-3", ctx).accepted());
        // nothing else happens — ADMITTED times out after 2 s

        SdrRecord sdr = awaitSdr(6000);
        assertEquals("FAILED", sdr.outcome());
        assertEquals("silent", sdr.endCause());
        assertEquals(0, sdr.activatedAtMs());
        assertTrue(supervisorHops(sdr).contains("ADMITTED>FAILED"), supervisorHops(sdr).toString());
    }

    @Test
    void signaling_failure_without_retry_ends_failed_with_cause() throws Exception {
        Registry reg = build();
        TestCtx ctx = new TestCtx();
        ctx.sessionKey = "k-4";
        assertTrue(reg.dispatch("k-4", ctx).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        reg.onInboundEvent("k-4", new FailNow());

        SdrRecord sdr = awaitSdr(5000);
        assertEquals("FAILED", sdr.outcome());
        assertEquals("sig-fail", sdr.endCause());
    }

    @Test
    void sync_settle_domain_completes_without_budget_child() throws Exception {
        Registry reg = build();
        TestCtx ctx = new TestCtx();
        ctx.sessionKey = "k-5";
        ctx.noBudget = true;
        assertTrue(reg.dispatch("k-5", ctx).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));

        reg.onInboundEvent("k-5", new GoOnline());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        reg.onInboundEvent("k-5", new ServiceEnd("cap"));

        SdrRecord sdr = awaitSdr(5000);
        assertEquals("SUCCEEDED", sdr.outcome());
        assertEquals("cap", sdr.endCause());
    }
}
