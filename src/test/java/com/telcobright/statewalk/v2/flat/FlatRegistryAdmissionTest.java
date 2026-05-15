package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.channel.Channel;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.registry.api.DispatchResult;
import com.telcobright.statewalk.v2.registry.api.QuotaKeys;
import com.telcobright.statewalk.v2.registry.api.QuotaLimits;
import com.telcobright.statewalk.v2.registry.api.RejectCause;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Admission + lifecycle gates added to {@link Registry}:
 *
 * <ol>
 *   <li>DispatchResult — accept/reject signal with cause.</li>
 *   <li>maxConcurrent — cap on active supervisor cells.</li>
 *   <li>globalTimeout — wall-clock cap → forced transition to final state.</li>
 *   <li>Quota — per-partner concurrent + TPS, per-route concurrent + TPS.</li>
 *   <li>Debug sampling — 1-in-N machines flagged debugMode=true.</li>
 *   <li>Channel SPI — exposed via {@code reg.getChannel()}.</li>
 * </ol>
 */
class FlatRegistryAdmissionTest {

    // ── tasks/events ────────────────────────────────────────────────

    public record Stop(String uuid) implements StatemachineEvent {}

    public static class Task {
        public String partner;
        public String route;
        public Task() {}
        public Task(String p, String r) { partner = p; route = r; }
    }

    // ── supervisor — stays in RUNNING until Stop, then DONE ─────────

    public static class AdmSupervisor extends Supervisor<Task> {
        @Override
        protected void defineRoutes(InternalEventResolver r) {
            r.selfHandle(Stop.class);
        }

        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("RUNNING")
                .state("RUNNING")
                    .interim()
                    .timeout(1, TimeUnit.HOURS, "DONE")
                    .on(Stop.class, "DONE")
                .state("DONE")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "DONE")
                .state("EXPIRED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "EXPIRED")
                .build();
        }
        @Override protected Task createContext() { return new Task(); }
    }

    // ── fixtures ────────────────────────────────────────────────────

    private Registry reg;

    @AfterEach
    void tearDown() { if (reg != null) reg.shutdown(); }

    // ─────────────────────────────────────────────────────────────
    // (1) DispatchResult — ok + duplicate id
    // ─────────────────────────────────────────────────────────────

    @Test
    void dispatch_returns_ok_then_duplicate_on_second_call() {
        reg = Registry.builder("adm-1")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 4)
            .build();

        DispatchResult first = reg.dispatch("id-1", new Task());
        assertTrue(first.accepted());
        assertNull(first.rejectCause());

        DispatchResult second = reg.dispatch("id-1", new Task());
        assertFalse(second.accepted());
        assertEquals(RejectCause.DUPLICATE_ID, second.rejectCause());
    }

    @Test
    void dispatch_returns_shutting_down_after_shutdown() {
        reg = Registry.builder("adm-shut")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 4)
            .build();
        reg.shutdown();

        DispatchResult r = reg.dispatch("x", new Task());
        assertEquals(RejectCause.SHUTTING_DOWN, r.rejectCause());
        reg = null; // shutdown already invoked
    }

    // ─────────────────────────────────────────────────────────────
    // (2) maxConcurrent
    // ─────────────────────────────────────────────────────────────

    @Test
    void max_concurrent_cap_rejects_overflow() {
        reg = Registry.builder("adm-cap")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 8)
            .maxConcurrent(2)
            .build();

        assertTrue(reg.dispatch("a", new Task()).accepted());
        assertTrue(reg.dispatch("b", new Task()).accepted());
        DispatchResult third = reg.dispatch("c", new Task());
        assertFalse(third.accepted());
        assertEquals(RejectCause.CAPACITY_EXCEEDED, third.rejectCause());
    }

    // ─────────────────────────────────────────────────────────────
    // (3) globalTimeout — wall-clock cap forces transition
    // ─────────────────────────────────────────────────────────────

    @Test
    void global_timeout_forces_transition_to_target_state() throws InterruptedException {
        reg = Registry.builder("adm-gto")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 4)
            .globalTimeout(200, TimeUnit.MILLISECONDS, "EXPIRED")
            .build();

        reg.dispatch("g-1", new Task());
        // Wait for timeout to fire and the supervisor to transition.
        Thread.sleep(500);
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        // Supervisor reached EXPIRED → terminal ritual → cell removed.
        assertEquals(0, reg.activeCellCount(), "supervisor terminated by global timeout");
    }

    // ─────────────────────────────────────────────────────────────
    // (4) Quota — per-partner concurrent
    // ─────────────────────────────────────────────────────────────

    @Test
    void per_partner_concurrent_quota_rejects_overflow() {
        reg = Registry.builder("adm-q")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 8)
            .quotaKeysExtractor(t -> {
                Task task = (Task) t;
                return QuotaKeys.of(task.partner, task.route);
            })
            .quotaLimits(new QuotaLimits(/*partner concurrent*/ 2, 0, 0, 0))
            .build();

        assertTrue(reg.dispatch("a", new Task("partnerX", null)).accepted());
        assertTrue(reg.dispatch("b", new Task("partnerX", null)).accepted());
        DispatchResult third = reg.dispatch("c", new Task("partnerX", null));
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED, third.rejectCause());

        // Different partner is unaffected
        assertTrue(reg.dispatch("d", new Task("partnerY", null)).accepted());
    }

    @Test
    void per_route_tps_quota_rejects_burst() {
        reg = Registry.builder("adm-q-tps")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 16)
            .quotaKeysExtractor(t -> {
                Task task = (Task) t;
                return QuotaKeys.of(task.partner, task.route);
            })
            .quotaLimits(new QuotaLimits(0, 0, 0, /*route tps*/ 3))
            .build();

        // 3 succeed within one second
        assertTrue(reg.dispatch("t1", new Task(null, "route-A")).accepted());
        assertTrue(reg.dispatch("t2", new Task(null, "route-A")).accepted());
        assertTrue(reg.dispatch("t3", new Task(null, "route-A")).accepted());
        // 4th in same second → rejected
        DispatchResult fourth = reg.dispatch("t4", new Task(null, "route-A"));
        assertEquals(RejectCause.ROUTE_TPS_EXCEEDED, fourth.rejectCause());
    }

    @Test
    void quota_release_on_terminate_frees_slot() throws InterruptedException {
        reg = Registry.builder("adm-q-rel")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 8)
            .quotaKeysExtractor(t -> QuotaKeys.ofPartner(((Task) t).partner))
            .quotaLimits(new QuotaLimits(1, 0, 0, 0))
            .build();

        assertTrue(reg.dispatch("r1", new Task("p", null)).accepted());
        // Second attempt blocked
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED,
            reg.dispatch("r2", new Task("p", null)).rejectCause());

        // Terminate r1
        reg.onInboundEvent("r1", new Stop("r1"));
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        // Now p's slot is free again
        assertTrue(reg.dispatch("r3", new Task("p", null)).accepted());
    }

    @Test
    void quota_keys_extractor_required_when_limits_set() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Registry.builder("adm-q-misconfig")
                .supervisor(AdmSupervisor.class, AdmSupervisor::new, 4)
                .quotaLimits(new QuotaLimits(1, 0, 0, 0))
                .build());
        assertTrue(ex.getMessage().contains("quotaKeysExtractor"));
    }

    // ─────────────────────────────────────────────────────────────
    // (5) Debug sampling — every Nth supervisor flagged
    // ─────────────────────────────────────────────────────────────

    @Test
    void debug_sampling_flags_one_in_n() {
        reg = Registry.builder("adm-dbg")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 16)
            .debugSampleRate(3)
            .build();

        AtomicInteger debugCount = new AtomicInteger();
        for (int i = 0; i < 9; i++) {
            String id = "d-" + i;
            reg.dispatch(id, new Task());
            Machine<?> m = reg.findInternal(id, AdmSupervisor.class);
            if (m != null && m.isDebugMode()) debugCount.incrementAndGet();
        }
        // 0, 3, 6 → exactly 3 debug-mode supervisors out of 9 dispatched.
        assertEquals(3, debugCount.get(), "1-in-3 sampling produces 3 debug supervisors out of 9");
    }

    // ─────────────────────────────────────────────────────────────
    // (6) Channel SPI — registry exposes the configured channel
    // ─────────────────────────────────────────────────────────────

    @Test
    void channel_is_exposed_via_get_channel() {
        FakeChannel channel = new FakeChannel("test-ch");
        reg = Registry.builder("adm-ch")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 4)
            .channel(channel)
            .build();

        Channel<?, ?> exposed = reg.getChannel();
        assertSame(channel, exposed);
        assertEquals("test-ch", exposed.getName());
    }

    @Test
    void channel_defaults_to_null_when_unconfigured() {
        reg = Registry.builder("adm-no-ch")
            .supervisor(AdmSupervisor.class, AdmSupervisor::new, 4)
            .build();
        assertNull(reg.getChannel());
    }

    // ── fake channel used by the (6) tests ─────────────────────────

    static class FakeChannel implements Channel<String, String> {
        private final String name;
        FakeChannel(String name) { this.name = name; }
        @Override public void send(String requestId, String command) {}
        @Override public void cancel(String requestId) {}
        @Override public void onInbound(BiConsumer<String, String> handler) {}
        @Override public boolean isConnected() { return true; }
        @Override public String getName() { return name; }
    }
}
