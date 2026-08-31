package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.registry.api.DispatchResult;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end coverage of the spec-based registration path: no subclasses, no
 * Class refs in the builder, supervisor + children all configured by passing
 * {@link SupervisorSpec} / {@link MachineSpec} records.
 *
 * <p>Mirrors the shape of {@link FlatRegistryTest} but uses
 * {@link SpecBackedSupervisor} / {@link SpecBackedMachine} under the hood.
 */
class SpecBasedRegistryTest {

    // ── events ─────────────────────────────────────────────────────

    public record StartCall(String uuid)         implements StatemachineEvent {}
    public record CallRinging(String uuid)       implements StatemachineEvent {}
    public record SignalingProgress(String uuid) implements StatemachineEvent {}
    public record SettleNow(String uuid)         implements StatemachineEvent {}
    public record FanoutPing(String uuid)        implements StatemachineEvent {}

    // ── contexts ───────────────────────────────────────────────────

    public static class SupCtx { public int rang; public SupCtx() {} }
    public static class SigCtx { public boolean rang; public SigCtx() {} }
    public static class BalCtx { public boolean ponged; public BalCtx() {} }

    static final AtomicInteger sigRangCount = new AtomicInteger();

    // ── state maps as static fields ────────────────────────────────

    static final StateMap SIGNALING_STATES = StateMap.builder()
        .initialState("WORKING")
        .state("WORKING")
            .interim()
            .timeout(5, TimeUnit.SECONDS, "DONE")
            .on(CallRinging.class, "RANG")
            .on(FanoutPing.class,  "WORKING")
            .onEntry(self -> { /* idle */ })
        .state("RANG")
            .interim()
            .timeout(5, TimeUnit.SECONDS, "DONE")
            .onEntry(self -> {
                Machine<SigCtx> m = (Machine<SigCtx>) self;
                m.getContext().rang = true;
                sigRangCount.incrementAndGet();
                // publish back to supervisor — its resolver routes via Self rule
                m.publishEvent(new SignalingProgress(stripChild(m.getMachineId())));
            })
        .state("DONE")
            .finalState()
            .timeout(1, TimeUnit.SECONDS, "DONE")
        .build();

    static final StateMap BALANCE_STATES = StateMap.builder()
        .initialState("HELD")
        .state("HELD")
            .interim()
            .timeout(5, TimeUnit.SECONDS, "CLOSED")
            .on(SettleNow.class,  "CLOSED")
            .on(FanoutPing.class, "HELD")
        .state("CLOSED")
            .finalState()
            .timeout(1, TimeUnit.SECONDS, "CLOSED")
        .build();

    static final StateMap SUPERVISOR_STATES = StateMap.builder()
        .initialState("ACTIVE")
        .state("ACTIVE")
            .interim()
            .timeout(5, TimeUnit.SECONDS, "DONE")
            .onEntry(self -> {
                Supervisor<SupCtx> s = (Supervisor<SupCtx>) self;
                s.resolver().spawnChild("Signaling", null);
                s.resolver().spawnChild("Balance",   null);
            })
            .on(SignalingProgress.class, "RANG")
            .on(StartCall.class, "ACTIVE")
        .state("RANG")
            .interim()
            .timeout(5, TimeUnit.SECONDS, "DONE")
            .onEntry(self -> ((Machine<SupCtx>) self).getContext().rang++)
        .state("DONE")
            .finalState()
            .timeout(1, TimeUnit.SECONDS, "DONE")
        .build();

    // ── specs ──────────────────────────────────────────────────────

    static final MachineSpec<SigCtx> SIGNALING_SPEC = MachineSpec.<SigCtx>builder()
        .name("Signaling")
        .contextFactory(SigCtx::new)
        .stateMap(SIGNALING_STATES)
        .build();

    static final MachineSpec<BalCtx> BALANCE_SPEC = MachineSpec.<BalCtx>builder()
        .name("Balance")
        .contextFactory(BalCtx::new)
        .stateMap(BALANCE_STATES)
        .build();

    static final SupervisorSpec<SupCtx> SUPERVISOR_SPEC = SupervisorSpec.<SupCtx>builder()
        .name("CallSupervisor")
        .contextFactory(SupCtx::new)
        .stateMap(SUPERVISOR_STATES)
        .routes(r -> {
            r.selfHandle(StartCall.class);
            r.selfHandle(SignalingProgress.class);
            r.forwardTo("Signaling", CallRinging.class);
            r.forwardTo("Balance",   SettleNow.class);
            r.forwardToAll(List.of("Signaling", "Balance"), FanoutPing.class);
        })
        .build();

    private static String stripChild(String fullId) {
        int i = fullId.indexOf('#');
        return i < 0 ? fullId : fullId.substring(0, i);
    }

    // ── system under test ──────────────────────────────────────────

    private Registry reg;

    @AfterEach
    void tearDown() { if (reg != null) reg.shutdown(); }

    private Registry build() {
        return Registry.builder("spec-call")
            .supervisor(SUPERVISOR_SPEC, 4)
            .child(SIGNALING_SPEC, 4)
            .child(BALANCE_SPEC,   4)
            .threads(2)
            .build();
    }

    // ─────────────────────────────────────────────────────────────

    @Test
    void supervisor_spec_dispatches_and_spawns_children() throws InterruptedException {
        sigRangCount.set(0);
        reg = build();
        String uuid = "spec-A";
        DispatchResult result = reg.dispatch(uuid, new SupCtx());
        assertTrue(result.accepted(), "dispatch should accept");
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        assertEquals(3, reg.activeCellCount(), "supervisor + 2 children alive");
    }

    @Test
    void forward_to_named_child_via_spec_routes() throws InterruptedException {
        sigRangCount.set(0);
        reg = build();
        String uuid = "spec-B";
        reg.dispatch(uuid, new SupCtx());
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        reg.onInboundEvent(uuid, new CallRinging(uuid));
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        // Signaling child's RANG.onEntry published SignalingProgress back, which the
        // supervisor's resolver self-handles → supervisor moves ACTIVE → RANG.
        Machine<?> sup = reg.findInternal(uuid, "CallSupervisor");
        assertNotNull(sup);
        SupCtx ctx = (SupCtx) sup.getContext();
        assertEquals(1, ctx.rang, "supervisor saw the publish-back");
        assertEquals(1, sigRangCount.get(), "signaling child fired its RANG entry");
    }

    @Test
    void fanout_event_reaches_all_named_children() throws InterruptedException {
        reg = build();
        String uuid = "spec-C";
        reg.dispatch(uuid, new SupCtx());
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        reg.onInboundEvent(uuid, new FanoutPing(uuid));
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));

        // Both children stayed alive (FanoutPing → self-transition on each).
        assertEquals(3, reg.activeCellCount());
    }

    @Test
    void supervisor_shutdown_cascades_to_children() throws InterruptedException {
        reg = build();
        String uuid = "spec-D";
        reg.dispatch(uuid, new SupCtx());
        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(3, reg.activeCellCount());

        reg.shutdown();
        assertEquals(0, reg.activeCellCount(), "all cells reclaimed after shutdown");
        assertEquals(0, reg.activeIdCount());
    }

    @Test
    void duplicate_spec_name_rejected_at_build() {
        MachineSpec<SigCtx> dupNameAsSupervisor = MachineSpec.<SigCtx>builder()
            .name("CallSupervisor")  // collides with SUPERVISOR_SPEC.name()
            .contextFactory(SigCtx::new)
            .stateMap(SIGNALING_STATES)
            .build();
        assertThrows(IllegalStateException.class, () -> Registry.builder("spec-dup")
            .supervisor(SUPERVISOR_SPEC, 4)
            .child(dupNameAsSupervisor, 4)
            .build());
    }
}
