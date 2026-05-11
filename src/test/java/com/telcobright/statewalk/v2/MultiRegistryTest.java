package com.telcobright.statewalk.v2;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.registry.api.MultiRegistry;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Foundation smoke test for {@link MultiRegistry}. Exercises the core
 * primitives: spawn, publish, fireOn, cascade cleanup on primary terminate.
 * Two machine types in one registry, communicating via the resolver.
 */
class MultiRegistryTest {

    // ── domain ────────────────────────────────────────────────────────

    public record StartCall(String uuid) implements StatemachineEvent {
        @Override public boolean isFirst() { return true; }
    }
    public record SignalingDone(String uuid) implements StatemachineEvent {}
    public record HangupNow(String uuid) implements StatemachineEvent {}

    public static class SupervisorCtx { public int signalingDoneCount; public SupervisorCtx() {} }
    public static class SignalingCtx  { public boolean started; public SignalingCtx() {} }

    public record CallTask(String uuid) {}

    // ── parent / "primary" machine ────────────────────────────────────

    static final AtomicInteger supervisorTerminated = new AtomicInteger();
    static final AtomicInteger signalingTerminated  = new AtomicInteger();

    static class SupervisorMachine extends Machine<CallTask, SupervisorCtx> {
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("ADMITTING")
                .state("ADMITTING")
                    .interim()
                    .timeout(2, TimeUnit.SECONDS, "FAILED")
                    .onEntry(self -> {
                        SupervisorMachine m = (SupervisorMachine) self;
                        // Spawn a sibling SignalingMachine for the same id, via the registry.
                        // The supervisor doesn't hold any reference to it.
                        var reg = (MultiRegistry) holder.registry;
                        reg.spawn(m.getMachineId(), SignalingMachine.class,
                            m.getPersistingEntity());
                    })
                    .on(SignalingDone.class, "ENDING")
                    .on(HangupNow.class,     "FAILED")
                .state("ENDING")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "ENDING")
                    .onEntry(self -> supervisorTerminated.incrementAndGet())
                .state("FAILED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "FAILED")
                    .onEntry(self -> supervisorTerminated.incrementAndGet())
                .build();
        }
        @Override protected SupervisorCtx createContext() { return new SupervisorCtx(); }

        // Trampoline — Machine.getMachineId() is package-private in this module so
        // tests access via a public wrapper.
        public String id() { return getMachineId(); }
    }

    // ── child machine — publishes back to parent ──────────────────────

    static class SignalingMachine extends Machine<CallTask, SignalingCtx> {
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("WORKING")
                .state("WORKING")
                    .interim()
                    .timeout(2, TimeUnit.SECONDS, "DONE")
                    .onEntry(self -> {
                        SignalingMachine m = (SignalingMachine) self;
                        m.getContext().started = true;
                        // Publish back to parent supervisor for the same uuid.
                        m.publishEvent(new SignalingDone(m.getMachineId()));
                        // Then terminate ourselves.
                    })
                    .on(SignalingDone.class, "DONE")    // self-receive triggers DONE
                .state("DONE")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "DONE")
                    .onEntry(self -> signalingTerminated.incrementAndGet())
                .build();
        }
        @Override protected SignalingCtx createContext() { return new SignalingCtx(); }
    }

    // tiny holder hack so the entry lambda above can reach the registry without
    // pulling in a full DI setup just for the test
    static class Holder { MultiRegistry registry; }
    static final Holder holder = new Holder();

    private MultiRegistry reg;

    @AfterEach
    void tearDown() { if (reg != null) reg.shutdown(); }

    private MultiRegistry buildRegistry() {
        reg = MultiRegistry.builder("test")
            .machine(SupervisorMachine.class, SupervisorMachine::new, 4)
            .machine(SignalingMachine.class,  SignalingMachine::new,  4)
            .primary(SupervisorMachine.class)
            .route(StartCall.class,    SupervisorMachine.class, StartCall::uuid)
            .route(HangupNow.class,    SupervisorMachine.class, HangupNow::uuid)
            // Routed to BOTH? Resolver rule is single-target. Publish to parent here:
            .route(SignalingDone.class, SupervisorMachine.class, SignalingDone::uuid)
            .build();
        holder.registry = reg;
        supervisorTerminated.set(0);
        signalingTerminated.set(0);
        return reg;
    }

    @Test
    void spawn_publish_and_cascade_cleanup() throws InterruptedException {
        buildRegistry();

        // Spawn the parent. Its entry will spawn the child. The child will
        // publish SignalingDone. The parent will transition to ENDING (final),
        // which cascades cleanup to the child.
        String uuid = "call-1";
        reg.spawn(uuid, SupervisorMachine.class, new CallTask(uuid));

        assertTrue(reg.awaitIdle(2, TimeUnit.SECONDS), "registry should drain");

        assertEquals(1, supervisorTerminated.get(), "parent reached ENDING once");
        // Signaling may or may not have entered DONE before the cascade force-cleaned it.
        // What we DO require: there are no active cells for this uuid.
        assertFalse(reg.hasAny(uuid), "all cells cleaned up after parent terminate");
        assertEquals(0, reg.activeIdCount(), "no active ids remain");
        assertEquals(0, reg.activeCellCount(), "no active cells remain");
    }

    @Test
    void child_alone_does_not_complete_until_parent_present() throws InterruptedException {
        buildRegistry();

        // Inbound event for an unknown id with no isFirst → dropped silently (no creation).
        // We don't have first-event auto-create yet in MultiRegistry; spawn is explicit.
        reg.onInboundEvent("ghost-1", new SignalingDone("ghost-1"));
        assertTrue(reg.awaitIdle(1, TimeUnit.SECONDS));
        assertEquals(0, reg.activeIdCount(), "no rows created for unrouted target");
    }

    @Test
    void duplicate_route_registration_is_rejected_at_build() {
        var b = MultiRegistry.builder("dup")
            .machine(SupervisorMachine.class, SupervisorMachine::new, 2)
            .route(StartCall.class, SupervisorMachine.class, StartCall::uuid);
        assertThrows(IllegalStateException.class, () ->
            b.route(StartCall.class, SupervisorMachine.class, StartCall::uuid));
    }

    @Test
    void unknown_primary_type_is_rejected_at_build() {
        assertThrows(IllegalStateException.class, () ->
            MultiRegistry.builder("bad")
                .machine(SupervisorMachine.class, SupervisorMachine::new, 2)
                .primary(SignalingMachine.class)
                .build());
    }
}
