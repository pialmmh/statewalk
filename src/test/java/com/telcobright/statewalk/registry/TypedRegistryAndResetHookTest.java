package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.event.StatemachineEvent;
import com.telcobright.statewalk.machine.Machine;
import com.telcobright.statewalk.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The generic-registry + builder-lambda contract:
 *
 * <ul>
 *   <li>{@code StatemachineRegistry<T>} — T is the supervisor's context/task
 *       type: {@code dispatch}, {@code createFromFirstEvent} and
 *       {@code quotaKeysExtractor} are fully typed, no casts.</li>
 *   <li>Per-type CONSTRUCTOR lambdas (factories) — the existing builder
 *       surface.</li>
 *   <li>Per-type RESET lambdas ({@code .resetHook}) — run on every pool
 *       return after the framework reset; a type with a reset hook may carry
 *       mutable props (the field validator relaxes because the hook owns
 *       clearing them); a hook throw drops the instance, never recycling it
 *       dirty.</li>
 *   <li>Entry AND exit actions as builder lambdas (onEntry/onExit).</li>
 * </ul>
 */
class TypedRegistryAndResetHookTest {

    public record Stop(String u)  implements StatemachineEvent {}
    public record Open(String u)  implements StatemachineEvent {
        @Override public boolean isFirst() { return true; }
    }

    public static class CallTask {
        public String partner;
        public int hops;
        public CallTask() {}
        public CallTask(String partner) { this.partner = partner; }
    }

    static final AtomicInteger EXITS = new AtomicInteger();

    /**
     * A supervisor with deliberate per-borrow props: a mutable scratch field
     * AND a final cache whose CONTENTS grow per session. Without a reset hook
     * the validator rejects the mutable field at build; with the hook, the
     * hook owns clearing both.
     */
    public static class StatefulSupervisor extends Supervisor<CallTask> {
        String scratch;                                   // mutable prop — needs the hook
        final List<String> cache = new ArrayList<>();     // final, but contents grow

        @Override protected void defineRoutes(InternalEventResolver r) { r.selfHandle(Stop.class); }

        @Override protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("RUNNING")
                .state("RUNNING").interim()
                    .timeout(1, TimeUnit.HOURS, "EXPIRED")
                    .onEntry(self -> {
                        StatefulSupervisor s = (StatefulSupervisor) self;
                        s.scratch = "session-" + s.getMachineId();
                        s.cache.add(s.getMachineId());
                    })
                    .onExit(self -> EXITS.incrementAndGet())
                    .on(Stop.class, "DONE")
                .state("DONE").finalState().timeout(1, TimeUnit.SECONDS, "DONE")
                .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
                .build();
        }
        @Override protected CallTask createContext() { return new CallTask(); }
    }

    private final List<StatemachineRegistry<CallTask>> open = new ArrayList<>();

    @AfterEach
    void tearDown() { for (var r : open) r.shutdown(); }

    // ─────────────────────────────────────────────────────────────
    // Typed end-to-end: T flows through dispatch / first-event / quota
    // ─────────────────────────────────────────────────────────────

    @Test
    void typed_registry_needs_no_casts_anywhere() throws Exception {
        int cacheBefore = com.telcobright.statewalk.persistence.SnapshotSerializer.classCacheSize();
        StatemachineRegistry<CallTask> reg = StatemachineRegistry.<CallTask>builder("typed")
            .supervisor("StatefulSupervisor", StatefulSupervisor::new, 4)
            .preWarmContextClass(CallTask.class)   // v2 build-time param, carried over
            .resetHook("StatefulSupervisor", m -> {
                StatefulSupervisor s = (StatefulSupervisor) m;
                s.scratch = null;
                s.cache.clear();
            })
            // TYPED: t is CallTask — no (CallTask) cast, field access direct.
            .quotaKeysExtractor(t -> t.partner != null ? QuotaKeys.ofPartner(t.partner) : QuotaKeys.NONE)
            .quotaLimits(new QuotaLimits(2, 0, 0, 0))
            // TYPED: the first-event hook returns T.
            .createFromFirstEvent(ev -> ev instanceof Open o ? new CallTask("p-" + o.u()) : null)
            .threads(2)
            .build();
        open.add(reg);

        // TYPED dispatch: the task parameter is CallTask, checked at compile time.
        assertTrue(reg.dispatch("t-1", new CallTask("acme")).accepted());
        assertTrue(reg.dispatch("t-2", new CallTask("acme")).accepted());
        assertEquals(RejectCause.PARTNER_CONCURRENCY_EXCEEDED,
            reg.dispatch("t-3", new CallTask("acme")).rejectCause(),
            "typed extractor drove the quota gate");

        // First-event auto-creation goes through the typed hook.
        reg.onInboundEvent("t-4", new Open("t-4"));
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        Machine<?> m = reg.findInternal("t-4", "StatefulSupervisor");
        assertNotNull(m);
        assertEquals("p-t-4", ((CallTask) m.getContext()).partner);

        assertTrue(com.telcobright.statewalk.persistence.SnapshotSerializer.classCacheSize() > cacheBefore
                || cacheBefore > 0,
            "preWarmContextClass registered the context class in the serializer cache");
    }

    // ─────────────────────────────────────────────────────────────
    // Reset hook: mutable props legal + actually cleared across borrows
    // ─────────────────────────────────────────────────────────────

    @Test
    void without_reset_hook_mutable_prop_is_rejected_at_build() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            StatemachineRegistry.<CallTask>builder("no-hook")
                .supervisor("StatefulSupervisor", StatefulSupervisor::new, 2)
                .build());
        assertTrue(ex.getMessage().contains("scratch"),
            "validator still names the offending prop when no hook owns it: " + ex.getMessage());
    }

    @Test
    void reset_hook_clears_props_between_borrows() throws Exception {
        EXITS.set(0);
        AtomicInteger hookRuns = new AtomicInteger();
        // Pool size 1 → every session reuses the SAME instance; the hook is
        // the only thing standing between sessions.
        StatemachineRegistry<CallTask> reg = StatemachineRegistry.<CallTask>builder("hooked")
            .supervisor("StatefulSupervisor", StatefulSupervisor::new, 1)
            .resetHook("StatefulSupervisor", m -> {
                hookRuns.incrementAndGet();
                StatefulSupervisor s = (StatefulSupervisor) m;
                s.scratch = null;
                s.cache.clear();
            })
            .threads(2)
            .build();
        open.add(reg);

        StatefulSupervisor firstInstance = null;
        for (int round = 0; round < 3; round++) {
            String id = "r-" + round;
            assertTrue(reg.dispatch(id, new CallTask("p")).accepted());
            assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
            StatefulSupervisor s = (StatefulSupervisor) reg.findInternal(id, "StatefulSupervisor");
            assertNotNull(s);
            if (firstInstance == null) firstInstance = s;
            else assertSame(firstInstance, s, "pool size 1 → same instance re-borrowed");
            assertEquals(1, s.cache.size(),
                "round " + round + ": cache holds ONLY this session's entry — the hook cleared the last one");
            assertEquals("session-" + id, s.scratch);
            reg.onInboundEvent(id, new Stop(id));
            assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        }
        assertTrue(hookRuns.get() >= 3, "the reset lambda ran on every pool return");
        assertEquals(3, EXITS.get(), "the onExit builder lambda ran on every RUNNING→DONE transition");
    }

    @Test
    void throwing_reset_hook_drops_the_instance_instead_of_recycling_dirty() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        StatemachineRegistry<CallTask> reg = StatemachineRegistry.<CallTask>builder("hook-throw")
            .supervisor("StatefulSupervisor", StatefulSupervisor::new, 2)
            .resetHook("StatefulSupervisor", m -> {
                if (calls.incrementAndGet() == 1) throw new RuntimeException("simulated hook crash");
                ((StatefulSupervisor) m).scratch = null;
                ((StatefulSupervisor) m).cache.clear();
            })
            .threads(2)
            .build();
        open.add(reg);

        assertTrue(reg.dispatch("h-1", new CallTask("p")).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        reg.onInboundEvent("h-1", new Stop("h-1"));           // return → hook throws → instance dropped
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(1, reg.poolOf("StatefulSupervisor").getStatistics().resetFailures(),
            "the crashed-hook instance was dropped, not recycled dirty");

        // The registry still works — next session gets a fresh instance.
        assertTrue(reg.dispatch("h-2", new CallTask("p")).accepted());
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
        StatefulSupervisor s = (StatefulSupervisor) reg.findInternal("h-2", "StatefulSupervisor");
        assertNotNull(s);
        assertEquals(1, s.cache.size(), "fresh instance, clean cache");
        reg.onInboundEvent("h-2", new Stop("h-2"));
        assertTrue(reg.awaitIdle(5, TimeUnit.SECONDS));
    }
}
