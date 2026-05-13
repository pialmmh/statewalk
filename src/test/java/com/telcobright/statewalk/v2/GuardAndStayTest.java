package com.telcobright.statewalk.v2;

import com.telcobright.statewalk.v2.channel.TestChannel;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.registry.api.Registry;
import com.telcobright.statewalk.v2.registry.api.Statewalk;
import com.telcobright.statewalk.v2.registry.api.StatewalkSystem;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the two builder features on transitions out of a state:
 *
 * <ul>
 *   <li>{@code stay(EventClass, handler)} — handle the event, run side effect,
 *       stay in the current state. Existing API; covered here for completeness.</li>
 *   <li>{@code on(EventClass, target, guard)} — guarded transition. Multiple
 *       guarded variants for the same event are evaluated in declaration order;
 *       first matching guard wins. An unguarded {@code on(...)} at the end is
 *       the fallback.</li>
 * </ul>
 */
class GuardAndStayTest {

    public record Open(String userId) implements StatemachineEvent {
        @Override public boolean isFirst() { return true; }
    }
    public record Bump()  implements StatemachineEvent {}
    public record Check() implements StatemachineEvent {}
    public record Close() implements StatemachineEvent {}

    public static class Ctx {
        public int    bumps;
        public int    threshold = 3;
        public boolean closed;
        public Ctx() {}
    }
    public record Task(String userId) {}

    static final AtomicInteger acceptedEntry = new AtomicInteger();
    static final AtomicInteger rejectedEntry = new AtomicInteger();
    static final AtomicInteger stayBumpRuns  = new AtomicInteger();

    static class Mach extends Machine<Task, Ctx> {
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("WORKING")
                .state("WORKING")
                    .interim()
                    .timeout(60, TimeUnit.SECONDS, "REJECTED")
                    // Stay: handle Bump without transitioning. Increments ctx.bumps.
                    .stay(Bump.class, (self, event) -> {
                        stayBumpRuns.incrementAndGet();
                        ((Mach) self).getContext().bumps++;
                    })
                    // Guarded: same event class, two possible targets based on guard.
                    .on(Check.class, "ACCEPTED",
                        (self, evt) -> ((Mach) self).getContext().bumps >= ((Mach) self).getContext().threshold)
                    // Fallback (unconditional) — if guard above is false, take this.
                    .on(Check.class, "REJECTED")
                    // Unguarded transition still works for other events.
                    .on(Close.class, "REJECTED")
                .state("ACCEPTED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "ACCEPTED")
                    .onEntry(self -> acceptedEntry.incrementAndGet())
                .state("REJECTED")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "REJECTED")
                    .onEntry(self -> rejectedEntry.incrementAndGet())
                .build();
        }
        @Override protected Ctx createContext() { return new Ctx(); }
    }

    static class Reg extends Registry<Mach, Ctx> {
        @Override protected String getRegistryName()    { return "guard"; }
        @Override protected int    getMaxConcurrent()   { return 16; }
        @Override protected long   getGlobalTimeoutMs() { return 60_000L; }
        @Override protected Mach   createMachineTemplate() { return new Mach(); }
        @Override
        protected Object createTaskFromFirstEvent(String requestId, StatemachineEvent first) {
            if (first instanceof Open o) return new Task(o.userId());
            return super.createTaskFromFirstEvent(requestId, first);
        }
    }

    private Reg          registry;
    private TestChannel  channel;
    private StatewalkSystem system;

    private void buildSystem() {
        acceptedEntry.set(0);
        rejectedEntry.set(0);
        stayBumpRuns.set(0);
        registry = new Reg();
        channel  = new TestChannel("g-ch");
        system   = Statewalk.builder()
            .registerEvent(Open.class)
            .registerEvent(Bump.class)
            .registerEvent(Check.class)
            .registerEvent(Close.class)
            .registry("guard", registry, 4, 2)
            .channel("guard", channel)
            .build();
    }

    @AfterEach
    void tearDown() { if (system != null) system.shutdown(); }

    @Test
    void stay_handler_runs_without_transition() throws InterruptedException {
        buildSystem();
        String id = "u-1";
        channel.inject(id, new Open("alice"));
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        // Three bumps — each fires the stay handler, increments ctx.bumps; state never changes.
        channel.inject(id, new Bump());
        channel.inject(id, new Bump());
        channel.inject(id, new Bump());
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        assertEquals(3, stayBumpRuns.get(), "stay handler ran for each Bump");
        Mach m = registry.getMachine(id);
        assertNotNull(m, "machine still alive (no transition out)");
        assertEquals("WORKING", m.getCurrentState());
        assertEquals(3, m.getContext().bumps);
        assertEquals(0, acceptedEntry.get());
        assertEquals(0, rejectedEntry.get());
    }

    @Test
    void guarded_transition_takes_first_matching_branch() throws InterruptedException {
        buildSystem();
        String id = "u-2";
        channel.inject(id, new Open("bob"));
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        // Bump three times — meets threshold.
        channel.inject(id, new Bump());
        channel.inject(id, new Bump());
        channel.inject(id, new Bump());
        // Now Check: guard "bumps >= threshold" is true → ACCEPTED.
        channel.inject(id, new Check());
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        assertEquals(1, acceptedEntry.get(), "guarded transition picked ACCEPTED");
        assertEquals(0, rejectedEntry.get());
    }

    @Test
    void guarded_transition_falls_through_to_unguarded_fallback() throws InterruptedException {
        buildSystem();
        String id = "u-3";
        channel.inject(id, new Open("carol"));
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        // Only one bump — below threshold (3).
        channel.inject(id, new Bump());
        // Check: guard "bumps >= 3" is false → fallback (unconditional on Check → REJECTED).
        channel.inject(id, new Check());
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        assertEquals(0, acceptedEntry.get());
        assertEquals(1, rejectedEntry.get(), "guard rejected → unconditional fallback fired");
    }

    @Test
    void guard_throwing_is_treated_as_false() throws InterruptedException {
        // Build a system with a guard that throws — should fall through to fallback.
        acceptedEntry.set(0); rejectedEntry.set(0);
        class Throwy extends Machine<Task, Ctx> {
            @Override protected StateMap defineStates() {
                return StateMap.builder()
                    .initialState("WORKING")
                    .state("WORKING")
                        .interim()
                        .timeout(60, TimeUnit.SECONDS, "REJECTED")
                        .on(Check.class, "ACCEPTED", (self, e) -> { throw new RuntimeException("guard fail"); })
                        .on(Check.class, "REJECTED")
                    .state("ACCEPTED")
                        .finalState()
                        .timeout(1, TimeUnit.SECONDS, "ACCEPTED")
                        .onEntry(self -> acceptedEntry.incrementAndGet())
                    .state("REJECTED")
                        .finalState()
                        .timeout(1, TimeUnit.SECONDS, "REJECTED")
                        .onEntry(self -> rejectedEntry.incrementAndGet())
                    .build();
            }
            @Override protected Ctx createContext() { return new Ctx(); }
        }
        var reg = new Registry<Throwy, Ctx>() {
            @Override protected String getRegistryName()    { return "throwy"; }
            @Override protected int    getMaxConcurrent()   { return 4; }
            @Override protected long   getGlobalTimeoutMs() { return 60_000L; }
            @Override protected Throwy createMachineTemplate() { return new Throwy(); }
            @Override
            protected Object createTaskFromFirstEvent(String requestId, StatemachineEvent first) {
                if (first instanceof Open o) return new Task(o.userId());
                return super.createTaskFromFirstEvent(requestId, first);
            }
        };
        var ch = new TestChannel("t-ch");
        var sys = Statewalk.builder()
            .registerEvent(Open.class)
            .registerEvent(Check.class)
            .registry("throwy", reg, 2, 2)
            .channel("throwy", ch)
            .build();
        try {
            ch.inject("u-4", new Open("dave"));
            assertTrue(sys.awaitIdle(Duration.ofSeconds(2)));
            ch.inject("u-4", new Check());
            assertTrue(sys.awaitIdle(Duration.ofSeconds(2)));
            assertEquals(0, acceptedEntry.get());
            assertEquals(1, rejectedEntry.get(), "throwing guard → false → fallback");
        } finally {
            sys.shutdown();
        }
    }
}
