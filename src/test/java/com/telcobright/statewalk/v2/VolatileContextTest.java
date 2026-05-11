package com.telcobright.statewalk.v2;

import com.telcobright.statewalk.v2.channel.TestChannel;
import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.persistence.InMemoryPersistenceProvider;
import com.telcobright.statewalk.v2.registry.api.Registry;
import com.telcobright.statewalk.v2.registry.api.Statewalk;
import com.telcobright.statewalk.v2.registry.api.StatewalkSystem;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;
import com.telcobright.statewalk.v2.state.StateMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class VolatileContextTest {

    public record Hello(String userId) implements StatemachineEvent {
        @Override public boolean isFirst() { return true; }
    }
    public record Bump() implements StatemachineEvent {}
    public record Close() implements StatemachineEvent {}

    public static class State {
        public String userId;
        public int bumps;
        public State() {}
    }
    public record Task(String userId) {}

    public static final class Resources {
        public final String tenantTag;
        public final int    poolSize;
        public Resources(String tenantTag, int poolSize) {
            this.tenantTag = tenantTag;
            this.poolSize  = poolSize;
        }
    }

    static final AtomicInteger loaderInvocations         = new AtomicInteger(0);
    static final AtomicInteger entryWithResourcesPresent = new AtomicInteger(0);
    static final AtomicInteger entryWithResourcesNull    = new AtomicInteger(0);

    static class M extends Machine<Task, State> {
        @Override
        protected StateMap defineStates() {
            return StateMap.builder()
                .initialState("LIVE")
                .state("LIVE")
                    .interim().offline()
                    .timeout(60, TimeUnit.SECONDS, "DONE")
                    .onEntry(self -> {
                        M m = (M) self;
                        if (m.getPersistingEntity() != null) {
                            m.getContext().userId = m.getPersistingEntity().userId();
                        }
                        // Volatile context must be populated BEFORE entry runs.
                        if (m.getVolatileContext() != null) {
                            entryWithResourcesPresent.incrementAndGet();
                        } else {
                            entryWithResourcesNull.incrementAndGet();
                        }
                    })
                    .on(Bump.class, "LIVE")
                    .on(Close.class, "DONE")
                .state("DONE")
                    .finalState()
                    .timeout(1, TimeUnit.SECONDS, "DONE")
                .build();
        }
        @Override protected State createContext() { return new State(); }
    }

    static class Reg extends Registry<M, State> {
        @Override protected String getRegistryName()    { return "vol"; }
        @Override protected int    getMaxConcurrent()   { return 16; }
        @Override protected long   getGlobalTimeoutMs() { return 60_000L; }
        @Override protected M createMachineTemplate()   { return new M(); }
        @Override
        protected Object createTaskFromFirstEvent(String requestId, StatemachineEvent firstEvent) {
            if (firstEvent instanceof Hello h) return new Task(h.userId());
            return super.createTaskFromFirstEvent(requestId, firstEvent);
        }
    }

    private InMemoryPersistenceProvider persistence;
    private Reg                         registry;
    private TestChannel                 channel;
    private StatewalkSystem             system;

    @BeforeEach
    void setUp() {
        loaderInvocations.set(0);
        entryWithResourcesPresent.set(0);
        entryWithResourcesNull.set(0);

        persistence = new InMemoryPersistenceProvider();
        registry    = new Reg();
        channel     = new TestChannel("vol-ch");

        system = Statewalk.builder()
            .registerEvent(Hello.class)
            .registerEvent(Bump.class)
            .registerEvent(Close.class)
            .persistence(persistence)
            .rehydrate(true)
            .registry("vol", registry, 8, 2)
            .channel("vol", channel)
            .volatileLoader("vol", machine -> {
                loaderInvocations.incrementAndGet();
                return new Resources("tenant-A", 5_000);
            })
            .build();
    }

    @AfterEach
    void tearDown() {
        if (system != null) system.shutdown();
    }

    @Test
    void loader_fires_on_creation_and_volatile_is_visible_to_entry_action()
            throws InterruptedException {
        channel.inject("id-1", new Hello("alice"));
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        assertEquals(1, loaderInvocations.get(),         "loader runs once on creation");
        assertEquals(1, entryWithResourcesPresent.get(), "entry saw volatile populated");
        assertEquals(0, entryWithResourcesNull.get(),    "entry never saw null volatile");
    }

    @Test
    void loader_fires_again_on_rehydration_after_offline_suspend()
            throws InterruptedException {
        channel.inject("id-2", new Hello("bob"));
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));
        assertEquals(1, loaderInvocations.get(), "loader ran on creation");
        assertNull(registry.getMachine("id-2"), "machine suspended (offline)");

        // Send a terminal event to trigger rehydration → transitions to DONE.
        channel.inject("id-2", new Close());
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        // Loader runs again on the rehydrate path — same callback, both paths.
        assertEquals(2, loaderInvocations.get(),
            "loader must fire a second time on rehydration");
        assertNull(registry.getMachine("id-2"), "terminated after Close");
        assertFalse(persistence.load("id-2").isPresent(), "snapshot deleted on terminal");
    }

    @Test
    void volatile_context_is_not_persisted() throws InterruptedException {
        channel.inject("id-3", new Hello("carol"));
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));
        var snap = persistence.load("id-3").orElseThrow();
        // Snapshot's contextJsonBase64 holds only the persistent State,
        // not the Resources object. Easiest cross-check: ensure the
        // tenantTag string never appears in the encoded blob.
        String encoded = snap.contextJsonBase64();
        String decoded = new String(java.util.Base64.getDecoder().decode(encoded),
                                     java.nio.charset.StandardCharsets.UTF_8);
        assertFalse(decoded.contains("tenant-A"),
            "volatile context fields must NOT be present in the persisted snapshot");
    }

    @Test
    void no_loader_registered_means_volatile_is_null() throws InterruptedException {
        // Tear down the shared system and rebuild without a loader.
        system.shutdown();
        loaderInvocations.set(0);
        entryWithResourcesPresent.set(0);
        entryWithResourcesNull.set(0);
        registry = new Reg();
        channel  = new TestChannel("vol-ch-2");

        system = Statewalk.builder()
            .registerEvent(Hello.class)
            .registerEvent(Bump.class)
            .registerEvent(Close.class)
            .persistence(persistence)
            .rehydrate(true)
            .registry("vol", registry, 8, 2)
            .channel("vol", channel)
            // NO volatileLoader registered.
            .build();

        channel.inject("id-4", new Hello("dave"));
        assertTrue(system.awaitIdle(Duration.ofSeconds(2)));

        assertEquals(0, loaderInvocations.get());
        assertEquals(0, entryWithResourcesPresent.get());
        assertEquals(1, entryWithResourcesNull.get(),
            "without a loader, entry action sees null volatile context");
    }
}
