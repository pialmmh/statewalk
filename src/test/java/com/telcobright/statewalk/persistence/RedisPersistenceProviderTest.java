package com.telcobright.statewalk.persistence;

import com.telcobright.statewalk.persistence.redis.RedisPersistenceProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Redis provider round-trip. SELF-SKIPS when no Redis answers on
 * 127.0.0.1:6379 — the build must not depend on external services. Run a
 * local Redis (e.g. in the LXC) to activate it.
 */
class RedisPersistenceProviderTest {

    private static final String REG = "redis-test-reg";
    private static JedisPool pool;

    @BeforeAll
    static void probe() {
        try {
            pool = new JedisPool("127.0.0.1", 6379);
            try (Jedis j = pool.getResource()) { j.ping(); }
        } catch (RuntimeException e) {
            pool = null;
        }
        assumeTrue(pool != null, "no Redis on 127.0.0.1:6379 — skipping Redis provider tests");
    }

    @AfterEach
    void cleanKeys() {
        if (pool == null) return;
        try (Jedis j = pool.getResource()) {
            var keys = j.keys("sw:*" + REG + "*");
            if (!keys.isEmpty()) j.del(keys.toArray(new String[0]));
        }
    }

    private static MachineSnapshot snap(String id, String state, long tDeadline, long gDeadline) {
        return new MachineSnapshot(id, REG, state, "java.lang.String",
            SnapshotSerializer.contextToBase64Json("ctx-" + id),
            System.currentTimeMillis(), "EXPIRED", tDeadline, gDeadline);
    }

    @Test
    void save_load_delete_roundtrip() {
        RedisPersistenceProvider p = new RedisPersistenceProvider(pool);
        long deadline = System.currentTimeMillis() + 60_000;
        p.save(snap("r-1", "RUNNING", deadline, deadline + 5000));

        Optional<MachineSnapshot> loaded = p.load("r-1", REG);
        assertTrue(loaded.isPresent());
        assertEquals("RUNNING", loaded.get().currentState());
        assertEquals(deadline, loaded.get().timeoutDeadlineMs());
        assertEquals(deadline + 5000, loaded.get().globalDeadlineMs());
        assertEquals("ctx-r-1", SnapshotSerializer.contextFromBase64Json(
            loaded.get().contextJsonBase64(), loaded.get().contextClassName()));
        assertEquals(1, p.size(REG));

        p.delete("r-1", REG);
        assertTrue(p.load("r-1", REG).isEmpty());
        assertEquals(0, p.size(REG), "index and data stay in lockstep");
    }

    @Test
    void save_replaces_fully_and_scans_use_the_index() {
        RedisPersistenceProvider p = new RedisPersistenceProvider(pool);
        p.save(snap("r-2", "A", 1000, 0));
        p.save(new MachineSnapshot("r-2", REG, "B", null, null,
            System.currentTimeMillis(), null, 0, 0));      // no ctx, no timeout: old fields must vanish
        Optional<MachineSnapshot> reloaded = p.load("r-2", REG);
        assertTrue(reloaded.isPresent());
        assertEquals("B", reloaded.get().currentState());
        assertNull(reloaded.get().contextClassName(), "full replace — stale fields gone");
        assertEquals(0, reloaded.get().timeoutDeadlineMs());

        p.save(snap("r-3", "RUNNING", System.currentTimeMillis() - 5000, 0));   // matured
        List<MachineSnapshot> all = p.loadAllForRegistry(REG);
        assertEquals(2, all.size());
        List<MachineSnapshot> matured = p.loadMatured(REG, System.currentTimeMillis());
        assertEquals(1, matured.size());
        assertEquals("r-3", matured.get(0).machineId());
    }

    @Test
    void quarantine_moves_row_out_of_live_store() {
        RedisPersistenceProvider p = new RedisPersistenceProvider(pool);
        p.save(snap("r-4", "RUNNING", 0, 0));
        p.quarantine("r-4", REG, "unit-test reason");
        assertTrue(p.load("r-4", REG).isEmpty(), "gone from the live store");
        assertEquals(0, p.size(REG));
        assertEquals(1, p.deadSize(REG), "kept in the dead-letter area");
        p.quarantine("r-4", REG, "again");                 // idempotent
        assertEquals(1, p.deadSize(REG));
    }
}
