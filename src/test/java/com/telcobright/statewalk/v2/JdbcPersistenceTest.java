package com.telcobright.statewalk.v2;

import com.telcobright.statewalk.v2.persistence.MachineSnapshot;
import com.telcobright.statewalk.v2.persistence.SnapshotSerializer;
import com.telcobright.statewalk.v2.persistence.jdbc.JdbcPersistenceProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the JDBC persistence provider against an in-memory SQLite. Same
 * SQL works against any JDBC-compliant database — SQLite chosen because it
 * runs anywhere with no external service.
 */
class JdbcPersistenceTest {

    public static class DemoCtx {
        public String name;
        public int counter;
        public DemoCtx() {}
    }

    private DataSource ds;
    private Path dbFile;
    private JdbcPersistenceProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        // SQLite per-test file — :memory: doesn't survive across connections,
        // and the cache=shared URL form is finicky. A fresh temp file is the
        // simplest portable choice for tests.
        dbFile = Files.createTempFile("statewalk-jdbc-", ".db");
        SQLiteDataSource sqlite = new SQLiteDataSource();
        sqlite.setUrl("jdbc:sqlite:" + dbFile.toAbsolutePath());
        this.ds = sqlite;
        this.provider = new JdbcPersistenceProvider(ds);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (dbFile != null) Files.deleteIfExists(dbFile);
    }

    @Test
    void schema_is_created_on_construction() {
        assertEquals(0, provider.size());
        assertEquals("statewalk_machine_snapshots", provider.getTableName());
    }

    @Test
    void save_then_load_round_trip() {
        DemoCtx ctx = new DemoCtx();
        ctx.name = "alice";
        ctx.counter = 42;
        String b64 = SnapshotSerializer.contextToBase64Json(ctx);

        MachineSnapshot original = new MachineSnapshot(
            "id-1", "demo-reg", "ACTIVE",
            DemoCtx.class.getName(), b64,
            1_700_000_000_000L, "CLOSED", 1_700_000_060_000L);

        provider.save(original);
        assertEquals(1, provider.size());

        MachineSnapshot loaded = provider.load("id-1").orElseThrow();
        assertEquals(original.machineId(), loaded.machineId());
        assertEquals(original.registryName(), loaded.registryName());
        assertEquals(original.currentState(), loaded.currentState());
        assertEquals(original.contextClassName(), loaded.contextClassName());
        assertEquals(original.contextJsonBase64(), loaded.contextJsonBase64());
        assertEquals(original.savedAtMs(), loaded.savedAtMs());
        assertEquals(original.timeoutTargetState(), loaded.timeoutTargetState());
        assertEquals(original.timeoutDeadlineMs(), loaded.timeoutDeadlineMs());

        // Deserialize the context back from base64
        DemoCtx restored = (DemoCtx) SnapshotSerializer.contextFromBase64Json(
            loaded.contextJsonBase64(), loaded.contextClassName());
        assertEquals("alice", restored.name);
        assertEquals(42, restored.counter);
    }

    @Test
    void save_again_is_an_upsert_not_a_duplicate() {
        MachineSnapshot v1 = new MachineSnapshot("id-2", "r", "S1", null, null,
            100, "CLOSED", 200);
        MachineSnapshot v2 = new MachineSnapshot("id-2", "r", "S2", null, null,
            150, "CLOSED", 250);

        provider.save(v1);
        provider.save(v2);
        assertEquals(1, provider.size(), "second save should UPDATE, not INSERT");

        MachineSnapshot loaded = provider.load("id-2").orElseThrow();
        assertEquals("S2", loaded.currentState());
        assertEquals(150, loaded.savedAtMs());
    }

    @Test
    void delete_removes_the_row() {
        provider.save(new MachineSnapshot("id-3", "r", "S", null, null, 0, "CLOSED", 0));
        assertEquals(1, provider.size());

        provider.delete("id-3");
        assertEquals(0, provider.size());
        assertTrue(provider.load("id-3").isEmpty());
    }

    @Test
    void load_missing_returns_empty() {
        assertTrue(provider.load("nope").isEmpty());
    }

    @Test
    void delete_missing_is_a_noop() {
        // No throw, no row count change.
        provider.delete("never-existed");
        assertEquals(0, provider.size());
    }

    @Test
    void custom_table_name_is_honored() throws Exception {
        Path f2 = Files.createTempFile("statewalk-jdbc2-", ".db");
        try {
            SQLiteDataSource sqlite = new SQLiteDataSource();
            sqlite.setUrl("jdbc:sqlite:" + f2.toAbsolutePath());
            JdbcPersistenceProvider p2 = new JdbcPersistenceProvider(sqlite, "custom_snaps_t1");
            assertEquals("custom_snaps_t1", p2.getTableName());
            p2.save(new MachineSnapshot("id-x", "r", "S", null, null, 0, "CLOSED", 0));
            assertEquals(1, p2.size());
        } finally {
            Files.deleteIfExists(f2);
        }
    }
}
