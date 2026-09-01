package com.telcobright.statewalk.persistence.jdbc;

import com.telcobright.statewalk.persistence.MachineSnapshot;
import com.telcobright.statewalk.persistence.PersistenceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Generic JDBC implementation of {@link PersistenceProvider}.
 *
 * <p>Single-table schema, portable across SQLite (tested), MySQL, PostgreSQL.
 * Default table name is {@code statewalk_machine_snapshots}; override via
 * the two-arg constructor to support multi-tenant deployments where each
 * tenant's snapshots live in its own table.
 *
 * <p>Composite PK on {@code (machine_id, registry_name)} so the same logical
 * request id can have multiple cells persisted (one per machine type in a
 * multi-machine registry).
 *
 * <p>Upsert is implemented as portable UPDATE-then-INSERT inside a transaction
 * — works on every JDBC database without dialect-specific syntax.
 *
 * <p>Schema is auto-created on construction via {@code CREATE TABLE IF NOT
 * EXISTS}; upgrading an existing v2 table gets the {@code global_deadline_ms}
 * column added in place. Quarantined snapshots (rehydration failures) are
 * moved to {@code <table>_dead} with the failure reason — recovery data is
 * preserved for a fixed build, never destroyed.
 *
 * <p><b>delete() throws on failure (v3).</b> A failed terminal delete is NOT
 * benign: the orphan row resurrects a finished session on the next restart.
 * The registry owns the retry policy, so this provider reports the failure
 * instead of swallowing it.
 */
public class JdbcPersistenceProvider implements PersistenceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcPersistenceProvider.class);

    private static final String DEFAULT_TABLE = "statewalk_machine_snapshots";

    private final DataSource dataSource;
    private final String table;
    private final String deadTable;

    public JdbcPersistenceProvider(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE);
    }

    public JdbcPersistenceProvider(DataSource dataSource, String table) {
        this.dataSource = dataSource;
        this.table = table;
        this.deadTable = table + "_dead";
        ensureSchema();
        ensureDeadTable();   // up-front: quarantines run CONCURRENTLY on the persist
                             // executor, and racing CREATE TABLE IF NOT EXISTS loses
                             // on MySQL — so the table must exist before any of them
    }

    private void ensureSchema() {
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " ("
            + "machine_id           VARCHAR(255) NOT NULL, "
            + "registry_name        VARCHAR(255) NOT NULL, "
            + "current_state        VARCHAR(255) NOT NULL, "
            + "context_class        VARCHAR(512), "
            + "context_json_b64     TEXT, "
            + "saved_at_ms          BIGINT NOT NULL, "
            + "timeout_target_state VARCHAR(255), "
            + "timeout_deadline_ms  BIGINT NOT NULL, "
            + "global_deadline_ms   BIGINT NOT NULL DEFAULT 0, "
            + "PRIMARY KEY (machine_id, registry_name)"
            + ")";
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate(ddl);
        } catch (SQLException e) {
            throw new RuntimeException("Schema init failed for " + table + ": " + e.getMessage(), e);
        }
        // v2 → v3 in-place upgrade: an existing table lacks the global-deadline
        // column. ADD COLUMN is portable across MySQL / PostgreSQL / SQLite;
        // failure here means the column already exists.
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("ALTER TABLE " + table + " ADD COLUMN global_deadline_ms BIGINT NOT NULL DEFAULT 0");
            LOG.info("upgraded {}: added global_deadline_ms column", table);
        } catch (SQLException columnExists) {
            // expected on every start after the first
        }
    }

    private void ensureDeadTable() {
        String ddl = "CREATE TABLE IF NOT EXISTS " + deadTable + " ("
            + "machine_id           VARCHAR(255) NOT NULL, "
            + "registry_name        VARCHAR(255) NOT NULL, "
            + "current_state        VARCHAR(255) NOT NULL, "
            + "context_class        VARCHAR(512), "
            + "context_json_b64     TEXT, "
            + "saved_at_ms          BIGINT NOT NULL, "
            + "timeout_target_state VARCHAR(255), "
            + "timeout_deadline_ms  BIGINT NOT NULL, "
            + "global_deadline_ms   BIGINT NOT NULL DEFAULT 0, "
            + "dead_reason          VARCHAR(1024), "
            + "dead_at_ms           BIGINT NOT NULL, "
            + "PRIMARY KEY (machine_id, registry_name)"
            + ")";
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate(ddl);
        } catch (SQLException e) {
            throw new RuntimeException("Dead-letter schema init failed for " + deadTable + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void save(MachineSnapshot snap) {
        String upd = "UPDATE " + table + " SET "
            + "current_state=?, context_class=?, context_json_b64=?, "
            + "saved_at_ms=?, timeout_target_state=?, timeout_deadline_ms=?, global_deadline_ms=? "
            + "WHERE machine_id=? AND registry_name=?";
        String ins = "INSERT INTO " + table + " ("
            + "machine_id, registry_name, current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms, global_deadline_ms) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection c = dataSource.getConnection()) {
            boolean prevAuto = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                int rowsUpdated;
                try (PreparedStatement ps = c.prepareStatement(upd)) {
                    bindUpdate(ps, snap);
                    rowsUpdated = ps.executeUpdate();
                }
                if (rowsUpdated == 0) {
                    try (PreparedStatement ps = c.prepareStatement(ins)) {
                        bindInsert(ps, snap);
                        ps.executeUpdate();
                    }
                }
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(prevAuto);
            }
        } catch (SQLException e) {
            throw new RuntimeException("save failed for "
                + snap.machineId() + "/" + snap.registryName() + ": " + e.getMessage(), e);
        }
    }

    private void bindUpdate(PreparedStatement ps, MachineSnapshot s) throws SQLException {
        ps.setString(1, s.currentState());
        ps.setString(2, s.contextClassName());
        ps.setString(3, s.contextJsonBase64());
        ps.setLong  (4, s.savedAtMs());
        ps.setString(5, s.timeoutTargetState());
        ps.setLong  (6, s.timeoutDeadlineMs());
        ps.setLong  (7, s.globalDeadlineMs());
        ps.setString(8, s.machineId());
        ps.setString(9, s.registryName());
    }

    private void bindInsert(PreparedStatement ps, MachineSnapshot s) throws SQLException {
        ps.setString(1, s.machineId());
        ps.setString(2, s.registryName());
        ps.setString(3, s.currentState());
        ps.setString(4, s.contextClassName());
        ps.setString(5, s.contextJsonBase64());
        ps.setLong  (6, s.savedAtMs());
        ps.setString(7, s.timeoutTargetState());
        ps.setLong  (8, s.timeoutDeadlineMs());
        ps.setLong  (9, s.globalDeadlineMs());
    }

    private static final String SELECT_COLS =
        "current_state, context_class, context_json_b64, saved_at_ms, "
        + "timeout_target_state, timeout_deadline_ms, global_deadline_ms";

    private static MachineSnapshot read(ResultSet rs, String machineId, String registryName) throws SQLException {
        return new MachineSnapshot(
            machineId,
            registryName,
            rs.getString(1),
            rs.getString(2),
            rs.getString(3),
            rs.getLong  (4),
            rs.getString(5),
            rs.getLong  (6),
            rs.getLong  (7));
    }

    @Override
    public Optional<MachineSnapshot> load(String machineId, String registryName) {
        String sql = "SELECT " + SELECT_COLS + " FROM " + table + " WHERE machine_id=? AND registry_name=?";
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, machineId);
            ps.setString(2, registryName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(read(rs, machineId, registryName));
            }
        } catch (SQLException e) {
            throw new RuntimeException("load failed for "
                + machineId + "/" + registryName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<MachineSnapshot> loadAll(String machineId) {
        String sql = "SELECT registry_name, " + SELECT_COLS + " FROM " + table + " WHERE machine_id=?";
        List<MachineSnapshot> out = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, machineId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new MachineSnapshot(
                        machineId,
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getLong  (5),
                        rs.getString(6),
                        rs.getLong  (7),
                        rs.getLong  (8)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("loadAll failed for " + machineId + ": " + e.getMessage(), e);
        }
        return out;
    }

    /**
     * Delete the snapshot for one cell. Throws on failure — a lost terminal
     * delete leaves a resurrection-capable orphan, so the registry must see
     * the failure and retry.
     */
    @Override
    public void delete(String machineId, String registryName) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "DELETE FROM " + table + " WHERE machine_id=? AND registry_name=?")) {
            ps.setString(1, machineId);
            ps.setString(2, registryName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("delete failed for "
                + machineId + "/" + registryName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void quarantine(String machineId, String registryName, String reason) {
        String copy = "INSERT INTO " + deadTable + " ("
            + "machine_id, registry_name, current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms, global_deadline_ms, dead_reason, dead_at_ms) "
            + "SELECT machine_id, registry_name, current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms, global_deadline_ms, ?, ? "
            + "FROM " + table + " WHERE machine_id=? AND registry_name=?";
        try (Connection c = dataSource.getConnection()) {
            boolean prevAuto = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                // Idempotence: drop any previous dead row for the same key first.
                try (PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM " + deadTable + " WHERE machine_id=? AND registry_name=?")) {
                    ps.setString(1, machineId);
                    ps.setString(2, registryName);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = c.prepareStatement(copy)) {
                    ps.setString(1, reason != null && reason.length() > 1024 ? reason.substring(0, 1024) : reason);
                    ps.setLong  (2, System.currentTimeMillis());
                    ps.setString(3, machineId);
                    ps.setString(4, registryName);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM " + table + " WHERE machine_id=? AND registry_name=?")) {
                    ps.setString(1, machineId);
                    ps.setString(2, registryName);
                    ps.executeUpdate();
                }
                c.commit();
                LOG.warn("quarantined snapshot {}/{} → {} ({})", machineId, registryName, deadTable, reason);
            } catch (SQLException e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(prevAuto);
            }
        } catch (SQLException e) {
            throw new RuntimeException("quarantine failed for "
                + machineId + "/" + registryName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<MachineSnapshot> loadMatured(String registryName, long nowMs) {
        String sql = "SELECT machine_id, " + SELECT_COLS + " FROM " + table + " WHERE registry_name=? "
            + "AND timeout_deadline_ms > 0 AND timeout_deadline_ms <= ?";
        List<MachineSnapshot> out = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, registryName);
            ps.setLong  (2, nowMs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new MachineSnapshot(
                        rs.getString(1), registryName,
                        rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getLong(5), rs.getString(6), rs.getLong(7), rs.getLong(8)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("loadMatured failed for " + registryName + ": " + e.getMessage(), e);
        }
        return out;
    }

    @Override
    public List<MachineSnapshot> loadAllForRegistry(String registryName) {
        String sql = "SELECT machine_id, " + SELECT_COLS + " FROM " + table + " WHERE registry_name=?";
        List<MachineSnapshot> out = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, registryName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new MachineSnapshot(
                        rs.getString(1), registryName,
                        rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getLong(5), rs.getString(6), rs.getLong(7), rs.getLong(8)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("loadAllForRegistry failed for " + registryName + ": " + e.getMessage(), e);
        }
        return out;
    }

    /** Test helper: how many rows are currently in the table. */
    public int size() {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException("count failed: " + e.getMessage(), e);
        }
    }

    /** Test helper: how many rows sit in the dead-letter table (0 if it was never created). */
    public int deadSize() {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + deadTable)) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            return 0;
        }
    }

    public String getTableName() { return table; }
    public String getDeadTableName() { return deadTable; }
}
