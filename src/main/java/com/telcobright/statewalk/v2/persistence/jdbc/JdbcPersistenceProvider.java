package com.telcobright.statewalk.v2.persistence.jdbc;

import com.telcobright.statewalk.v2.persistence.MachineSnapshot;
import com.telcobright.statewalk.v2.persistence.PersistenceProvider;

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
 * <p>Schema is auto-created on construction via {@code CREATE TABLE IF NOT EXISTS}.
 */
public class JdbcPersistenceProvider implements PersistenceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcPersistenceProvider.class);

    private static final String DEFAULT_TABLE = "statewalk_machine_snapshots";

    private final DataSource dataSource;
    private final String table;

    public JdbcPersistenceProvider(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE);
    }

    public JdbcPersistenceProvider(DataSource dataSource, String table) {
        this.dataSource = dataSource;
        this.table = table;
        ensureSchema();
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
            + "PRIMARY KEY (machine_id, registry_name)"
            + ")";
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate(ddl);
        } catch (SQLException e) {
            throw new RuntimeException("Schema init failed for " + table + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void save(MachineSnapshot snap) {
        String upd = "UPDATE " + table + " SET "
            + "current_state=?, context_class=?, context_json_b64=?, "
            + "saved_at_ms=?, timeout_target_state=?, timeout_deadline_ms=? "
            + "WHERE machine_id=? AND registry_name=?";
        String ins = "INSERT INTO " + table + " ("
            + "machine_id, registry_name, current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

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
        ps.setString(7, s.machineId());
        ps.setString(8, s.registryName());
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
    }

    @Override
    public Optional<MachineSnapshot> load(String machineId, String registryName) {
        String sql = "SELECT current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms "
            + "FROM " + table + " WHERE machine_id=? AND registry_name=?";
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, machineId);
            ps.setString(2, registryName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new MachineSnapshot(
                    machineId,
                    registryName,
                    rs.getString(1),
                    rs.getString(2),
                    rs.getString(3),
                    rs.getLong  (4),
                    rs.getString(5),
                    rs.getLong  (6)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("load failed for "
                + machineId + "/" + registryName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<MachineSnapshot> loadAll(String machineId) {
        String sql = "SELECT registry_name, current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms "
            + "FROM " + table + " WHERE machine_id=?";
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
                        rs.getLong  (7)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("loadAll failed for " + machineId + ": " + e.getMessage(), e);
        }
        return out;
    }

    @Override
    public void delete(String machineId, String registryName) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "DELETE FROM " + table + " WHERE machine_id=? AND registry_name=?")) {
            ps.setString(1, machineId);
            ps.setString(2, registryName);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.warn("delete failed for {}/{}: {}", machineId, registryName, e.toString());
        }
    }

    @Override
    public List<MachineSnapshot> loadMatured(String registryName, long nowMs) {
        String sql = "SELECT machine_id, current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms "
            + "FROM " + table + " WHERE registry_name=? "
            + "AND timeout_deadline_ms > 0 AND timeout_deadline_ms <= ?";
        List<MachineSnapshot> out = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, registryName);
            ps.setLong  (2, nowMs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new MachineSnapshot(
                        rs.getString(1),
                        registryName,
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getLong  (5),
                        rs.getString(6),
                        rs.getLong  (7)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("loadMatured failed for " + registryName + ": " + e.getMessage(), e);
        }
        return out;
    }

    @Override
    public List<MachineSnapshot> loadAllForRegistry(String registryName) {
        String sql = "SELECT machine_id, current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms "
            + "FROM " + table + " WHERE registry_name=?";
        List<MachineSnapshot> out = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, registryName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new MachineSnapshot(
                        rs.getString(1),
                        registryName,
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getLong  (5),
                        rs.getString(6),
                        rs.getLong  (7)));
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

    public String getTableName() { return table; }
}
