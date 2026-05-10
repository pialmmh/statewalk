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
import java.util.Optional;

/**
 * Generic JDBC implementation of {@link PersistenceProvider}.
 *
 * <p>Single-table schema, portable across SQLite (tested), MySQL, PostgreSQL.
 * Default table name is {@code statewalk_machine_snapshots}; override via
 * the two-arg constructor to support multi-tenant deployments where each
 * tenant's snapshots live in its own table.
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
            + "machine_id           VARCHAR(255) PRIMARY KEY, "
            + "registry_name        VARCHAR(255) NOT NULL, "
            + "current_state        VARCHAR(255) NOT NULL, "
            + "context_class        VARCHAR(512), "
            + "context_json_b64     TEXT, "
            + "saved_at_ms          BIGINT NOT NULL, "
            + "timeout_target_state VARCHAR(255), "
            + "timeout_deadline_ms  BIGINT NOT NULL"
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
            + "registry_name=?, current_state=?, context_class=?, context_json_b64=?, "
            + "saved_at_ms=?, timeout_target_state=?, timeout_deadline_ms=? "
            + "WHERE machine_id=?";
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
            throw new RuntimeException("save failed for " + snap.machineId() + ": " + e.getMessage(), e);
        }
    }

    private void bindUpdate(PreparedStatement ps, MachineSnapshot s) throws SQLException {
        ps.setString(1, s.registryName());
        ps.setString(2, s.currentState());
        ps.setString(3, s.contextClassName());
        ps.setString(4, s.contextJsonBase64());
        ps.setLong  (5, s.savedAtMs());
        ps.setString(6, s.timeoutTargetState());
        ps.setLong  (7, s.timeoutDeadlineMs());
        ps.setString(8, s.machineId());
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
    public Optional<MachineSnapshot> load(String machineId) {
        String sql = "SELECT registry_name, current_state, context_class, context_json_b64, "
            + "saved_at_ms, timeout_target_state, timeout_deadline_ms "
            + "FROM " + table + " WHERE machine_id=?";
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, machineId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new MachineSnapshot(
                    machineId,
                    rs.getString(1),
                    rs.getString(2),
                    rs.getString(3),
                    rs.getString(4),
                    rs.getLong  (5),
                    rs.getString(6),
                    rs.getLong  (7)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("load failed for " + machineId + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String machineId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("DELETE FROM " + table + " WHERE machine_id=?")) {
            ps.setString(1, machineId);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.warn("delete failed for {}: {}", machineId, e.toString());
        }
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
