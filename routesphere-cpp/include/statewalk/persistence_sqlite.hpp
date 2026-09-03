// statewalk-cpp — SQLite persistence provider (requires libsqlite3).
//
// Same single-table schema as the Java JdbcPersistenceProvider, plus a
// <table>_dead dead-letter table for quarantined snapshots. One connection
// opened in FULLMUTEX mode, serialized by a provider mutex (persist executor
// threads may call concurrently). Suitable for a FreeSWITCH module that wants
// durable hibernation without a network database.
#pragma once

#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "persistence.hpp"

namespace statewalk {

class SqlitePersistenceProvider : public PersistenceProvider {
public:
    explicit SqlitePersistenceProvider(const std::string& path, std::string table = "statewalk_machine_snapshots")
        : table_(std::move(table)), dead_(table_ + "_dead") {
        if (sqlite3_open_v2(path.c_str(), &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr) != SQLITE_OK)
            throw std::runtime_error("sqlite open failed: " + std::string(db_ ? sqlite3_errmsg(db_) : "?"));
        exec("PRAGMA journal_mode=WAL");
        exec("PRAGMA synchronous=NORMAL");
        exec("CREATE TABLE IF NOT EXISTS " + table_ + " ("
             "machine_id TEXT NOT NULL, registry_name TEXT NOT NULL, current_state TEXT NOT NULL, "
             "context_type TEXT, context_encoded TEXT, saved_at_ms INTEGER NOT NULL, "
             "timeout_target_state TEXT, timeout_deadline_ms INTEGER NOT NULL, global_deadline_ms INTEGER NOT NULL DEFAULT 0, "
             "PRIMARY KEY (machine_id, registry_name))");
        exec("CREATE TABLE IF NOT EXISTS " + dead_ + " ("
             "machine_id TEXT NOT NULL, registry_name TEXT NOT NULL, current_state TEXT NOT NULL, "
             "context_type TEXT, context_encoded TEXT, saved_at_ms INTEGER NOT NULL, "
             "timeout_target_state TEXT, timeout_deadline_ms INTEGER NOT NULL, global_deadline_ms INTEGER NOT NULL DEFAULT 0, "
             "dead_reason TEXT, dead_at_ms INTEGER NOT NULL, PRIMARY KEY (machine_id, registry_name))");
    }

    ~SqlitePersistenceProvider() override { if (db_) sqlite3_close(db_); }

    void save(const Snapshot& s) override {
        std::lock_guard<std::mutex> g(mx_);
        Stmt st(db_, "INSERT OR REPLACE INTO " + table_ + " (machine_id, registry_name, current_state, context_type, context_encoded, "
                     "saved_at_ms, timeout_target_state, timeout_deadline_ms, global_deadline_ms) VALUES (?,?,?,?,?,?,?,?,?)");
        st.text(1, s.machineId).text(2, s.registryName).text(3, s.currentState).text(4, s.contextType).text(5, s.contextEncoded)
          .i64(6, s.savedAtMs).optText(7, s.timeoutTargetState).i64(8, s.timeoutDeadlineMs).i64(9, s.globalDeadlineMs);
        st.step("save");
    }

    std::optional<Snapshot> load(const std::string& id, const std::string& reg) override {
        std::lock_guard<std::mutex> g(mx_);
        Stmt st(db_, "SELECT machine_id, registry_name, current_state, context_type, context_encoded, saved_at_ms, "
                     "timeout_target_state, timeout_deadline_ms, global_deadline_ms FROM " + table_ + " WHERE machine_id=? AND registry_name=?");
        st.text(1, id).text(2, reg);
        if (!st.row()) return std::nullopt;
        return read(st);
    }

    void remove(const std::string& id, const std::string& reg) override {
        std::lock_guard<std::mutex> g(mx_);
        Stmt st(db_, "DELETE FROM " + table_ + " WHERE machine_id=? AND registry_name=?");
        st.text(1, id).text(2, reg).step("delete");
    }

    std::vector<Snapshot> loadAllForRegistry(const std::string& reg) override {
        std::lock_guard<std::mutex> g(mx_);
        Stmt st(db_, "SELECT machine_id, registry_name, current_state, context_type, context_encoded, saved_at_ms, "
                     "timeout_target_state, timeout_deadline_ms, global_deadline_ms FROM " + table_ + " WHERE registry_name=?");
        st.text(1, reg);
        std::vector<Snapshot> out;
        while (st.row()) out.push_back(read(st));
        return out;
    }

    void quarantine(const std::string& id, const std::string& reg, const std::string& reason) override {
        std::lock_guard<std::mutex> g(mx_);
        // Idempotence: a repeat call for an id whose live row is already gone
        // must NOT touch the existing dead-letter row (deleting it and copying
        // nothing would destroy the very data quarantine exists to keep).
        {
            Stmt e(db_, "SELECT 1 FROM " + table_ + " WHERE machine_id=? AND registry_name=?");
            e.text(1, id).text(2, reg);
            if (!e.row()) return;
        }
        exec("BEGIN");
        try {
            { Stmt d(db_, "DELETE FROM " + dead_ + " WHERE machine_id=? AND registry_name=?"); d.text(1, id).text(2, reg).step("dead-delete"); }
            { Stmt c(db_, "INSERT INTO " + dead_ + " SELECT machine_id, registry_name, current_state, context_type, context_encoded, saved_at_ms, "
                          "timeout_target_state, timeout_deadline_ms, global_deadline_ms, ?, ? FROM " + table_ + " WHERE machine_id=? AND registry_name=?");
              c.text(1, reason).i64(2, nowMsLocal()).text(3, id).text(4, reg).step("dead-copy"); }
            { Stmt r(db_, "DELETE FROM " + table_ + " WHERE machine_id=? AND registry_name=?"); r.text(1, id).text(2, reg).step("live-delete"); }
            exec("COMMIT");
        } catch (...) { exec("ROLLBACK"); throw; }
    }

    int size() { std::lock_guard<std::mutex> g(mx_); return count(table_); }
    int deadSize() { std::lock_guard<std::mutex> g(mx_); return count(dead_); }

private:
    struct Stmt {
        sqlite3_stmt* s = nullptr;
        Stmt(sqlite3* db, const std::string& sql) {
            if (sqlite3_prepare_v2(db, sql.c_str(), -1, &s, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed: " + std::string(sqlite3_errmsg(db)));
        }
        ~Stmt() { if (s) sqlite3_finalize(s); }
        Stmt& text(int i, const std::string& v) { sqlite3_bind_text(s, i, v.c_str(), -1, SQLITE_TRANSIENT); return *this; }
        Stmt& optText(int i, const std::optional<std::string>& v) { if (v) sqlite3_bind_text(s, i, v->c_str(), -1, SQLITE_TRANSIENT); else sqlite3_bind_null(s, i); return *this; }
        Stmt& i64(int i, std::int64_t v) { sqlite3_bind_int64(s, i, v); return *this; }
        void step(const char* what) { int rc = sqlite3_step(s); if (rc != SQLITE_DONE && rc != SQLITE_ROW) throw std::runtime_error(std::string("sqlite ") + what + " failed rc=" + std::to_string(rc)); }
        bool row() { int rc = sqlite3_step(s); if (rc == SQLITE_ROW) return true; if (rc == SQLITE_DONE) return false; throw std::runtime_error("sqlite step failed rc=" + std::to_string(rc)); }
        std::string col(int i) { auto* t = sqlite3_column_text(s, i); return t ? reinterpret_cast<const char*>(t) : ""; }
        bool isNull(int i) { return sqlite3_column_type(s, i) == SQLITE_NULL; }
        std::int64_t colI64(int i) { return sqlite3_column_int64(s, i); }
    };

    static Snapshot read(Stmt& st) {
        Snapshot s;
        s.machineId = st.col(0); s.registryName = st.col(1); s.currentState = st.col(2);
        s.contextType = st.col(3); s.contextEncoded = st.col(4); s.savedAtMs = st.colI64(5);
        if (!st.isNull(6)) s.timeoutTargetState = st.col(6);
        s.timeoutDeadlineMs = st.colI64(7); s.globalDeadlineMs = st.colI64(8);
        return s;
    }

    void exec(const std::string& sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            std::string m = err ? err : "?"; sqlite3_free(err);
            throw std::runtime_error("sqlite exec failed: " + m + " [" + sql.substr(0, 40) + "]");
        }
    }
    int count(const std::string& t) { Stmt st(db_, "SELECT COUNT(*) FROM " + t); st.row(); return static_cast<int>(st.colI64(0)); }
    static std::int64_t nowMsLocal() { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(); }

    std::string table_, dead_;
    sqlite3* db_ = nullptr;
    std::mutex mx_;
};

}  // namespace statewalk
