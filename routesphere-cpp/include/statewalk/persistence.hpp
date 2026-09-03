// statewalk-cpp — persistence contract + in-memory provider.
//
// One snapshot per cell, keyed by (machineId, registryName). Written
// asynchronously on every transition; the recovery, rehydration and
// hibernation stories are built on it. Durable providers: sqlite (bundled,
// optional), mysql (optional), or your own.
#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace statewalk {

struct Snapshot {
    std::string machineId;
    std::string registryName;
    std::string currentState;
    std::string contextType;       // codec-declared type tag (informational)
    std::string contextEncoded;    // opaque — produced by the machine's codec
    std::int64_t savedAtMs = 0;
    std::optional<std::string> timeoutTargetState;
    std::int64_t timeoutDeadlineMs = 0;   // 0 = none
    std::int64_t globalDeadlineMs = 0;    // supervisor rows only; 0 = none

    bool timeoutFiredBy(std::int64_t nowMs) const { return timeoutDeadlineMs > 0 && nowMs >= timeoutDeadlineMs; }
    bool globalDeadlinePassedBy(std::int64_t nowMs) const { return globalDeadlineMs > 0 && nowMs >= globalDeadlineMs; }
};

class PersistenceProvider {
public:
    virtual ~PersistenceProvider() = default;

    virtual void save(const Snapshot& s) = 0;
    virtual std::optional<Snapshot> load(const std::string& machineId, const std::string& registryName) = 0;
    /// Delete one cell's snapshot. MAY throw — the registry retries.
    virtual void remove(const std::string& machineId, const std::string& registryName) = 0;
    /// Every snapshot of a registry (= every unfinished machine). Default: none
    /// (opts out of startup recovery; lazy rehydration still works).
    virtual std::vector<Snapshot> loadAllForRegistry(const std::string& registryName) { (void)registryName; return {}; }
    /// Snapshots whose state deadline matured at nowMs. Default: filter loadAllForRegistry.
    virtual std::vector<Snapshot> loadMatured(const std::string& registryName, std::int64_t nowMs) {
        std::vector<Snapshot> out;
        for (auto& s : loadAllForRegistry(registryName)) if (s.timeoutFiredBy(nowMs)) out.push_back(s);
        return out;
    }
    /// Move a bad snapshot to a dead-letter area (never destroy recovery
    /// data). Default: leave in place.
    virtual void quarantine(const std::string& machineId, const std::string& registryName, const std::string& reason) {
        (void)machineId; (void)registryName; (void)reason;
    }
};

class InMemoryPersistenceProvider : public PersistenceProvider {
public:
    void save(const Snapshot& s) override { std::lock_guard<std::mutex> g(mx_); store_[key(s.machineId, s.registryName)] = s; }
    std::optional<Snapshot> load(const std::string& id, const std::string& reg) override {
        std::lock_guard<std::mutex> g(mx_);
        auto it = store_.find(key(id, reg));
        return it == store_.end() ? std::nullopt : std::optional<Snapshot>(it->second);
    }
    void remove(const std::string& id, const std::string& reg) override { std::lock_guard<std::mutex> g(mx_); store_.erase(key(id, reg)); }
    std::vector<Snapshot> loadAllForRegistry(const std::string& reg) override {
        std::lock_guard<std::mutex> g(mx_);
        std::vector<Snapshot> out;
        for (auto& kv : store_) if (kv.second.registryName == reg) out.push_back(kv.second);
        return out;
    }
    void quarantine(const std::string& id, const std::string& reg, const std::string& reason) override {
        std::lock_guard<std::mutex> g(mx_);
        auto it = store_.find(key(id, reg));
        if (it == store_.end()) return;
        dead_[key(id, reg)] = {it->second, reason};
        store_.erase(it);
    }

    std::size_t size() const { std::lock_guard<std::mutex> g(mx_); return store_.size(); }
    std::size_t deadSize() const { std::lock_guard<std::mutex> g(mx_); return dead_.size(); }
    std::optional<std::string> deadReason(const std::string& id, const std::string& reg) const {
        std::lock_guard<std::mutex> g(mx_);
        auto it = dead_.find(key(id, reg));
        return it == dead_.end() ? std::nullopt : std::optional<std::string>(it->second.second);
    }
    void clear() { std::lock_guard<std::mutex> g(mx_); store_.clear(); dead_.clear(); }

private:
    static std::string key(const std::string& id, const std::string& reg) { return reg + "\x1f" + id; }
    mutable std::mutex mx_;
    std::map<std::string, Snapshot> store_;
    std::map<std::string, std::pair<Snapshot, std::string>> dead_;
};

}  // namespace statewalk
