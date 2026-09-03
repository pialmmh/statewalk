// statewalk-cpp — quota admission (partner / route × concurrent / TPS).
#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "executor.hpp"

namespace statewalk {

enum class RejectCause {
    CapacityExceeded,
    PartnerConcurrencyExceeded,
    RouteConcurrencyExceeded,
    PartnerTpsExceeded,
    RouteTpsExceeded,
    DuplicateId,
    ShuttingDown,
    PoolIntegrityError
};

inline const char* toString(RejectCause c) {
    switch (c) {
        case RejectCause::CapacityExceeded:           return "CAPACITY_EXCEEDED";
        case RejectCause::PartnerConcurrencyExceeded: return "PARTNER_CONCURRENCY_EXCEEDED";
        case RejectCause::RouteConcurrencyExceeded:   return "ROUTE_CONCURRENCY_EXCEEDED";
        case RejectCause::PartnerTpsExceeded:         return "PARTNER_TPS_EXCEEDED";
        case RejectCause::RouteTpsExceeded:           return "ROUTE_TPS_EXCEEDED";
        case RejectCause::DuplicateId:                return "DUPLICATE_ID";
        case RejectCause::ShuttingDown:               return "SHUTTING_DOWN";
        default:                                      return "POOL_INTEGRITY_ERROR";
    }
}

struct DispatchResult {
    bool accepted = true;
    std::optional<RejectCause> rejectCause;
    static DispatchResult ok() { return {}; }
    static DispatchResult rejected(RejectCause c) { return DispatchResult{false, c}; }
};

/// Either key may be empty — empty disables that dimension for the request.
struct QuotaKeys {
    std::string partnerKey;
    std::string routeKey;
    static QuotaKeys none() { return {}; }
    static QuotaKeys ofPartner(std::string p) { return {std::move(p), {}}; }
    static QuotaKeys ofRoute(std::string r) { return {{}, std::move(r)}; }
    static QuotaKeys of(std::string p, std::string r) { return {std::move(p), std::move(r)}; }
    bool isNone() const { return partnerKey.empty() && routeKey.empty(); }
    bool operator==(const QuotaKeys& o) const { return partnerKey == o.partnerKey && routeKey == o.routeKey; }
};

/// 0 on any field disables that check.
struct QuotaLimits {
    int maxConcurrentPerPartner = 0;
    int maxConcurrentPerRoute = 0;
    int maxTpsPerPartner = 0;
    int maxTpsPerRoute = 0;
    bool enforces() const {
        return maxConcurrentPerPartner > 0 || maxConcurrentPerRoute > 0 || maxTpsPerPartner > 0 || maxTpsPerRoute > 0;
    }
    static QuotaLimits unlimited() { return {}; }
};

/// Exact counters: a rejected acquire rolls back EVERYTHING it took (TPS
/// included); zero-count entries are pruned; rebind uses the per-dimension
/// acquire-new-before-release-old protocol.
class QuotaController {
public:
    std::optional<RejectCause> tryAcquire(const QuotaKeys& k, const QuotaLimits& l) {
        if (!l.enforces()) return std::nullopt;
        std::lock_guard<std::mutex> g(mx_);
        bool pConc = false, rConc = false, pTps = false;
        std::optional<RejectCause> failure;

        if (l.maxConcurrentPerPartner > 0 && !k.partnerKey.empty()) {
            pConc = true;
            if (++partnerActive_[k.partnerKey] > l.maxConcurrentPerPartner) failure = RejectCause::PartnerConcurrencyExceeded;
        }
        if (!failure && l.maxConcurrentPerRoute > 0 && !k.routeKey.empty()) {
            rConc = true;
            if (++routeActive_[k.routeKey] > l.maxConcurrentPerRoute) failure = RejectCause::RouteConcurrencyExceeded;
        }
        if (!failure && l.maxTpsPerPartner > 0 && !k.partnerKey.empty()) {
            if (partnerTps_[k.partnerKey].tryAcquire(l.maxTpsPerPartner)) pTps = true;
            else failure = RejectCause::PartnerTpsExceeded;
        }
        if (!failure && l.maxTpsPerRoute > 0 && !k.routeKey.empty()) {
            if (!routeTps_[k.routeKey].tryAcquire(l.maxTpsPerRoute)) failure = RejectCause::RouteTpsExceeded;
        }
        if (failure) {
            if (pConc) decPrune(partnerActive_, k.partnerKey);
            if (rConc) decPrune(routeActive_, k.routeKey);
            if (pTps) partnerTps_[k.partnerKey].release();
        }
        return failure;
    }

    /// Restore-path re-acquire: unchecked, mirrors tryAcquire's gating.
    void acquireUnchecked(const QuotaKeys& k, const QuotaLimits& l) {
        if (!l.enforces()) return;
        std::lock_guard<std::mutex> g(mx_);
        if (l.maxConcurrentPerPartner > 0 && !k.partnerKey.empty()) partnerActive_[k.partnerKey]++;
        if (l.maxConcurrentPerRoute > 0 && !k.routeKey.empty()) routeActive_[k.routeKey]++;
    }

    void release(const QuotaKeys& k, const QuotaLimits& l) {
        if (!l.enforces()) return;
        std::lock_guard<std::mutex> g(mx_);
        if (l.maxConcurrentPerPartner > 0 && !k.partnerKey.empty()) decPrune(partnerActive_, k.partnerKey);
        if (l.maxConcurrentPerRoute > 0 && !k.routeKey.empty()) decPrune(routeActive_, k.routeKey);
    }

    std::optional<RejectCause> tryAcquirePartner(const std::string& key, const QuotaLimits& l) {
        if (l.maxConcurrentPerPartner <= 0 || key.empty()) return std::nullopt;
        std::lock_guard<std::mutex> g(mx_);
        if (++partnerActive_[key] > l.maxConcurrentPerPartner) { decPrune(partnerActive_, key); return RejectCause::PartnerConcurrencyExceeded; }
        return std::nullopt;
    }
    std::optional<RejectCause> tryAcquireRoute(const std::string& key, const QuotaLimits& l) {
        if (l.maxConcurrentPerRoute <= 0 || key.empty()) return std::nullopt;
        std::lock_guard<std::mutex> g(mx_);
        if (++routeActive_[key] > l.maxConcurrentPerRoute) { decPrune(routeActive_, key); return RejectCause::RouteConcurrencyExceeded; }
        return std::nullopt;
    }
    void releasePartner(const std::string& key, const QuotaLimits& l) {
        if (l.maxConcurrentPerPartner <= 0 || key.empty()) return;
        std::lock_guard<std::mutex> g(mx_); decPrune(partnerActive_, key);
    }
    void releaseRoute(const std::string& key, const QuotaLimits& l) {
        if (l.maxConcurrentPerRoute <= 0 || key.empty()) return;
        std::lock_guard<std::mutex> g(mx_); decPrune(routeActive_, key);
    }

    int partnerActive(const std::string& key) const { std::lock_guard<std::mutex> g(mx_); auto it = partnerActive_.find(key); return it == partnerActive_.end() ? 0 : it->second; }
    int routeActive(const std::string& key) const { std::lock_guard<std::mutex> g(mx_); auto it = routeActive_.find(key); return it == routeActive_.end() ? 0 : it->second; }
    std::size_t trackedKeyCount() const { std::lock_guard<std::mutex> g(mx_); return partnerActive_.size() + routeActive_.size(); }

    void pruneStaleTpsBuckets() {
        std::lock_guard<std::mutex> g(mx_);
        auto nowSec = nowMs() / 1000;
        for (auto it = partnerTps_.begin(); it != partnerTps_.end();) it = it->second.isStale(nowSec) ? partnerTps_.erase(it) : std::next(it);
        for (auto it = routeTps_.begin(); it != routeTps_.end();) it = it->second.isStale(nowSec) ? routeTps_.erase(it) : std::next(it);
    }

private:
    struct TpsBucket {
        std::int64_t windowSec = 0;
        int count = 0;
        bool tryAcquire(int max) {
            auto now = nowMs() / 1000;
            if (windowSec != now) { windowSec = now; count = 1; return true; }
            if (count >= max) return false;
            count++; return true;
        }
        void release() { if (windowSec == nowMs() / 1000 && count > 0) count--; }
        bool isStale(std::int64_t nowSec) const { return windowSec < nowSec - 2; }
    };

    static void decPrune(std::unordered_map<std::string, int>& m, const std::string& key) {
        auto it = m.find(key);
        if (it == m.end()) { SW_WARN("quota release for untracked key '", key, "' — over-release bug upstream"); return; }
        if (--it->second <= 0) m.erase(it);
    }

    mutable std::mutex mx_;
    std::unordered_map<std::string, int> partnerActive_, routeActive_;
    std::unordered_map<std::string, TpsBucket> partnerTps_, routeTps_;
};

}  // namespace statewalk
