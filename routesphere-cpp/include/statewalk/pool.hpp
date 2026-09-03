// statewalk-cpp — the machine pool.
//
// Instances are shared_ptr-owned: the pool holds idle ones, cells hold live
// ones, queued tasks hold whatever they captured. Nothing can dangle — a
// stale task simply finds its epoch check failing (see machine.hpp).
#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

#include "log.hpp"

namespace statewalk {

struct PoolStats {
    std::string name;
    int available = 0, totalCreated = 0, totalBorrowed = 0, totalReturned = 0;
    int resetFailures = 0, doubleReturns = 0, capDrops = 0, maxSize = 0;
    /// Every borrow is accounted for: back in the pool, dropped at cap, or dropped on a failed reset.
    int reclaimed() const { return totalReturned + capDrops + resetFailures; }
};

/// T must expose `void resetForReuse()`.
template <class T>
class ObjectPool {
public:
    using Factory = std::function<std::shared_ptr<T>()>;
    using ResetHook = std::function<void(T&)>;

    ObjectPool(std::string name, Factory factory, int maxSize, ResetHook hook = nullptr)
        : name_(std::move(name)), factory_(std::move(factory)), maxSize_(maxSize), hook_(std::move(hook)) {
        int prewarm = std::min(maxSize_ / 4, 100);
        for (int i = 0; i < prewarm; i++) {
            auto obj = factory_();
            obj->resetForReuse();
            pooled_.insert(obj.get());
            available_.push_back(obj);
            created_++;
        }
    }

    std::shared_ptr<T> borrow() {
        std::lock_guard<std::mutex> g(mx_);
        std::shared_ptr<T> obj;
        if (!available_.empty()) {
            obj = available_.front();
            available_.pop_front();
            pooled_.erase(obj.get());
        } else {
            obj = factory_();
            created_++;
        }
        borrowed_++;
        return obj;
    }

    /// Reset + return. A double return is rejected (containment guard); a
    /// reset/hook throw drops the instance; at cap the instance is dropped.
    void returnObject(const std::shared_ptr<T>& obj) {
        if (!obj) return;
        std::lock_guard<std::mutex> g(mx_);
        if (!pooled_.insert(obj.get()).second) {
            doubleReturns_++;
            SW_ERROR("[", name_, "] double return of pooled instance rejected — caller bug");
            return;
        }
        try {
            obj->resetForReuse();
            if (hook_) hook_(*obj);
        } catch (const std::exception& e) {
            SW_WARN("[", name_, "] reset (or reset hook) threw — instance dropped: ", e.what());
            resetFailures_++;
            pooled_.erase(obj.get());
            return;
        }
        if (static_cast<int>(available_.size()) >= maxSize_) {
            pooled_.erase(obj.get());
            capDrops_++;
            return;
        }
        available_.push_back(obj);
        returned_++;
    }

    void clear() {
        std::lock_guard<std::mutex> g(mx_);
        available_.clear();
        pooled_.clear();
    }

    PoolStats stats() const {
        std::lock_guard<std::mutex> g(mx_);
        PoolStats s;
        s.name = name_;
        s.available = static_cast<int>(available_.size());
        s.totalCreated = created_;
        s.totalBorrowed = borrowed_;
        s.totalReturned = returned_;
        s.resetFailures = resetFailures_;
        s.doubleReturns = doubleReturns_;
        s.capDrops = capDrops_;
        s.maxSize = maxSize_;
        return s;
    }

    const std::string& name() const { return name_; }

private:
    std::string name_;
    Factory factory_;
    int maxSize_;
    ResetHook hook_;
    mutable std::mutex mx_;
    std::deque<std::shared_ptr<T>> available_;
    std::unordered_set<T*> pooled_;
    int created_ = 0, borrowed_ = 0, returned_ = 0, resetFailures_ = 0, doubleReturns_ = 0, capDrops_ = 0;
};

}  // namespace statewalk
