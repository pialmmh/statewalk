// statewalk-cpp — execution primitives.
//
// StrandPool: N worker threads, FIFO per key ("strand"). One task per key runs
// at a time; different keys run in parallel. This is the per-cell serial
// invariant: everything touching a cell (events, timers, teardown) is
// submitted under the cell's key and therefore never races.
//
// TimerService: one scheduler thread + a deadline heap. Callbacks must be
// cheap — the registry's callbacks only re-submit onto a strand.
//
// Both are safe to drive from foreign threads (FreeSWITCH event threads,
// HTTP handlers) and never block the caller beyond a brief mutex.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "log.hpp"

namespace statewalk {

class StrandPool {
public:
    using Task = std::function<void()>;

    StrandPool(std::string name, unsigned threads)
        : name_(std::move(name)) {
        if (threads == 0) threads = 1;
        for (unsigned i = 0; i < threads; i++) {
            workers_.emplace_back([this, i] { workerLoop(i); });
        }
    }

    ~StrandPool() { shutdown(); }

    StrandPool(const StrandPool&) = delete;
    StrandPool& operator=(const StrandPool&) = delete;

    /// Enqueue task under key; FIFO per key. Always accepted — after shutdown
    /// the task runs on the caller thread so terminal work still lands.
    void submit(const std::string& key, Task task) {
        std::shared_ptr<Strand> strand;
        {
            std::lock_guard<std::mutex> g(mx_);
            if (closed_) {
                pending_.fetch_add(1);
                runInline(task);
                return;
            }
            auto it = strands_.find(key);
            if (it == strands_.end()) {
                strand = std::make_shared<Strand>();
                strands_.emplace(key, strand);
            } else {
                strand = it->second;
            }
            pending_.fetch_add(1);
            std::lock_guard<std::mutex> sg(strand->mx);
            strand->queue.push_back(std::move(task));
            if (!strand->scheduled) {
                strand->scheduled = true;
                ready_.push_back(strand);
            }
        }
        cv_.notify_one();
    }

    /// Block until every submitted task has finished.
    bool awaitIdle(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> g(idleMx_);
        return idleCv_.wait_for(g, timeout, [this] { return pending_.load() == 0; });
    }

    int inFlight() const { return pending_.load(); }
    const std::string& name() const { return name_; }

    void shutdown() {
        {
            std::lock_guard<std::mutex> g(mx_);
            if (closed_) return;
            closed_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) if (t.joinable()) t.join();
        workers_.clear();
        // Drain anything still queued on the caller thread so nothing is lost.
        std::vector<std::shared_ptr<Strand>> leftovers;
        {
            std::lock_guard<std::mutex> g(mx_);
            for (auto& kv : strands_) leftovers.push_back(kv.second);
            strands_.clear();
            ready_.clear();
        }
        for (auto& s : leftovers) {
            std::deque<Task> q;
            { std::lock_guard<std::mutex> sg(s->mx); q.swap(s->queue); }
            for (auto& t : q) runInline(t);
        }
    }

private:
    struct Strand {
        std::mutex mx;
        std::deque<Task> queue;
        bool scheduled = false;
    };

    void runInline(Task& t) {
        try { t(); } catch (const std::exception& e) {
            SW_WARN("[", name_, "] task threw: ", e.what());
        } catch (...) {
            SW_WARN("[", name_, "] task threw (non-std)");
        }
        finishOne();
    }

    void finishOne() {
        if (pending_.fetch_sub(1) == 1) {
            std::lock_guard<std::mutex> g(idleMx_);
            idleCv_.notify_all();
        }
    }

    void workerLoop(unsigned) {
        for (;;) {
            std::shared_ptr<Strand> strand;
            {
                std::unique_lock<std::mutex> g(mx_);
                cv_.wait(g, [this] { return closed_ || !ready_.empty(); });
                if (ready_.empty()) {
                    if (closed_) return;
                    continue;
                }
                strand = ready_.front();
                ready_.pop_front();
            }
            Task task;
            {
                std::lock_guard<std::mutex> sg(strand->mx);
                if (!strand->queue.empty()) {
                    task = std::move(strand->queue.front());
                    strand->queue.pop_front();
                }
            }
            if (task) runInline(task);
            // Re-arm or park the strand (global lock first, then strand lock).
            {
                std::lock_guard<std::mutex> g(mx_);
                std::lock_guard<std::mutex> sg(strand->mx);
                if (!strand->queue.empty()) {
                    ready_.push_back(strand);           // stays scheduled — fair round-robin
                } else {
                    strand->scheduled = false;
                    // Reclaim the map entry: any concurrent submit takes mx_ first
                    // and will create a fresh strand.
                    for (auto it = strands_.begin(); it != strands_.end(); ++it) {
                        if (it->second == strand) { strands_.erase(it); break; }
                    }
                }
            }
            if (!strand->queue.empty()) cv_.notify_one();
        }
    }

    std::string name_;
    std::mutex mx_;
    std::condition_variable cv_;
    std::unordered_map<std::string, std::shared_ptr<Strand>> strands_;
    std::deque<std::shared_ptr<Strand>> ready_;
    std::vector<std::thread> workers_;
    bool closed_ = false;

    std::atomic<int> pending_{0};
    std::mutex idleMx_;
    std::condition_variable idleCv_;
};

/// Deadline scheduler. schedule() returns a handle usable with cancel(); a
/// cancelled timer never fires. Callbacks run on the scheduler thread — keep
/// them tiny (the registry only re-submits onto a strand).
class TimerService {
public:
    using Clock = std::chrono::steady_clock;
    using Handle = std::uint64_t;

    explicit TimerService(std::string name) : name_(std::move(name)), thread_([this] { loop(); }) {}
    ~TimerService() { shutdown(); }

    TimerService(const TimerService&) = delete;
    TimerService& operator=(const TimerService&) = delete;

    Handle schedule(std::chrono::milliseconds delay, std::function<void()> fn) {
        std::lock_guard<std::mutex> g(mx_);
        if (closed_) return 0;
        Handle h = ++seq_;
        heap_.push(Entry{Clock::now() + delay, h, std::move(fn)});
        live_.insert(h);
        cv_.notify_one();
        return h;
    }

    /// Returns true if the timer had not fired yet (and now never will).
    bool cancel(Handle h) {
        if (h == 0) return false;
        std::lock_guard<std::mutex> g(mx_);
        if (live_.erase(h) == 0) return false;      // already fired (or unknown)
        cancelled_.insert(h);                       // the heap entry is skipped when popped
        return true;
    }

    std::size_t pendingCount() { std::lock_guard<std::mutex> g(mx_); return heap_.size(); }

    void shutdown() {
        {
            std::lock_guard<std::mutex> g(mx_);
            if (closed_) return;
            closed_ = true;
        }
        cv_.notify_all();
        if (thread_.joinable()) thread_.join();
    }

private:
    struct Entry {
        Clock::time_point due;
        Handle handle;
        std::function<void()> fn;
        bool operator>(const Entry& o) const { return due > o.due; }
    };

    void loop() {
        for (;;) {
            std::function<void()> fire;
            {
                std::unique_lock<std::mutex> g(mx_);
                if (closed_) return;
                if (heap_.empty()) { cv_.wait(g); continue; }
                auto& top = heap_.top();
                if (top.due > Clock::now()) { cv_.wait_until(g, top.due); continue; }
                Entry e = std::move(const_cast<Entry&>(top));
                heap_.pop();
                if (cancelled_.erase(e.handle) > 0) continue;
                live_.erase(e.handle);
                fire = std::move(e.fn);
            }
            if (fire) {
                try { fire(); } catch (const std::exception& ex) {
                    SW_WARN("[", name_, "] timer callback threw: ", ex.what());
                } catch (...) {}
            }
        }
    }

    std::string name_;
    std::mutex mx_;
    std::condition_variable cv_;
    std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> heap_;
    std::unordered_set<Handle> cancelled_;   // scheduled-then-cancelled: skipped at pop
    std::unordered_set<Handle> live_;        // scheduled, not yet fired or cancelled
    Handle seq_ = 0;
    bool closed_ = false;
    std::thread thread_;
};

inline std::int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace statewalk
