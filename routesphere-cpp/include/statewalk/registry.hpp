// statewalk-cpp — StatemachineRegistry<T>: the runtime.
//
// One registry per domain. Hosts every live request as a row of cells
// (cell 0 = supervisor, 1+ = children). Concurrency model (v3, same as the
// Java library):
//   * one serial strand per cell — events, timers and teardown never race;
//   * atomic lifecycle claims per cell (Live → Terminating | Suspending);
//   * epoch identity on every deferred task;
//   * atomic dispatch / single-flight restore per id;
//   * terminal work unconditional (forced failover on shutdown etc.);
//   * persistence async on a dedicated strand pool; the hot path never
//     touches the store.
//
// Construction is builder-only: StatemachineRegistry<T>::builder(name).
#pragma once

#include <algorithm>
#include <any>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "channel.hpp"
#include "event.hpp"
#include "executor.hpp"
#include "log.hpp"
#include "machine.hpp"
#include "persistence.hpp"
#include "pool.hpp"
#include "quota.hpp"
#include "spec.hpp"
#include "supervisor.hpp"

namespace statewalk {

/// Thrown by onInboundEvent for an unknown id with no recovery path.
struct UnknownRequest : std::logic_error { using std::logic_error::logic_error; };
/// Overload / shutdown rejection of an inbound event.
struct InboundRejected : std::runtime_error { using std::runtime_error::runtime_error; };

template <class T>
class StatemachineRegistry final : public RegistryBase {
public:
    static constexpr char CHILD_ID_SEPARATOR = '#';
    static constexpr std::int64_t FINISHED_TOMBSTONE_MS = 5 * 60 * 1000;
    static constexpr std::int64_t QUARANTINE_RETRY_MS = 10 * 60 * 1000;

    class Builder;
    static Builder builder(std::string name) { return Builder(std::move(name)); }

    ~StatemachineRegistry() override { shutdown(); }
    StatemachineRegistry(const StatemachineRegistry&) = delete;
    StatemachineRegistry& operator=(const StatemachineRegistry&) = delete;

    const std::string& name() const override { return name_; }

    // ─────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────

    /// Start a request. Admission: SHUTTING_DOWN → DUPLICATE_ID (atomic claim)
    /// → CAPACITY → QUOTA → POOL_INTEGRITY. Every failure unwinds what it took.
    DispatchResult dispatch(const std::string& parentId, T task) {
        if (shuttingDown_.load()) return DispatchResult::rejected(RejectCause::ShuttingDown);
        auto row = std::make_shared<Row>();
        {
            std::lock_guard<std::mutex> g(activeMx_);
            if (!active_.emplace(parentId, row).second) {
                SW_WARN("[", name_, "] duplicate dispatch for id=", parentId);
                return DispatchResult::rejected(RejectCause::DuplicateId);
            }
        }
        bool committed = false;
        QuotaKeys keys;
        bool quotaHeld = false;
        auto unwind = [&] {
            if (committed) return;
            removeRow(parentId, row);
            cancelGlobalTimeout(parentId);
            { std::lock_guard<std::mutex> g(globalMx_); globalDeadlines_.erase(parentId); }
            if (quotaHeld) {
                std::lock_guard<std::mutex> g(quotaMx_);
                dispatchQuotaKeys_.erase(parentId);
                quota_.release(keys, quotaLimits_);
            }
        };
        try {
            if (maxConcurrent_ > 0 && activeIdCount() > maxConcurrent_) { unwind(); return DispatchResult::rejected(RejectCause::CapacityExceeded); }
            if (quotaKeysExtractor_) keys = quotaKeysExtractor_(task);
            if (auto rc = quota_.tryAcquire(keys, quotaLimits_)) { unwind(); return DispatchResult::rejected(*rc); }
            quotaHeld = true;
            if (!keys.isNone()) { std::lock_guard<std::mutex> g(quotaMx_); dispatchQuotaKeys_[parentId] = keys; }
            { std::lock_guard<std::mutex> g(tombMx_); recentlyFinished_.erase(parentId); }
            if (globalTimeoutMs_ > 0) {
                std::int64_t deadline = nowMs() + globalTimeoutMs_;
                { std::lock_guard<std::mutex> g(globalMx_); globalDeadlines_[parentId] = deadline; }
                scheduleGlobalTimeoutAt(parentId, deadline);
            }
            auto cell = bindAndStart(row, supervisorName_, parentId, parentId, std::any(std::move(task)));
            if (!cell) { unwind(); return DispatchResult::rejected(RejectCause::PoolIntegrityError); }
            committed = true;
            return DispatchResult::ok();
        } catch (...) {
            unwind();
            throw;
        }
    }

    /// Fire-and-forget wire event. Throws UnknownRequest for an id with no
    /// recovery path; overload/shutdown are logged drops.
    void onInboundEvent(const std::string& parentId, EventPtr ev) {
        auto ack = submitInboundInternal(parentId, std::move(ev), false, true);
        if (ack.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            try { ack.get(); } catch (const UnknownRequest&) { throw; } catch (...) { /* logged */ }
        }
    }

    /// Wire event with an ack: completes when the cell processed it, fails
    /// with UnknownRequest / InboundRejected.
    Ack submitInbound(const std::string& parentId, EventPtr ev) { return submitInboundInternal(parentId, std::move(ev), false, true); }

    /// Swap the quota identity a live request holds — acquire-new-before-
    /// release-old per changed dimension; nullopt on success.
    std::optional<RejectCause> rebindQuotaKeys(const std::string& id, const QuotaKeys& wanted) {
        std::lock_guard<std::mutex> g(quotaMx_);
        if (!supervisorCell(id)) throw std::logic_error("[" + name_ + "] rebindQuotaKeys: no live request with id=" + id);
        QuotaKeys old = dispatchQuotaKeys_.count(id) ? dispatchQuotaKeys_[id] : QuotaKeys::none();
        if (old == wanted) return std::nullopt;
        bool partnerChanges = old.partnerKey != wanted.partnerKey;
        bool routeChanges = old.routeKey != wanted.routeKey;
        if (partnerChanges && !wanted.partnerKey.empty())
            if (auto rc = quota_.tryAcquirePartner(wanted.partnerKey, quotaLimits_)) return rc;
        if (routeChanges && !wanted.routeKey.empty()) {
            if (auto rc = quota_.tryAcquireRoute(wanted.routeKey, quotaLimits_)) {
                if (partnerChanges && !wanted.partnerKey.empty()) quota_.releasePartner(wanted.partnerKey, quotaLimits_);
                return rc;
            }
        }
        if (partnerChanges && !old.partnerKey.empty()) quota_.releasePartner(old.partnerKey, quotaLimits_);
        if (routeChanges && !old.routeKey.empty()) quota_.releaseRoute(old.routeKey, quotaLimits_);
        if (wanted.isNone()) dispatchQuotaKeys_.erase(id); else dispatchQuotaKeys_[id] = wanted;
        return std::nullopt;
    }

    QuotaKeys quotaKeysOf(const std::string& id) { std::lock_guard<std::mutex> g(quotaMx_); auto it = dispatchQuotaKeys_.find(id); return it == dispatchQuotaKeys_.end() ? QuotaKeys::none() : it->second; }
    int quotaPartnerActive(const std::string& k) const { return quota_.partnerActive(k); }
    int quotaRouteActive(const std::string& k) const { return quota_.routeActive(k); }

    bool hasAny(const std::string& id) const { std::lock_guard<std::mutex> g(activeMx_); return active_.count(id) > 0; }
    int activeIdCount() const { std::lock_guard<std::mutex> g(activeMx_); return static_cast<int>(active_.size()); }
    int activeCellCount() const {
        std::vector<std::shared_ptr<Row>> rows;
        { std::lock_guard<std::mutex> g(activeMx_); for (auto& kv : active_) rows.push_back(kv.second); }
        int n = 0; for (auto& r : rows) n += static_cast<int>(r->snapshot().size());
        return n;
    }
    bool wasRecentlyFinished(const std::string& id) const { std::lock_guard<std::mutex> g(tombMx_); return recentlyFinished_.count(id) > 0; }

    /// LIVE supervisor state (nullopt when not in memory — check the store for hibernated ones).
    std::optional<std::string> supervisorStateOf(const std::string& id) const {
        auto c = supervisorCell(id); return c ? std::optional<std::string>(c->machine->currentState()) : std::nullopt;
    }
    /// LIVE state of any cell type of the request.
    std::optional<std::string> stateOf(const std::string& id, const std::string& typeName) const {
        auto c = findLiveCell(id, typeName); return c ? std::optional<std::string>(c->machine->currentState()) : std::nullopt;
    }
    /// A COPY of the live supervisor's context (nullopt when not in memory).
    std::optional<T> supervisorContextOf(const std::string& id) const {
        auto c = supervisorCell(id);
        if (!c) return std::nullopt;
        auto* typed = dynamic_cast<Machine<T>*>(c->machine.get());
        return typed ? std::optional<T>(typed->context()) : std::nullopt;
    }
    /// The live machine of a cell — introspection for tests/tooling.
    std::shared_ptr<MachineBase> machineOf(const std::string& id, const std::string& typeName) const {
        auto c = findLiveCell(id, typeName); return c ? c->machine : nullptr;
    }
    PoolStats poolStats(const std::string& typeName) const { auto it = pools_.find(typeName); return it == pools_.end() ? PoolStats{} : it->second->stats(); }

    bool awaitIdle(std::chrono::milliseconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        int prev = -1, stable = 0;
        for (int pass = 0; pass < 60; pass++) {
            auto remaining = [&] { return std::chrono::duration_cast<std::chrono::milliseconds>(std::max(std::chrono::steady_clock::duration::zero(), deadline - std::chrono::steady_clock::now())); };
            if (!work_.awaitIdle(remaining())) return false;
            if (persistWork_ && !persistWork_->awaitIdle(remaining())) return false;
            int cells = activeCellCount();
            if (cells == prev) { if (++stable >= 2) return true; } else { stable = 0; prev = cells; }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            if (std::chrono::steady_clock::now() >= deadline) return false;
        }
        return false;
    }

    /// Stop the channel, drive every live request through its failover state
    /// (records ship), drain, close. Hibernated rows are untouched.
    void shutdown() {
        if (shuttingDown_.exchange(true)) return;
        if (channel_) { try { channel_->stop(); } catch (const std::exception& e) { SW_WARN("[", name_, "] channel stop threw: ", e.what()); } }
        std::vector<std::string> ids;
        { std::lock_guard<std::mutex> g(activeMx_); for (auto& kv : active_) ids.push_back(kv.first); }
        for (auto& id : ids) abortRequest(id, "registry shutdown");
        work_.awaitIdle(std::chrono::seconds(10));
        std::vector<std::shared_ptr<Row>> rows;
        { std::lock_guard<std::mutex> g(activeMx_); for (auto& kv : active_) rows.push_back(kv.second); }
        for (auto& r : rows) for (auto& c : r->snapshot()) if (c->claimTerminating()) { try { doRetire(c); } catch (const std::exception& e) { SW_WARN("[", name_, "] shutdown hard-retire threw: ", e.what()); } }
        work_.awaitIdle(std::chrono::seconds(5));
        if (persistWork_) { persistWork_->awaitIdle(std::chrono::seconds(5)); persistWork_->shutdown(); }
        work_.shutdown();
        timers_.shutdown();
        for (auto& kv : pools_) kv.second->clear();
    }

    // ─────────────────────────────────────────────────────────────────
    // RegistryBase (resolver-facing)
    // ─────────────────────────────────────────────────────────────────

    void spawnChildInternal(const std::string& parentId, const std::string& childType, std::any initial) override {
        if (shuttingDown_.load()) return;
        if (childType == supervisorName_) throw std::invalid_argument("Cannot spawn supervisor as a child");
        if (!findType(childType)) throw std::invalid_argument("Unknown machine type: " + childType);
        auto row = rowOf(parentId);
        if (!row) { SW_WARN("[", name_, "] spawnChild ", childType, " for unknown id=", parentId, " — ignored"); return; }
        if (findLiveCell(parentId, childType)) { SW_DEBUG("[", name_, "] child ", childType, " already present for id=", parentId); return; }
        bindAndStart(row, childType, childId(parentId, childType), parentId, std::move(initial));
    }

    void cleanupChildInternal(const std::string& parentId, const std::string& childType) override {
        auto cell = findLiveCell(parentId, childType);
        if (!cell || cell->isSupervisor) return;
        if (cell->claimTerminating()) chainSubmit(cell->chainKey, [this, cell] { doRetire(cell); });
    }

    void cleanupAllChildrenInternal(const std::string& parentId) override {
        auto row = rowOf(parentId);
        if (!row) return;
        for (auto& c : row->snapshot()) if (!c->isSupervisor && c->claimTerminating()) chainSubmit(c->chainKey, [this, c] { doRetire(c); });
    }

    void forwardToChild(const std::string& parentId, const std::string& childType, EventPtr ev) override {
        auto cell = findLiveCell(parentId, childType);
        if (!cell) { SW_DEBUG("[", name_, "] no ", childType, " for id=", parentId, ", drop ", ev->name()); return; }
        chainSubmit(cell->chainKey, [this, cell, ev] {
            if (!cell->epochValid() || !cell->live()) return;
            try { cell->machine->fire(*ev); } catch (const std::exception& e) { SW_WARN("[", name_, "] child fire threw for ", cell->chainKey, ": ", e.what()); }
        });
    }

    // ─────────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────────

    struct RegistryType {
        std::function<std::shared_ptr<MachineBase>()> factory;
        int poolSize = 16;
        std::function<std::any(MachineBase&)> volatileLoader;
        std::function<void(MachineBase&)> resetHook;
    };

    class Builder {
    public:
        explicit Builder(std::string name) : name_(std::move(name)) {}

        Builder& supervisor(const SupervisorSpec<T>& spec, int poolSize) {
            auto sp = std::make_shared<const SupervisorSpec<T>>(spec);
            return supervisor(spec.name(), [sp] { return std::shared_ptr<MachineBase>(std::make_shared<SpecSupervisor<T>>(sp)); }, poolSize);
        }
        Builder& supervisor(const std::string& typeName, std::function<std::shared_ptr<MachineBase>()> factory, int poolSize) {
            requireUnique(typeName);
            supervisorName_ = typeName;
            types_.emplace_back(typeName, RegistryType{std::move(factory), poolSize, nullptr, nullptr});
            return *this;
        }
        template <class C>
        Builder& child(const MachineSpec<C>& spec, int poolSize) {
            auto sp = std::make_shared<const MachineSpec<C>>(spec);
            return child(spec.name(), [sp] { return std::shared_ptr<MachineBase>(std::make_shared<SpecMachine<C>>(sp)); }, poolSize);
        }
        Builder& child(const std::string& typeName, std::function<std::shared_ptr<MachineBase>()> factory, int poolSize) {
            if (supervisorName_.empty()) throw std::logic_error("Declare .supervisor(...) before .child(...)");
            requireUnique(typeName);
            types_.emplace_back(typeName, RegistryType{std::move(factory), poolSize, nullptr, nullptr});
            return *this;
        }
        Builder& threads(unsigned n) { threads_ = n; return *this; }
        Builder& persistence(std::shared_ptr<PersistenceProvider> p) { persistence_ = std::move(p); return *this; }
        Builder& rehydrate(bool on) { rehydrate_ = on; return *this; }
        Builder& volatileLoader(const std::string& typeName, std::function<std::any(MachineBase&)> loader) { typeRef(typeName, "volatileLoader").volatileLoader = std::move(loader); return *this; }
        /// Per-type pool-return lambda (custom props / cache contents). A throw drops the instance.
        Builder& resetHook(const std::string& typeName, std::function<void(MachineBase&)> hook) { typeRef(typeName, "resetHook").resetHook = std::move(hook); return *this; }
        Builder& createFromFirstEvent(std::function<std::optional<T>(const Event&)> fn) { firstEvent_ = std::move(fn); return *this; }
        Builder& maxConcurrent(int n) { maxConcurrent_ = n; return *this; }
        Builder& globalTimeout(std::chrono::milliseconds d, std::string target) {
            if (d.count() <= 0) throw std::invalid_argument("globalTimeout duration must be > 0");
            if (target.empty()) throw std::invalid_argument("globalTimeout target required");
            globalTimeoutMs_ = d.count(); globalTarget_ = std::move(target); return *this;
        }
        Builder& debugSampleRate(int n) { debugSampleRate_ = n; return *this; }
        Builder& quotaKeysExtractor(std::function<QuotaKeys(const T&)> fn) { quotaExtractor_ = std::move(fn); return *this; }
        Builder& quotaLimits(QuotaLimits l) { limits_ = l; return *this; }
        Builder& channel(std::shared_ptr<ChannelBase> ch) { channel_ = std::move(ch); return *this; }
        Builder& maxPendingInbound(int n) { if (n <= 0) throw std::invalid_argument("maxPendingInbound must be > 0"); maxPendingInbound_ = n; return *this; }

        std::shared_ptr<StatemachineRegistry<T>> build() {
            if (supervisorName_.empty()) throw std::logic_error("No supervisor declared — call .supervisor(...) first");
            if (rehydrate_ && !persistence_) throw std::logic_error("rehydrate(true) requires .persistence(...)");
            if (limits_.enforces() && !quotaExtractor_) throw std::logic_error("quotaLimits enforced but no quotaKeysExtractor");
            return std::shared_ptr<StatemachineRegistry<T>>(new StatemachineRegistry<T>(*this));
        }

    private:
        friend class StatemachineRegistry<T>;
        void requireUnique(const std::string& t) {
            if (t.empty()) throw std::invalid_argument("typeName must be non-blank");
            for (auto& kv : types_) if (kv.first == t) throw std::logic_error("Duplicate machine type name: " + t);
        }
        RegistryType& typeRef(const std::string& t, const char* what) {
            for (auto& kv : types_) if (kv.first == t) return kv.second;
            throw std::logic_error(std::string(what) + ": machine type not registered: " + t);
        }
        std::string name_, supervisorName_;
        std::vector<std::pair<std::string, RegistryType>> types_;
        unsigned threads_ = 2;
        std::shared_ptr<PersistenceProvider> persistence_;
        bool rehydrate_ = false;
        std::function<std::optional<T>(const Event&)> firstEvent_;
        int maxConcurrent_ = 0;
        std::int64_t globalTimeoutMs_ = 0;
        std::string globalTarget_;
        int debugSampleRate_ = 0;
        std::function<QuotaKeys(const T&)> quotaExtractor_;
        QuotaLimits limits_;
        std::shared_ptr<ChannelBase> channel_;
        int maxPendingInbound_ = 10000;
    };

private:
    // ─────────────────────────────────────────────────────────────────
    // Cells and rows
    // ─────────────────────────────────────────────────────────────────

    enum class Phase { Live, Terminating, Suspending };

    struct Cell;
    struct Row {
        std::mutex mx;
        std::vector<std::shared_ptr<Cell>> cells;
        std::vector<std::shared_ptr<Cell>> snapshot() { std::lock_guard<std::mutex> g(mx); return cells; }
        void add(std::shared_ptr<Cell> c) { std::lock_guard<std::mutex> g(mx); cells.push_back(std::move(c)); }
        void remove(const std::shared_ptr<Cell>& c) { std::lock_guard<std::mutex> g(mx); cells.erase(std::remove(cells.begin(), cells.end(), c), cells.end()); }
        bool empty() { std::lock_guard<std::mutex> g(mx); return cells.empty(); }
        std::shared_ptr<Cell> first() { std::lock_guard<std::mutex> g(mx); return cells.empty() ? nullptr : cells.front(); }
    };

    struct Cell {
        std::string parentId, typeName, machineId, chainKey;
        std::shared_ptr<MachineBase> machine;
        std::uint64_t epoch = 0;
        std::shared_ptr<Row> row;
        bool isSupervisor = false;
        std::atomic<Phase> phase{Phase::Live};
        bool live() const { return phase.load() == Phase::Live; }
        bool claimTerminating() { Phase e = Phase::Live; return phase.compare_exchange_strong(e, Phase::Terminating); }
        bool claimSuspending() { Phase e = Phase::Live; return phase.compare_exchange_strong(e, Phase::Suspending); }
        bool epochValid() const { return machine->epoch() == epoch; }
    };

    class PerCellHandle final : public RegistryHandle {
    public:
        PerCellHandle(StatemachineRegistry* reg, std::weak_ptr<Cell> cell, std::string parentId)
            : reg_(reg), cell_(std::move(cell)), parentId_(std::move(parentId)) {}
        TimerService::Handle schedule(std::chrono::milliseconds delay, std::function<void()> fn) override {
            auto weak = cell_; auto* reg = reg_;
            return reg->timers_.schedule(delay, [reg, weak, fn] {
                if (auto c = weak.lock()) reg->chainSubmit(c->chainKey, [c, fn] { if (c->epochValid()) fn(); });
            });
        }
        void cancelTimer(TimerService::Handle h) override { reg_->timers_.cancel(h); }
        void onMachineReachedTerminal() override { if (auto c = cell_.lock()) if (c->claimTerminating()) reg_->doRetire(c); }
        void publish(EventPtr ev) override { reg_->submitInboundInternal(parentId_, std::move(ev), true, true); }
        void onStateTransitioned(const std::string& st, std::int64_t deadline, const std::optional<std::string>& target) override {
            if (auto c = cell_.lock()) reg_->persistCellSnapshot(c, st, deadline, target);
        }
        void onMachineWentOffline() override { if (auto c = cell_.lock()) reg_->onCellWentOffline(c); }
        RegistryBase& registry() override { return *reg_; }
        const std::string& parentId() const override { return parentId_; }
    private:
        StatemachineRegistry* reg_;
        std::weak_ptr<Cell> cell_;
        std::string parentId_;
    };

    // ─────────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────────

    explicit StatemachineRegistry(const Builder& b)
        : name_(b.name_), supervisorName_(b.supervisorName_), types_(b.types_),
          work_(b.name_, std::max(2u, b.threads_)), timers_(b.name_ + "-timers"),
          persistence_(b.persistence_), rehydrate_(b.rehydrate_), firstEvent_(b.firstEvent_),
          maxConcurrent_(std::max(0, b.maxConcurrent_)), globalTimeoutMs_(std::max<std::int64_t>(0, b.globalTimeoutMs_)),
          globalTarget_(b.globalTarget_), debugSampleRate_(std::max(0, b.debugSampleRate_)),
          quotaKeysExtractor_(b.quotaExtractor_), quotaLimits_(b.limits_), channel_(b.channel_),
          maxPendingInbound_(std::max(64, b.maxPendingInbound_)) {
        if (persistence_) persistWork_ = std::make_unique<StrandPool>(name_ + "-persist", std::max(2u, b.threads_));

        // Build-time hardening: freeze graphs, validate offline/persistence/
        // codec/route configuration — misconfiguration dies here.
        for (auto& kv : types_) {
            const auto& typeName = kv.first;
            auto sample = kv.second.factory();
            const StateMap& graph = sample->peekStateMap();
            typeStateMaps_.emplace(typeName, graph);
            if (graph.hasOfflineState() && !persistence_) throw std::logic_error("[" + name_ + "] type '" + typeName + "' declares an offline state but no persistence is configured");
            if (graph.hasOfflineState() && !rehydrate_) throw std::logic_error("[" + name_ + "] type '" + typeName + "' declares an offline state but rehydration is off — a suspended machine could never wake");
            if (persistence_ && !sample->hasCodec()) throw std::logic_error("[" + name_ + "] type '" + typeName + "' has no context codec but persistence is configured — set a Codec<C> on the spec (or setCodec in the subclass)");
            if (auto* sup = dynamic_cast<SupervisorCore*>(sample.get())) {
                for (auto& target : sup->resolver().referencedChildNames()) {
                    if (target == supervisorName_ || !findType(target))
                        throw std::logic_error("[" + name_ + "] supervisor '" + typeName + "' routes to unknown child '" + target + "'");
                }
            }
            pools_.emplace(typeName, std::make_unique<ObjectPool<MachineBase>>(name_ + "-" + typeName, kv.second.factory, kv.second.poolSize, kv.second.resetHook));
        }
        SW_INFO("[", name_, "] registry initialized — supervisor=", supervisorName_, ", types=", types_.size(), ", persistence=", persistence_ != nullptr, ", rehydrate=", rehydrate_);

        if (persistence_ && rehydrate_) recoverUnfinishedOnStartup();
        if (channel_) {
            channel_->start([this](const std::string& id, EventPtr ev) { return submitInboundInternal(id, std::move(ev), false, true); });
            SW_INFO("[", name_, "] channel '", channel_->name(), "' started (inbound wired)");
        }
        if (quotaLimits_.enforces()) scheduleQuotaPrune();
    }

    void scheduleQuotaPrune() {
        timers_.schedule(std::chrono::seconds(60), [this] {
            if (shuttingDown_.load()) return;
            quota_.pruneStaleTpsBuckets();
            scheduleQuotaPrune();
        });
    }

    // ─────────────────────────────────────────────────────────────────
    // Inbound
    // ─────────────────────────────────────────────────────────────────

    static Ack failedAck(std::exception_ptr ex) { std::promise<void> p; p.set_exception(std::move(ex)); return p.get_future().share(); }
    static Ack readyAck() { std::promise<void> p; p.set_value(); return p.get_future().share(); }

    Ack submitInboundInternal(const std::string& parentId, EventPtr ev, bool internal, bool allowResubmit) {
        if (shuttingDown_.load()) return failedAck(std::make_exception_ptr(InboundRejected("[" + name_ + "] shutting down — event dropped for id=" + parentId)));
        auto supCell = supervisorCell(parentId);
        if (!supCell) {
            if (ev->isFirst() && firstEvent_) {
                if (auto ctx = firstEvent_(*ev)) { dispatch(parentId, std::move(*ctx)); supCell = supervisorCell(parentId); }
            }
            if (!supCell && rehydrate_ && !wasRecentlyFinished(parentId)) { restoreAllCellsFor(parentId); supCell = supervisorCell(parentId); }
            if (!supCell) {
                if (wasRecentlyFinished(parentId)) { SW_DEBUG("[", name_, "] dropping late event ", ev->name(), " for finished id=", parentId); return readyAck(); }
                return failedAck(std::make_exception_ptr(UnknownRequest("[" + name_ + "] no supervisor for id=" + parentId + " and event " + ev->name() + " is not first / no creation hook / no rehydration")));
            }
        }
        if (!internal) {
            if (pendingInbound_.fetch_add(1) >= maxPendingInbound_) {
                pendingInbound_.fetch_sub(1);
                warnOverload();
                return failedAck(std::make_exception_ptr(InboundRejected("[" + name_ + "] inbound backlog over bound — event " + ev->name() + " for id=" + parentId + " shed")));
            }
        }
        auto ack = std::make_shared<std::promise<void>>();
        Ack fut = ack->get_future().share();
        auto cell = supCell;
        chainSubmit(cell->chainKey, [this, cell, ev, ack, parentId, internal, allowResubmit] {
            bool deferred = false;
            try {
                if (!cell->epochValid() || !cell->live()) {
                    if (cell->phase.load() == Phase::Suspending && allowResubmit && rehydrate_) {
                        deferred = true;
                        Ack inner = submitInboundInternal(parentId, ev, true, false);
                        std::thread([inner, ack] {
                            try { inner.get(); ack->set_value(); } catch (...) { try { ack->set_exception(std::current_exception()); } catch (...) {} }
                        }).detach();
                    } else {
                        SW_DEBUG("[", name_, "] dropping late event ", ev->name(), " for retired/replaced supervisor id=", parentId);
                    }
                } else {
                    auto* sup = dynamic_cast<SupervisorCore*>(cell->machine.get());
                    try { if (sup) sup->handleInbound(ev); }
                    catch (const std::exception& e) { SW_WARN("[", name_, "] handleInbound threw for id=", parentId, ": ", e.what()); }
                }
            } catch (...) {}
            if (!internal) pendingInbound_.fetch_sub(1);
            if (!deferred) { try { ack->set_value(); } catch (...) {} }
        });
        return fut;
    }

    void warnOverload() {
        auto now = nowMs(); auto last = overloadWarnedAtMs_.load();
        if (now - last > 5000 && overloadWarnedAtMs_.compare_exchange_strong(last, now))
            SW_WARN("[", name_, "] inbound backlog over bound (", maxPendingInbound_, ") — shedding wire events");
    }

    // ─────────────────────────────────────────────────────────────────
    // Global timeout
    // ─────────────────────────────────────────────────────────────────

    void scheduleGlobalTimeoutAt(const std::string& parentId, std::int64_t deadlineMs) {
        auto delay = std::chrono::milliseconds(std::max<std::int64_t>(0, deadlineMs - nowMs()));
        cancelGlobalTimeout(parentId);
        auto h = timers_.schedule(delay, [this, parentId] {
            auto cell = supervisorCell(parentId);
            if (!cell) return;
            SW_INFO("[", name_, "] global timeout fired id=", parentId, " state=", cell->machine->currentState());
            chainSubmit(cell->chainKey, [this, cell] {
                if (!cell->epochValid() || !cell->live()) return;
                if (!globalTarget_.empty()) {
                    try { if (!cell->machine->isTerminated()) cell->machine->transitionTo(globalTarget_); }
                    catch (const std::exception& e) { SW_ERROR("[", name_, "] global-timeout transition threw: ", e.what()); abortCellNow(cell, "global-timeout transition threw"); }
                } else abortCellNow(cell, "global timeout");
            });
        });
        std::lock_guard<std::mutex> g(globalMx_);
        globalTimers_[parentId] = h;
    }
    void cancelGlobalTimeout(const std::string& parentId) {
        std::lock_guard<std::mutex> g(globalMx_);
        auto it = globalTimers_.find(parentId);
        if (it != globalTimers_.end()) { timers_.cancel(it->second); globalTimers_.erase(it); }
    }

    // ─────────────────────────────────────────────────────────────────
    // Persistence
    // ─────────────────────────────────────────────────────────────────

    void persistCellSnapshot(const std::shared_ptr<Cell>& cell, const std::string& newState, std::int64_t deadlineMs, const std::optional<std::string>& target) {
        if (!persistence_ || !cell->epochValid()) return;
        Snapshot snap;
        try {
            snap.machineId = cell->machineId; snap.registryName = name_; snap.currentState = newState;
            snap.contextType = cell->machine->contextTypeName();
            snap.contextEncoded = cell->machine->encodeContext();
            snap.savedAtMs = nowMs(); snap.timeoutTargetState = target; snap.timeoutDeadlineMs = deadlineMs;
            if (cell->isSupervisor) { std::lock_guard<std::mutex> g(globalMx_); auto it = globalDeadlines_.find(cell->parentId); snap.globalDeadlineMs = it == globalDeadlines_.end() ? 0 : it->second; }
        } catch (const std::exception& e) {
            SW_ERROR("[", name_, "] snapshot encode failed for ", cell->machineId, ": ", e.what(), " — aborting the request");
            abortRequest(cell->parentId, "snapshot encode failed");
            return;
        }
        auto parentId = cell->parentId; auto mid = cell->machineId;
        persistWork_->submit(mid, [this, snap, parentId, mid] {
            try { persistence_->save(snap); }
            catch (const std::exception& e) { SW_ERROR("[", name_, "] persistence write failed for ", mid, ": ", e.what(), " — aborting the request"); abortRequest(parentId, "persistence write failed"); }
        });
    }

    void submitDeleteWithRetry(const std::string& machineId, int attempt) {
        if (!persistWork_) return;
        persistWork_->submit(machineId, [this, machineId, attempt] {
            try { persistence_->remove(machineId, name_); }
            catch (const std::exception& e) {
                if (attempt < 3 && !shuttingDown_.load()) {
                    SW_WARN("[", name_, "] snapshot delete failed for ", machineId, " (attempt ", attempt, "): ", e.what(), " — retrying");
                    timers_.schedule(std::chrono::seconds(attempt), [this, machineId, attempt] { submitDeleteWithRetry(machineId, attempt + 1); });
                } else SW_ERROR("[", name_, "] snapshot delete failed permanently for ", machineId, ": ", e.what(), " — orphan row is a tombstone");
            }
        });
    }

    void quarantineSnapshot(const std::string& machineId, const std::string& reason) {
        std::string parentId = parentOf(machineId);
        { std::lock_guard<std::mutex> g(quarantineMx_); quarantinedIds_[parentId] = nowMs(); }
        persistWork_->submit(machineId, [this, machineId, reason] {
            try { persistence_->quarantine(machineId, name_, reason); }
            catch (const std::exception& e) { SW_ERROR("[", name_, "] quarantine of ", machineId, " failed: ", e.what()); }
        });
    }

    bool isFinalStateOf(const std::string& typeName, const std::string& state) const {
        auto it = typeStateMaps_.find(typeName); return it != typeStateMaps_.end() && it->second.has(state) && it->second.get(state).finalState;
    }
    bool isOfflineStateOf(const std::string& typeName, const std::string& state) const {
        auto it = typeStateMaps_.find(typeName); return it != typeStateMaps_.end() && it->second.has(state) && it->second.get(state).offline;
    }

    void purgeAllSnapshots(const std::string& parentId) {
        submitDeleteWithRetry(parentId, 1);
        for (auto& kv : types_) if (kv.first != supervisorName_) submitDeleteWithRetry(childId(parentId, kv.first), 1);
    }

    void recoverUnfinishedOnStartup() {
        std::vector<Snapshot> all;
        try { all = persistence_->loadAllForRegistry(name_); }
        catch (const std::exception& e) { SW_WARN("[", name_, "] startup recovery: loadAllForRegistry threw: ", e.what(), " — skipping"); return; }
        if (all.empty()) return;
        std::map<std::string, std::map<std::string, Snapshot>> byParent;
        for (auto& s : all) byParent[parentOf(s.machineId)][s.machineId] = s;
        SW_INFO("[", name_, "] startup recovery: ", all.size(), " unfinished snapshot(s) across ", byParent.size(), " request(s)");
        int resumed = 0, hibernating = 0;
        auto now = nowMs();
        for (auto& kv : byParent) {
            const auto& parentId = kv.first;
            auto supIt = kv.second.find(parentId);
            if (supIt == kv.second.end()) {
                for (auto& orphan : kv.second) { SW_WARN("[", name_, "] startup recovery: orphan child snapshot ", orphan.first, " — quarantining"); quarantineSnapshot(orphan.first, "orphan child snapshot without supervisor"); }
                continue;
            }
            const Snapshot& sup = supIt->second;
            if (isOfflineStateOf(supervisorName_, sup.currentState) && !sup.timeoutFiredBy(now) && !sup.globalDeadlinePassedBy(now)) { hibernating++; continue; }
            try { resumed += restoreAllCellsFor(parentId); }
            catch (const std::exception& e) { SW_WARN("[", name_, "] startup recovery: resume failed for ", parentId, ": ", e.what()); }
        }
        SW_INFO("[", name_, "] startup recovery: ", resumed, " cell(s) resumed; ", hibernating, " hibernating request(s) left db-only");
    }

    int restoreAllCellsFor(const std::string& parentId) {
        if (!persistence_ || !rehydrate_) return 0;
        std::lock_guard<std::mutex> gate(restoreMx_);
        if (hasAny(parentId) || wasRecentlyFinished(parentId)) return 0;
        {
            std::lock_guard<std::mutex> g(quarantineMx_);
            auto q = quarantinedIds_.find(parentId);
            if (q != quarantinedIds_.end()) { if (nowMs() - q->second < QUARANTINE_RETRY_MS) return 0; quarantinedIds_.erase(q); }
        }
        std::optional<Snapshot> sup;
        try { sup = persistence_->load(parentId, name_); }
        catch (const std::exception& e) { SW_WARN("[", name_, "] restore: store load threw for ", parentId, ": ", e.what()); return 0; }
        if (!sup) return 0;
        if (isFinalStateOf(supervisorName_, sup->currentState)) {
            SW_INFO("[", name_, "] restore: id=", parentId, " snapshot is terminal (", sup->currentState, ") — purging tombstone");
            purgeAllSnapshots(parentId);
            return 0;
        }
        auto row = std::make_shared<Row>();
        { std::lock_guard<std::mutex> g(activeMx_); if (!active_.emplace(parentId, row).second) return 0; }
        int restored = 0;
        if (restoreOneCell(row, *sup, supervisorName_, parentId)) restored++;
        else { removeRow(parentId, row); return 0; }
        for (auto& kv : types_) {
            if (kv.first == supervisorName_) continue;
            std::string cid = childId(parentId, kv.first);
            std::optional<Snapshot> cs;
            try { cs = persistence_->load(cid, name_); } catch (const std::exception& e) { SW_WARN("[", name_, "] restore: load threw for ", cid, ": ", e.what()); continue; }
            if (!cs) continue;
            if (isFinalStateOf(kv.first, cs->currentState)) { submitDeleteWithRetry(cid, 1); continue; }
            if (restoreOneCell(row, *cs, kv.first, parentId)) restored++;
        }
        if (sup->globalDeadlineMs > 0) {
            { std::lock_guard<std::mutex> g(globalMx_); globalDeadlines_[parentId] = sup->globalDeadlineMs; }
            scheduleGlobalTimeoutAt(parentId, sup->globalDeadlineMs);
        }
        if (restored > 0) SW_INFO("[", name_, "] rehydration for id=", parentId, " restored ", restored, " cell(s)");
        return restored;
    }

    bool restoreOneCell(const std::shared_ptr<Row>& row, const Snapshot& snap, const std::string& typeName, const std::string& parentId) {
        auto git = typeStateMaps_.find(typeName);
        if (git == typeStateMaps_.end() || !git->second.has(snap.currentState)) {
            SW_ERROR("[", name_, "] restore: snapshot ", snap.machineId, " holds unknown state '", snap.currentState, "' — quarantining");
            quarantineSnapshot(snap.machineId, "unknown saved state '" + snap.currentState + "'");
            return false;
        }
        auto* type = findType(typeName);
        auto machine = borrowIdle(typeName);
        if (!machine) return false;
        // Decode BEFORE the cell is visible (a corrupt payload quarantines and never leaks a borrow).
        try { machine->bind(nullptr, snap.machineId, typeName); machine->decodeContext(snap.contextEncoded); }
        catch (const std::exception& e) {
            SW_ERROR("[", name_, "] restore: context decode failed for ", snap.machineId, ": ", e.what(), " — quarantining");
            machine->resetForReuse(); pools_[typeName]->returnObject(machine);
            quarantineSnapshot(snap.machineId, std::string("context decode failed: ") + e.what());
            return false;
        }
        auto cell = std::make_shared<Cell>();
        cell->parentId = parentId; cell->typeName = typeName; cell->machineId = snap.machineId;
        cell->chainKey = cellKey(parentId, typeName); cell->machine = machine; cell->epoch = machine->epoch();
        cell->row = row; cell->isSupervisor = typeName == supervisorName_;
        machine->bind(std::make_shared<PerCellHandle>(this, cell, parentId), snap.machineId, typeName);
        if (type->volatileLoader) machine->setVolatileLoader(type->volatileLoader);
        if (cell->isSupervisor) reacquireQuotaOnRestore(parentId, *machine);
        row->add(cell);
        auto st = snap.currentState; auto tgt = snap.timeoutTargetState; auto dl = snap.timeoutDeadlineMs;
        chainSubmit(cell->chainKey, [this, cell, st, tgt, dl] {
            if (!cell->epochValid() || !cell->live()) return;
            try { cell->machine->rehydrate(st, std::nullopt, tgt, dl); }
            catch (const std::exception& e) {
                SW_ERROR("[", name_, "] rehydrate threw for ", cell->machineId, ": ", e.what(), " — quarantining");
                quarantineSnapshot(cell->machineId, std::string("rehydrate threw: ") + e.what());
                if (cell->claimTerminating()) doRetire(cell);
            }
        });
        return true;
    }

    void reacquireQuotaOnRestore(const std::string& parentId, MachineBase& m) {
        if (!quotaKeysExtractor_ || !quotaLimits_.enforces()) return;
        auto* typed = dynamic_cast<Machine<T>*>(&m);
        if (!typed) return;
        QuotaKeys keys;
        try { keys = quotaKeysExtractor_(typed->context()); } catch (const std::exception& e) { SW_WARN("[", name_, "] restore: quotaKeysExtractor threw for ", parentId, ": ", e.what()); return; }
        if (keys.isNone()) return;
        std::lock_guard<std::mutex> g(quotaMx_);
        if (dispatchQuotaKeys_.count(parentId)) return;
        quota_.acquireUnchecked(keys, quotaLimits_);
        dispatchQuotaKeys_[parentId] = keys;
    }

    // ─────────────────────────────────────────────────────────────────
    // Suspend (hibernation)
    // ─────────────────────────────────────────────────────────────────

    void onCellWentOffline(const std::shared_ptr<Cell>& cell) {
        if (!persistence_) { SW_WARN("[", name_, "] ", cell->chainKey, " entered an offline state without persistence — terminating"); if (cell->claimTerminating()) doRetire(cell); return; }
        if (cell->isSupervisor) {
            for (auto& sib : cell->row->snapshot()) if (sib != cell && sib->claimSuspending()) chainSubmit(sib->chainKey, [this, sib] { doSuspend(sib); });
            if (cell->claimSuspending()) doSuspend(cell);
            removeRow(cell->parentId, cell->row);
            cancelGlobalTimeout(cell->parentId);
            { std::lock_guard<std::mutex> g(globalMx_); globalDeadlines_.erase(cell->parentId); }
            SW_DEBUG("[", name_, "] request ", cell->parentId, " suspended (supervisor offline) — snapshots retained");
        } else if (cell->claimSuspending()) doSuspend(cell);
    }

    void doSuspend(const std::shared_ptr<Cell>& cell) {
        cell->row->remove(cell);
        try { cell->machine->resetForReuse(); } catch (const std::exception& e) { SW_WARN("[", name_, "] reset threw suspending ", cell->chainKey, ": ", e.what()); }
        if (cell->machine->isIdle()) pools_[cell->typeName]->returnObject(cell->machine);
    }

    // ─────────────────────────────────────────────────────────────────
    // Borrow / start / retire
    // ─────────────────────────────────────────────────────────────────

    std::shared_ptr<MachineBase> borrowIdle(const std::string& typeName) {
        auto& pool = pools_[typeName];
        for (int i = 0; i < 3; i++) {
            auto m = pool->borrow();
            if (m->isIdle()) return m;
            SW_ERROR("[", name_, "] pool handed a non-IDLE ", typeName, " instance — dropping it");
        }
        SW_ERROR("[", name_, "] pool integrity error for ", typeName);
        return nullptr;
    }

    std::shared_ptr<Cell> bindAndStart(const std::shared_ptr<Row>& row, const std::string& typeName, const std::string& id, const std::string& parentId, std::any initial) {
        auto* type = findType(typeName);
        if (!type) { SW_ERROR("[", name_, "] unknown machine type ", typeName); return nullptr; }
        auto machine = borrowIdle(typeName);
        if (!machine) return nullptr;
        auto cell = std::make_shared<Cell>();
        cell->parentId = parentId; cell->typeName = typeName; cell->machineId = id; cell->chainKey = cellKey(parentId, typeName);
        cell->machine = machine; cell->epoch = machine->epoch(); cell->row = row; cell->isSupervisor = typeName == supervisorName_;
        machine->bind(std::make_shared<PerCellHandle>(this, cell, parentId), id, typeName);
        if (type->volatileLoader) machine->setVolatileLoader(type->volatileLoader);
        try { machine->setInitialContextAny(std::move(initial)); }
        catch (const std::exception& e) {
            SW_ERROR("[", name_, "] initial context rejected for ", cell->chainKey, ": ", e.what());
            machine->resetForReuse(); pools_[typeName]->returnObject(machine);
            return nullptr;
        }
        if (cell->isSupervisor && debugSampleRate_ > 0) machine->setDebug((dispatchCounter_.fetch_add(1) % debugSampleRate_) == 0);
        row->add(cell);
        chainSubmit(cell->chainKey, [this, cell, parentId, typeName] {
            if (!cell->epochValid() || !cell->live()) return;
            try { cell->machine->start(); }
            catch (const std::exception& e) { SW_ERROR("[", name_, "] start threw for ", cell->chainKey, ": ", e.what(), " — aborting the request"); abortRequest(parentId, "start threw for " + typeName); }
        });
        return cell;
    }

    void abortRequest(const std::string& parentId, const std::string& reason) {
        auto cell = supervisorCell(parentId);
        if (!cell) return;
        chainSubmit(cell->chainKey, [this, cell, reason] { abortCellNow(cell, reason); });
    }

    void abortCellNow(const std::shared_ptr<Cell>& cell, const std::string& reason) {
        if (!cell->claimTerminating()) return;
        if (cell->epochValid()) { try { cell->machine->forceFailover(reason); } catch (const std::exception& e) { SW_WARN("[", name_, "] forceFailover threw for ", cell->chainKey, ": ", e.what()); } }
        doRetire(cell);
    }

    /// The termination ritual for one CLAIMED cell — exactly one owner reaches here.
    void doRetire(const std::shared_ptr<Cell>& cell) {
        cell->row->remove(cell);
        if (cell->epochValid()) {
            try { cell->machine->resetForReuse(); } catch (const std::exception& e) { SW_WARN("[", name_, "] reset threw for ", cell->chainKey, ": ", e.what()); }
            if (cell->machine->isIdle()) pools_[cell->typeName]->returnObject(cell->machine);
        }
        if (persistence_) submitDeleteWithRetry(cell->machineId, 1);
        if (cell->isSupervisor) {
            for (auto& child : cell->row->snapshot()) chainSubmit(child->chainKey, [this, child] { if (child->claimTerminating()) doRetire(child); });
            removeRow(cell->parentId, cell->row);
            cancelGlobalTimeout(cell->parentId);
            { std::lock_guard<std::mutex> g(globalMx_); globalDeadlines_.erase(cell->parentId); }
            const std::int64_t stamp = nowMs();
            { std::lock_guard<std::mutex> g(tombMx_); recentlyFinished_[cell->parentId] = stamp; }
            auto pid = cell->parentId;
            timers_.schedule(std::chrono::milliseconds(FINISHED_TOMBSTONE_MS), [this, pid, stamp] {
                std::lock_guard<std::mutex> g(tombMx_);
                auto it = recentlyFinished_.find(pid);
                if (it != recentlyFinished_.end() && it->second == stamp) recentlyFinished_.erase(it);
            });
            std::lock_guard<std::mutex> g(quotaMx_);
            auto it = dispatchQuotaKeys_.find(cell->parentId);
            if (it != dispatchQuotaKeys_.end()) { quota_.release(it->second, quotaLimits_); dispatchQuotaKeys_.erase(it); }
        } else if (cell->row->empty()) {
            removeRow(cell->parentId, cell->row);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Lookup helpers
    // ─────────────────────────────────────────────────────────────────

    void chainSubmit(const std::string& key, std::function<void()> task) { work_.submit(key, std::move(task)); }

    std::shared_ptr<Row> rowOf(const std::string& parentId) const { std::lock_guard<std::mutex> g(activeMx_); auto it = active_.find(parentId); return it == active_.end() ? nullptr : it->second; }
    void removeRow(const std::string& parentId, const std::shared_ptr<Row>& row) { std::lock_guard<std::mutex> g(activeMx_); auto it = active_.find(parentId); if (it != active_.end() && it->second == row) active_.erase(it); }

    std::shared_ptr<Cell> findLiveCell(const std::string& parentId, const std::string& typeName) const {
        auto row = rowOf(parentId); if (!row) return nullptr;
        for (auto& c : row->snapshot()) if (c->live() && c->typeName == typeName) return c;
        return nullptr;
    }
    std::shared_ptr<Cell> supervisorCell(const std::string& parentId) const {
        auto row = rowOf(parentId); if (!row) return nullptr;
        auto first = row->first();
        return (first && first->isSupervisor && first->live()) ? first : nullptr;
    }
    const RegistryType* findType(const std::string& t) const { for (auto& kv : types_) if (kv.first == t) return &kv.second; return nullptr; }

    static std::string cellKey(const std::string& p, const std::string& t) { return p + CHILD_ID_SEPARATOR + t; }
    static std::string childId(const std::string& p, const std::string& t) { return p + CHILD_ID_SEPARATOR + t; }
    static std::string parentOf(const std::string& machineId) { auto i = machineId.find(CHILD_ID_SEPARATOR); return i == std::string::npos ? machineId : machineId.substr(0, i); }

    // ─────────────────────────────────────────────────────────────────
    // State
    // ─────────────────────────────────────────────────────────────────

    std::string name_, supervisorName_;
    std::vector<std::pair<std::string, RegistryType>> types_;
    std::map<std::string, StateMap> typeStateMaps_;
    std::map<std::string, std::unique_ptr<ObjectPool<MachineBase>>> pools_;

    mutable std::mutex activeMx_;
    std::unordered_map<std::string, std::shared_ptr<Row>> active_;

    StrandPool work_;
    std::unique_ptr<StrandPool> persistWork_;
    TimerService timers_;
    std::atomic<bool> shuttingDown_{false};

    std::shared_ptr<PersistenceProvider> persistence_;
    bool rehydrate_;
    std::function<std::optional<T>(const Event&)> firstEvent_;
    int maxConcurrent_;
    std::int64_t globalTimeoutMs_;
    std::string globalTarget_;
    int debugSampleRate_;
    std::atomic<std::uint64_t> dispatchCounter_{0};

    std::function<QuotaKeys(const T&)> quotaKeysExtractor_;
    QuotaLimits quotaLimits_;
    QuotaController quota_;
    std::mutex quotaMx_;
    std::unordered_map<std::string, QuotaKeys> dispatchQuotaKeys_;

    std::shared_ptr<ChannelBase> channel_;
    int maxPendingInbound_;
    std::atomic<int> pendingInbound_{0};
    std::atomic<std::int64_t> overloadWarnedAtMs_{0};

    std::mutex globalMx_;
    std::unordered_map<std::string, TimerService::Handle> globalTimers_;
    std::unordered_map<std::string, std::int64_t> globalDeadlines_;

    mutable std::mutex tombMx_;
    std::unordered_map<std::string, std::int64_t> recentlyFinished_;
    std::mutex quarantineMx_;
    std::unordered_map<std::string, std::int64_t> quarantinedIds_;
    std::mutex restoreMx_;
};

}  // namespace statewalk
