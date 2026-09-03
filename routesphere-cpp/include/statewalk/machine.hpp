// statewalk-cpp — the machine: lifecycle skeleton owned by the framework,
// policy (states/actions) owned by the graph.
//
// Identity tokens: `epoch` bumps on every pool reset, `visit` on every state
// switch — deferred work (queued tasks, timers) captures them and no-ops when
// they no longer match. This is what makes pooled reuse safe.
//
// Thread model: every public mutator takes the machine's recursive mutex; in
// practice all calls arrive serialized on the cell's strand anyway.
#pragma once

#include <any>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>

#include "event.hpp"
#include "executor.hpp"
#include "log.hpp"
#include "state_map.hpp"

namespace statewalk {

class RegistryBase;

/// The per-cell handle a machine talks back through. Implemented by the
/// registry; schedule() MUST route the callback through the cell's strand.
class RegistryHandle {
public:
    virtual ~RegistryHandle() = default;
    virtual TimerService::Handle schedule(std::chrono::milliseconds delay, std::function<void()> fn) = 0;
    virtual void cancelTimer(TimerService::Handle h) = 0;
    virtual void onMachineReachedTerminal() = 0;
    virtual void publish(EventPtr ev) = 0;
    virtual void onStateTransitioned(const std::string& newState, std::int64_t deadlineMs,
                                     const std::optional<std::string>& timeoutTarget) = 0;
    virtual void onMachineWentOffline() = 0;
    virtual RegistryBase& registry() = 0;
    virtual const std::string& parentId() const = 0;
};

/// Context codec — how a context becomes a persisted string and back. The
/// library is format-agnostic (JSON, protobuf, CSV — your call).
template <class C>
struct Codec {
    std::string typeName;
    std::function<std::string(const C&)> encode;
    std::function<C(const std::string&)> decode;
};

class MachineBase {
public:
    virtual ~MachineBase() = default;

    // ── framework binding (registry only) ─────────────────────────────
    void bind(std::shared_ptr<RegistryHandle> handle, std::string machineId, std::string typeName) {
        std::lock_guard<std::recursive_mutex> g(mx_);
        handle_ = std::move(handle);
        machineId_ = std::move(machineId);
        typeName_ = std::move(typeName);
    }
    void setVolatileLoader(std::function<std::any(MachineBase&)> loader) { volatileLoader_ = std::move(loader); }
    void setDebug(bool d) { debug_ = d; }

    // ── read side ─────────────────────────────────────────────────────
    const std::string& machineId() const { return machineId_; }
    const std::string& typeName() const { return typeName_; }
    std::string currentState() const { std::lock_guard<std::recursive_mutex> g(mx_); return currentState_; }
    bool isStarted() const { return started_.load(); }
    bool isTerminated() const { return terminated_.load(); }
    bool isIdle() const { std::lock_guard<std::recursive_mutex> g(mx_); return currentState_ == StateMap::IDLE; }
    std::uint64_t epoch() const { return epoch_.load(); }
    bool isDebug() const { return debug_; }
    RegistryHandle* handle() const { return handle_.get(); }
    const std::any& volatileContext() const { return volatileContext_; }

    const StateMap& peekStateMap() {
        std::lock_guard<std::recursive_mutex> g(mx_);
        if (!stateMap_) stateMap_ = std::make_unique<StateMap>(defineStates());
        return *stateMap_;
    }

    // ── context type erasure (implemented by Machine<C>) ───────────────
    virtual void setInitialContextAny(std::any initial) = 0;
    virtual bool hasCodec() const = 0;
    virtual std::string encodeContext() const = 0;
    virtual void decodeContext(const std::string& encoded) = 0;
    virtual std::string contextTypeName() const = 0;
    virtual void clearContext() = 0;

    // ── lifecycle (final skeleton) ────────────────────────────────────
    void start() {
        std::lock_guard<std::recursive_mutex> g(mx_);
        if (!handle_) { SW_WARN("Machine start() skipped — no registry handle. id=", machineId_); return; }
        if (started_) throw std::logic_error("Machine already started: " + machineId_);
        if (currentState_ != StateMap::IDLE) throw std::logic_error("Machine not IDLE at start (" + currentState_ + ") — pool reset bug suspected");
        peekStateMap();
        populateVolatileContext();
        started_ = true;
        terminated_ = false;
        transitionTo(stateMap_->initialState());
    }

    void fire(const Event& ev) {
        std::lock_guard<std::recursive_mutex> g(mx_);
        if (!started_ || terminated_ || !handle_) return;
        const StateConfig& cur = stateMap_->get(currentState_);
        pendingCause_ = ev.name();
        if (debug_) SW_DEBUG("[", machineId_, "] fire state=", currentState_, " event=", ev.name());

        auto it = cur.transitions.find(ev.type());
        if (it != cur.transitions.end()) {
            for (const auto& opt : it->second) {
                bool passes = true;
                if (opt.guard) {
                    try { passes = opt.guard(*this, ev); }
                    catch (const std::exception& e) {
                        SW_WARN("[", machineId_, "] guard threw for event=", ev.name(), " state=", currentState_, ": ", e.what(), " — treating as false");
                        passes = false;
                    }
                }
                if (passes) {
                    if (opt.action) {
                        try { opt.action(*this, ev); }
                        catch (const std::exception& e) {
                            SW_WARN("[", machineId_, "] transition action threw for event=", ev.name(), " ", currentState_, "→", opt.targetState, ": ", e.what());
                        }
                    }
                    transitionTo(opt.targetState);
                    return;
                }
            }
        }
        auto st = cur.stays.find(ev.type());
        if (st != cur.stays.end()) {
            try { st->second(*this, ev); }
            catch (const std::exception& e) {
                SW_WARN("[", machineId_, "] stay handler threw for event=", ev.name(), " state=", currentState_, ": ", e.what());
            }
            if (!terminated_ && handle_) handle_->onStateTransitioned(currentState_, currentDeadlineMs_, currentTimeoutTarget_);
        }
    }

    void transitionTo(const std::string& target) {
        std::lock_guard<std::recursive_mutex> g(mx_);
        if (terminated_ || !handle_) return;
        const StateConfig* cur = currentState_.empty() ? nullptr : &stateMap_->get(currentState_);
        if (cur && cur->onExit) {
            try { cur->onExit(*this); }
            catch (const std::exception& e) { SW_WARN("[", machineId_, "] exit action threw leaving state=", currentState_, ": ", e.what()); }
        }
        if (stateTimer_ != 0) { handle_->cancelTimer(stateTimer_); stateTimer_ = 0; }

        const StateConfig& next = stateMap_->get(target);
        std::string from = currentState_;
        currentState_ = next.name;
        const std::uint64_t visit = ++visit_;
        std::optional<std::string> cause = std::move(pendingCause_);
        pendingCause_.reset();
        try { onTransitioned(from, currentState_, cause); }
        catch (const std::exception& e) { SW_WARN("[", machineId_, "] onTransitioned tap threw: ", e.what()); }
        if (debug_) SW_DEBUG("[", machineId_, "] transition ", from, " -> ", currentState_);

        if (next.timeout) scheduleStateTimer(next, *next.timeout, visit, next.timeout->duration);

        if (next.onEntry) {
            try { next.onEntry(*this); }
            catch (const std::exception& e) { SW_WARN("[", machineId_, "] entry action threw entering state=", currentState_, ": ", e.what()); }
        }

        std::int64_t deadline = next.timeout ? nowMs() + next.timeout->duration.count() : 0;
        std::optional<std::string> tgt = (next.timeout && !next.timeout->stay) ? std::optional<std::string>(next.timeout->targetState) : std::nullopt;
        currentDeadlineMs_ = deadline;
        currentTimeoutTarget_ = tgt;
        if (!handle_) return;      // reset raced the entry action — nothing to notify
        handle_->onStateTransitioned(next.name, deadline, tgt);

        if (next.offline && !terminated_ && handle_) { handle_->onMachineWentOffline(); return; }
        if (next.finalState && !terminated_ && handle_) {
            terminated_ = true;
            handle_->onMachineReachedTerminal();
        }
    }

    /// Registry-forced failover: drive to the current state's timeout target
    /// (always final) so terminal work runs on every exit path. Stay-mode
    /// states have no target → hard stop with a WARN.
    void forceFailover(const std::string& reason) {
        std::lock_guard<std::recursive_mutex> g(mx_);
        if (!started_ || terminated_ || !handle_) return;
        const StateConfig& cur = stateMap_->get(currentState_);
        if (cur.finalState) return;
        try { onForcedFailover(reason); } catch (const std::exception& e) { SW_WARN("[", machineId_, "] onForcedFailover hook threw: ", e.what()); }
        if (!cur.timeout || cur.timeout->stay || cur.timeout->targetState.empty()) {
            SW_WARN("[", machineId_, "] forceFailover(", reason, ") from state=", currentState_, " — no timeout target; hard-terminating");
            terminated_ = true;
            return;
        }
        SW_INFO("[", machineId_, "] forced failover from state=", currentState_, " → ", cur.timeout->targetState, " (", reason, ")");
        transitionTo(cur.timeout->targetState);
    }

    /// Restore into a saved state WITHOUT replaying its entry action; honour
    /// elapsed time (matured target → transition now; matured stay →
    /// checkpoint now + re-arm; unmatured → remaining slice).
    void rehydrate(const std::string& savedState, const std::optional<std::string>& encodedCtx,
                   const std::optional<std::string>& timeoutTarget, std::int64_t deadlineMs) {
        std::lock_guard<std::recursive_mutex> g(mx_);
        if (!handle_) throw std::logic_error("Machine cannot rehydrate without a registry");
        if (started_) throw std::logic_error("Machine already started: " + machineId_);
        if (currentState_ != StateMap::IDLE) throw std::logic_error("Machine not IDLE at rehydrate");
        peekStateMap();
        if (!stateMap_->has(savedState)) throw std::invalid_argument("Saved state '" + savedState + "' not found in graph");
        if (encodedCtx) decodeContext(*encodedCtx);
        populateVolatileContext();
        started_ = true;
        terminated_ = false;
        currentState_ = savedState;
        const std::uint64_t visit = ++visit_;
        currentDeadlineMs_ = deadlineMs;
        currentTimeoutTarget_ = timeoutTarget;

        const StateConfig& saved = stateMap_->get(savedState);
        const auto now = nowMs();
        const bool matured = deadlineMs > 0 && now >= deadlineMs;
        if (saved.timeout && saved.timeout->stay) {
            if (matured) runStayCheckpoint(saved, *saved.timeout, visit);
            else scheduleStateTimer(saved, *saved.timeout, visit,
                    std::chrono::milliseconds(deadlineMs > 0 ? std::max<std::int64_t>(0, deadlineMs - now) : saved.timeout->duration.count()));
        } else if (matured) {
            std::string target = timeoutTarget ? *timeoutTarget : (saved.timeout ? saved.timeout->targetState : "");
            if (!target.empty()) { transitionTo(target); return; }
        } else if (saved.timeout && deadlineMs > 0) {
            scheduleStateTimer(saved, *saved.timeout, visit, std::chrono::milliseconds(std::max<std::int64_t>(0, deadlineMs - now)));
        }
        if (debug_) SW_DEBUG("[", machineId_, "] rehydrated to state=", currentState_);
    }

    /// Pool reset: after this the machine is indistinguishable from fresh.
    /// Bumps both identity tokens.
    void resetForReuse() {
        std::lock_guard<std::recursive_mutex> g(mx_);
        if (stateTimer_ != 0 && handle_) handle_->cancelTimer(stateTimer_);
        stateTimer_ = 0;
        try { onResetSubclass(); } catch (const std::exception& e) { SW_WARN("[", machineId_, "] onResetSubclass threw: ", e.what()); }
        epoch_++;
        visit_++;
        handle_.reset();
        machineId_.clear();
        typeName_.clear();
        clearContext();
        volatileContext_.reset();
        volatileLoader_ = nullptr;
        started_ = false;
        terminated_ = false;
        debug_ = false;
        currentState_ = StateMap::IDLE;
        currentDeadlineMs_ = 0;
        currentTimeoutTarget_.reset();
        pendingCause_.reset();
    }

    void publishEvent(EventPtr ev) {
        std::shared_ptr<RegistryHandle> h;
        { std::lock_guard<std::recursive_mutex> g(mx_); h = handle_; }
        if (!h) { SW_WARN("[", machineId_, "] publishEvent ignored — no registry handle"); return; }
        h->publish(std::move(ev));
    }

protected:
    virtual StateMap defineStates() = 0;
    virtual void onTransitioned(const std::string& from, const std::string& to, const std::optional<std::string>& cause) { (void)from; (void)to; (void)cause; }
    virtual void onForcedFailover(const std::string& reason) { (void)reason; }
    virtual void onResetSubclass() {}

private:
    void populateVolatileContext() {
        if (!volatileLoader_) return;
        try { volatileContext_ = volatileLoader_(*this); }
        catch (const std::exception& e) { SW_WARN("[", machineId_, "] volatile loader threw: ", e.what()); volatileContext_.reset(); }
    }

    void scheduleStateTimer(const StateConfig& state, const Timeout& /*to*/, std::uint64_t visit, std::chrono::milliseconds delay) {
        const std::string stateName = state.name;
        stateTimer_ = handle_->schedule(delay, [this, stateName, visit] { onStateTimerFired(stateName, visit); });
    }

    void onStateTimerFired(const std::string& stateName, std::uint64_t visit) {
        std::lock_guard<std::recursive_mutex> g(mx_);
        if (terminated_ || !handle_ || visit_ != visit) return;
        const StateConfig& state = stateMap_->get(stateName);
        if (!state.timeout) return;
        const Timeout to = *state.timeout;
        TimeoutEvent te(stateName, to.stay ? std::nullopt : std::optional<std::string>(to.targetState));
        fire(te);
        if (terminated_ || !handle_ || visit_ != visit) return;
        if (to.stay) runStayCheckpoint(state, to, visit);
        else transitionTo(to.targetState);
    }

    void runStayCheckpoint(const StateConfig& state, const Timeout& to, std::uint64_t visit) {
        if (to.onTimeoutStay) {
            try { to.onTimeoutStay(*this); }
            catch (const std::exception& e) { SW_WARN("[", machineId_, "] timeoutStay action threw in state=", currentState_, ": ", e.what()); }
        }
        if (terminated_ || !handle_ || visit_ != visit) return;
        std::int64_t newDeadline = nowMs() + to.duration.count();
        currentDeadlineMs_ = newDeadline;
        currentTimeoutTarget_.reset();
        handle_->onStateTransitioned(currentState_, newDeadline, std::nullopt);
        scheduleStateTimer(state, to, visit, to.duration);
    }

    mutable std::recursive_mutex mx_;
    std::unique_ptr<StateMap> stateMap_;
    std::shared_ptr<RegistryHandle> handle_;
    std::string machineId_, typeName_;
    std::atomic<std::uint64_t> epoch_{0};
    std::uint64_t visit_ = 0;
    std::any volatileContext_;
    std::function<std::any(MachineBase&)> volatileLoader_;
    std::string currentState_ = StateMap::IDLE;
    std::atomic<bool> started_{false}, terminated_{false};
    bool debug_ = false;
    TimerService::Handle stateTimer_ = 0;
    std::int64_t currentDeadlineMs_ = 0;
    std::optional<std::string> currentTimeoutTarget_;
    std::optional<std::string> pendingCause_;
};

/// Typed machine: owns a C context by value; a Codec<C> (optional) makes it
/// persistable.
template <class C>
class Machine : public MachineBase {
public:
    C& context() { return ctx_; }
    const C& context() const { return ctx_; }

    void setCodec(Codec<C> codec) { codec_ = std::move(codec); }

    void setInitialContextAny(std::any initial) override {
        if (!initial.has_value()) { ctx_ = createContext(); return; }
        if (auto* p = std::any_cast<C>(&initial)) { ctx_ = *p; return; }
        throw std::invalid_argument("initial context type mismatch for machine type '" + typeName() + "' (expected " + Event::demangle(typeid(C).name()) + ", got " + Event::demangle(initial.type().name()) + ")");
    }
    bool hasCodec() const override { return codec_.has_value(); }
    std::string encodeContext() const override {
        if (!codec_) throw std::logic_error("no codec for context of machine type '" + typeName() + "'");
        return codec_->encode(ctx_);
    }
    void decodeContext(const std::string& encoded) override {
        if (!codec_) throw std::logic_error("no codec for context of machine type '" + typeName() + "'");
        ctx_ = codec_->decode(encoded);
    }
    std::string contextTypeName() const override { return codec_ ? codec_->typeName : Event::demangle(typeid(C).name()); }
    void clearContext() override { ctx_ = C{}; }

protected:
    /// Fresh context when dispatch supplied none.
    virtual C createContext() { return C{}; }

private:
    C ctx_{};
    std::optional<Codec<C>> codec_;
};

/// Convenience for actions: the typed context of a MachineBase.
template <class C>
inline C& ctx(MachineBase& m) { return static_cast<Machine<C>&>(m).context(); }

}  // namespace statewalk
