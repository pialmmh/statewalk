// statewalk-cpp — the StateMap DSL: a frozen, validated state graph.
//
//   auto graph = StateMap::builder()
//       .initialState("RINGING")
//       .state("RINGING").interim()
//           .timeout(90s, "FAILED")
//           .onEntry([](MachineBase& m){ ... })
//           .on<Answered>("ACTIVE")
//           .on<Answered>("ACTIVE", guard, action)
//           .stay<Progress>([](MachineBase& m, const Progress& e){ ... })
//       .state("HOLD").interim().timeoutStay(15s, heartbeat)
//       .state("PARKED").interim().offline().timeout(30min, "EXPIRED")
//       .state("FAILED").finalState().timeout(1s, "FAILED")
//       .build();
//
// Build-time invariants (throw std::invalid_argument):
//   every state declares interim() or finalState(); exactly one timeout mode;
//   target-mode timeouts point at FINAL states; final+offline and
//   final+timeoutStay are contradictions; all targets exist.
#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <vector>

#include "event.hpp"

namespace statewalk {

class MachineBase;  // machine.hpp

using EntryExitAction = std::function<void(MachineBase&)>;
using Guard = std::function<bool(MachineBase&, const Event&)>;
using EventAction = std::function<void(MachineBase&, const Event&)>;

struct Timeout {
    std::chrono::milliseconds duration{0};
    std::string targetState;           // empty for stay mode
    bool stay = false;
    EntryExitAction onTimeoutStay;     // optional, stay mode only
};

struct Transition {
    Guard guard;                       // null = unconditional
    std::string targetState;
    EventAction action;                // optional, runs after the guard passes
};

struct StateConfig {
    std::string name;
    EntryExitAction onEntry;
    EntryExitAction onExit;
    std::unordered_map<std::type_index, std::vector<Transition>> transitions;
    std::unordered_map<std::type_index, EventAction> stays;
    std::optional<Timeout> timeout;
    bool finalState = false;
    bool offline = false;
    bool kindDeclared = false;         // .interim() or .finalState() was called
};

class StateMap {
public:
    static constexpr const char* IDLE = "IDLE";

    const std::string& initialState() const { return initial_; }
    const StateConfig& get(const std::string& name) const {
        auto it = states_.find(name);
        if (it == states_.end()) throw std::invalid_argument("Unknown state: " + name);
        return it->second;
    }
    bool has(const std::string& name) const { return states_.count(name) > 0; }
    bool hasOfflineState() const { for (auto& kv : states_) if (kv.second.offline) return true; return false; }
    const std::map<std::string, StateConfig>& states() const { return states_; }

    class Builder;
    static Builder builder();

private:
    friend class Builder;
    std::string initial_;
    std::map<std::string, StateConfig> states_;
};

class StateMap::Builder {
public:
    class StateBuilder {
    public:
        StateBuilder(Builder& parent, StateConfig& cfg) : parent_(parent), cfg_(cfg) {}

        StateBuilder& interim() { cfg_.kindDeclared = true; return *this; }
        StateBuilder& finalState() { cfg_.finalState = true; cfg_.kindDeclared = true; return *this; }
        StateBuilder& offline() { cfg_.offline = true; return *this; }
        StateBuilder& onEntry(EntryExitAction a) { cfg_.onEntry = std::move(a); return *this; }
        StateBuilder& onExit(EntryExitAction a) { cfg_.onExit = std::move(a); return *this; }

        /// Unconditional transition on E.
        template <class E>
        StateBuilder& on(std::string target) { return on<E>(std::move(target), nullptr, nullptr); }

        /// Guarded transition. Guards must be PURE — a throw counts as false.
        template <class E>
        StateBuilder& on(std::string target, std::function<bool(MachineBase&, const E&)> guard) {
            return on<E>(std::move(target), std::move(guard), nullptr);
        }

        /// Guarded transition with an ACTION (the payload-copy step): runs after
        /// the guard passes, before the transition. A throw is logged and does
        /// not veto. Pass a null guard for "always".
        template <class E>
        StateBuilder& on(std::string target,
                         std::function<bool(MachineBase&, const E&)> guard,
                         std::function<void(MachineBase&, const E&)> action) {
            Transition t;
            t.targetState = std::move(target);
            if (guard) t.guard = [g = std::move(guard)](MachineBase& m, const Event& e) { return g(m, static_cast<const E&>(e)); };
            if (action) t.action = [a = std::move(action)](MachineBase& m, const Event& e) { a(m, static_cast<const E&>(e)); };
            cfg_.transitions[std::type_index(typeid(E))].push_back(std::move(t));
            return *this;
        }

        /// Handle E without leaving the state; the context is re-persisted with
        /// the UNCHANGED deadline. Runs only when no guarded on<E> passes.
        template <class E>
        StateBuilder& stay(std::function<void(MachineBase&, const E&)> handler) {
            cfg_.stays[std::type_index(typeid(E))] = [h = std::move(handler)](MachineBase& m, const Event& e) { h(m, static_cast<const E&>(e)); };
            return *this;
        }

        /// Target-mode timeout: on maturity transition to a FINAL state.
        StateBuilder& timeout(std::chrono::milliseconds d, std::string target) {
            Timeout t; t.duration = d; t.targetState = std::move(target); t.stay = false;
            cfg_.timeout = std::move(t);
            return *this;
        }

        /// Stay-mode timeout: on maturity STAY — run the action, re-persist the
        /// context with the refreshed deadline, re-arm. For states that
        /// legitimately wait indefinitely (the global timeout stays the cap).
        StateBuilder& timeoutStay(std::chrono::milliseconds d, EntryExitAction action = nullptr) {
            Timeout t; t.duration = d; t.stay = true; t.onTimeoutStay = std::move(action);
            cfg_.timeout = std::move(t);
            return *this;
        }

        StateBuilder state(const std::string& next) { return parent_.state(next); }
        StateMap build() { return parent_.build(); }

    private:
        Builder& parent_;
        StateConfig& cfg_;
    };

    Builder& initialState(std::string s) { initial_ = std::move(s); return *this; }

    StateBuilder state(const std::string& name) {
        if (name == StateMap::IDLE) throw std::invalid_argument("IDLE is reserved and auto-injected");
        auto& cfg = states_[name];
        cfg.name = name;
        return StateBuilder(*this, cfg);
    }

    StateMap build() {
        if (initial_.empty()) throw std::invalid_argument("initialState required");
        if (!states_.count(initial_)) throw std::invalid_argument("initialState '" + initial_ + "' is not a declared state");

        for (auto& kv : states_) {
            const auto& s = kv.second;
            if (!s.kindDeclared) throw std::invalid_argument("State '" + s.name + "' is missing a mandatory kind declaration: call .interim() or .finalState()");
            if (!s.timeout || s.timeout->duration.count() <= 0 || (!s.timeout->stay && s.timeout->targetState.empty()))
                throw std::invalid_argument("State '" + s.name + "' is missing a mandatory timeout. Use .timeout(d, target) or .timeoutStay(d[, action])");
            if (s.timeout->stay && s.finalState)
                throw std::invalid_argument("State '" + s.name + "' is final and cannot use .timeoutStay");
            if (s.finalState && s.offline)
                throw std::invalid_argument("State '" + s.name + "' cannot be both final and offline");
            if (!s.timeout->stay) {
                const auto& tgt = s.timeout->targetState;
                if (tgt == StateMap::IDLE) throw std::invalid_argument("State '" + s.name + "' timeout targets IDLE");
                auto t = states_.find(tgt);
                if (t == states_.end()) throw std::invalid_argument("State '" + s.name + "' timeout targets unknown state '" + tgt + "'");
                if (!t->second.finalState) throw std::invalid_argument("State '" + s.name + "' timeout targets '" + tgt + "' which is not a final state");
            }
            for (auto& tr : s.transitions)
                for (auto& opt : tr.second)
                    if (opt.targetState != StateMap::IDLE && !states_.count(opt.targetState))
                        throw std::invalid_argument("State '" + s.name + "' transitions to unknown state '" + opt.targetState + "'");
        }

        StateMap map;
        map.initial_ = initial_;
        StateConfig idle; idle.name = StateMap::IDLE;
        map.states_[StateMap::IDLE] = std::move(idle);
        for (auto& kv : states_) map.states_[kv.first] = kv.second;
        return map;
    }

private:
    std::string initial_;
    std::map<std::string, StateConfig> states_;
};

inline StateMap::Builder StateMap::builder() { return Builder(); }

}  // namespace statewalk
