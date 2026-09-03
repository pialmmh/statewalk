// statewalk-cpp — the generic session base (SessionSupervisor).
//
//  ADMITTING ──accept──► ADMITTED ──SignalingDone──► ACTIVE ──ServiceEnd──► TEARING_DOWN ──Settled──► SUCCEEDED
//      │                    │ ▲ retry(nextAttempt)      │                       │
//      └─ reject ───────────┴─ SignalingFailed / abort ─┴──(dead-man)───────────┴─────────────────────► FAILED
//
// Exactly ONE SdrRecord per session on EVERY exit path (graceful, timeout,
// registry-forced failover). Domain hooks are exception-shielded transition
// actions. Children extend RecordingMachine<C> to write into the shared
// SessionHistory (a shared_ptr carried inside every context of the cell).
#pragma once

#include <any>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "../machine.hpp"
#include "../supervisor.hpp"

namespace statewalk::session {

struct TransitionRecord {
    std::int64_t atMs = 0;
    std::string machine;
    std::optional<std::string> fromState, toState;
    std::string cause;      // event name, or the note text
    bool isNote() const { return !fromState && !toState; }
};

/// Bounded head+tail timeline shared by every machine of a cell.
class SessionHistory {
public:
    static constexpr std::size_t MAX_ENTRIES = 1000;
    void transition(const std::string& machine, const std::optional<std::string>& from, const std::optional<std::string>& to, const std::string& cause) {
        std::lock_guard<std::mutex> g(mx_); append({nowMs(), machine, from, to, cause});
    }
    void note(const std::string& machine, const std::string& text) { std::lock_guard<std::mutex> g(mx_); append({nowMs(), machine, std::nullopt, std::nullopt, text}); }
    std::vector<TransitionRecord> snapshot() const {
        std::lock_guard<std::mutex> g(mx_);
        std::vector<TransitionRecord> out(head_.begin(), head_.end());
        out.insert(out.end(), tail_.begin(), tail_.end());
        return out;
    }
    int droppedCount() const { std::lock_guard<std::mutex> g(mx_); return dropped_; }
private:
    void append(TransitionRecord r) {
        if (head_.size() < MAX_ENTRIES / 2) { head_.push_back(std::move(r)); return; }
        tail_.push_back(std::move(r));
        if (tail_.size() > MAX_ENTRIES - MAX_ENTRIES / 2) { tail_.pop_front(); dropped_++; }
    }
    mutable std::mutex mx_;
    std::vector<TransitionRecord> head_;
    std::deque<TransitionRecord> tail_;
    int dropped_ = 0;
};

/// Base context every domain session context extends.
struct SessionContext {
    std::string sessionKey;
    std::int64_t createdAtMs = 0, activatedAtMs = 0, endedAtMs = 0;
    int attempts = 0;
    std::string endCause;      // empty until known
    bool tornDown = false;
    std::string outcome;       // SUCCEEDED | FAILED; empty while running
    std::shared_ptr<SessionHistory> history = std::make_shared<SessionHistory>();
    std::string historyName() const { return "supervisor"; }
};

struct SdrRecord {
    std::string sessionKey, outcome, endCause;
    std::int64_t createdAtMs = 0, activatedAtMs = 0, endedAtMs = 0;
    int attempts = 0;
    std::any domain;
    std::vector<TransitionRecord> history;
    int historyDropped = 0;
};
using SdrSink = std::function<void(const SdrRecord&)>;

struct SessionTimings {
    std::chrono::seconds admitting{5}, admitted{30}, activeMax{3600}, tearingDown{10};
};

struct AdmissionVerdict {
    bool accepted = true; std::string rejectCause; std::any data;
    static AdmissionVerdict accept(std::any d = {}) { return {true, "", std::move(d)}; }
    static AdmissionVerdict reject(std::string cause) { return {false, std::move(cause), {}}; }
};

// ── the base vocabulary ──────────────────────────────────────────────
struct AdmissionDecided : Event { bool accepted; std::string cause; AdmissionDecided(bool a, std::string c) : accepted(a), cause(std::move(c)) {} };
struct SignalingProgress : Event { std::string phase; explicit SignalingProgress(std::string p) : phase(std::move(p)) {} };
struct SignalingDone : Event { std::any grant; explicit SignalingDone(std::any g = {}) : grant(std::move(g)) {} };
struct SignalingFailed : Event { std::string cause; explicit SignalingFailed(std::string c) : cause(std::move(c)) {} };
struct ServiceEnd : Event { std::string cause; explicit ServiceEnd(std::string c) : cause(std::move(c)) {} };
struct SettleRequest : Event {};
struct Settled : Event { std::any totals; explicit Settled(std::any t = {}) : totals(std::move(t)) {} };

/// Child machines of a session cell: every transition lands in the shared
/// history. C must expose `history` (shared_ptr<SessionHistory>) and
/// `historyName()`.
template <class C>
class RecordingMachine : public Machine<C> {
protected:
    void onTransitioned(const std::string& from, const std::string& to, const std::optional<std::string>& cause) override {
        auto& c = this->context();
        if (c.history) c.history->transition(c.historyName(), from.empty() ? std::nullopt : std::optional<std::string>(from), to, cause.value_or(""));
    }
};

template <class C>
class SessionSupervisor : public Supervisor<C> {
    static_assert(std::is_base_of_v<SessionContext, C>, "C must derive from session::SessionContext");
public:
    static constexpr const char* ADMITTING = "ADMITTING";
    static constexpr const char* ADMITTED = "ADMITTED";
    static constexpr const char* ACTIVE = "ACTIVE";
    static constexpr const char* TEARING_DOWN = "TEARING_DOWN";
    static constexpr const char* SUCCEEDED = "SUCCEEDED";
    static constexpr const char* FAILED = "FAILED";

protected:
    // ── the override surface ──
    virtual SessionTimings timings() = 0;
    virtual AdmissionVerdict runAdmission(C& ctx) = 0;
    virtual void spawnChildren(InternalEventResolver& r, C& ctx) = 0;
    virtual void onActive(C& ctx) = 0;
    virtual void onTeardown(C& ctx) = 0;
    virtual std::any buildSdr(C& ctx, const std::string& outcome) = 0;
    virtual SdrSink sdrSink() = 0;
    virtual void defineDomainRoutes(InternalEventResolver& r) = 0;

    virtual void onSignalingDone(C&, const std::any&) {}
    virtual void onSettled(C&, const std::any&) {}
    virtual void onSignalingProgress(C&, const std::string&) {}
    virtual bool nextAttempt(C&, const std::string&) { return false; }
    virtual void cleanupBeforeRetry(InternalEventResolver& r, C&) { r.cleanupChildren(); }
    virtual void onEnded(C&, const std::string&) {}
    virtual bool settlesAsync() { return true; }
    virtual bool sessionSucceeded(C& ctx) { return ctx.activatedAtMs > 0; }

    void defineRoutes(InternalEventResolver& r) final {
        r.selfHandle<AdmissionDecided>(); r.selfHandle<SignalingProgress>(); r.selfHandle<SignalingDone>();
        r.selfHandle<SignalingFailed>(); r.selfHandle<ServiceEnd>(); r.selfHandle<Settled>();
        defineDomainRoutes(r);
    }

    StateMap defineStates() final {
        using namespace std::chrono;
        SessionTimings t = timings();
        auto self = [](MachineBase& m) -> SessionSupervisor& { return static_cast<SessionSupervisor&>(m); };
        return StateMap::builder()
            .initialState(ADMITTING)
            .state(ADMITTING).interim().timeout(t.admitting, FAILED)
                .onEntry([self](MachineBase& m) { self(m).admit(); })
                .template on<AdmissionDecided>(ADMITTED, [](MachineBase&, const AdmissionDecided& e) { return e.accepted; })
                .template on<AdmissionDecided>(FAILED)
            .state(ADMITTED).interim().timeout(t.admitted, FAILED)
                .onEntry([self](MachineBase& m) { self(m).firstSpawn(); })
                .template on<SignalingDone>(ACTIVE, nullptr, [self](MachineBase& m, const SignalingDone& e) { self(m).onSignalingDone(self(m).context(), e.grant); })
                .template stay<SignalingProgress>([self](MachineBase& m, const SignalingProgress& e) { self(m).onSignalingProgress(self(m).context(), e.phase); })
                .template stay<SignalingFailed>([self](MachineBase& m, const SignalingFailed& e) { self(m).signalingFailed(e); })
                .template stay<ServiceEnd>([self](MachineBase& m, const ServiceEnd& e) { self(m).abortPreActive(e); })
            .state(ACTIVE).interim().timeout(t.activeMax, FAILED)
                .onEntry([self](MachineBase& m) { self(m).activate(); })
                .template on<ServiceEnd>(TEARING_DOWN, nullptr, [self](MachineBase& m, const ServiceEnd& e) { auto& c = self(m).context(); if (c.endCause.empty()) c.endCause = e.cause; })
            .state(TEARING_DOWN).interim().timeout(t.tearingDown, FAILED)
                .onEntry([self](MachineBase& m) { self(m).teardown(); })
                .template on<Settled>(SUCCEEDED, [self](MachineBase& m, const Settled& e) { return self(m).settleDecision(e); })
                .template on<Settled>(FAILED)
            .state(SUCCEEDED).finalState().timeout(seconds(1), SUCCEEDED).onEntry([self](MachineBase& m) { self(m).close(SUCCEEDED); })
            .state(FAILED).finalState().timeout(seconds(1), FAILED).onEntry([self](MachineBase& m) { self(m).close(FAILED); })
            .build();
    }

    void onTransitioned(const std::string& from, const std::string& to, const std::optional<std::string>& cause) final {
        auto& c = this->context();
        if (c.history) c.history->transition(c.historyName(), from.empty() ? std::nullopt : std::optional<std::string>(from), to, cause.value_or(""));
    }
    void onForcedFailover(const std::string& reason) final {
        auto& c = this->context();
        if (c.endCause.empty()) c.endCause = reason;
        if (c.history) c.history->note(c.historyName(), "forced failover: " + reason);
    }

private:
    void admit() {
        auto& c = this->context();
        if (c.createdAtMs == 0) c.createdAtMs = nowMs();
        AdmissionVerdict v;
        try { v = runAdmission(c); }
        catch (const std::exception& e) { if (c.history) c.history->note(c.historyName(), std::string("admission threw: ") + e.what()); v = AdmissionVerdict::reject(std::string("admission-error: ") + e.what()); }
        if (!v.accepted && c.endCause.empty()) c.endCause = v.rejectCause;
        this->publishEvent(makeEvent<AdmissionDecided>(v.accepted, v.rejectCause));
    }
    void firstSpawn() { auto& c = this->context(); c.attempts = 1; spawnChildren(this->resolver(), c); }
    void signalingFailed(const SignalingFailed& f) {
        auto& c = this->context();
        bool retry = false;
        try { retry = nextAttempt(c, f.cause); } catch (const std::exception& e) { if (c.history) c.history->note(c.historyName(), std::string("nextAttempt threw: ") + e.what()); }
        if (retry) {
            c.attempts++;
            if (c.history) c.history->note(c.historyName(), "retry attempt " + std::to_string(c.attempts) + " after: " + f.cause);
            try { cleanupBeforeRetry(this->resolver(), c); } catch (const std::exception& e) { if (c.history) c.history->note(c.historyName(), std::string("cleanupBeforeRetry threw: ") + e.what()); }
            spawnChildren(this->resolver(), c);
            return;
        }
        if (c.endCause.empty()) c.endCause = f.cause;
        this->transitionTo(FAILED);
    }
    void abortPreActive(const ServiceEnd& e) { auto& c = this->context(); if (c.endCause.empty()) c.endCause = e.cause; this->transitionTo(FAILED); }
    void activate() { auto& c = this->context(); c.activatedAtMs = nowMs(); onActive(c); }
    void teardown() {
        auto& c = this->context();
        c.tornDown = true;
        try { onTeardown(c); } catch (const std::exception& e) { if (c.history) c.history->note(c.historyName(), std::string("teardown threw: ") + e.what()); }
        if (settlesAsync()) this->publishEvent(makeEvent<SettleRequest>());
        else this->transitionTo(succeededNow() ? SUCCEEDED : FAILED);
    }
    bool settleDecision(const Settled& s) {
        auto& c = this->context();
        try { onSettled(c, s.totals); } catch (const std::exception& e) { SW_WARN("[", this->machineId(), "] onSettled threw: ", e.what()); if (c.history) c.history->note(c.historyName(), std::string("onSettled threw: ") + e.what()); }
        return succeededNow();
    }
    bool succeededNow() {
        auto& c = this->context();
        try { return sessionSucceeded(c); } catch (const std::exception& e) { SW_WARN("[", this->machineId(), "] sessionSucceeded threw: ", e.what()); return c.activatedAtMs > 0; }
    }
    void close(const std::string& outcome) {
        auto& c = this->context();
        if (!c.outcome.empty()) return;
        c.outcome = outcome;
        c.endedAtMs = nowMs();
        if (c.endCause.empty()) c.endCause = c.activatedAtMs > 0 ? "timeout" : "silent";
        if (c.activatedAtMs > 0 && !c.tornDown) {
            c.tornDown = true;
            try { onTeardown(c); } catch (const std::exception& e) { if (c.history) c.history->note(c.historyName(), std::string("backstop teardown threw: ") + e.what()); }
        }
        try { onEnded(c, outcome); } catch (const std::exception& e) { if (c.history) c.history->note(c.historyName(), std::string("onEnded threw: ") + e.what()); }
        std::any domain;
        try { domain = buildSdr(c, outcome); }
        catch (const std::exception& e) { SW_ERROR("[", this->machineId(), "] buildSdr threw for session ", c.sessionKey, " — shipping fallback SDR: ", e.what()); if (c.history) c.history->note(c.historyName(), std::string("buildSdr threw: ") + e.what()); }
        SdrRecord rec{c.sessionKey, outcome, c.endCause, c.createdAtMs, c.activatedAtMs, c.endedAtMs, c.attempts, std::move(domain),
                      c.history ? c.history->snapshot() : std::vector<TransitionRecord>{}, c.history ? c.history->droppedCount() : 0};
        try { auto sink = sdrSink(); if (sink) sink(rec); }
        catch (const std::exception& e) { SW_ERROR("[", this->machineId(), "] SDR SINK WRITE FAILED for session ", c.sessionKey, " outcome=", outcome, " — record lost: ", e.what()); }
    }
};

}  // namespace statewalk::session
