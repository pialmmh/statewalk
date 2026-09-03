#include <mutex>

#include "check.hpp"
#include "fixtures.hpp"
#include "statewalk/session/session.hpp"

using namespace statewalk::session;

struct GoOnline : Event {};
struct FailNow : Event {};

struct TestCtx : SessionContext { bool admitReject = false; bool noBudget = false; bool retryOnce = false; std::string grant, totals; };
struct SigCtx { std::shared_ptr<SessionHistory> history; std::string historyName() const { return "signaling"; } };
struct BudCtx { std::shared_ptr<SessionHistory> history; std::string historyName() const { return "budget"; } };

struct Sig : RecordingMachine<SigCtx> {
protected:
    StateMap defineStates() override {
        return StateMap::builder().initialState("RUN")
            .state("RUN").interim().timeout(60s, "NO_AUTH").on<GoOnline>("GRANTED").on<FailNow>("NO_AUTH")
            .state("GRANTED").finalState().timeout(1s, "GRANTED").onEntry([](MachineBase& m) { m.publishEvent(makeEvent<SignalingDone>(std::any(std::string("grant-1")))); })
            .state("NO_AUTH").finalState().timeout(1s, "NO_AUTH").onEntry([](MachineBase& m) { m.publishEvent(makeEvent<SignalingFailed>("sig-fail")); })
            .build();
    }
};
struct Budget : RecordingMachine<BudCtx> {
protected:
    StateMap defineStates() override {
        return StateMap::builder().initialState("METERING")
            .state("METERING").interim().timeout(60s, "CLOSED").on<SettleRequest>("CLOSED")
            .state("CLOSED").finalState().timeout(1s, "CLOSED").onEntry([](MachineBase& m) { m.publishEvent(makeEvent<Settled>(std::any(std::string("totals-1")))); })
            .build();
    }
};

struct SdrBox { std::mutex mx; std::vector<SdrRecord> records; void add(const SdrRecord& r) { std::lock_guard<std::mutex> g(mx); records.push_back(r); } std::size_t size() { std::lock_guard<std::mutex> g(mx); return records.size(); } SdrRecord at(std::size_t i) { std::lock_guard<std::mutex> g(mx); return records.at(i); } };

struct TestSup : SessionSupervisor<TestCtx> {
    explicit TestSup(std::shared_ptr<SdrBox> box) : box_(std::move(box)) {}
protected:
    SessionTimings timings() override { return SessionTimings{5s, 2s, 60s, 5s}; }
    AdmissionVerdict runAdmission(TestCtx& c) override { c.history->note("supervisor", "admission ran"); return c.admitReject ? AdmissionVerdict::reject("no-balance") : AdmissionVerdict::accept(); }
    void spawnChildren(InternalEventResolver& r, TestCtx& c) override { r.spawnChild("Sig", SigCtx{c.history}); if (!c.noBudget) r.spawnChild("Budget", BudCtx{c.history}); }
    void onActive(TestCtx& c) override { c.history->note("supervisor", "service delivered"); }
    void onTeardown(TestCtx& c) override { c.history->note("supervisor", "service stopped"); }
    void onSignalingDone(TestCtx& c, const std::any& g) override { c.grant = std::any_cast<std::string>(g); }
    void onSettled(TestCtx& c, const std::any& t) override { c.totals = std::any_cast<std::string>(t); }
    bool settlesAsync() override { return !context().noBudget; }
    bool nextAttempt(TestCtx& c, const std::string&) override { return c.retryOnce && c.attempts < 2; }
    std::any buildSdr(TestCtx&, const std::string& outcome) override { return std::string("domain:") + outcome; }
    SdrSink sdrSink() override { auto b = box_; return [b](const SdrRecord& r) { b->add(r); }; }
    void defineDomainRoutes(InternalEventResolver& r) override { r.forwardTo<GoOnline>("Sig"); r.forwardTo<FailNow>("Sig"); r.forwardTo<SettleRequest>("Budget"); }
private:
    std::shared_ptr<SdrBox> box_;
};

static std::shared_ptr<StatemachineRegistry<TestCtx>> build(std::shared_ptr<SdrBox> box) {
    return StatemachineRegistry<TestCtx>::builder("session")
        .supervisor("TestSup", [box] { return std::shared_ptr<MachineBase>(std::make_shared<TestSup>(box)); }, 16)
        .child("Sig", [] { return std::shared_ptr<MachineBase>(std::make_shared<Sig>()); }, 16)
        .child("Budget", [] { return std::shared_ptr<MachineBase>(std::make_shared<Budget>()); }, 16)
        .threads(2).build();
}

static std::vector<std::string> hops(const SdrRecord& r) {
    std::vector<std::string> out;
    for (auto& t : r.history) if (!t.isNote() && t.machine == "supervisor") out.push_back(t.fromState.value_or("-") + ">" + t.toState.value_or("-"));
    return out;
}
static bool has(const std::vector<std::string>& v, const std::string& s) { for (auto& x : v) if (x == s) return true; return false; }

TEST(full_success_path_one_sdr_with_cell_history) {
    auto box = std::make_shared<SdrBox>(); auto reg = build(box);
    TestCtx c; c.sessionKey = "k-1";
    CHECK(reg->dispatch("k-1", c).accepted);
    CHECK(reg->awaitIdle(5s));
    reg->onInboundEvent("k-1", makeEvent<GoOnline>());
    CHECK(reg->awaitIdle(5s));
    reg->onInboundEvent("k-1", makeEvent<ServiceEnd>("deauth"));
    CHECK(awaitUntil([&] { return box->size() == 1; }, 5000));
    auto sdr = box->at(0);
    CHECK_EQ(sdr.outcome, std::string("SUCCEEDED"));
    CHECK_EQ(sdr.endCause, std::string("deauth"));
    CHECK(sdr.activatedAtMs > 0);
    CHECK_EQ(std::any_cast<std::string>(sdr.domain), std::string("domain:SUCCEEDED"));
    auto h = hops(sdr);
    CHECK(has(h, "ADMITTING>ADMITTED")); CHECK(has(h, "ADMITTED>ACTIVE")); CHECK(has(h, "ACTIVE>TEARING_DOWN")); CHECK(has(h, "TEARING_DOWN>SUCCEEDED"));
    bool sig = false, bud = false;
    for (auto& t : sdr.history) { if (t.machine == "signaling" && !t.isNote()) sig = true; if (t.machine == "budget" && !t.isNote()) bud = true; }
    CHECK(sig); CHECK(bud);
    reg->shutdown();
}

TEST(admission_reject_and_silent_session_end_failed) {
    auto box = std::make_shared<SdrBox>(); auto reg = build(box);
    TestCtx r; r.sessionKey = "k-2"; r.admitReject = true;
    CHECK(reg->dispatch("k-2", r).accepted);
    CHECK(awaitUntil([&] { return box->size() == 1; }, 5000));
    CHECK_EQ(box->at(0).outcome, std::string("FAILED"));
    CHECK_EQ(box->at(0).endCause, std::string("no-balance"));
    TestCtx s; s.sessionKey = "k-3";
    CHECK(reg->dispatch("k-3", s).accepted);                              // nothing happens → ADMITTED timeout (2s)
    CHECK(awaitUntil([&] { return box->size() == 2; }, 6000));
    CHECK_EQ(box->at(1).endCause, std::string("silent"));
    CHECK(has(hops(box->at(1)), "ADMITTED>FAILED"));
    reg->shutdown();
}

TEST(fast_retry_respawns_same_child_and_succeeds) {
    auto box = std::make_shared<SdrBox>(); auto reg = build(box);
    TestCtx c; c.sessionKey = "k-r"; c.retryOnce = true;
    CHECK(reg->dispatch("k-r", c).accepted);
    CHECK(reg->awaitIdle(5s));
    reg->onInboundEvent("k-r", makeEvent<FailNow>());                     // attempt 1 fails → retry respawns Sig
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(box->size(), std::size_t(0));
    reg->onInboundEvent("k-r", makeEvent<GoOnline>());
    CHECK(reg->awaitIdle(5s));
    reg->onInboundEvent("k-r", makeEvent<ServiceEnd>("done"));
    CHECK(awaitUntil([&] { return box->size() == 1; }, 5000));
    CHECK_EQ(box->at(0).outcome, std::string("SUCCEEDED"));
    CHECK_EQ(box->at(0).attempts, 2);
    reg->shutdown();
}

TEST(shutdown_emits_sdr_for_every_live_session_with_reason) {
    auto box = std::make_shared<SdrBox>(); auto reg = build(box);
    TestCtx a; a.sessionKey = "s-a"; TestCtx b; b.sessionKey = "s-b";
    CHECK(reg->dispatch("s-a", a).accepted); CHECK(reg->dispatch("s-b", b).accepted);
    CHECK(reg->awaitIdle(5s));
    reg->onInboundEvent("s-a", makeEvent<GoOnline>());
    CHECK(reg->awaitIdle(5s));
    reg->shutdown();
    CHECK_EQ(box->size(), std::size_t(2));
    for (std::size_t i = 0; i < 2; i++) { CHECK_EQ(box->at(i).outcome, std::string("FAILED")); CHECK_EQ(box->at(i).endCause, std::string("registry shutdown")); }
}

TEST_MAIN("session")
