#include <atomic>

#include "check.hpp"
#include "fixtures.hpp"

TEST(dispatch_event_terminal_and_pool_reclaim) {
    auto reg = StatemachineRegistry<Ctx>::builder("core").supervisor(runningSpec(), 4).threads(2).build();
    CHECK(reg->dispatch("c-1", Ctx{}).accepted);
    CHECK(!reg->dispatch("c-1", Ctx{}).accepted);                        // duplicate id
    CHECK_EQ(*reg->dispatch("c-1", Ctx{}).rejectCause, RejectCause::DuplicateId);
    reg->onInboundEvent("c-1", makeEvent<Touch>());
    reg->onInboundEvent("c-1", makeEvent<Touch>());
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->supervisorStateOf("c-1").value_or(""), std::string("RUNNING"));
    CHECK_EQ(reg->supervisorContextOf("c-1")->touches, 2);
    reg->onInboundEvent("c-1", makeEvent<Stop>());
    CHECK(reg->awaitIdle(5s));
    CHECK(!reg->hasAny("c-1"));
    CHECK(reg->wasRecentlyFinished("c-1"));
    auto st = reg->poolStats("Sup");
    CHECK_EQ(st.totalBorrowed, st.reclaimed());
    reg->shutdown();
}

TEST(unknown_id_throws_and_first_event_creates) {
    auto reg = StatemachineRegistry<Ctx>::builder("first").supervisor(
        SupervisorSpec<Ctx>::builder().name("Sup").stateMap(runningGraph()).codec(ctxCodec())
            .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); r.selfHandle<Touch>(); r.selfHandle<Open>(); r.selfHandle<Ping>(); }).build(), 4)
        .createFromFirstEvent([](const Event& e) -> std::optional<Ctx> {
            if (auto* o = dynamic_cast<const Open*>(&e)) { Ctx c; c.mark = o->caller; return c; }
            return std::nullopt;
        }).threads(2).build();
    CHECK_THROWS(reg->onInboundEvent("nobody", makeEvent<Touch>()));
    reg->onInboundEvent("f-1", makeEvent<Open>("alice"));
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->supervisorContextOf("f-1")->mark, std::string("alice"));
    reg->shutdown();
}

TEST(cascade_reclaims_live_children_exactly) {
    auto reg = StatemachineRegistry<Ctx>::builder("cascade").supervisor(spawningSpec(), 4)
        .child(childSpec("A"), 4).child(childSpec("B"), 4).threads(2).build();
    for (int i = 0; i < 10; i++) CHECK(reg->dispatch("c-" + std::to_string(i), Ctx{}).accepted);
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->activeCellCount(), 30);
    reg->onInboundEvent("c-3", makeEvent<Ping>());                      // fan-out to both children
    for (int i = 0; i < 10; i++) reg->onInboundEvent("c-" + std::to_string(i), makeEvent<Stop>());
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->activeCellCount(), 0);
    CHECK_EQ(reg->activeIdCount(), 0);
    for (const char* t : {"Sup", "A", "B"}) { auto s = reg->poolStats(t); CHECK_MSG(s.totalBorrowed == s.reclaimed(), t); CHECK_EQ(s.doubleReturns, 0); }
    reg->shutdown();
}

TEST(id_reuse_immediately_after_finish) {
    auto reg = StatemachineRegistry<Ctx>::builder("reuse").supervisor(runningSpec(), 2).threads(2).build();
    for (int round = 0; round < 5; round++) {
        CHECK(reg->dispatch("same", Ctx{}).accepted);
        reg->onInboundEvent("same", makeEvent<Touch>());
        CHECK(reg->awaitIdle(5s));
        CHECK_EQ(reg->supervisorContextOf("same")->touches, 1);         // fresh context every round
        reg->onInboundEvent("same", makeEvent<Stop>());
        CHECK(reg->awaitIdle(5s));
        CHECK(!reg->hasAny("same"));
    }
    reg->shutdown();
}

TEST(stale_timer_never_fires_into_next_session) {
    std::atomic<int> expired{0};
    auto spec = SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("WAITING")
            .state("WAITING").interim().timeout(400ms, "EXPIRED").on<Stop>("DONE")
            .state("DONE").finalState().timeout(1s, "DONE")
            .state("EXPIRED").finalState().timeout(1s, "EXPIRED").onEntry([&](MachineBase&) { expired++; }).build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); }).build();
    auto reg = StatemachineRegistry<Ctx>::builder("stale").supervisor(spec, 1).threads(2).build();   // pool 1 → same instance
    CHECK(reg->dispatch("t", Ctx{}).accepted);
    sleepMs(150);
    reg->onInboundEvent("t", makeEvent<Stop>());
    CHECK(reg->awaitIdle(5s));
    CHECK(!reg->hasAny("t"));
    CHECK(reg->dispatch("t", Ctx{}).accepted);                            // session B, same machine
    sleepMs(300);                                                         // A's deadline crossed
    CHECK_EQ(reg->supervisorStateOf("t").value_or("gone"), std::string("WAITING"));
    CHECK_EQ(expired.load(), 0);
    sleepMs(500);                                                         // B's own timer
    CHECK(reg->awaitIdle(5s));
    CHECK(!reg->hasAny("t"));
    CHECK_EQ(expired.load(), 1);
    reg->shutdown();
}

TEST(timeout_stay_checkpoints_and_machine_stays) {
    auto spec = SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("HOLD")
            .state("HOLD").interim().timeoutStay(120ms, [](MachineBase& m) { ctx<Ctx>(m).beats++; }).on<Stop>("DONE")
            .state("DONE").finalState().timeout(1s, "DONE").build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); }).build();
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    auto reg = StatemachineRegistry<Ctx>::builder("stay").supervisor(spec, 2).persistence(store).rehydrate(true).threads(2).build();
    CHECK(reg->dispatch("h", Ctx{}).accepted);
    CHECK(reg->awaitIdle(5s));
    auto first = store->load("h", "stay")->timeoutDeadlineMs;
    sleepMs(450);
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->supervisorStateOf("h").value_or("gone"), std::string("HOLD"));
    CHECK(reg->supervisorContextOf("h")->beats >= 2);
    auto snap = store->load("h", "stay");
    CHECK(snap && snap->timeoutDeadlineMs > first);                       // refreshed deadline persisted
    CHECK(ctxCodec().decode(snap->contextEncoded).beats >= 2);            // beats reached the store
    reg->shutdown();
}

TEST(route_typo_dies_at_build) {
    auto spec = SupervisorSpec<Ctx>::builder().name("Sup").stateMap(runningGraph())
        .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); r.forwardTo<Ping>("Signalling"); }).build();
    CHECK_THROWS(StatemachineRegistry<Ctx>::builder("typo").supervisor(spec, 2).child(childSpec("Signaling"), 2).build());
}

TEST(reset_hook_runs_on_every_pool_return) {
    std::atomic<int> hooks{0};
    auto reg = StatemachineRegistry<Ctx>::builder("hook").supervisor(runningSpec(), 1)
        .resetHook("Sup", [&](MachineBase&) { hooks++; }).threads(2).build();
    for (int i = 0; i < 3; i++) {
        CHECK(reg->dispatch("r", Ctx{}).accepted);
        reg->onInboundEvent("r", makeEvent<Stop>());
        CHECK(reg->awaitIdle(5s));
    }
    CHECK(hooks.load() >= 3);
    reg->shutdown();
}

TEST(child_publish_reaches_supervisor) {
    struct Rang : Event {};
    auto sig = MachineSpec<ChildCtx>::builder().name("Sig").codec(childCodec())
        .stateMap(StateMap::builder().initialState("W")
            .state("W").interim().timeout(1h, "C").on<Ping>("RANG")
            .state("RANG").interim().timeout(1h, "C").onEntry([](MachineBase& m) { m.publishEvent(makeEvent<Rang>()); })
            .state("C").finalState().timeout(1s, "C").build()).build();
    auto sup = SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("ACTIVE")
            .state("ACTIVE").interim().timeout(1h, "DONE").onEntry([](MachineBase& m) { dynamic_cast<SupervisorCore&>(m).resolver().spawnChild("Sig", ChildCtx{}); })
                .on<Rang>("RANG")
            .state("RANG").interim().timeout(1h, "DONE").onEntry([](MachineBase& m) { ctx<Ctx>(m).touches++; })
            .state("DONE").finalState().timeout(1s, "DONE").build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Rang>(); r.forwardTo<Ping>("Sig"); }).build();
    auto reg = StatemachineRegistry<Ctx>::builder("pub").supervisor(sup, 2).child(sig, 2).threads(2).build();
    CHECK(reg->dispatch("p", Ctx{}).accepted);
    CHECK(reg->awaitIdle(5s));
    reg->onInboundEvent("p", makeEvent<Ping>());                          // → Sig → RANG → publishes Rang → Sup RANG
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->supervisorStateOf("p").value_or(""), std::string("RANG"));
    CHECK_EQ(reg->stateOf("p", "Sig").value_or(""), std::string("RANG"));
    reg->shutdown();
}

TEST(shutdown_force_fails_live_sessions_through_final_state) {
    std::atomic<int> expired{0};
    auto spec = SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("RUNNING")
            .state("RUNNING").interim().timeout(1h, "EXPIRED")
            .state("EXPIRED").finalState().timeout(1s, "EXPIRED").onEntry([&](MachineBase&) { expired++; }).build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); }).build();
    auto reg = StatemachineRegistry<Ctx>::builder("shut").supervisor(spec, 4).threads(2).build();
    for (int i = 0; i < 3; i++) CHECK(reg->dispatch("s" + std::to_string(i), Ctx{}).accepted);
    CHECK(reg->awaitIdle(5s));
    reg->shutdown();
    CHECK_EQ(expired.load(), 3);                                          // terminal work ran on every exit path
    CHECK_EQ(reg->activeCellCount(), 0);
}

TEST_MAIN("registry_core")
