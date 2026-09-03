#include <atomic>

#include "check.hpp"
#include "fixtures.hpp"

static Snapshot snap(const std::string& id, const std::string& reg, const std::string& state, const Ctx& c,
                     std::optional<std::string> target, std::int64_t deadline, std::int64_t global = 0) {
    Snapshot s; s.machineId = id; s.registryName = reg; s.currentState = state; s.contextType = "Ctx";
    s.contextEncoded = ctxCodec().encode(c); s.savedAtMs = nowMs(); s.timeoutTargetState = target; s.timeoutDeadlineMs = deadline; s.globalDeadlineMs = global;
    return s;
}

TEST(saves_on_every_transition_and_stay) {
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    auto reg = StatemachineRegistry<Ctx>::builder("save").supervisor(runningSpec(), 2).persistence(store).rehydrate(true).threads(2).build();
    CHECK(reg->dispatch("s", Ctx{}).accepted);
    reg->onInboundEvent("s", makeEvent<Touch>());
    reg->onInboundEvent("s", makeEvent<Touch>());
    CHECK(reg->awaitIdle(5s));
    auto row = store->load("s", "save");
    CHECK(row && row->currentState == "RUNNING");
    CHECK_EQ(ctxCodec().decode(row->contextEncoded).touches, 2);
    reg->onInboundEvent("s", makeEvent<Stop>());
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(store->size(), std::size_t(0));
    reg->shutdown();
}

TEST(crash_restart_resumes_without_entry_replay) {
    std::atomic<int> entries{0};
    auto mk = [&] {
        return SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
            .stateMap(StateMap::builder().initialState("RUNNING")
                .state("RUNNING").interim().timeout(1h, "EXPIRED").onEntry([&](MachineBase&) { entries++; })
                    .stay<Touch>([](MachineBase& m, const Touch&) { ctx<Ctx>(m).touches++; }).on<Stop>("DONE")
                .state("DONE").finalState().timeout(1s, "DONE")
                .state("EXPIRED").finalState().timeout(1s, "EXPIRED").build())
            .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); r.selfHandle<Touch>(); }).build();
    };
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    {
        auto a = StatemachineRegistry<Ctx>::builder("fo").supervisor(mk(), 2).persistence(store).rehydrate(true).threads(2).build();
        Ctx c; c.mark = "kept";
        CHECK(a->dispatch("k", c).accepted);
        for (int i = 0; i < 3; i++) a->onInboundEvent("k", makeEvent<Touch>());
        CHECK(a->awaitIdle(5s));
        CHECK_EQ(entries.load(), 1);
        // node A "crashes": we drop it WITHOUT shutdown (shutdown would end the session) —
        // simulate by detaching: leak the shared_ptr on purpose? No: we can't skip its
        // destructor; instead hold it alive in an outer scope and never call shutdown until after B checks.
        auto b = StatemachineRegistry<Ctx>::builder("fo").supervisor(mk(), 2).persistence(store).rehydrate(true).threads(2).build();
        CHECK(b->awaitIdle(5s));
        CHECK(b->hasAny("k"));
        CHECK_EQ(b->supervisorStateOf("k").value_or(""), std::string("RUNNING"));
        CHECK_EQ(b->supervisorContextOf("k")->touches, 3);
        CHECK_EQ(b->supervisorContextOf("k")->mark, std::string("kept"));
        CHECK_EQ(entries.load(), 1);                                      // NO entry replay on B
        b->onInboundEvent("k", makeEvent<Stop>());
        CHECK(b->awaitIdle(5s));
        CHECK(!b->hasAny("k"));
        b->shutdown();
        a->shutdown();
    }
}

TEST(matured_target_deadline_transitions_on_restore) {
    std::atomic<int> expired{0};
    auto spec = SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("RUNNING")
            .state("RUNNING").interim().timeout(1h, "EXPIRED")
            .state("EXPIRED").finalState().timeout(1s, "EXPIRED").onEntry([&](MachineBase&) { expired++; }).build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); }).build();
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    store->save(snap("late", "mat", "RUNNING", Ctx{}, "EXPIRED", nowMs() - 5000));
    auto reg = StatemachineRegistry<Ctx>::builder("mat").supervisor(spec, 2).persistence(store).rehydrate(true).threads(2).build();
    CHECK(reg->awaitIdle(5s));
    CHECK(!reg->hasAny("late"));
    CHECK_EQ(expired.load(), 1);
    CHECK_EQ(store->size(), std::size_t(0));
    reg->shutdown();
}

TEST(matured_stay_deadline_checkpoints_on_restore) {
    auto spec = SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("HOLD")
            .state("HOLD").interim().timeoutStay(60s, [](MachineBase& m) { ctx<Ctx>(m).beats++; }).on<Stop>("DONE")
            .state("DONE").finalState().timeout(1s, "DONE").build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); }).build();
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    Ctx prior; prior.beats = 3;
    store->save(snap("hb", "stayr", "HOLD", prior, std::nullopt, nowMs() - 500));
    auto reg = StatemachineRegistry<Ctx>::builder("stayr").supervisor(spec, 2).persistence(store).rehydrate(true).threads(2).build();
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->supervisorStateOf("hb").value_or(""), std::string("HOLD"));
    CHECK_EQ(reg->supervisorContextOf("hb")->beats, 4);                  // missed checkpoint ran once
    CHECK(store->load("hb", "stayr")->timeoutDeadlineMs > nowMs() - 50); // re-armed future deadline persisted
    reg->shutdown();
}

static SupervisorSpec<Ctx> hibernatingSpec(std::chrono::milliseconds window, std::atomic<int>* expired = nullptr) {
    return SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("ACTIVE")
            .state("ACTIVE").interim().timeout(1h, "EXPIRED").on<Park>("PARKED")
                .stay<Touch>([](MachineBase& m, const Touch&) { ctx<Ctx>(m).touches++; })
            .state("PARKED").interim().offline().timeout(window, "EXPIRED").on<Stop>("DONE")
                .stay<Touch>([](MachineBase& m, const Touch&) { ctx<Ctx>(m).touches++; })
            .state("DONE").finalState().timeout(1s, "DONE")
            .state("EXPIRED").finalState().timeout(1s, "EXPIRED").onEntry([expired](MachineBase&) { if (expired) (*expired)++; }).build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Park>(); r.selfHandle<Stop>(); r.selfHandle<Touch>(); }).build();
}

TEST(offline_suspend_frees_memory_and_lazy_wakes) {
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    auto reg = StatemachineRegistry<Ctx>::builder("hib").supervisor(hibernatingSpec(1h), 4).persistence(store).rehydrate(true).threads(2).build();
    CHECK(reg->dispatch("h", Ctx{}).accepted);
    reg->onInboundEvent("h", makeEvent<Touch>());
    reg->onInboundEvent("h", makeEvent<Park>());
    CHECK(reg->awaitIdle(5s));
    CHECK(!reg->hasAny("h"));                                             // evicted: db-only
    CHECK_EQ(store->load("h", "hib")->currentState, std::string("PARKED"));
    reg->onInboundEvent("h", makeEvent<Touch>());                        // lazy wake
    CHECK(reg->awaitIdle(5s));
    CHECK(reg->hasAny("h"));
    CHECK_EQ(reg->supervisorContextOf("h")->touches, 2);
    reg->shutdown();
}

TEST(offline_without_rehydrate_rejected_at_build) {
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    CHECK_THROWS(StatemachineRegistry<Ctx>::builder("bad").supervisor(hibernatingSpec(1h), 2).persistence(store).build());
}

TEST(startup_leaves_hibernated_db_only_and_settles_matured) {
    std::atomic<int> expired{0};
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    {
        auto a = StatemachineRegistry<Ctx>::builder("hs").supervisor(hibernatingSpec(1h, &expired), 4).persistence(store).rehydrate(true).threads(2).build();
        for (const char* id : {"h1", "h2"}) { CHECK(a->dispatch(id, Ctx{}).accepted); a->onInboundEvent(id, makeEvent<Park>()); }
        CHECK(a->awaitIdle(5s));
        CHECK_EQ(a->activeIdCount(), 0);
        a->shutdown();                                                    // hibernated rows survive shutdown
    }
    store->save(snap("old", "hs", "PARKED", Ctx{}, "EXPIRED", nowMs() - 5000));
    CHECK_EQ(store->size(), std::size_t(3));
    auto b = StatemachineRegistry<Ctx>::builder("hs").supervisor(hibernatingSpec(1h, &expired), 4).persistence(store).rehydrate(true).threads(2).build();
    CHECK(b->awaitIdle(5s));
    CHECK_EQ(b->activeIdCount(), 0);                                      // no memory flood
    CHECK_EQ(expired.load(), 1);                                          // matured one settled
    CHECK_EQ(store->size(), std::size_t(2));
    b->onInboundEvent("h1", makeEvent<Touch>());
    CHECK(b->awaitIdle(5s));
    CHECK(b->hasAny("h1"));
    b->shutdown();
}

TEST(final_state_tombstone_purged_not_resurrected) {
    std::atomic<int> expired{0};
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    store->save(snap("dead", "tomb", "DONE", Ctx{}, "DONE", nowMs() + 1000));
    auto reg = StatemachineRegistry<Ctx>::builder("tomb").supervisor(hibernatingSpec(1h, &expired), 2).persistence(store).rehydrate(true).threads(2).build();
    CHECK(reg->awaitIdle(5s));
    CHECK(!reg->hasAny("dead"));
    CHECK_EQ(store->size(), std::size_t(0));
    CHECK_EQ(expired.load(), 0);
    reg->shutdown();
}

TEST(corrupt_and_drifted_rows_are_quarantined) {
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    Snapshot bad = snap("bad-ctx", "q", "RUNNING", Ctx{}, "EXPIRED", nowMs() + 3600000); bad.contextEncoded = "garbage";
    store->save(bad);
    store->save(snap("bad-state", "q", "STATE_FROM_OLD_BUILD", Ctx{}, "EXPIRED", nowMs() + 3600000));
    auto reg = StatemachineRegistry<Ctx>::builder("q").supervisor(runningSpec(), 2).persistence(store).rehydrate(true).threads(2).build();
    CHECK(reg->awaitIdle(5s));
    CHECK(!reg->hasAny("bad-ctx")); CHECK(!reg->hasAny("bad-state"));
    CHECK_EQ(store->size(), std::size_t(0));
    CHECK_EQ(store->deadSize(), std::size_t(2));
    CHECK(store->deadReason("bad-state", "q")->find("STATE_FROM_OLD_BUILD") != std::string::npos);
    auto st = reg->poolStats("Sup");
    CHECK_EQ(st.totalBorrowed, st.reclaimed());                           // no borrow leaked on the corrupt row
    reg->shutdown();
}

TEST(global_deadline_persisted_and_enforced_after_restart) {
    auto store = std::make_shared<InMemoryPersistenceProvider>();
    {
        auto a = StatemachineRegistry<Ctx>::builder("gto").supervisor(runningSpec(), 2).persistence(store).rehydrate(true).globalTimeout(600ms, "EXPIRED").threads(2).build();
        CHECK(a->dispatch("g", Ctx{}).accepted);
        CHECK(a->awaitIdle(5s));
        CHECK(store->load("g", "gto")->globalDeadlineMs > 0);
        // "crash": keep A alive but ignore it; B on the same store after the cap matured
        sleepMs(800);
        auto b = StatemachineRegistry<Ctx>::builder("gto").supervisor(runningSpec(), 2).persistence(store).rehydrate(true).globalTimeout(600ms, "EXPIRED").threads(2).build();
        CHECK(awaitUntil([&] { return !b->hasAny("g") && store->size() == 0; }, 3000));
        b->shutdown();
        a->shutdown();
    }
}

TEST(slow_store_never_blocks_the_hot_path) {
    struct Slow : InMemoryPersistenceProvider {
        void save(const Snapshot& s) override { sleepMs(800); InMemoryPersistenceProvider::save(s); }
        void remove(const std::string& a, const std::string& b) override { sleepMs(800); InMemoryPersistenceProvider::remove(a, b); }
    };
    auto store = std::make_shared<Slow>();
    auto reg = StatemachineRegistry<Ctx>::builder("slow").supervisor(runningSpec(), 2).persistence(store).threads(2).build();
    CHECK(reg->dispatch("s", Ctx{}).accepted);
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 5; i++) reg->onInboundEvent("s", makeEvent<Touch>());
    CHECK(awaitUntil([&] { auto c = reg->supervisorContextOf("s"); return c && c->touches == 5; }, 700));
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    CHECK_MSG(ms < 700, "events waited on disk: " + std::to_string(ms) + "ms");
    reg->onInboundEvent("s", makeEvent<Stop>());
    CHECK(reg->awaitIdle(20s));
    CHECK_EQ(store->size(), std::size_t(0));
    reg->shutdown();
}

TEST_MAIN("persistence_rehydration")
