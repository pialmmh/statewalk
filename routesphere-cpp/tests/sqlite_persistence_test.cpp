#include <filesystem>

#include "check.hpp"
#include "fixtures.hpp"
#include "statewalk/persistence_sqlite.hpp"

static std::string tmpDb(const char* tag) {
    auto p = std::filesystem::temp_directory_path() / ("statewalk_" + std::string(tag) + "_" + std::to_string(::getpid()) + ".db");
    std::filesystem::remove(p);
    return p.string();
}

TEST(sqlite_roundtrip_and_quarantine) {
    auto path = tmpDb("rt");
    SqlitePersistenceProvider p(path, "sw_rt");
    Snapshot s; s.machineId = "m1"; s.registryName = "r"; s.currentState = "A"; s.contextType = "Ctx"; s.contextEncoded = "1|2|x|y";
    s.savedAtMs = nowMs(); s.timeoutTargetState = "B"; s.timeoutDeadlineMs = 42; s.globalDeadlineMs = 7;
    p.save(s);
    auto l = p.load("m1", "r");
    CHECK(l && l->currentState == "A" && l->contextEncoded == "1|2|x|y" && l->timeoutTargetState == "B" && l->timeoutDeadlineMs == 42 && l->globalDeadlineMs == 7);
    s.currentState = "C"; s.timeoutTargetState.reset(); p.save(s);      // replace
    CHECK_EQ(p.load("m1", "r")->currentState, std::string("C"));
    CHECK(!p.load("m1", "r")->timeoutTargetState);
    CHECK_EQ(p.loadAllForRegistry("r").size(), std::size_t(1));
    p.quarantine("m1", "r", "unit reason");
    CHECK(!p.load("m1", "r"));
    CHECK_EQ(p.size(), 0); CHECK_EQ(p.deadSize(), 1);
    p.quarantine("m1", "r", "again");                                      // idempotent
    CHECK_EQ(p.deadSize(), 1);
    std::filesystem::remove(path);
}

TEST(sqlite_registry_crash_restart_resumes_hibernated_and_live) {
    auto path = tmpDb("reg");
    auto store = std::make_shared<SqlitePersistenceProvider>(path, "sw_reg");
    auto spec = SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("ACTIVE")
            .state("ACTIVE").interim().timeout(1h, "EXPIRED").on<Park>("PARKED").stay<Touch>([](MachineBase& m, const Touch&) { ctx<Ctx>(m).touches++; })
            .state("PARKED").interim().offline().timeout(1h, "EXPIRED").on<Stop>("DONE").stay<Touch>([](MachineBase& m, const Touch&) { ctx<Ctx>(m).touches++; })
            .state("DONE").finalState().timeout(1s, "DONE")
            .state("EXPIRED").finalState().timeout(1s, "EXPIRED").build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Park>(); r.selfHandle<Stop>(); r.selfHandle<Touch>(); }).build();
    {
        auto a = StatemachineRegistry<Ctx>::builder("sq").supervisor(spec, 4).persistence(store).rehydrate(true).threads(2).build();
        Ctx c; c.mark = "durable";
        CHECK(a->dispatch("live", c).accepted);
        CHECK(a->dispatch("parked", c).accepted);
        a->onInboundEvent("live", makeEvent<Touch>());
        a->onInboundEvent("parked", makeEvent<Park>());
        CHECK(a->awaitIdle(5s));
        CHECK_EQ(store->size(), 2);
        // A crashes: destroyed WITHOUT shutdown is impossible in C++ (dtor shuts down), so
        // build B while A is still alive — from the store's point of view identical.
        auto b = StatemachineRegistry<Ctx>::builder("sq").supervisor(spec, 4).persistence(store).rehydrate(true).threads(2).build();
        CHECK(b->awaitIdle(5s));
        CHECK(b->hasAny("live"));                                           // live one resumed
        CHECK_EQ(b->supervisorContextOf("live")->touches, 1);
        CHECK_EQ(b->supervisorContextOf("live")->mark, std::string("durable"));
        CHECK(!b->hasAny("parked"));                                        // hibernated one stays db-only
        b->onInboundEvent("parked", makeEvent<Touch>());                    // wakes from sqlite
        CHECK(b->awaitIdle(5s));
        CHECK_EQ(b->supervisorStateOf("parked").value_or(""), std::string("PARKED"));
        b->onInboundEvent("parked", makeEvent<Stop>());
        b->onInboundEvent("live", makeEvent<Stop>());
        CHECK(b->awaitIdle(5s));
        // A still holds the (now stale) live cell in memory; its shutdown may abort its copy —
        // shut it down BEFORE asserting the store so its delete lands too.
        a->shutdown();
        b->shutdown();
        CHECK_EQ(store->size(), 0);
    }
    std::filesystem::remove(path);
}

TEST_MAIN("sqlite_persistence")
