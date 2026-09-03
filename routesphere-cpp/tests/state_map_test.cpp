#include "check.hpp"
#include "fixtures.hpp"

TEST(builds_valid_graph_and_injects_idle) {
    auto g = runningGraph();
    CHECK(g.has(StateMap::IDLE));
    CHECK(g.has("RUNNING"));
    CHECK_EQ(g.initialState(), std::string("RUNNING"));
    CHECK(g.get("DONE").finalState);
    CHECK(!g.hasOfflineState());
}

TEST(rejects_missing_kind) {
    CHECK_THROWS(StateMap::builder().initialState("A").state("A").timeout(1s, "B").state("B").finalState().timeout(1s, "B").build());
}

TEST(rejects_missing_timeout) {
    CHECK_THROWS(StateMap::builder().initialState("A").state("A").interim().state("B").finalState().timeout(1s, "B").build());
}

TEST(rejects_non_final_timeout_target) {
    CHECK_THROWS(StateMap::builder().initialState("A")
        .state("A").interim().timeout(1s, "B")
        .state("B").interim().timeout(1s, "C")
        .state("C").finalState().timeout(1s, "C").build());
}

TEST(rejects_final_offline_and_final_stay) {
    CHECK_THROWS(StateMap::builder().initialState("A").state("A").interim().timeout(1s, "B").state("B").finalState().offline().timeout(1s, "B").build());
    CHECK_THROWS(StateMap::builder().initialState("A").state("A").interim().timeout(1s, "B").state("B").finalState().timeoutStay(1s).build());
}

TEST(rejects_unknown_transition_target) {
    CHECK_THROWS(StateMap::builder().initialState("A").state("A").interim().timeout(1s, "B").on<Stop>("NOPE").state("B").finalState().timeout(1s, "B").build());
}

TEST(stay_mode_and_offline_are_recorded) {
    auto g = StateMap::builder().initialState("A")
        .state("A").interim().timeoutStay(2s)
        .state("P").interim().offline().timeout(1s, "B")
        .state("B").finalState().timeout(1s, "B").build();
    CHECK(g.get("A").timeout->stay);
    CHECK(g.get("P").offline);
    CHECK(g.hasOfflineState());
}

TEST_MAIN("state_map")
