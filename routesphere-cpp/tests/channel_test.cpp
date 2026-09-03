#include "check.hpp"
#include "fixtures.hpp"

TEST(channel_inbound_is_wired_and_acked) {
    auto ch = std::make_shared<TestChannel<std::string>>("wire");
    auto reg = StatemachineRegistry<Ctx>::builder("ch").supervisor(runningSpec(), 2).channel(ch).threads(2).build();
    CHECK(ch->isStarted());
    CHECK(reg->dispatch("c", Ctx{}).accepted);
    CHECK(reg->awaitIdle(5s));
    auto ack = ch->inject("c", makeEvent<Touch>());
    ack.get();                                                            // completes when processed
    CHECK_EQ(reg->supervisorContextOf("c")->touches, 1);
    auto bad = ch->inject("unknown-id", makeEvent<Touch>());
    CHECK_THROWS(bad.get());                                              // UnknownRequest through the ack
    reg->shutdown();
    CHECK(!ch->isStarted());
    CHECK_THROWS(ch->inject("c", makeEvent<Touch>()).get());              // post-shutdown: loud, not silent
}

TEST(overload_sheds_with_failed_ack) {
    auto reg = StatemachineRegistry<Ctx>::builder("ov").supervisor(runningSpec(), 2).maxPendingInbound(64).threads(1).build();
    CHECK(reg->dispatch("o", Ctx{}).accepted);
    // Burst far beyond the bound before the single worker drains; some acks must fail (shed), none may hang.
    int shed = 0; std::vector<Ack> acks;
    for (int i = 0; i < 2000; i++) acks.push_back(reg->submitInbound("o", makeEvent<Touch>()));
    for (auto& a : acks) { try { a.get(); } catch (const InboundRejected&) { shed++; } }
    CHECK(shed > 0);
    CHECK(reg->awaitIdle(10s));
    CHECK(reg->supervisorContextOf("o")->touches + shed == 2000);        // every event either processed or shed — none lost silently
    reg->shutdown();
}

TEST_MAIN("channel")
