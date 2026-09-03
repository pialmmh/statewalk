#include "check.hpp"
#include "fixtures.hpp"

static SupervisorSpec<Ctx> quotaSpec() { return runningSpec(); }

TEST(partner_concurrency_cap_and_release) {
    auto reg = StatemachineRegistry<Ctx>::builder("qc").supervisor(quotaSpec(), 8)
        .quotaKeysExtractor([](const Ctx& c) { return QuotaKeys::ofPartner(c.partner); })
        .quotaLimits(QuotaLimits{2, 0, 0, 0}).threads(2).build();
    Ctx x; x.partner = "X";
    CHECK(reg->dispatch("a", x).accepted);
    CHECK(reg->dispatch("b", x).accepted);
    CHECK_EQ(*reg->dispatch("c", x).rejectCause, RejectCause::PartnerConcurrencyExceeded);
    CHECK_EQ(reg->quotaPartnerActive("X"), 2);
    reg->onInboundEvent("a", makeEvent<Stop>());
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->quotaPartnerActive("X"), 1);
    CHECK(reg->dispatch("c", x).accepted);
    reg->shutdown();
}

TEST(rejected_dispatch_burns_no_tps_token) {
    QuotaController qc;
    QuotaLimits l{10, 1, 5, 0};
    CHECK(!qc.tryAcquire(QuotaKeys::of("P", "R"), l));
    CHECK_EQ(*qc.tryAcquire(QuotaKeys::of("P", "R"), l), RejectCause::RouteConcurrencyExceeded);
    CHECK_EQ(qc.partnerActive("P"), 1);
    for (int i = 0; i < 4; i++) CHECK(!qc.tryAcquire(QuotaKeys::of("P", "R" + std::to_string(i)), l));
    CHECK_EQ(*qc.tryAcquire(QuotaKeys::of("P", "R-last"), l), RejectCause::PartnerTpsExceeded);
}

TEST(counters_prune_at_zero) {
    QuotaController qc;
    QuotaLimits l{10, 10, 0, 0};
    for (int i = 0; i < 100; i++) CHECK(!qc.tryAcquire(QuotaKeys::of("u" + std::to_string(i), "r" + std::to_string(i)), l));
    CHECK_EQ(qc.trackedKeyCount(), std::size_t(200));
    for (int i = 0; i < 100; i++) qc.release(QuotaKeys::of("u" + std::to_string(i), "r" + std::to_string(i)), l);
    CHECK_EQ(qc.trackedKeyCount(), std::size_t(0));
}

TEST(rebind_acquires_new_before_releasing_old) {
    auto reg = StatemachineRegistry<Ctx>::builder("rb").supervisor(quotaSpec(), 8)
        .quotaKeysExtractor([](const Ctx& c) { return c.partner.empty() ? QuotaKeys::none() : QuotaKeys::ofPartner(c.partner); })
        .quotaLimits(QuotaLimits{2, 0, 0, 0}).threads(2).build();
    for (const char* id : {"a", "b", "c"}) CHECK(reg->dispatch(id, Ctx{}).accepted);   // anonymous
    CHECK(reg->awaitIdle(5s));
    CHECK(!reg->rebindQuotaKeys("a", QuotaKeys::ofPartner("u1")));
    CHECK(!reg->rebindQuotaKeys("b", QuotaKeys::ofPartner("u1")));
    CHECK_EQ(*reg->rebindQuotaKeys("c", QuotaKeys::ofPartner("u1")), RejectCause::PartnerConcurrencyExceeded);
    CHECK_EQ(reg->quotaPartnerActive("u1"), 2);
    CHECK(reg->quotaKeysOf("c").isNone());
    CHECK(!reg->rebindQuotaKeys("a", QuotaKeys::ofPartner("u2")));         // move slot
    CHECK_EQ(reg->quotaPartnerActive("u1"), 1);
    CHECK_EQ(reg->quotaPartnerActive("u2"), 1);
    CHECK_THROWS(reg->rebindQuotaKeys("nope", QuotaKeys::ofPartner("u1")));
    reg->onInboundEvent("a", makeEvent<Stop>());
    CHECK(reg->awaitIdle(5s));
    CHECK_EQ(reg->quotaPartnerActive("u2"), 0);                            // terminal releases the REBOUND keys
    reg->shutdown();
}

TEST_MAIN("quota")
