// statewalk-cpp example — a FreeSWITCH-shaped inbound call supervisor.
//
// The shape a mod_routesphere would use: CHANNEL_PARK creates the request
// (first event), the supervisor answers through the channel, CHANNEL_ANSWER
// activates, CHANNEL_HANGUP tears down, the terminal state emits a record.
// Here the channel is the in-memory TestChannel; in the module it is
// statewalk::fs::FreeSwitchChannel (same interface).
#include <iostream>

#include "statewalk/registry.hpp"

using namespace statewalk;
using namespace std::chrono_literals;

// FreeSWITCH-shaped events (in the module these come out of FsEvent headers).
struct ChannelPark : Event { std::string caller, callee; ChannelPark(std::string a, std::string b) : caller(std::move(a)), callee(std::move(b)) {} bool isFirst() const override { return true; } };
struct ChannelAnswer : Event {};
struct ChannelHangup : Event { std::string cause; explicit ChannelHangup(std::string c) : cause(std::move(c)) {} };

struct CallCtx { std::string uuid, caller, callee, hangupCause; std::int64_t answeredAtMs = 0, endedAtMs = 0; };

static Codec<CallCtx> callCodec() {
    return Codec<CallCtx>{"CallCtx",
        [](const CallCtx& c) { return c.uuid + "|" + c.caller + "|" + c.callee + "|" + c.hangupCause + "|" + std::to_string(c.answeredAtMs) + "|" + std::to_string(c.endedAtMs); },
        [](const std::string& s) {
            CallCtx c; std::vector<std::string> p; std::string cur;
            for (char ch : s) { if (ch == '|') { p.push_back(cur); cur.clear(); } else cur += ch; } p.push_back(cur);
            c.uuid = p[0]; c.caller = p[1]; c.callee = p[2]; c.hangupCause = p[3]; c.answeredAtMs = std::stoll(p[4]); c.endedAtMs = std::stoll(p[5]); return c; }};
}

int main() {
    auto channel = std::make_shared<TestChannel<std::string>>("fs");   // → fs::FreeSwitchChannel in the module
    std::shared_ptr<StatemachineRegistry<CallCtx>> registry;

    auto spec = SupervisorSpec<CallCtx>::builder().name("CallSupervisor").codec(callCodec())
        .stateMap(StateMap::builder()
            .initialState("RINGING")
            .state("RINGING").interim().timeout(90s, "FAILED")
                .onEntry([&](MachineBase& m) { channel->send(m.machineId(), "uuid_answer " + m.machineId()); })
                .on<ChannelAnswer>("ACTIVE", nullptr, [](MachineBase& m, const ChannelAnswer&) { ctx<CallCtx>(m).answeredAtMs = nowMs(); })
                .on<ChannelHangup>("FAILED", nullptr, [](MachineBase& m, const ChannelHangup& e) { ctx<CallCtx>(m).hangupCause = e.cause; })
            .state("ACTIVE").interim().timeout(4h, "HANGUP")
                .on<ChannelHangup>("HANGUP", nullptr, [](MachineBase& m, const ChannelHangup& e) { ctx<CallCtx>(m).hangupCause = e.cause; })
            .state("HANGUP").finalState().timeout(1s, "HANGUP")
                .onEntry([](MachineBase& m) { auto& c = ctx<CallCtx>(m); c.endedAtMs = nowMs();
                    std::cout << "CDR uuid=" << c.uuid << " " << c.caller << "->" << c.callee << " answered=" << (c.answeredAtMs > 0) << " cause=" << c.hangupCause << "\n"; })
            .state("FAILED").finalState().timeout(1s, "FAILED")
                .onEntry([](MachineBase& m) { auto& c = ctx<CallCtx>(m); std::cout << "CDR uuid=" << c.uuid << " FAILED cause=" << c.hangupCause << "\n"; })
            .build())
        .routes([](InternalEventResolver& r) { r.selfHandle<ChannelAnswer>(); r.selfHandle<ChannelHangup>(); r.selfHandle<ChannelPark>(); })
        .build();

    registry = StatemachineRegistry<CallCtx>::builder("call")
        .supervisor(spec, 1024)
        .channel(channel)
        .createFromFirstEvent([](const Event& e) -> std::optional<CallCtx> {
            if (auto* p = dynamic_cast<const ChannelPark*>(&e)) { CallCtx c; c.caller = p->caller; c.callee = p->callee; return c; }
            return std::nullopt;
        })
        .volatileLoader("CallSupervisor", [](MachineBase& m) { ctx<CallCtx>(m).uuid = m.machineId(); return std::any{}; })
        .threads(2)
        .build();

    // A call arrives on the "wire": CHANNEL_PARK creates it, ANSWER activates it, HANGUP ends it.
    const std::string uuid = "7f1c2e10-esl-less-call";
    channel->inject(uuid, makeEvent<ChannelPark>("+8801711000000", "16247")).get();
    channel->inject(uuid, makeEvent<ChannelAnswer>()).get();
    channel->inject(uuid, makeEvent<ChannelHangup>("NORMAL_CLEARING")).get();
    registry->awaitIdle(5s);

    std::cout << "commands sent to FreeSWITCH: ";
    for (auto& s : channel->sends) std::cout << "[" << s.command << "] ";
    std::cout << "\nlive requests after hangup: " << registry->activeIdCount() << "\n";
    registry->shutdown();
    return registry->activeIdCount() == 0 ? 0 : 1;
}
