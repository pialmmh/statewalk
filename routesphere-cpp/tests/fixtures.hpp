// Shared fixtures: a small context with a codec, events, graphs.
#pragma once

#include <sstream>
#include <string>
#include <vector>

#include "statewalk/registry.hpp"

using namespace statewalk;
using namespace std::chrono_literals;

struct Ctx {
    int touches = 0;
    int beats = 0;
    std::string mark;
    std::string partner;
};

inline std::vector<std::string> splitPipe(const std::string& s) {
    std::vector<std::string> out; std::string cur;
    for (char c : s) { if (c == '|') { out.push_back(cur); cur.clear(); } else cur += c; }
    out.push_back(cur);
    return out;
}

inline Codec<Ctx> ctxCodec() {
    return Codec<Ctx>{
        "Ctx",
        [](const Ctx& c) { std::ostringstream os; os << c.touches << '|' << c.beats << '|' << c.mark << '|' << c.partner; return os.str(); },
        [](const std::string& s) {
            auto p = splitPipe(s);
            if (p.size() != 4) throw std::runtime_error("bad Ctx payload: " + s);
            Ctx c; c.touches = std::stoi(p[0]); c.beats = std::stoi(p[1]); c.mark = p[2]; c.partner = p[3]; return c;
        }};
}

struct Touch : Event {};
struct Stop : Event {};
struct Park : Event {};
struct Ping : Event {};
struct Advance : Event {};
struct Open : Event { std::string caller; explicit Open(std::string c) : caller(std::move(c)) {} bool isFirst() const override { return true; } };

/// RUNNING (1h → EXPIRED) with Touch stays, Stop → DONE.
inline StateMap runningGraph() {
    return StateMap::builder()
        .initialState("RUNNING")
        .state("RUNNING").interim().timeout(1h, "EXPIRED")
            .on<Stop>("DONE")
            .stay<Touch>([](MachineBase& m, const Touch&) { ctx<Ctx>(m).touches++; })
            .stay<Ping>([](MachineBase&, const Ping&) {})
        .state("DONE").finalState().timeout(1s, "DONE")
        .state("EXPIRED").finalState().timeout(1s, "EXPIRED")
        .build();
}

inline SupervisorSpec<Ctx> runningSpec(const char* name = "Sup") {
    return SupervisorSpec<Ctx>::builder().name(name).stateMap(runningGraph()).codec(ctxCodec())
        .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); r.selfHandle<Touch>(); r.selfHandle<Ping>(); })
        .build();
}

struct ChildCtx { int marks = 0; };
inline Codec<ChildCtx> childCodec() {
    return Codec<ChildCtx>{"ChildCtx", [](const ChildCtx& c) { return std::to_string(c.marks); }, [](const std::string& s) { ChildCtx c; c.marks = std::stoi(s); return c; }};
}
inline MachineSpec<ChildCtx> childSpec(const char* name) {
    return MachineSpec<ChildCtx>::builder().name(name).codec(childCodec())
        .stateMap(StateMap::builder().initialState("WORKING")
            .state("WORKING").interim().timeout(1h, "CLOSED").stay<Ping>([](MachineBase&, const Ping&) {})
            .state("CLOSED").finalState().timeout(1s, "CLOSED").build())
        .build();
}

/// Supervisor that spawns children A and B on entry.
inline SupervisorSpec<Ctx> spawningSpec() {
    return SupervisorSpec<Ctx>::builder().name("Sup").codec(ctxCodec())
        .stateMap(StateMap::builder().initialState("RUNNING")
            .state("RUNNING").interim().timeout(1h, "EXPIRED")
                .onEntry([](MachineBase& m) { auto& s = dynamic_cast<SupervisorCore&>(m); s.resolver().spawnChild("A", ChildCtx{}); s.resolver().spawnChild("B", ChildCtx{}); })
                .on<Stop>("DONE")
            .state("DONE").finalState().timeout(1s, "DONE")
            .state("EXPIRED").finalState().timeout(1s, "EXPIRED").build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Stop>(); r.forwardToAll<Ping>({"A", "B"}); })
        .build();
}
