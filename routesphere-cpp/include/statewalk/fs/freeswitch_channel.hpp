// statewalk-cpp — in-process FreeSWITCH channel (NO ESL).
//
// Link this into a FreeSWITCH module (mod_routesphere): events are received
// through switch_event_bind on FreeSWITCH's own event threads and offered to
// the registry (non-blocking); commands go straight to switch_api_execute.
//
//   auto ch = std::make_shared<statewalk::fs::FreeSwitchChannel>("routesphere",
//                std::set<std::string>{"CHANNEL_PARK"});          // first events
//   auto reg = StatemachineRegistry<CallCtx>::builder("call")
//                .supervisor(callSpec, 4096)
//                .channel(ch)                                     // registry starts/stops it
//                .createFromFirstEvent([](const Event& e){ ... }) // CHANNEL_PARK → CallCtx
//                .build();
//   // inside a state action:
//   registry.channelAs<std::string>().send(uuid, "uuid_answer " + uuid);
//
// Compile with -I<freeswitch>/include/freeswitch; only built when
// STATEWALK_WITH_FREESWITCH is ON.
#pragma once

#include <map>
#include <mutex>
#include <set>
#include <string>

#include <switch.h>

#include "../channel.hpp"
#include "../log.hpp"

namespace statewalk::fs {

/// A FreeSWITCH event as a statewalk Event: name (CHANNEL_ANSWER…), subclass
/// (for CUSTOM), all headers, body. Match it in state graphs with a small
/// adapter event type per name if you prefer typed dispatch — or route on
/// FsEvent and branch on `name` inside a stay handler.
struct FsEvent : Event {
    std::string eventName;
    std::string subclass;
    std::map<std::string, std::string> headers;
    std::string body;
    bool first = false;

    bool isFirst() const override { return first; }
    std::string header(const std::string& h) const { auto it = headers.find(h); return it == headers.end() ? "" : it->second; }
    std::string uuid() const { return header("Unique-ID"); }
};

class FreeSwitchChannel final : public Channel<std::string> {
public:
    /// @param firstEventNames FreeSWITCH event names that CREATE a request
    ///        (typically CHANNEL_PARK for inbound-call supervision).
    /// @param idHeader the header carrying the request id (default Unique-ID).
    FreeSwitchChannel(std::string name, std::set<std::string> firstEventNames, std::string idHeader = "Unique-ID")
        : name_(std::move(name)), firstEvents_(std::move(firstEventNames)), idHeader_(std::move(idHeader)) {}

    /// command = "api_name args" — executed via switch_api_execute on the
    /// caller's thread (state actions run on registry worker threads).
    void send(const std::string& requestId, const std::string& command) override {
        std::string api = command, args;
        auto sp = command.find(' ');
        if (sp != std::string::npos) { api = command.substr(0, sp); args = command.substr(sp + 1); }
        switch_stream_handle_t stream = { 0 };
        SWITCH_STANDARD_STREAM(stream);
        switch_status_t st = switch_api_execute(api.c_str(), args.c_str(), nullptr, &stream);
        if (st != SWITCH_STATUS_SUCCESS) SW_WARN("[", name_, "] api '", api, "' failed for ", requestId, ": ", stream.data ? static_cast<const char*>(stream.data) : "");
        switch_safe_free(stream.data);
    }

    void cancel(const std::string& requestId) override { send(requestId, "uuid_kill " + requestId); }

    void start(InboundGateway gateway) override {
        std::lock_guard<std::mutex> g(mx_);
        gateway_ = std::move(gateway);
        if (bound_) return;
        if (switch_event_bind_removable(name_.c_str(), SWITCH_EVENT_ALL, SWITCH_EVENT_SUBCLASS_ANY, &FreeSwitchChannel::onEvent, this, &node_) != SWITCH_STATUS_SUCCESS) {
            SW_ERROR("[", name_, "] switch_event_bind failed");
            return;
        }
        bound_ = true;
        SW_INFO("[", name_, "] bound to FreeSWITCH events (in-process, no ESL)");
    }

    void stop() override {
        std::lock_guard<std::mutex> g(mx_);
        if (bound_) { switch_event_unbind(&node_); bound_ = false; }
        gateway_ = nullptr;
    }

    bool isConnected() const override { return bound_; }
    std::string name() const override { return name_; }

private:
    static void onEvent(switch_event_t* ev) {
        auto* self = static_cast<FreeSwitchChannel*>(ev->bind_user_data);
        if (!self) return;
        self->deliver(ev);
    }

    void deliver(switch_event_t* ev) {
        InboundGateway gw;
        { std::lock_guard<std::mutex> g(mx_); gw = gateway_; }
        if (!gw) return;
        auto e = std::make_shared<FsEvent>();
        e->eventName = switch_event_name(ev->event_id);
        for (switch_event_header_t* hp = ev->headers; hp; hp = hp->next) if (hp->name && hp->value) e->headers[hp->name] = hp->value;
        e->subclass = e->header("Event-Subclass");
        if (ev->body) e->body = ev->body;
        e->first = firstEvents_.count(e->eventName) > 0;
        std::string id = e->header(idHeader_);
        if (id.empty()) return;                     // not a per-channel event
        gw(id, e);                                   // non-blocking; failures travel through the ack
    }

    std::string name_;
    std::set<std::string> firstEvents_;
    std::string idHeader_;
    std::mutex mx_;
    InboundGateway gateway_;
    switch_event_node_t* node_ = nullptr;
    bool bound_ = false;
};

}  // namespace statewalk::fs
