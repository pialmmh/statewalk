// statewalk-cpp — wire I/O contract.
//
// The registry OWNS the channel lifecycle: start(gateway) at build, stop()
// first in shutdown. Inbound events carry an ack future that completes when
// the cell PROCESSED the event (or fails on reject) — an at-least-once
// consumer commits on it. Inside FreeSWITCH the adapter converts
// switch_event_t into Event subclasses and offers them here from the event
// thread; offer() never blocks beyond a brief mutex.
#pragma once

#include <functional>
#include <future>
#include <memory>
#include <string>

#include "event.hpp"

namespace statewalk {

using Ack = std::shared_future<void>;

/// The registry-side gateway: (requestId, event) -> ack.
using InboundGateway = std::function<Ack(const std::string& requestId, EventPtr event)>;

class ChannelBase {
public:
    virtual ~ChannelBase() = default;
    virtual void start(InboundGateway gateway) = 0;
    virtual void stop() = 0;
    virtual bool isConnected() const = 0;
    virtual std::string name() const = 0;
    virtual void cancel(const std::string& requestId) { (void)requestId; }
};

/// Typed outbound side. O = the command type (a FreeSWITCH API line, a SIP
/// message, a Kafka record…).
template <class O>
class Channel : public ChannelBase {
public:
    virtual void send(const std::string& requestId, const O& command) = 0;
};

/// In-memory channel for tests: records sends, injects inbound events and
/// hands back the ack so tests can synchronise on actual processing.
template <class O>
class TestChannel : public Channel<O> {
public:
    struct Sent { std::string requestId; O command; };

    explicit TestChannel(std::string name = "test") : name_(std::move(name)) {}

    void send(const std::string& id, const O& cmd) override { std::lock_guard<std::mutex> g(mx_); sends.push_back({id, cmd}); }
    void cancel(const std::string& id) override { std::lock_guard<std::mutex> g(mx_); cancels.push_back(id); }
    void start(InboundGateway gw) override { std::lock_guard<std::mutex> g(mx_); gateway_ = std::move(gw); started_ = true; }
    void stop() override { std::lock_guard<std::mutex> g(mx_); started_ = false; gateway_ = nullptr; }
    bool isConnected() const override { return connected_; }
    std::string name() const override { return name_; }
    bool isStarted() const { std::lock_guard<std::mutex> g(mx_); return started_; }

    /// Simulate an inbound event. Fails loudly (not silently) when the channel
    /// is not bound to a registry.
    Ack inject(const std::string& id, EventPtr ev) {
        InboundGateway gw;
        { std::lock_guard<std::mutex> g(mx_); gw = gateway_; }
        if (!gw) {
            std::promise<void> p;
            p.set_exception(std::make_exception_ptr(std::runtime_error("TestChannel '" + name_ + "' is not started")));
            return p.get_future().share();
        }
        return gw(id, std::move(ev));
    }

    std::vector<Sent> sends;
    std::vector<std::string> cancels;

private:
    std::string name_;
    mutable std::mutex mx_;
    InboundGateway gateway_;
    bool started_ = false;
    bool connected_ = true;
};

}  // namespace statewalk
