// statewalk-cpp — Supervisor + InternalEventResolver.
//
// One supervisor per request; children are reachable ONLY through the
// supervisor's resolver (spawn / cleanup / forward). Every event — wire or
// child-published — enters at handleInbound and the resolver decides:
// self / forward-one / forward-many / drop.
#pragma once

#include <any>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <vector>

#include "machine.hpp"

namespace statewalk {

/// Non-template face of the registry the resolver talks to.
class RegistryBase {
public:
    virtual ~RegistryBase() = default;
    virtual const std::string& name() const = 0;
    virtual void spawnChildInternal(const std::string& parentId, const std::string& childType, std::any initialCtx) = 0;
    virtual void cleanupChildInternal(const std::string& parentId, const std::string& childType) = 0;
    virtual void cleanupAllChildrenInternal(const std::string& parentId) = 0;
    virtual void forwardToChild(const std::string& parentId, const std::string& childType, EventPtr ev) = 0;
};

class InternalEventResolver;

/// Non-template face of a supervisor (the registry holds MachineBase*).
class SupervisorCore {
public:
    virtual ~SupervisorCore() = default;
    virtual void handleInbound(const EventPtr& ev) = 0;
    virtual InternalEventResolver& resolver() = 0;
};

class InternalEventResolver {
public:
    enum class Kind { Self, ForwardOne, ForwardMany, Drop };
    struct Rule { Kind kind; std::vector<std::string> targets; };

    explicit InternalEventResolver(MachineBase& owner) : owner_(owner) {}

    template <class E> void selfHandle() { rules_[std::type_index(typeid(E))] = Rule{Kind::Self, {}}; }
    template <class E> void forwardTo(std::string child) { rules_[std::type_index(typeid(E))] = Rule{Kind::ForwardOne, {std::move(child)}}; }
    template <class E> void forwardToAll(std::vector<std::string> children) { rules_[std::type_index(typeid(E))] = Rule{Kind::ForwardMany, std::move(children)}; }
    template <class E> void drop() { rules_[std::type_index(typeid(E))] = Rule{Kind::Drop, {}}; }

    /// Spawn a child with an initial context (any type the child's Machine<C> accepts).
    template <class CC>
    void spawnChild(const std::string& childType, CC initialCtx) {
        registry().spawnChildInternal(parentId(), childType, std::any(std::move(initialCtx)));
    }
    void spawnChild(const std::string& childType) { registry().spawnChildInternal(parentId(), childType, std::any{}); }
    void cleanupChild(const std::string& childType) { registry().cleanupChildInternal(parentId(), childType); }
    /// Retire every live child — the retry contract (respawn may follow immediately).
    void cleanupChildren() { registry().cleanupAllChildrenInternal(parentId()); }

    void route(const EventPtr& ev) {
        auto it = rules_.find(ev->type());
        if (it == rules_.end()) {
            SW_WARN("[", owner_.machineId(), "] unrouted event ", ev->name(), " — drop (declare selfHandle/forwardTo/drop)");
            return;
        }
        switch (it->second.kind) {
            case Kind::Self: owner_.fire(*ev); break;
            case Kind::ForwardOne: registry().forwardToChild(parentId(), it->second.targets.front(), ev); break;
            case Kind::ForwardMany: for (auto& t : it->second.targets) registry().forwardToChild(parentId(), t, ev); break;
            case Kind::Drop: break;
        }
    }

    std::size_t ruleCount() const { return rules_.size(); }
    std::set<std::string> referencedChildNames() const {
        std::set<std::string> out;
        for (auto& kv : rules_) if (kv.second.kind != Kind::Self && kv.second.kind != Kind::Drop) out.insert(kv.second.targets.begin(), kv.second.targets.end());
        return out;
    }

private:
    RegistryBase& registry() {
        auto* h = owner_.handle();
        if (!h) throw std::logic_error("Supervisor " + owner_.machineId() + " is not bound to a registry");
        return h->registry();
    }
    const std::string& parentId() { return owner_.handle()->parentId(); }

    MachineBase& owner_;
    std::unordered_map<std::type_index, Rule> rules_;
};

/// Supervisor<C>: a Machine<C> with a resolver. Subclasses override
/// defineRoutes(); spec-backed supervisors pass a routes lambda. Routes are
/// materialised lazily (C++ forbids virtual dispatch from the constructor).
template <class C>
class Supervisor : public Machine<C>, public SupervisorCore {
public:
    Supervisor() : resolver_(*this) {}
    explicit Supervisor(std::function<void(InternalEventResolver&)> routes) : resolver_(*this), routesFn_(std::move(routes)) {}

    void handleInbound(const EventPtr& ev) override { resolver().route(ev); }

    InternalEventResolver& resolver() override {
        std::call_once(routesOnce_, [this] {
            if (routesFn_) routesFn_(resolver_);
            else defineRoutes(resolver_);
        });
        return resolver_;
    }

protected:
    /// Subclass hook: declare routes (event classes + child NAMES only).
    virtual void defineRoutes(InternalEventResolver& r) { (void)r; }

private:
    InternalEventResolver resolver_;
    std::function<void(InternalEventResolver&)> routesFn_;
    std::once_flag routesOnce_;
};

}  // namespace statewalk
