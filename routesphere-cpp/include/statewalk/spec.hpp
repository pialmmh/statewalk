// statewalk-cpp — declarative machine types: MachineSpec<C> / SupervisorSpec<C>.
// Builder-only. One Java-style "class" per type is never required — a spec is
// name + graph + codec (+ routes for supervisors).
#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include "machine.hpp"
#include "supervisor.hpp"

namespace statewalk {

template <class C>
class MachineSpec {
public:
    class Builder {
    public:
        Builder& name(std::string n) { s_.name_ = std::move(n); return *this; }
        Builder& stateMap(StateMap m) { s_.map_ = std::move(m); return *this; }
        Builder& codec(Codec<C> c) { s_.codec_ = std::move(c); return *this; }
        Builder& contextFactory(std::function<C()> f) { s_.ctxFactory_ = std::move(f); return *this; }
        MachineSpec<C> build() {
            if (s_.name_.empty()) throw std::invalid_argument("MachineSpec.name must be non-blank");
            if (!s_.map_) throw std::invalid_argument("MachineSpec.stateMap is required");
            return s_;
        }
    private:
        MachineSpec<C> s_;
    };
    static Builder builder() { return Builder(); }

    const std::string& name() const { return name_; }
    const StateMap& stateMap() const { return *map_; }
    const std::optional<Codec<C>>& codec() const { return codec_; }
    C makeContext() const { return ctxFactory_ ? ctxFactory_() : C{}; }

private:
    friend class Builder;
    std::string name_;
    std::optional<StateMap> map_;
    std::optional<Codec<C>> codec_;
    std::function<C()> ctxFactory_;
};

template <class C>
class SupervisorSpec {
public:
    class Builder {
    public:
        Builder& name(std::string n) { s_.name_ = std::move(n); return *this; }
        Builder& stateMap(StateMap m) { s_.map_ = std::move(m); return *this; }
        Builder& codec(Codec<C> c) { s_.codec_ = std::move(c); return *this; }
        Builder& contextFactory(std::function<C()> f) { s_.ctxFactory_ = std::move(f); return *this; }
        Builder& routes(std::function<void(InternalEventResolver&)> r) { s_.routes_ = std::move(r); return *this; }
        SupervisorSpec<C> build() {
            if (s_.name_.empty()) throw std::invalid_argument("SupervisorSpec.name must be non-blank");
            if (!s_.map_) throw std::invalid_argument("SupervisorSpec.stateMap is required");
            if (!s_.routes_) throw std::invalid_argument("SupervisorSpec.routes is required");
            return s_;
        }
    private:
        SupervisorSpec<C> s_;
    };
    static Builder builder() { return Builder(); }

    const std::string& name() const { return name_; }
    const StateMap& stateMap() const { return *map_; }
    const std::optional<Codec<C>>& codec() const { return codec_; }
    const std::function<void(InternalEventResolver&)>& routes() const { return routes_; }
    C makeContext() const { return ctxFactory_ ? ctxFactory_() : C{}; }

private:
    friend class Builder;
    std::string name_;
    std::optional<StateMap> map_;
    std::optional<Codec<C>> codec_;
    std::function<C()> ctxFactory_;
    std::function<void(InternalEventResolver&)> routes_;
};

/// The framework's generic child machine for a MachineSpec.
template <class C>
class SpecMachine final : public Machine<C> {
public:
    explicit SpecMachine(std::shared_ptr<const MachineSpec<C>> spec) : spec_(std::move(spec)) {
        if (spec_->codec()) this->setCodec(*spec_->codec());
    }
protected:
    StateMap defineStates() override { return spec_->stateMap(); }
    C createContext() override { return spec_->makeContext(); }
private:
    std::shared_ptr<const MachineSpec<C>> spec_;
};

/// The framework's generic supervisor for a SupervisorSpec.
template <class C>
class SpecSupervisor final : public Supervisor<C> {
public:
    explicit SpecSupervisor(std::shared_ptr<const SupervisorSpec<C>> spec)
        : Supervisor<C>(spec->routes()), spec_(std::move(spec)) {
        if (spec_->codec()) this->setCodec(*spec_->codec());
    }
protected:
    StateMap defineStates() override { return spec_->stateMap(); }
    C createContext() override { return spec_->makeContext(); }
private:
    std::shared_ptr<const SupervisorSpec<C>> spec_;
};

}  // namespace statewalk
