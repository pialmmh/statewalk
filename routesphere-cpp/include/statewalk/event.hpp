// statewalk-cpp — the event vocabulary.
//
// Events are plain C++ types deriving from Event; dispatch is keyed on the
// DYNAMIC type (std::type_index), the C++ analogue of Java's Class-keyed
// tables. Events travel as shared_ptr<const Event> because they are queued on
// per-cell serial chains and may outlive the caller's frame.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <utility>

#if defined(__GNUG__)
#include <cxxabi.h>
#include <cstdlib>
#endif

namespace statewalk {

struct Event {
    virtual ~Event() = default;

    /// True for creation events (CHANNEL_PARK, SubmitSm…): an unknown id
    /// with a first event may auto-create the request (createFromFirstEvent).
    virtual bool isFirst() const { return false; }

    /// Human-readable type name for logs.
    std::string name() const { return demangle(typeid(*this).name()); }

    std::type_index type() const { return std::type_index(typeid(*this)); }

    static std::string demangle(const char* mangled) {
#if defined(__GNUG__)
        int status = 0;
        char* d = abi::__cxa_demangle(mangled, nullptr, nullptr, &status);
        if (status == 0 && d) { std::string s(d); std::free(d); auto p = s.rfind("::"); return p == std::string::npos ? s : s.substr(p + 2); }
#endif
        return mangled;
    }
};

using EventPtr = std::shared_ptr<const Event>;

/// Convenience: wrap any Event subclass value into an EventPtr.
template <class E, class... Args>
inline EventPtr makeEvent(Args&&... args) { return std::make_shared<const E>(std::forward<Args>(args)...); }

/// Fired into a machine when a state's timeout matures. target is empty for
/// stay-mode timeouts.
struct TimeoutEvent : Event {
    std::string fromState;
    std::optional<std::string> targetState;
    TimeoutEvent(std::string from, std::optional<std::string> target)
        : fromState(std::move(from)), targetState(std::move(target)) {}
};

}  // namespace statewalk
