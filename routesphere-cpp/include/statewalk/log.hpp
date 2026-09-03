// statewalk-cpp — pluggable logging.
//
// The library never writes to stdout/stderr on its own once a sink is
// installed. Inside FreeSWITCH, route this to switch_log_printf; in tests the
// default stderr sink is fine.
#pragma once

#include <atomic>
#include <cstdio>
#include <functional>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>

namespace statewalk {

enum class LogLevel : int { Debug = 0, Info = 1, Warn = 2, Error = 3 };

class Log {
public:
    using Sink = std::function<void(LogLevel, const std::string&)>;

    static void setSink(Sink sink) { std::lock_guard<std::mutex> g(mx()); sinkRef() = std::move(sink); }
    static void setLevel(LogLevel lvl) { level().store(static_cast<int>(lvl)); }
    static bool enabled(LogLevel lvl) { return static_cast<int>(lvl) >= level().load(); }

    static void write(LogLevel lvl, const std::string& msg) {
        if (!enabled(lvl)) return;
        Sink s;
        { std::lock_guard<std::mutex> g(mx()); s = sinkRef(); }
        if (s) { s(lvl, msg); return; }
        std::fprintf(stderr, "[%s] %s\n", levelName(lvl), msg.c_str());
    }

    static const char* levelName(LogLevel l) {
        switch (l) {
            case LogLevel::Debug: return "DEBUG";
            case LogLevel::Info:  return "INFO";
            case LogLevel::Warn:  return "WARN";
            default:              return "ERROR";
        }
    }

private:
    static std::atomic<int>& level() { static std::atomic<int> l{static_cast<int>(LogLevel::Info)}; return l; }
    static Sink& sinkRef() { static Sink s; return s; }
    static std::mutex& mx() { static std::mutex m; return m; }
};

// Tiny stream-based formatter: sw::fmt("a=", a, " b=", b)
namespace detail {
inline void fmtInto(std::ostringstream&) {}
template <class T, class... Rest>
inline void fmtInto(std::ostringstream& os, const T& t, const Rest&... rest) { os << t; fmtInto(os, rest...); }
}
template <class... Args>
inline std::string fmt(const Args&... args) { std::ostringstream os; detail::fmtInto(os, args...); return os.str(); }

}  // namespace statewalk

#define SW_LOG(lvl, ...) do { if (::statewalk::Log::enabled(lvl)) ::statewalk::Log::write(lvl, ::statewalk::fmt(__VA_ARGS__)); } while (0)
#define SW_DEBUG(...) SW_LOG(::statewalk::LogLevel::Debug, __VA_ARGS__)
#define SW_INFO(...)  SW_LOG(::statewalk::LogLevel::Info,  __VA_ARGS__)
#define SW_WARN(...)  SW_LOG(::statewalk::LogLevel::Warn,  __VA_ARGS__)
#define SW_ERROR(...) SW_LOG(::statewalk::LogLevel::Error, __VA_ARGS__)
