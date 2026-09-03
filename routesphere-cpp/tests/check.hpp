// Minimal test harness — no framework dependency.
#pragma once

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <string>
#include <thread>
#include <vector>

struct TestCase { const char* name; void (*fn)(); };
inline std::vector<TestCase>& allTests() { static std::vector<TestCase> t; return t; }
struct TestRegistrar { TestRegistrar(const char* n, void (*f)()) { allTests().push_back({n, f}); } };

#define TEST(name) static void name(); static TestRegistrar name##_registrar(#name, name); static void name()

#define CHECK(cond) do { if (!(cond)) { std::fprintf(stderr, "  CHECK FAILED %s:%d: %s\n", __FILE__, __LINE__, #cond); std::exit(1); } } while (0)
#define CHECK_MSG(cond, msg) do { if (!(cond)) { std::fprintf(stderr, "  CHECK FAILED %s:%d: %s — %s\n", __FILE__, __LINE__, #cond, std::string(msg).c_str()); std::exit(1); } } while (0)
#define CHECK_EQ(a, b) do { auto _a = (a); auto _b = (b); if (!(_a == _b)) { std::fprintf(stderr, "  CHECK_EQ FAILED %s:%d: %s == %s\n", __FILE__, __LINE__, #a, #b); std::exit(1); } } while (0)
#define CHECK_THROWS(expr) do { bool _t = false; try { expr; } catch (...) { _t = true; } if (!_t) { std::fprintf(stderr, "  CHECK_THROWS FAILED %s:%d: %s did not throw\n", __FILE__, __LINE__, #expr); std::exit(1); } } while (0)

inline void sleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

/// Poll until pred() or the budget expires; returns whether it became true.
inline bool awaitUntil(std::function<bool()> pred, int budgetMs) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budgetMs);
    while (std::chrono::steady_clock::now() < deadline) { if (pred()) return true; sleepMs(5); }
    return pred();
}

inline int runAllTests(const char* suite) {
    int n = 0;
    for (auto& t : allTests()) { std::fprintf(stderr, "[%s] %s\n", suite, t.name); t.fn(); n++; }
    std::fprintf(stderr, "[%s] %d test(s) passed\n", suite, n);
    return 0;
}
#define TEST_MAIN(suite) int main() { return runAllTests(suite); }
