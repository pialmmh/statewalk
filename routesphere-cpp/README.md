# statewalk-cpp (routesphere-cpp)

The C++20 port of statewalk v3 — the same registry-driven state machine framework, built to
run **inside the FreeSWITCH process** (a `mod_*` module) with no ESL hop: FreeSWITCH events
arrive on FreeSWITCH's own threads and are offered to the registry non-blocking; commands go
straight to `switch_api_execute`.

Header-only, `std::thread`-based, no third-party dependencies (SQLite provider optional).

```
routesphere-cpp/
├─ include/statewalk/
│  ├─ registry.hpp        StatemachineRegistry<T> (builder-only) — the runtime
│  ├─ machine.hpp         Machine<C>, RegistryHandle, Codec<C>
│  ├─ supervisor.hpp      Supervisor<C>, InternalEventResolver, RegistryBase
│  ├─ spec.hpp            MachineSpec<C> / SupervisorSpec<C> builders
│  ├─ state_map.hpp       the StateMap DSL (+ build-time invariants)
│  ├─ event.hpp           Event, TimeoutEvent, makeEvent<E>()
│  ├─ executor.hpp        StrandPool (per-key FIFO workers), TimerService
│  ├─ pool.hpp            ObjectPool (containment-guarded, reset hooks)
│  ├─ quota.hpp           QuotaKeys/Limits, RejectCause, QuotaController
│  ├─ persistence.hpp     Snapshot, PersistenceProvider, InMemoryPersistenceProvider
│  ├─ persistence_sqlite.hpp   SqlitePersistenceProvider (libsqlite3)
│  ├─ channel.hpp         Channel<O>, InboundGateway, Ack, TestChannel
│  ├─ session/session.hpp SessionSupervisor<C>, SessionContext/History, SdrRecord…
│  └─ fs/freeswitch_channel.hpp   in-process FreeSWITCH adapter (switch.h)
├─ tests/                 ctest suites (no framework dependency)
└─ examples/call_fsm/     a FreeSWITCH-shaped call supervisor
```

## Build

```bash
cd routesphere-cpp
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/call_fsm_example

# compile-check the FreeSWITCH adapter against your installed headers:
cmake -B build -DSTATEWALK_WITH_FREESWITCH=ON -DFREESWITCH_INCLUDE_DIR=/usr/local/freeswitch/include/freeswitch
```

## Same model as the Java library

| Concept | Java (`com.telcobright:statewalk`) | C++ |
|---|---|---|
| Registry | `StatemachineRegistry<T>` | `StatemachineRegistry<T>` (`shared_ptr` from `builder(name).build()`) |
| Events | `record X implements StatemachineEvent` | `struct X : Event`; passed as `EventPtr` via `makeEvent<X>(...)` |
| Graph DSL | `StateMap.builder().state("A").interim().timeout(d,u,"B").on(E.class, "B", guard, action)` | `StateMap::builder().state("A").interim().timeout(90s, "B").on<E>("B", guard, action)` |
| Stay-mode timeout | `.timeoutStay(d, u, action)` | `.timeoutStay(15s, action)` |
| Hibernation | `.offline()` | `.offline()` |
| Context persistence | Jackson JSON automatically | `Codec<C>{typeName, encode, decode}` on the spec — format is yours |
| Volatile context | `.volatileLoader(type, m -> obj)` | `.volatileLoader(type, [](MachineBase&) -> std::any)` |
| Reset hook | `.resetHook(type, m -> …)` | `.resetHook(type, [](MachineBase&) {...})` |
| Wire | `Channel<O,I>` (start/stop, acked `submitInbound`) | `Channel<O>` / `ChannelBase`, `Ack = std::shared_future<void>` |
| Session base | `SessionSupervisor<C>` | `session::SessionSupervisor<C>` |
| Persistence | JDBC / Redis / in-memory | in-memory / SQLite (bundled) / your `PersistenceProvider` |

Behavioural guarantees carried over verbatim: per-cell serial strands, epoch + visit identity
tokens, atomic cell claims, atomic dispatch / single-flight restore, forced failover on
shutdown (records ship on every exit path), async persistence on a dedicated strand pool (the
hot path never touches the store), final-state tombstones, quarantine instead of destroy,
hibernated rows stay db-only at startup, persisted global deadline, exact quota with
acquire-before-release rebind, entry-point backpressure.

Differences to know: contexts are held **by value** inside the machine (`ctx<C>(m)` in
actions) and must be default-constructible + copyable; there is no reflective pooled-field
validator (C++ has no reflection) — keep per-request state in the context or register a
`resetHook`; the supervisor's routes are materialised lazily (C++ forbids virtual dispatch
from a constructor).

## Embedding in FreeSWITCH (no ESL)

```cpp
auto ch  = std::make_shared<statewalk::fs::FreeSwitchChannel>("routesphere", std::set<std::string>{"CHANNEL_PARK"});
auto reg = statewalk::StatemachineRegistry<CallCtx>::builder("call")
    .supervisor(callSpec, 4096)
    .channel(ch)                                   // registry binds/unbinds switch_event_bind
    .createFromFirstEvent(parkToCallCtx)           // CHANNEL_PARK → CallCtx
    .persistence(std::make_shared<SqlitePersistenceProvider>("/var/lib/freeswitch/statewalk.db"))
    .rehydrate(true)
    .build();
// state actions: ch->send(uuid, "uuid_answer " + uuid);  ch->cancel(uuid);  // uuid_kill
```

Events arrive as `fs::FsEvent` (name, subclass, all headers, body; `uuid()` helper). Route on
`FsEvent` and branch on `eventName` in a stay handler, or map names to your own typed events
in a thin decoder. Build the module with `-I<freeswitch>/include/freeswitch` and link the
module normally; the registry's worker/timer threads are ordinary `std::thread`s.

The full conceptual guide is the Java library's `GUIDE.md` in the repo root — the semantics
are identical; this README covers only the C++ surface.
