# mod_routesphere — building the in-process FreeSWITCH module on statewalk-cpp

**Audience:** the agent working in the FreeSWITCH source tree.
**Goal:** a FreeSWITCH module (`mod_routesphere`) that short-circuits incoming calls straight
into our code — no ESL socket — and runs the SAME call-processing logic the Java ESL version
runs today (routesphere-core on statewalk v2.0), on the C++ port of statewalk v3.

Everything referenced below exists on this machine today (2026-09-04).

---

## 0. TL;DR

```
                     FreeSWITCH process (one binary, no ESL)
 ┌───────────────────────────────────────────────────────────────────────────┐
 │  sofia (SIP)  ──INVITE──►  dialplan  ──<action application="routesphere"/>──┐
 │                                                                            ▼
 │   mod_routesphere.cpp                                                       │
 │     app fn  ── builds CallCtx from channel vars ──► registry.dispatch(uuid) │
 │              ── switch_ivr_park(session)  (channel now belongs to the FSM)  │
 │     FreeSwitchChannel  ◄─ switch_event_bind ── CHANNEL_PROGRESS/ANSWER/HANGUP…
 │                        ── switch_api_execute("uuid_transfer"/"uuid_answer"/"uuid_kill"…)
 │     StatemachineRegistry<CallCtx>   INIT → TRYING → RINGING → ANSWERED → COMPLETED/FAILED
 │            (worker + timer threads are std::thread, owned by the module)    │
 └───────────────────────────────────────────────────────────────────────────┘
```

1. Build the library first and make its tests pass (`routesphere-cpp/`, section 1).
2. Write `mod_routesphere` as a C++20 module: load → build registry + channel; app function
   → dispatch + park; shutdown → `registry->shutdown()` (section 3).
3. Port the Java `EslCallMachineFactory` graph state-for-state onto the statewalk DSL
   (section 4 has the exact mapping table — do not invent a new flow).
4. Keep admission/billing/CDR **behaviour** identical; where that logic lives (C++ vs. calling the
   Java service) is the one architectural decision to confirm with Mustafa (section 6).

---

## 1. What you are given

| Thing | Path |
|---|---|
| The C++ library (header-only, C++20) | `/home/mustafa/telcobright-projects/statewalk/routesphere-cpp/` |
| Its README (Java↔C++ mapping, embedding notes) | `routesphere-cpp/README.md` |
| The in-process FreeSWITCH adapter | `routesphere-cpp/include/statewalk/fs/freeswitch_channel.hpp` |
| A FreeSWITCH-shaped example (park → answer → hangup → CDR line) | `routesphere-cpp/examples/call_fsm/main.cpp` |
| Tests = living API examples (36 cases) | `routesphere-cpp/tests/*.cpp` (`fixtures.hpp` shows contexts + codecs) |
| The conceptual manual (semantics are identical to the Java lib) | `/home/mustafa/telcobright-projects/statewalk/GUIDE.md` |
| **The Java v2 logic you must mirror** | `/home/mustafa/telcobright-projects/routesphere/routesphere-core/src/main/java/com/telcobright/routesphere/pipeline/call/` |
| FreeSWITCH headers on this box | `/usr/local/freeswitch/include/freeswitch/switch.h` |

Build + verify the library (must be green before you touch the module):

```bash
cd /home/mustafa/telcobright-projects/statewalk/routesphere-cpp
cmake -B build -DSTATEWALK_WITH_FREESWITCH=ON -DFREESWITCH_INCLUDE_DIR=/usr/local/freeswitch/include/freeswitch
cmake --build build -j
ctest --test-dir build --output-on-failure      # 7 suites, all pass today
./build/call_fsm_example                         # prints a CDR line, exit 0
```

Requirements: g++ ≥ 13 (C++20), CMake ≥ 3.20, optional libsqlite3-dev. No other deps.

---

## 2. The Java v2 flow you are porting (read these files first)

All under `routesphere-core/.../pipeline/call/`:

| File | What it is | What you take from it |
|---|---|---|
| `statemachines/EslCallMachineFactory.java` | **THE state machine** (old fluent builder, statewalk v2.0 era) | states, timeouts, every entry action, DB update policy |
| `converter/EslEventConverter.java` | ESL event → call event mapping | the CHANNEL_* → event table (section 4.2) |
| `statemachines/source/EslCallEventSource.java` + `ICallEventSource.java` | the ESL command surface | the exact `originate` / `uuid_transfer` / `uuid_setvar` / `uuid_answer` / `hangup` strings (section 4.3) |
| `statemachines/EslCallRegistry.java` | wiring: CHANNEL_PARK → new machine, tenant/level context, CDR on terminal | dispatch-time context population, CDR trigger |
| `admission/CallAdmissionController.java`, `admission/CallAdmissionContext.java` | multi-level partner identification + balance reservation | the admission algorithm (INIT entry action) |
| `cdr/CdrGenerator.java`, `cdr/CallDetailRecord.java`, `cdr/CdrCsvWriter.java` | CDR production | the record fields + CSV format |
| `context/CallVolatileContext.java` | non-persisted per-call service handles | becomes the statewalk **volatile context** |

The Java machine, in one picture (this is what `EslCallMachineFactory.createMachineTemplate()` builds):

```
IDLE ─dispatch─► INIT ─(admission ok)─► TRYING ─CHANNEL_PROGRESS_MEDIA─► RINGING ─CHANNEL_ANSWER─► ANSWERED ─HANGUP_COMPLETE─► COMPLETED
                   │ reject                │ 30s / hangup                  │ 60s / hangup            │ max-duration / hangup(early)
                   ▼                       ▼                               ▼                         ▼
                 FAILED                  FAILED                          FAILED                    COMPLETED
```

| State | Java timeout | Entry action (Java) |
|---|---|---|
| INIT | — (synchronous) | multi-level admission over `tenant.getAncestorChain()`: per level `CallAdmissionController.processLevel(...)` (partner by IP/domain/SIP user at entry level, by child dbName at parent levels), build `LevelBillingContext`, reserve balance via `levelReservationCallback`; any hard reject → FAILED with `errorReason` |
| TRYING | 30 s → FAILED | best-effort DB state 11; then EITHER `uuid_transfer <uuid> <dest> XML <dialplanContext>` (parked SIP call — `determineDialplanContext`, `uuid_setvar effective_caller_id_*` first) OR `originate {origination_uuid=…}sofia/gateway/<gw>/<dest> &park` (outbound) OR `originate … &playback(file)` (broadcast) |
| RINGING | 60 s → FAILED | best-effort DB state 10 |
| ANSWERED | `answeredTimeoutSeconds` (default 3600) → hangup | `task.markAnswered()`, **direct** DB state 1 / "answered"; periodic balance re-reservation at 58 s then every 60 s (`periodicReservationCallback`) |
| COMPLETED (final) | — | cancel reservation timer, direct DB state 1 / hangup cause, `releaseBalance()`, cleanup, CDR |
| FAILED (final) | — | direct DB state 5 / cause-or-errorReason, `releaseBalance()`, `hangup(uuid)` if still up, cleanup, CDR |

---

## 3. The module skeleton

Create `src/mod/applications/mod_routesphere/` in the FreeSWITCH tree. C++ modules are normal
(mod_v8 was one); the module source is C++20, the FreeSWITCH API is C.

### 3.1 `mod_routesphere.cpp` — the shape

```cpp
#include <switch.h>
#include "statewalk/registry.hpp"
#include "statewalk/fs/freeswitch_channel.hpp"
// #include "statewalk/persistence_sqlite.hpp"   // only if you enable persistence (see 5.4)

using namespace statewalk;

struct CallCtx { /* section 4.1 */ };

static std::shared_ptr<fs::FreeSwitchChannel>          g_channel;
static std::shared_ptr<StatemachineRegistry<CallCtx>>  g_registry;

SWITCH_MODULE_LOAD_FUNCTION(mod_routesphere_load);
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_routesphere_shutdown);
SWITCH_MODULE_DEFINITION(mod_routesphere, mod_routesphere_load, mod_routesphere_shutdown, NULL);

// ── the dialplan application: <action application="routesphere" data="..."/> ──
SWITCH_STANDARD_APP(routesphere_app_function) {
    try {
        switch_channel_t* ch = switch_core_session_get_channel(session);
        const std::string uuid = switch_core_session_get_uuid(session);

        CallCtx ctx;                                    // what EslCallRegistry built from CHANNEL_PARK
        ctx.uuid          = uuid;
        ctx.callingNumber = sv(switch_channel_get_variable(ch, "caller_id_number"));
        ctx.calledNumber  = sv(switch_channel_get_variable(ch, "destination_number"));
        ctx.sipFromIp     = sv(switch_channel_get_variable(ch, "sip_network_ip"));   // partner-by-IP
        ctx.sipUser       = sv(switch_channel_get_variable(ch, "sip_auth_username"));
        ctx.sofiaProfile  = sv(switch_channel_get_variable(ch, "sofia_profile_name"));
        ctx.context       = sv(switch_channel_get_variable(ch, "context"));

        auto r = g_registry->dispatch(uuid, ctx);       // INIT runs on a registry worker, not here
        if (!r.accepted) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                              "routesphere: dispatch rejected %s\n", toString(*r.rejectCause));
            switch_channel_hangup(ch, SWITCH_CAUSE_CALL_REJECTED);
            return;
        }
        switch_ivr_park(session, NULL);                 // the channel now belongs to the FSM;
                                                        // TRYING's uuid_transfer moves it on
    } catch (const std::exception& e) {                 // NEVER let a C++ exception reach C
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "routesphere app: %s\n", e.what());
    }
}

SWITCH_MODULE_LOAD_FUNCTION(mod_routesphere_load) {
    Log::setSink([](LogLevel l, const std::string& m) {
        switch_log_printf(SWITCH_CHANNEL_LOG, l == LogLevel::Error ? SWITCH_LOG_ERROR : l == LogLevel::Warn ? SWITCH_LOG_WARNING : l == LogLevel::Info ? SWITCH_LOG_INFO : SWITCH_LOG_DEBUG, "%s\n", m.c_str());
    });
    g_channel  = std::make_shared<fs::FreeSwitchChannel>("routesphere", std::set<std::string>{});   // no first events: the APP dispatches
    g_registry = StatemachineRegistry<CallCtx>::builder("call")
        .supervisor(callSpec(), 4096)                   // pool size = expected concurrent calls
        .channel(g_channel)                             // registry binds switch_event_bind now, unbinds in shutdown
        .volatileLoader("CallSupervisor", [](MachineBase&) { return std::any(g_services); })
        .globalTimeout(std::chrono::seconds(cfg.answeredTimeoutSec + 120), "COMPLETED")           // max call life (5.2)
        .threads(4)
        .build();

    *module_interface = switch_loadable_module_create_module_interface(pool, modname);
    switch_application_interface_t* app;
    SWITCH_ADD_APP(app, "routesphere", "RouteSphere call FSM", "Hands the call to mod_routesphere", routesphere_app_function, "", SAF_NONE);
    return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_routesphere_shutdown) {
    if (g_registry) g_registry->shutdown();             // stops the channel FIRST (unbind), drives live calls
    g_registry.reset(); g_channel.reset();              // through their failover state (CDR on every path), joins threads
    return SWITCH_STATUS_SUCCESS;
}
```

Dialplan entry: route the inbound context to the app instead of the old `park` + ESL pickup:

```xml
<extension name="routesphere">
  <condition field="destination_number" expression="^(.*)$">
    <action application="routesphere"/>
  </condition>
</extension>
```

Why the app + park instead of catching `CHANNEL_PARK` in the event bind: the app runs on the
session's own thread with the channel in hand (variables are readable, dispatch failures can
hang up cleanly), and it removes the ESL-era race where the machine did not exist yet when the
first events arrived. The event path remains available (`FreeSwitchChannel` accepts a set of
"first event" names) if you ever need pure-event entry.

### 3.2 `Makefile.am`

```makefile
include $(top_srcdir)/build/modmake.rulesam
MODNAME=mod_routesphere
mod_LTLIBRARIES = mod_routesphere.la
mod_routesphere_la_SOURCES  = mod_routesphere.cpp
mod_routesphere_la_CXXFLAGS = $(AM_CXXFLAGS) -std=c++20 -I/home/mustafa/telcobright-projects/statewalk/routesphere-cpp/include
mod_routesphere_la_LDFLAGS  = -avoid-version -module -no-undefined -shared
mod_routesphere_la_LIBADD   = $(switch_builddir)/libfreeswitch.la -lpthread   # add -lsqlite3 if persistence is on
```

Add `applications/mod_routesphere` to `modules.conf` and load it via `modules.conf.xml`.
Vendor the `routesphere-cpp/include` tree into the FreeSWITCH source (or a submodule) rather
than an absolute path once it works.

---

## 4. Porting the state machine — the exact mapping

### 4.1 Context (persisted by value; `Codec<CallCtx>` only needed if persistence is on)

Mirror `CallAdmissionContext` + the parts of `CampaignTask` the FSM reads/writes:

```cpp
struct LevelBilling { std::string dbName, partnerName; int levelIndex = 0; long partnerId = 0, accountId = 0; double rate = 0, reservedAmount = 0; };
struct CallCtx {
    std::string uuid, callingNumber, calledNumber, effectiveCallerId, sipFromIp, sipUser, sofiaProfile, context;
    std::string dialplanContext, destinationNumber, matchedPrefix;       // routing result (TRYING)
    std::vector<LevelBilling> levels;                                     // multi-level admission
    std::string errorReason, hangupCause;
    std::int64_t startedAtMs = 0, answeredAtMs = 0, endedAtMs = 0; int billsec = 0;
    bool useTransfer = true;                                              // inbound SIP: uuid_transfer path
};
```

### 4.2 Events — the `EslEventConverter` table, now from `switch_event_bind`

`fs::FreeSwitchChannel` hands you an `fs::FsEvent` (event name + all headers + body). Route by
name in a thin decoder to typed events, exactly as the converter did:

| FreeSWITCH event | Java event | C++ event (define these) | Notes |
|---|---|---|---|
| `CHANNEL_PARK` / `CHANNEL_CREATE` | INVITE / `ChannelParkEvent` | *(not needed — the app dispatches)* | keep only if you choose event-entry |
| `CHANNEL_PROGRESS` | TRYING | `Progress` | early media not yet |
| `CHANNEL_PROGRESS_MEDIA` | RINGING / `RingingEvent` | `Ringing` | → RINGING |
| `CHANNEL_ANSWER` | ANSWER / `AnsweredEvent(Answer-State)` | `Answered{answerState}` | → ANSWERED |
| `CHANNEL_HANGUP_COMPLETE` | HANGUP / `HangupEvent(Hangup-Cause, sip code, billsec…)` | `Hangup{cause, sipCode, billsec}` | **only COMPLETE**, ignore plain `CHANNEL_HANGUP` (duplicate CDR guard, same as Java) |
| `DTMF` | DTMF | `Dtmf{digit}` | stay-handler in ANSWERED if needed |

Wire it with the decoder overload so the registry receives typed events:

```cpp
// in load(): instead of .channel(g_channel) use a decoder that maps FsEvent → typed event:
// (the C++ builder takes Channel<O> directly; do the mapping INSIDE your own subclass of
//  FreeSwitchChannel::deliver or wrap the gateway — see freeswitch_channel.hpp, it is ~40 lines)
```

Request id = the `Unique-ID` header (`FsEvent::uuid()`), which is also the machine id you
dispatched — the same key the Java `Channel-Call-UUID` mapping used.

### 4.3 Commands — `ICallEventSource` → `switch_api_execute`

Every ESL command the Java source sent is a FreeSWITCH API; the channel's `send(uuid, "api args")`
executes it in-process (`FreeSwitchChannel::send`). Keep the strings identical:

| Java (`EslCallEventSource`) | C++ `g_channel->send(uuid, …)` |
|---|---|
| `uuid_transfer <uuid> <dest> XML <ctx>` | `"uuid_transfer " + uuid + " " + dest + " XML " + dialplanContext` |
| `uuid_setvar <uuid> effective_caller_id_number <n>` (sync, before transfer) | same string, twice (name + number) — `switch_api_execute` is synchronous, so ordering is preserved |
| `originate {origination_uuid=<uuid>,origination_caller_id_number=<n>}sofia/gateway/<gw>/<dest> &park` | same string (`"originate {…}… &park"`) |
| `originate {…} … &playback(<file>)` (broadcast) | same |
| `uuid_answer <uuid>` | `"uuid_answer " + uuid` |
| `hangup` with cause | `"uuid_kill " + uuid + " " + cause` (channel `cancel()` = `uuid_kill`) |

Because we are in-process you MAY use `switch_core_session_locate(uuid)` + `switch_ivr_*` from a
state action instead of an API string — but you must `switch_core_session_rwunlock` on every
path and never block (no `switch_ivr_play_file` from an action). API strings are the safe
default and give 1:1 parity with the ESL version; start there.

### 4.4 The graph in the statewalk DSL

```cpp
using namespace std::chrono_literals;
static SupervisorSpec<CallCtx> callSpec() {
    return SupervisorSpec<CallCtx>::builder().name("CallSupervisor")
        .stateMap(StateMap::builder()
            .initialState("INIT")
            .state("INIT").interim().timeout(10s, "FAILED")            // Java had no timer here; statewalk requires one
                .onEntry(runAdmission)                                 // section 4.5
            .state("TRYING").interim().timeout(30s, "FAILED")
                .onEntry(sendTransferOrOriginate)
                .on<Ringing>("RINGING")
                .on<Answered>("ANSWERED")                             // answer without ringing is legal
                .on<Hangup>("FAILED", nullptr, copyHangup)
            .state("RINGING").interim().timeout(60s, "FAILED")
                .on<Answered>("ANSWERED")
                .on<Hangup>("FAILED", nullptr, copyHangup)
            .state("ANSWERED").interim()
                .timeoutStay(60s, periodicReserve)                    // Java: 58s then every 60s — see 5.2
                .onEntry(markAnswered)
                .on<Hangup>("COMPLETED", nullptr, copyHangup)
            .state("COMPLETED").finalState().timeout(1s, "COMPLETED").onEntry(completeCall)
            .state("FAILED").finalState().timeout(1s, "FAILED").onEntry(failCall)
            .build())
        .routes([](InternalEventResolver& r) { r.selfHandle<Ringing>(); r.selfHandle<Answered>(); r.selfHandle<Hangup>(); r.selfHandle<Progress>(); r.drop<Dtmf>(); })
        .build();
}
```

Rules that differ from the old fluent builder (the DSL enforces them at build time — you will get
an exception, not a silent difference):

- every state declares `.interim()` or `.finalState()` and exactly one timeout;
- a `.timeout(d, target)` target MUST be a final state — that is why INIT/TRYING/RINGING point at
  FAILED and ANSWERED uses a **stay-mode** timeout (its max-duration cap is the registry
  `globalTimeout`, see 5.2);
- guards are pure; put payload copying (hangup cause, answer state) in the **action** argument;
- no `machine.transitionTo("FAILED")` from the middle of an entry action to "return early" —
  it works (transitionTo is reentrant) but set `ctx.errorReason` first, and return right after.

### 4.5 Entry actions — where the Java logic goes

| Java entry action | C++ | Notes |
|---|---|---|
| INIT admission loop | `runAdmission(MachineBase& m)`: `auto& c = ctx<CallCtx>(m); auto& svc = std::any_cast<Services&>(m.volatileContext());` → per-level partner identification + reservation → fill `c.levels` or `c.errorReason` + `m.transitionTo("FAILED")` | see section 6 for WHERE the partner/balance services run |
| TRYING command | `sendTransferOrOriginate`: routing (`determineDialplanContext` equivalent) → `svc.channel->send(uuid, "uuid_setvar …")` ×2 → `send(uuid, "uuid_transfer …")` | best-effort DB update state 11 goes to the async DB writer (section 5.3) |
| ANSWERED | `markAnswered`: `c.answeredAtMs = nowMs()`; direct DB state 1 "answered" (async writer, but flagged critical = flushed immediately) | |
| periodic reservation | `periodicReserve` (the stay action): call the reservation service; on insufficient balance → `m.transitionTo("COMPLETED")` after `send(uuid, "uuid_kill " + uuid + " NORMAL_CLEARING")` | Java hung up on failed re-reservation |
| COMPLETED / FAILED | `completeCall` / `failCall`: DB direct update, release reservations, CDR record, and on FAILED `send(uuid, "uuid_kill …")` if the channel may still be up | CDR = the terminal record; emit it from BOTH terminals (it is the SDR of this domain) |

Services (`Services` struct: channel, config, admission client, DB writer, CDR writer) are
attached as **volatile context** — `.volatileLoader("CallSupervisor", …)` — exactly the role
`CallVolatileContext` plays in Java. Never store them in the persisted context.

---

## 5. Framework rules the module must respect

### 5.1 Threads
- FreeSWITCH event thread → `FreeSwitchChannel::deliver` → `registry.submitInbound` — **queues and
  returns**; never blocks. Do not do work in the event callback.
- State actions run on the registry's worker threads (`.threads(n)`), NOT on a session thread.
  `switch_api_execute` is thread-safe. `switch_core_session_locate` is allowed (returns a
  read-locked session — `switch_core_session_rwunlock` on every path, keep the lock scope tiny).
- One call = one serial strand: your actions for a given uuid never run concurrently, in event
  order. No locks needed inside the FSM.
- Registry threads are plain `std::thread`s created in load and joined in `shutdown()`; call
  `g_registry->shutdown()` from the module shutdown function before anything else is torn down.

### 5.2 Timeouts
- TRYING 30 s, RINGING 60 s → FAILED (as Java). INIT gets a short safety timeout (10 s).
- ANSWERED: Java armed a max-duration timer AND a 58 s/60 s reservation timer. A statewalk
  state has ONE timeout, so: reservation = `.timeoutStay(60s, periodicReserve)`; max duration =
  the registry-level `.globalTimeout(answeredTimeoutSec + 120s, "COMPLETED")` (counts from
  dispatch; the +120 s covers TRYING+RINGING). If you need the first tick at exactly 58 s, fire
  the first reservation from `markAnswered` with a `handle`-scheduled timer.
- `Hangup` while TRYING/RINGING → FAILED (Java: early hangup = failed attempt); while ANSWERED →
  COMPLETED.

### 5.3 Side effects: DB updates and CDR
- Java did best-effort batched upserts (TRYING/RINGING, 1 s / 500 rows) and direct upserts for
  ANSWERED/COMPLETED/FAILED (`campaign_task`, or `dynamicTableName`). Keep the SAME SQL and
  policy; run it on your own `StrandPool("db", 2)` (from `statewalk/executor.hpp`) so no DB
  round-trip ever runs on a registry worker. libmysqlclient headers are NOT installed on this
  box — either install them for the module build, or delegate writes (section 6).
- CDR: reproduce `CdrGenerator` → `CallDetailRecord` → `CdrCsvWriter` output byte-for-byte; the
  billing pipeline downstream depends on the CSV shape. Write from the terminal entry action via
  the same async writer.

### 5.4 Persistence — probably OFF for calls in v1
Live calls die with the FreeSWITCH process, so crash-rehydration of *call* machines buys little
(the channel is gone). Start with no `.persistence(...)`. If you want durable in-flight state
(e.g. to write CDRs for calls that were up when FreeSWITCH died), enable
`SqlitePersistenceProvider` + `.rehydrate(true)` and give `CallCtx` a `Codec<CallCtx>`
(`fixtures.hpp` shows a 15-line pipe-delimited codec; JSON is fine too). Never use `.offline()`
for calls.

### 5.5 Pool + contexts
- `supervisor(spec, poolSize)`: poolSize ≈ expected concurrent calls (4096 is fine; instances
  are shared_ptr-owned and reused).
- `CallCtx` is held by value and default-constructed on reset; keep it plain data. Service
  handles → volatile context. If you subclass `Supervisor<CallCtx>` and add member fields,
  register a `.resetHook("CallSupervisor", …)` that clears them.

### 5.6 Logging
`Log::setSink` → `switch_log_printf` (shown in 3.1). All swallowed action exceptions are logged
at WARN with the uuid; pool integrity and overload at ERROR.

---

## 6. The one architectural decision to confirm with Mustafa

The Java INIT/ANSWERED/terminal logic calls services that only exist in Java today:
`CallAdmissionController.processLevel` (partner by IP/domain/SIP user, tenant ancestor chain),
`MultiLevelBillingService` / `levelReservationCallback` / `periodicReservationCallback`
(balance reservation & release), `MnpLookupService`, `DigitFilterProcessor`, dialplan routing
(`determineDialplanContext`), `CdrGenerator`.

Two ways to get "the same processing logic":

| Option | What moves to C++ | Pros | Cons |
|---|---|---|---|
| **A. Signaling in C++, services in Java (recommended v1)** | the FSM + FreeSWITCH I/O only; entry actions call routesphere-core over a **local HTTP API** (`POST /admission`, `/reserve`, `/release`, `/cdr`) with short timeouts | identical business behaviour by construction, one billing codebase, fastest to ship | one local HTTP hop per call phase (sub-ms on loopback; run it on the "db" strand pool, never on the event thread) |
| **B. Everything in C++** | admission, multilevel billing, MNP, digit filter, CDR | zero Java dependency | large port + MySQL client in C++; two implementations of billing to keep in sync |

Prepare the C++ entry actions behind a small `Services` interface (`admit()`, `reserve()`,
`release()`, `route()`, `writeCdr()`) so either option plugs in. Ask Mustafa which one before
writing the service code. Also confirm: which sofia profiles/contexts feed the app, and whether
the `gpm`/`RequestMachine` feature-flag paths in `EslCallRegistry` are in scope (assume NO).

---

## 7. Test plan

1. **Library**: `ctest` green (done today). Add your typed events/graph as a new test in
   `routesphere-cpp/tests/` using `TestChannel` — the FSM must be fully testable without
   FreeSWITCH (see `examples/call_fsm/main.cpp` for the shape).
2. **Module load**: `fs_cli -x "load mod_routesphere"`; confirm the INFO line
   `[call] channel 'routesphere' started (inbound wired)`.
3. **Loopback call**: `originate loopback/1000/default &park` into the routesphere extension,
   watch `uuid_transfer` fire, `uuid_dump <uuid>` shows the transferred context, hang up, check the
   CDR line appears and `[call] … retired` in logs. Then the same via a real SIP phone.
4. **Parity**: run the same scenario against the Java ESL build and diff the CDR CSV rows.
5. **Failure paths**: reject at admission (expect FAILED + CDR + hangup cause), no answer within
   60 s (FAILED), hangup during ringing, module shutdown with live calls (every call gets its
   terminal CDR — that is the forced-failover guarantee).
6. **Load**: 500 concurrent loopback calls; watch `[call] inbound backlog` never appears and
   pool stats show `totalBorrowed == reclaimed`.

---

## 8. Definition of done

- [ ] `mod_routesphere` builds inside the FreeSWITCH tree with `-std=c++20`, loads/unloads cleanly.
- [ ] Inbound calls reach the FSM via the `routesphere` dialplan app; no ESL process involved.
- [ ] Graph, timeouts, commands and DB/CDR side effects match section 4 line by line.
- [ ] The FSM has a FreeSWITCH-free unit test in `routesphere-cpp/tests/`.
- [ ] Shutdown with live calls emits a CDR for each.
- [ ] Services option (section 6) agreed and implemented behind the `Services` interface.

Questions about the library itself (semantics, DSL, registry): read `GUIDE.md` (Java) — the C++
behaviour is identical — then `routesphere-cpp/README.md` for the C++ surface.
