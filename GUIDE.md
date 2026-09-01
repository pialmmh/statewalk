# The Statewalk Guide

**`com.telcobright:statewalk:3.0.0-SNAPSHOT` · Java 21**

Statewalk is a registry-driven state machine framework for realtime session software —
calls, SMS, payments, WiFi data sessions, HTTP workflows: anything that *lives for a while,
must react to events, must time out safely, and must leave a record*.

This guide is the complete manual: concepts, the full builder API, persistence and
recovery, hibernation, channels, quota, the session base, and worked examples.
The test suites under `src/test/java` are living examples of every feature;
`examples/payment-gateway/` is a full reference application.

---

## Table of contents

1. [Concepts in five minutes](#1-concepts-in-five-minutes)
2. [Installation](#2-installation)
3. [Quick start](#3-quick-start)
4. [Defining state graphs — the StateMap DSL](#4-defining-state-graphs--the-statemap-dsl)
5. [Machines, contexts, supervisors, children](#5-machines-contexts-supervisors-children)
6. [The registry — `StatemachineRegistry<T>`](#6-the-registry--statemachineregistryt)
7. [Timeouts: target mode and stay mode](#7-timeouts-target-mode-and-stay-mode)
8. [Persistence, recovery, rehydration](#8-persistence-recovery-rehydration)
9. [Hibernation — `.offline()` states](#9-hibernation--offline-states)
10. [Channels — wire I/O](#10-channels--wire-io)
11. [Quota admission](#11-quota-admission)
12. [The session base — `SessionSupervisor`](#12-the-session-base--sessionsupervisor)
13. [Pooling, reset hooks, leak safety](#13-pooling-reset-hooks-leak-safety)
14. [Operations: shutdown, monitoring, failure behaviour](#14-operations-shutdown-monitoring-failure-behaviour)
15. [Testing your machines](#15-testing-your-machines)
16. [The payment-gateway example](#16-the-payment-gateway-example)

---

## 1. Concepts in five minutes

```
                        StatemachineRegistry<T>          (one per domain: "call", "sms", "pgw")
                        ┌──────────────────────────────────────────────┐
 wire events ──────────►│  row per request id                          │
 dispatch(id, task) ───►│  ┌─────────────────────────────────────┐     │
                        │  │ cell 0: Supervisor   (id = "call-7")│     │
                        │  │ cell 1: child "Signaling"           │     │──► records (SDR / PaymentRecord)
                        │  │ cell 2: child "Budget"              │     │──► Channel.send (wire out)
                        │  └─────────────────────────────────────┘     │
                        │  pools · timers · quota · persistence        │
                        └──────────────────────────────────────────────┘
                                            │ snapshots on every transition
                                            ▼
                              PersistenceProvider (JDBC / Redis / in-memory)
```

- **Registry** — hosts every live request of one domain. A request id owns a *row* of
  cells: position 0 is the **supervisor** (the only externally visible machine),
  positions 1+ are **children** it spawns.
- **Cell = one machine bound to one request.** Everything that touches a cell — events,
  its state timers, its teardown — runs in FIFO order on the cell's own serial chain.
  You never need locks in your state actions.
- **Machines are pooled.** Borrowed at dispatch, reset and returned at terminal. Every
  timer and queued task carries the machine's *borrow epoch* and proves ownership before
  running, so a reused instance can never receive a previous request's stale work.
- **Every state has a timeout** (enforced at build): either a fallback transition to a
  final state, or a stay-mode checkpoint. Nothing can hang forever.
- **Persistence is a snapshot per transition** (async, never blocking event flow).
  Recovery, rehydration, and hibernation are built on those snapshots.
- **Construction is builder-only, everywhere.** Registries, state maps, specs — no
  public constructors. Configuration errors (route typos, missing timeouts, leak-prone
  fields, offline-without-persistence) fail at **build time**, not in production.

## 2. Installation

```xml
<dependency>
    <groupId>com.telcobright</groupId>
    <artifactId>statewalk</artifactId>
    <version>3.0.0-SNAPSHOT</version>
</dependency>
```

Ships with `slf4j-api` (bring your own binding) and `jackson-databind` (context
serialization). Optional: `redis.clients:jedis` if you use `RedisPersistenceProvider`,
your JDBC driver if you use `JdbcPersistenceProvider`.

## 3. Quick start

A single-machine domain: a job that runs, accepts progress touches, and completes or
expires.

```java
import com.telcobright.statewalk.event.StatemachineEvent;
import com.telcobright.statewalk.registry.*;
import com.telcobright.statewalk.state.StateMap;
import java.util.concurrent.TimeUnit;

// 1. Events are plain records implementing the marker interface.
public record Touch(String id) implements StatemachineEvent {}
public record Done(String id)  implements StatemachineEvent {}

// 2. The context is a Jackson-friendly POJO — public fields, no-arg constructor.
public class JobCtx { public String owner; public int touches; public JobCtx() {} }

// 3. The graph + routing, declared as a spec (no subclassing needed).
SupervisorSpec<JobCtx> spec = SupervisorSpec.<JobCtx>builder()
    .name("Job")
    .contextFactory(JobCtx::new)
    .stateMap(StateMap.builder()
        .initialState("RUNNING")
        .state("RUNNING").interim()
            .timeout(30, TimeUnit.MINUTES, "EXPIRED")           // mandatory fallback
            .stay(Touch.class, (self, e) ->                      // handle without leaving
                ((com.telcobright.statewalk.machine.Machine<JobCtx>) self).getContext().touches++)
            .on(Done.class, "COMPLETED")                         // transition
        .state("COMPLETED").finalState().timeout(1, TimeUnit.SECONDS, "COMPLETED")
        .state("EXPIRED").finalState().timeout(1, TimeUnit.SECONDS, "EXPIRED")
        .build())
    .routes(r -> { r.selfHandle(Touch.class); r.selfHandle(Done.class); })
    .build();

// 4. The registry — typed by the supervisor's context.
StatemachineRegistry<JobCtx> reg = StatemachineRegistry.<JobCtx>builder("jobs")
    .supervisor(spec, 256)                                       // 256 = pool size
    .threads(4)
    .build();

// 5. Drive it.
JobCtx task = new JobCtx(); task.owner = "alice";
reg.dispatch("job-1", task);                  // DispatchResult tells you accept/reject
reg.onInboundEvent("job-1", new Touch("job-1"));
reg.onInboundEvent("job-1", new Done("job-1"));   // → COMPLETED → machine pooled again
reg.shutdown();
```

That is the whole loop: **declare graph → build registry → dispatch → feed events**.
Everything else in this guide adds capability to that loop.

## 4. Defining state graphs — the StateMap DSL

`StateMap.builder()` produces a frozen, immutable graph shared by every instance of a
machine type.

```java
StateMap.builder()
    .initialState("A")
    .state("A").interim()                       // every state declares interim OR finalState
        .timeout(30, TimeUnit.SECONDS, "FAILED")     //   and exactly one timeout mode
        .onEntry(self -> ...)                   // entry action (lambda)
        .onExit(self -> ...)                    // exit action (lambda)
        .on(Ev.class, "B")                                        // unconditional transition
        .on(Ev.class, "B", (self, e) -> guard(e))                 // guarded (pure predicate!)
        .on(Ev.class, "B", guardOrNull, (self, e) -> copy(e))     // + transition ACTION
        .stay(Ev.class, (self, e) -> ...)       // handle event, stay put (context re-persisted)
    .state("HOLD").interim()
        .timeoutStay(15, TimeUnit.SECONDS, self -> heartbeat())   // stay-mode timeout (§7)
    .state("PARKED").interim().offline()        // hibernation state (§9)
        .timeout(30, TimeUnit.MINUTES, "EXPIRED")
    .state("FAILED").finalState()
        .timeout(1, TimeUnit.SECONDS, "FAILED") // final states declare a self-target by convention
    .build();
```

### Semantics you can rely on

| Rule | Meaning |
|---|---|
| Guards are evaluated in declaration order; first pass wins; `null` guard = always | Declare specific guards first, an unconditional fallback last |
| Guards must be **pure** — a guard throw is logged and counts as *false* | Put mutation in transition **actions**, which run after the guard passes, before the transition; an action throw is logged and does NOT veto |
| A guarded `on(...)` beats a `stay(...)` for the same event; the stay runs only when no guard passes | "Transition on condition, otherwise observe" is expressible |
| A `stay` handler re-persists the context **with the unchanged state deadline** | Stays never reset the state timer |
| Entry/exit actions may throw — logged at WARN, never fatal | Keep them idempotent anyway |
| `IDLE` is reserved and auto-injected (the pooled resting state) | Never declare it |

### Build-time invariants (violations throw at `build()`)

- `initialState` must be a declared state.
- Every state declares `.interim()` or `.finalState()` — intent is explicit.
- Every state has exactly one timeout: `.timeout(d, u, target)` **or** `.timeoutStay(...)`.
- A target-mode timeout must point at a **final** state (the fallback always lands terminal).
- `.finalState()` + `.offline()` is a contradiction; `.finalState()` + `.timeoutStay` too.
- All transition targets must exist.

## 5. Machines, contexts, supervisors, children

### Contexts

The context `C` is the machine's only per-request state. Rules:

- **Jackson-friendly**: public no-arg constructor, public fields or accessors, JDK types.
  It is JSON-serialized into every snapshot when persistence is on.
- Mutated freely inside entry/exit/stay/transition actions — the cell chain serializes
  all access.
- Cleared on pool return; a fresh dispatch gets the object you passed to `dispatch`.

**Volatile context** is the other half: service handles, tenant config, provider
clients — things that must NOT be persisted and must be re-attached after a restart:

```java
.volatileLoader("Job", machine -> myServiceBundle)   // fires on creation AND rehydration
// inside actions:  MyServices s = (MyServices) machine.getVolatileContext();
```

### Two ways to define a machine type

**Spec-based (preferred)** — no subclass, everything is data + lambdas:
`MachineSpec<C>` for children, `SupervisorSpec<C>` for the supervisor (adds `.routes`).

**Subclass-based** — extend `Supervisor<C>` (supervisors) or `Machine<C>` (children)
and override `defineStates()` / `defineRoutes(r)` / `createContext()`. Use when you want
named step methods (see `PaymentSupervisor` in the example). Subclass fields must be
`final` (§13) unless you register a reset hook.

### Routing — `InternalEventResolver`

Every event for a request — wire-inbound or published by a child — arrives at the
supervisor, whose resolver decides:

```java
.routes(r -> {
    r.selfHandle(AdmissionDecided.class);              // supervisor's own graph handles it
    r.forwardTo("Signaling", CallRinging.class);       // one child
    r.forwardToAll(List.of("Signaling", "Budget"), Ping.class);   // fan-out
    r.drop(NoisyEvent.class);                          // explicit ignore (no WARN)
})
```

Unrouted events log a WARN and drop. **Route targets are validated at build** — a typo
in a child name kills the build, not production traffic.

Supervisor state actions spawn and retire children through the resolver:

```java
s.resolver().spawnChild("Signaling", childCtx);   // childCtx should be Jackson-friendly too
s.resolver().cleanupChild("Signaling");           // retire one
s.resolver().cleanupChildren();                   // retire all (the retry contract, §12)
```

Children address the supervisor by **publishing**: `publishEvent(new SignalingDone(grant))`
inside any state action routes back through the supervisor's resolver.

Machine ids: supervisor = the request id (`call-7`); child = `call-7#Signaling`.

## 6. The registry — `StatemachineRegistry<T>`

`T` is the **supervisor's context type** — the registry's public API is typed by it.
Children keep their own context types via their specs.

### Builder reference

```java
StatemachineRegistry<CallCtx> reg = StatemachineRegistry.<CallCtx>builder("call")
    .supervisor(spec, poolSize)                    // or .supervisor(name, MySup::new, poolSize)
    .child(childSpec, poolSize)                    // or .child(name, MyChild::new, poolSize)
    .threads(4)                                    // timer threads (workers are virtual)
    .persistence(provider)                         // §8 — JDBC / Redis / in-memory / custom
    .rehydrate(true)                               // §8 — enables recovery + rehydration
    .preWarmContextClass(CallCtx.class)            // avoids cold Class.forName on first restore
    .volatileLoader("CallSup", m -> services)      // per-type service bundle (§5)
    .resetHook("CallSup", m -> {...})              // per-type pool-return cleanup (§13)
    .createFromFirstEvent(ev -> ...)               // auto-create on isFirst() events (returns T)
    .maxConcurrent(50_000)                         // hard cap on LIVE requests (0 = off)
    .globalTimeout(4, TimeUnit.HOURS, "FAILED")    // whole-lifetime cap, survives restarts
    .maxPendingInbound(10_000)                     // entry backpressure: shed, don't drown
    .quotaKeysExtractor(t -> QuotaKeys.of(...))    // §11
    .quotaLimits(new QuotaLimits(...))             // §11
    .debugSampleRate(50)                           // 1-in-N machines log DEBUG traces
    .channel(myChannel)                            // §10 (+ overload with a decoder)
    .build();
```

### Runtime API

| Method | What it does |
|---|---|
| `dispatch(id, T task)` | Start a request. Returns `DispatchResult` (`ok` or a `RejectCause`: DUPLICATE_ID, CAPACITY_EXCEEDED, quota causes, SHUTTING_DOWN, POOL_INTEGRITY_ERROR). The task object becomes the supervisor's context. |
| `onInboundEvent(id, event)` | Fire-and-forget wire event. Throws `IllegalStateException` for an unknown id with no recovery path. |
| `submitInbound(id, event)` | Same, with an **ack**: the returned future completes when the cell actually processed the event (commit Kafka offsets on it), or fails (unknown id / overload / shutdown). |
| `rebindQuotaKeys(id, keys)` | Atomically swap the quota identity a live request holds (§11). |
| `supervisorStateOf(id)` / `supervisorContextOf(id)` | Peek the LIVE supervisor (null when not in memory — check the store for hibernated ones). |
| `hasAny(id)`, `activeIdCount()`, `activeCellCount()` | Liveness introspection. |
| `wasRecentlyFinished(id)` | True during the post-terminal tombstone window — distinguishes "just finished" from "never seen". |
| `awaitIdle(timeout, unit)` | Drain: all chains + persistence writes settled. The test synchronization primitive. |
| `shutdown()` | §14. Drives every live request through its failover state (records ship), drains, closes. |

### First-event auto-creation

An event whose `isFirst()` returns true can create the request on arrival — no explicit
dispatch call — when `createFromFirstEvent` is configured:

```java
public record ChannelPark(String uuid, String caller) implements StatemachineEvent {
    @Override public boolean isFirst() { return true; }
}
// builder: .createFromFirstEvent(ev -> ev instanceof ChannelPark p ? CallCtx.from(p) : null)
```

## 7. Timeouts: target mode and stay mode

Every state must declare one — the discipline that makes hangs impossible.

**Target mode** — the classic fallback. On maturity, transition to a *final* state:

```java
.state("RINGING").interim().timeout(90, TimeUnit.SECONDS, "FAILED")
```

**Stay mode** — a periodic checkpoint for states that legitimately wait indefinitely.
On maturity the machine STAYS: the optional action runs (heartbeat, keepalive, counters),
the context is re-persisted with the refreshed deadline, and the timer re-arms:

```java
.state("HOLDING").interim()
    .timeoutStay(15, TimeUnit.SECONDS, self -> sendKeepalive(self))
```

The registry's `globalTimeout` remains the hard lifetime ceiling either way.

Before either fires, a `TimeoutEvent(fromState, targetOrNull)` is offered to the state's
own rules first — a domain `.on(TimeoutEvent.class, ...)` transition **wins** over both
the fallback and the checkpoint.

Timers run **through the cell's serial chain** (never against the machine from a
scheduler thread) and carry a state-visit token: a timer from a previous visit — or a
previous borrow — is provably stale and no-ops.

## 8. Persistence, recovery, rehydration

Three separate switches, deliberately:

| Config | Behaviour |
|---|---|
| *(neither)* | Pure in-memory operation. |
| `.persistence(p)` only | **Recovery data**: every transition snapshots asynchronously; nothing is ever restored by this process. |
| `.persistence(p).rehydrate(true)` | Full story: startup recovery + lazy rehydration + hibernation. |

### What is persisted

One snapshot per cell: machine id, registry name, current state, the context as JSON,
the state-timeout deadline + target, and (supervisor rows) the global-lifetime deadline.
Writes are FIFO per machine on a dedicated executor — **event processing never waits on
the store**. A snapshot that cannot be serialized or written aborts the request through
its failover state (it never runs un-persistably — and the record still ships).

### Providers

- `InMemoryPersistenceProvider` — tests, dev.
- `JdbcPersistenceProvider(dataSource[, table])` — portable SQL (MySQL 5.7+/8,
  PostgreSQL, SQLite tested). Auto-creates/upgrades its table; quarantines into
  `<table>_dead`.
- `RedisPersistenceProvider(jedisPool[, ttlSeconds])` — hash per machine + index set,
  MULTI-consistent, pipelined restore scans. The high-TPS choice.
- **Custom**: implement `PersistenceProvider` (save/load/loadAll/delete + optional
  `loadMatured`, `loadAllForRegistry`, `quarantine`).

### Recovery semantics (the part you should memorize)

- **Startup recovery** (build time, `rehydrate(true)`): loads every unfinished snapshot.
  Sessions resume seated in their saved state; **entry actions are never replayed**.
- **Elapsed downtime counts**: a matured target-mode deadline transitions to its target
  immediately; a matured stay-mode deadline checkpoints and re-arms; unmatured deadlines
  arm for the remaining slice only. The persisted global deadline is re-armed too.
- **Final-state rows are tombstones** — a finished session whose terminal delete was
  lost in a crash is purged, never resurrected. No duplicate terminal side effects, no
  zombie quota.
- **Bad data is quarantined, not destroyed**: an unknown saved state (deploy renamed
  it), an undeserializable context, a rehydrate throw — the row moves to the dead-letter
  area with the reason, the id is blocked from retry loops, and an operator (or a fixed
  build) gets the data back.
- **Lazy rehydration**: an inbound event for an id that is not in memory probes the
  store and restores the whole row (supervisor first, then children) — single-flight per
  id, atomic against a concurrent dispatch.
- **Crash-failover pattern**: run instance B against the same store (an external
  coordinator decides when). B's startup recovery resumes everything in flight; quota
  counters are re-acquired from the restored contexts. Nothing else to wire.

## 9. Hibernation — `.offline()` states

Mark a state `.offline()` and entering it **suspends** the request: the snapshot is the
resume point, the machine is reset and returned to the pool, memory footprint drops to
zero. For flows that wait on the outside world for minutes-to-days (a customer on a
payment page, a captured payment inside its refund window, a parked call), this is the
core resilience tool.

```java
.state("AWAITING_CALLBACK").interim().offline()
    .timeout(30, TimeUnit.MINUTES, "EXPIRED")
```

Rules and behaviour:

- Requires `.persistence(...)` **and** `.rehydrate(true)` — both enforced at build
  (a hibernating graph with no wake path would strand sessions).
- Supervisor goes offline → the whole request suspends (children too). A child alone can
  also suspend.
- **Wake** = any inbound event for the id (`submitInbound`/`onInboundEvent`). The machine
  rehydrates into its saved state and the event is delivered. An event that *races* the
  suspend is re-submitted, not lost.
- **Startup does not flood memory**: unmatured hibernated sessions stay db-only at
  recovery; matured ones are woken so their expiry ritual runs.
- **Sweeping**: while nothing arrives, a hibernated deadline cannot fire by itself.
  Periodically wake matured rows: `provider.loadMatured(registryName, now)` → send each
  id a domain no-op event (give offline states a `.stay(Sweep.class, ...)`). See
  `PaymentGateway.sweepExpired()` for the pattern.
- Quota slots stay held across hibernation (the session logically exists).

## 10. Channels — wire I/O

`Channel<O, I>` is the conduit to the outside protocol (ESL, sigtran, Kafka, HTTP…).
The registry OWNS its lifecycle: `channel.start(gateway)` at build, `channel.stop()`
first thing in shutdown.

```java
public interface Channel<O, I> {
    void send(String requestId, O command);          // outbound, fire-and-forget
    default void cancel(String requestId) {}
    void start(Inbound<I> gateway);                  // registry hands you the inbound gateway
    void stop();
    boolean isConnected();
    String getName();
    interface Inbound<I> { CompletionStage<Void> offer(String requestId, I event); }
}
```

- `offer` returns a stage that completes when the cell **processed** the event —
  at-least-once consumers (Kafka) commit offsets on it; failures (unknown id, overload,
  shutdown) arrive exceptionally, never as throws into your consumer thread.
- Bind with `.channel(ch)` when `I` already implements `StatemachineEvent`, or
  `.channel(ch, (id, frame) -> decode(frame))` with a decoder (return null to ignore).
- State actions reach the wire via `registry.getChannel()` / typed `channelAs()`.
- `TestChannel` is the in-memory implementation for tests — `inject(id, event)` returns
  the ack stage.

## 11. Quota admission

Two dimensions (partner, route), two kinds of limits each (concurrent, TPS):

```java
.quotaKeysExtractor(task -> QuotaKeys.of(task.partnerId, task.routeId))   // typed: task is T
.quotaLimits(new QuotaLimits(
    /*maxConcurrentPerPartner*/ 100, /*maxConcurrentPerRoute*/ 30,
    /*maxTpsPerPartner*/ 50,        /*maxTpsPerRoute*/ 20))               // 0 disables a check
```

- Checked at dispatch; a rejection returns the precise `RejectCause` and rolls back
  **everything** it took — including TPS tokens (a rejected dispatch never starves the
  next one).
- Released exactly once at terminal. Counters prune at zero (safe for per-user /
  per-MAC cardinality). Restores re-acquire from the restored context, so caps stay
  truthful across restarts — put the identity IN the context.
- **Rebind** (`rebindQuotaKeys(id, newKeys)`) — for anonymous-at-birth sessions (a WiFi
  MAC before login): acquire-new-before-release-old per changed dimension; on rejection
  the old keys were never let go. Idempotent; burns no TPS.

## 12. The session base — `SessionSupervisor<C extends SessionContext>`

The reusable supervisor for "a session is a call" domains (voice, SMS, WiFi, HTTP):

```
ADMITTING ──accept──► ADMITTED ──SignalingDone──► ACTIVE ──ServiceEnd──► TEARING_DOWN ──Settled──► SUCCEEDED
    │                    │ ▲ retry(nextAttempt)      │                       │
    └─ reject ───────────┴─ SignalingFailed / abort ─┴──(dead-man timeout)───┴─────────────────────► FAILED
```

You extend it and implement the hooks; the base owns the graph, the retry ritual, and the
**one-SDR-per-session-on-every-exit-path guarantee** (graceful, timeout, shutdown,
persistence failure — all of them ship the record, with the reason in `endCause`).

| Hook | You implement |
|---|---|
| `timings()` | `SessionTimings(admittingSec, admittedSec, activeMaxSec, tearingDownSec)` |
| `runAdmission(ctx)` | identify / authorize / reserve → `AdmissionVerdict.accept/reject` |
| `spawnChildren(r, ctx)` | spawn the signaling child (+ budget child); share `ctx.history` |
| `onActive(ctx)` / `onTeardown(ctx)` | deliver / stop the service |
| `buildSdr(ctx, outcome)` / `sdrSink()` | the domain record payload and where it goes |
| `defineDomainRoutes(r)` | forward rules for your child events |
| optional: `onSignalingDone/Progress`, `onSettled`, `nextAttempt`, `cleanupBeforeRetry`, `onEnded`, `settlesAsync`, `sessionSucceeded` | payload copies, retry policy, outcome rule |

Children extend `RecordingMachine<C implements HistoryCarrier>` so the whole cell writes
one ordered `SessionHistory` timeline into the SDR. Domain hooks run exception-shielded
as transition actions: a hook throw cannot drop an event or flip an outcome, and a
`buildSdr` crash still ships the envelope.

## 13. Pooling, reset hooks, leak safety

- Pools are per machine type, sized in the builder, containment-guarded (a double return
  is rejected loudly), capped (surplus instances are dropped for GC).
- **The field validator** (build time): a machine subclass may only declare `final`
  fields — a mutable field would leak one request's state into the next borrow.
- **Escape hatch — reset hooks**: register a per-type pool-return lambda and mutable
  props become legal, because the hook now owns clearing them (they are listed in an
  INFO log at build for review):

```java
.resetHook("CallSup", m -> {
    var s = (MyCallSupervisor) m;
    s.scratch = null;
    s.cache.clear();          // final-field CONTENTS need clearing too
})
```

A hook throw drops the instance — a machine is never recycled dirty.

## 14. Operations: shutdown, monitoring, failure behaviour

- **`shutdown()`**: stops the channel first (no consumed-and-lost events), then drives
  every LIVE request through its declared failover state — domain terminal work and
  records run — then drains persistence and closes. **Hibernated rows are untouched**:
  they are the sessions, and the next start resumes them.
- **Forced failover** (shutdown, global timeout, persistence failure) = transition to
  the current state's timeout target, with the reason handed to
  `Machine.onForcedFailover(reason)` (the session base stamps it into `endCause`).
- **Backpressure**: `maxPendingInbound` sheds wire events at the entry with a failed ack
  when the backlog is over bound — internal progress is never blocked and never runs
  inline in map operations.
- **What gets logged**: every swallowed domain exception (entry/exit/guard/action/hook)
  at WARN with the machine id; overload and pool integrity at ERROR; quarantines at
  WARN/ERROR with reasons.
- Introspection: pool statistics (borrowed/returned/capDrops/doubleReturns), quota
  counters, `activeCellCount`, timer counts.

## 15. Testing your machines

- Use `InMemoryPersistenceProvider` + `awaitIdle(...)` after driving events — it drains
  chains AND persistence, making assertions deterministic.
- Compress `SessionTimings`/timeouts to seconds; a state timeout to a final state makes
  "nothing happened" paths assertable.
- `TestChannel.inject(...).toCompletableFuture().get(...)` synchronizes on actual
  processing.
- Simulate a crash by building a SECOND registry on the same store **without** shutting
  the first one down (shutdown is graceful and completes sessions; a crash is the
  absence of it).
- The framework's own suites are the richest examples:
  `RegistryHardeningV3Test` (concurrency + recovery corner cases),
  `StayTimeoutAndRehydrationTest` (timeout modes, hibernation-at-startup),
  `MySqlRehydrationTest` (everything against real MySQL),
  `SessionSupervisorBaseTest` (the session base contract).

## 16. The payment-gateway example

`examples/payment-gateway/` is a complete reference application — a reusable
payment-gateway library where **every wait on the outside world is a hibernated store
row**: initiate → customer at the provider site (hibernated) → callback rehydrates →
captured (hibernated, refundable) → refund / sweep / expiry. It demonstrates:

- `.offline()` hibernation + wake-on-callback + `sweepExpired()` (via `loadMatured`),
- crash-failover onto a second instance over the same MySQL,
- a stay-mode timeout as a refund-retry safety net,
- volatile-context service injection (`PgwProviderClient`),
- one `PaymentRecord` per payment from every terminal,
- a clean facade (`PaymentGateway`) a web app calls.

Build and run it:

```bash
mvn -q install          # from the repo root: installs the library
cd examples/payment-gateway
mvn test                # MySQL tests self-skip without a server on 127.0.0.1:3306
```

---

*Migration from v2 is covered in the README. The v2 audit that shaped v3 (15 verified
findings and the hardening plan) lives in `docs/audit/`.*
