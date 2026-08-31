# statewalk

Registry-driven state machine framework for realtime session software (calls, SMS, HTTP, WiFi
data sessions — anything that lives, times out, and must leave a record).

**Maven: `com.telcobright:statewalk:3.0.0-SNAPSHOT`** · Java 21 · Build: `mvn test`

- One supervisor + child machines per session cell, driven serially on a per-cell FIFO chain
  (events, timers, and teardown all run in order — the serial invariant is structural, not
  best-effort).
- Object-pooled machines with **epoch identity**: every timer and queued task proves it still
  belongs to its borrow before touching a machine, so a re-used instance can never receive a
  previous session's stale work.
- **Atomic lifecycle claims** per cell (LIVE → TERMINATING | SUSPENDING): exactly one owner runs
  a cell's retirement; request-id reuse and fast retry are first-class, not races.
- **Mandatory per-state timeouts** in one of two modes: `.timeout(d, u, target)` — the classic
  fallback to a final state — or `.timeoutStay(d, u[, action])` — the machine STAYS, runs the
  per-period action (heartbeat/keepalive), re-persists the context with the refreshed deadline,
  and re-arms (for states that legitimately wait indefinitely). A persisted **global lifetime
  cap** survives restarts and remains the hard ceiling; quota admission with exact counters
  (`rebindQuotaKeys` for anonymous-at-birth sessions binds acquire-before-release); entry-point
  backpressure (`maxPendingInbound`) instead of mid-pipeline drops.
- **Rehydration honours elapsed time, never replays entry actions**: the machine is seated
  directly in its last saved state; a target-mode deadline that matured during downtime
  transitions to its target immediately; a matured stay-mode deadline checkpoints immediately
  and re-arms; an unmatured deadline is armed for the remaining slice only.
- Persistence + rehydration: JDBC, **Redis** (optional `redis.clients:jedis`), in-memory.
  Crash-consistent: final-state snapshots are tombstones (a failed delete can never resurrect a
  finished session), rehydration failures are **quarantined** to a dead-letter area instead of
  destroyed, startup recovery resumes every unfinished session.
- `Channel` v2: the registry owns the channel lifecycle (`start`/`stop`), inbound events carry a
  completion ack (`submitInbound` returns a future that completes when the cell processed the
  event) — Kafka-style at-least-once consumers commit on it.
- `session.SessionSupervisor`: the generic base — ADMITTING → ADMITTED → ACTIVE → TEARING_DOWN →
  SUCCEEDED/FAILED with abstract hooks; **exactly ONE SDR per session on EVERY exit path** —
  graceful, timeout, retry-exhausted, persistence failure, global timeout, and registry
  shutdown all ship the record (forced failover drives live sessions through their terminal
  state first). Domain hooks run as transition actions, exception-shielded: a hook throw can
  neither drop an event nor flip an outcome, and a `buildSdr` crash still ships the envelope.

Construction is **builder-only** everywhere: `Registry.builder(name)…build()`,
`SupervisorSpec.builder()`, `MachineSpec.builder()`, `StateMap.builder()`. Route targets,
pooled-field safety, offline-requires-persistence, and quota configuration are all validated at
build — typos die at startup, not as production DEBUG drops.

## Package map (v3)

```
com.telcobright.statewalk
├─ registry      Registry (builder-only), Supervisor, InternalEventResolver,
│                SupervisorSpec/MachineSpec, DispatchResult, QuotaKeys/QuotaLimits/RejectCause
├─ machine       Machine (epoch + state-visit identity tokens, forced failover)
├─ state         StateMap/StateConfig (guards + transition actions, mandatory timeouts)
├─ session       SessionSupervisor base, SessionContext/History/Timings, SdrRecord/SdrSink,
│                RecordingMachine, events.*
├─ channel       Channel v2 (start/stop lifecycle, acked inbound), TestChannel
├─ persistence   PersistenceProvider (+quarantine), MachineSnapshot, SnapshotSerializer,
│                InMemory / jdbc.JdbcPersistenceProvider / redis.RedisPersistenceProvider
├─ event         StatemachineEvent, TimeoutEvent
├─ pool          ObjectPoolManager (containment-guarded), Poolable
├─ timeout       TimeoutManager
├─ executor      BoundedVirtualThreadExecutor
└─ pipeline      ProcessingStep, StepMode
```

## Migrating from v2 (`com.telcobright:statewalk-v2`)

1. Coordinates: `com.telcobright:statewalk-v2:1.0-SNAPSHOT` → `com.telcobright:statewalk:3.0.0-SNAPSHOT`.
2. Packages: drop the `v2.` segment; `v2.flat.*` → `registry.*`; `v2.registry.api.{DispatchResult,
   QuotaKeys,QuotaLimits,RejectCause}` → `registry.*`; `v2.registry.consumes.*` → `event.*`.
3. The legacy api stack (`registry.api.Registry`, `MultiRegistry`, `Statewalk`,
   `StatewalkSystem`, `EventTypeRegistry`) is **gone** — build a `Registry` directly; event
   registration no longer exists (routing is declared, validated at build).
4. `Channel` implementations: replace `onInbound(BiConsumer)` with
   `start(Inbound<I>)` / `stop()`; the registry now calls both.
5. `PersistenceProvider.delete` may throw (the registry retries); implement `quarantine`
   (optional but recommended).
6. Supervisors extending flat `Supervisor` directly should migrate onto
   `session.SessionSupervisor` — admission/signaling/retry/SDR/teardown come from the base;
   current consumers to migrate: routesphere `CallSupervisor`/`SmsSupervisor`, wifi-sphere
   `WifiSessionSupervisor`.

The full v2 audit that drove this release (15 verified findings + the M1–M6 plan) is in
`docs/audit/statewalk-v2-audit-v3-plan.md`.

History: extracted 2026-09-01 from `routesphere/statewalk-v2` with full git history.
First production consumers: routesphere call/SMS v2 supervision, BTCL Carrier WiFi
(wifi-sphere F37 "a session is a call").
