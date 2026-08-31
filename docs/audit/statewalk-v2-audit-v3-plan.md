# Statewalk v2 Audit — Verified Findings + v3.0 Fix Plan

**For the fixing agent.** This is the final report of a max-effort multi-agent review
(10 finder angles, cross-verification, full-file sweep) of the statewalk library at
`/home/mustafa/telcobright-projects/statewalk` (branch `master`, commit `bd0eb18`).
All 15 findings below were verified against the source; every `file:line` was checked.

How to use this document:
1. Read the 15-finding JSON first — each entry has the exact file/line, the defect,
   and a concrete failure scenario to reproduce/assert against.
2. Then read "Assessment beyond the findings" — the 4 systemic root causes. Fix those
   structurally (epoch token, single cleanup claim, unconditional terminal work,
   timeouts on the cell chain); do NOT patch the 15 symptoms individually.
3. Follow the M1–M6 plan in order. M1 (collapse to one stack) comes first so every
   later fix lands once. Findings 1–3 are the pre-production blockers.
4. Supplementary lower-severity findings are at the bottom — fold them into M1/M3.
5. Target coordinates for v3: `com.telcobright:statewalk`. Current consumers to
   migrate in M6: routesphere CallSupervisor/SmsSupervisor, wifi-sphere
   WifiSessionSupervisor (today they extend flat `Supervisor` directly).

---


## Verified findings (max-effort review; 10 finder angles + verification + sweep; all quotes checked against source)

```json
[
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 1010,
    "summary": "Supervisor-terminal cascade orphans every live child: active.remove(parentId) runs before the queued per-child cleanups execute, so each queued onCellTerminated finds list==null and bails — children are never reset, never returned to the pool, snapshots never deleted, state timers left armed; forceCleanupAll (global timeout, shutdown, failRequest) has the same defect because its first iteration (the supervisor) removes the row.",
    "failure_scenario": "Supervisor reaches SUCCEEDED/FAILED while a signaling child is alive (the normal ADMITTED-timeout path). Lines 1003-1009 queue `chainSubmit(sibKey, () -> onCellTerminated(parentId, sibType))`, then line 1010 executes `active.remove(parentId)` synchronously. The queued task later hits `if (list == null) { terminated.remove(key); return; }` (line 949). The orphan child's mandatory state timeout later fires into it (registry still bound, terminated=false), runs wire-side entry actions after the session ended, and its DB snapshot survives to be resurrected by startup recovery. Under load the leak also depletes the pool. If the parentId is re-dispatched before the orphan's timer matures, the orphan's terminal ritual then finds and tears down the NEW session's child of the same type."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 946,
    "summary": "The 60-second `terminated` dedup set silently skips the entire termination ritual for any cell whose (parentId, typeName) key was terminated within the last 60s — which structurally breaks BOTH request-id reuse AND the SessionSupervisor retry contract, since a retry respawns the signaling child under the SAME cellKey.",
    "failure_scenario": "Retry: attempt-1 child fails and terminates ('pid#Sig' cached in `terminated` for 60s, line 1020); signalingFailed() respawns the child under the same name (SessionSupervisor.java:251); attempt-2 fails 3s later -> `if (!terminated.add(key)) return;` (line 946) skips cleanup: child stays in `active`, never pooled, quota/snapshot never touched, and the next spawnChildInternal hits 'child already present' (line 547) so attempt 3 is silently dead — the session hangs until timeout with endCause='silent'. Same mechanism for a whole request id re-dispatched within 60s (dispatch never consults the set, line 249): the second session's terminal is a no-op — machine, active row, quota slot, and snapshot all leak permanently, and later dispatches of that id are rejected DUPLICATE_ID. The api stack (registry/api/Registry.java:496) additionally drops ALL inbound events for a legitimately re-dispatched id for up to 60s."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 592,
    "summary": "Every force-cleanup path retires sessions WITHOUT driving them to a terminal state, so SessionSupervisor.close() never runs: no SDR, no onTeardown backstop, no onEnded — the library's headline 'exactly ONE SDR per session, ALWAYS' guarantee is violated on shutdown, on any persistence save/serialize failure (failRequest), on global timeout without a target, and on rehydrate failure.",
    "failure_scenario": "registry.shutdown() with 500 wifi sessions ACTIVE: for each id forceCleanupAll -> onCellTerminated resets the supervisor straight to IDLE — the FSM never enters SUCCEEDED/FAILED, so close() (SessionSupervisor.java:299, the only SDR writer and the teardown backstop) never executes: 500 sessions end with no SDR, wifi gates left open, acct-stop never sent; the snapshots are also deleted so post-restart recovery cannot reconstruct them. Same silent loss for any session whose context fails snapshot serialization (line 689 -> failRequest line 728)."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/machine/Machine.java",
    "line": 442,
    "summary": "The timeout subsystem breaks the per-cell serial invariant and has no generation/epoch token: state timeouts execute on TimeoutManager scheduler threads directly against the machine (never through the cell chain), the runnable's guard compares only state NAMES across three separate synchronized blocks, cancel(false) cannot stop an already-running runnable, the rehydrate copy of the runnable (lines 587-591) lacks the registry==null guards the main copy has, and TimeoutManager.scheduleTracked's `finally { activeTimeouts.remove(id) }` (TimeoutManager.java:72) unconditionally deletes a NEWER tracked global timer for a reused id, leaving it uncancellable.",
    "failure_scenario": "(a) Queue-jump: an event queued on the cell chain before the deadline is beaten to the machine monitor by the timer thread, which drives the state to its (mandatorily final) timeout target and terminates the cell — the in-time event is then dropped against a reset machine. (b) Stale fire into the next session: machine in ADMITTING at its deadline; the runnable starts, the session terminates concurrently, resetForReuse's cancel(false) returns false, the instance is re-borrowed and the new session enters ADMITTING; the runnable's guard `terminated || registry == null || !currentState.equals(next.name())` (line 451) passes on all counts and transitions the fresh session to FAILED at ~0ms. (c) A same-name re-entry (RINGING->TRYING->RINGING) at the deadline is killed by the old visit's timer. (d) The scheduleTracked clobber leaves a reused id's global timeout untracked, so it cannot be cancelled and later force-cleans an innocent session."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 249,
    "summary": "Duplicate-dispatch and duplicate-restore guards are check-then-act with no atomic claim: concurrent dispatch() calls (or concurrent inbound events triggering restoreAllCellsFor) for the same id all pass the null/containsKey check and each appends a supervisor to the same row via `active.computeIfAbsent(...).add(m)` — the api stack's putIfAbsent race guard (registry/api/Registry.java:428) was dropped in the flat rewrite.",
    "failure_scenario": "Two wire threads dispatch id X simultaneously: both pass `active.containsKey(parentId)` (line 249), both acquire quota, and B's `dispatchQuotaKeys.put(parentId, keys)` (line 266) overwrites A's, so A's quota acquire can never be released (permanent over-count). The row becomes [sup1, sup2]; supervisorOf() returns only list.get(0), so sup2 runs unreachable, leaks its pool slot, and its terminal ritual consumes the shared cellKey dedup entry, corrupting sup1's cleanup. Identical race on restoreOneCell (line 849 null-check vs line 870 add) for two concurrent events rehydrating the same offline id — quota re-acquired twice (line 868)."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 570,
    "summary": "Already-queued chain continuations survive chains.remove() and fire into machines that have been reset and re-borrowed: forwardToChild's queued `m.fire(event)` has NO identity re-check at all, borrowAndStart's queued `machine.start()` can start a machine for the WRONG session, the global-timeout queued `machine.transitionTo(...)` re-checks only the reset-cleared isTerminated flag, and offline suspend (suspendCell) drops queued live events silently.",
    "failure_scenario": "Cross-session delivery: forwardToChild (lines 568-575) captures the machine object; the child terminates on an earlier queued task (reset -> pool return -> chains.remove), is re-borrowed by another session, then the stale task runs `m.fire(E)` — fire's guard `!started || terminated || registry == null` (Machine.java:334) all re-armed by the new session, so session A's hangup drives a transition in session B. Stale start: cleanupChildInternal resets a spawned-but-not-yet-started child inline; after re-borrow the OLD queued start() starts the machine for the new session, and the new session's own queued start() then throws 'already started' -> forceCleanupAll kills the innocent session (line 934-939). Offline: suspendCell (line 775) removes the chain key and resets the machine while queued events on the old chain still execute — the late-event guard drops them at DEBUG, so an event that arrived while the session was live (e.g. ServiceEnd) is lost forever and the suspended session over-bills until a dead-man timeout."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 755,
    "summary": "Pool integrity is unprotected: suspendCell (offline path) has no dedup against a concurrent onCellTerminated, so both return the SAME machine to the pool (ObjectPoolManager.returnObject has no containment check and an unbounded queue) — and borrowAndStart's 'remedy' for borrowing a non-idle instance is returnObject(m), which unconditionally resetForReuse()'s a machine that is LIVE in another session.",
    "failure_scenario": "Supervisor enters an .offline() state; onCellWentOffline iterates `suspendCell(parentId, m.getTypeName())` (line 755) while a child concurrently reaches terminal on its own chain: both scan the list before either removes (neither checks list.remove's return), both reach pool.returnObject -> the instance sits in `available` twice; two later borrows hand ONE machine to TWO sessions sharing currentState/context. Worse, when a later borrow detects the duplicate via `if (!m.isIdle()) { pool.returnObject(m); ... }` (line 902), returnObject runs resetForReuse() on the OTHER session's live machine mid-call — its registry/context/machineId are nulled, every subsequent fire silently no-ops, and no SDR is ever written — then offers the instance a third time."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 1030,
    "summary": "chainSubmit + BoundedVirtualThreadExecutor interact fatally under saturation: with the base future complete and the executor at capacity, thenRunAsync's executeOrInline runs the task INSIDE ConcurrentHashMap.compute — a state action that publishes to its own cell (the standard SessionSupervisor.admit() path) then throws IllegalStateException 'Recursive update' and the event is swallowed (empirically reproduced); BVTE additionally leaks a permit and throws into the caller when execute() is rejected after close (BoundedVirtualThreadExecutor.java:77), and inline tasks hold no permit so awaitIdle() reports idle while a cell task is still running, letting shutdown() close executors/timeouts/pools under a live state action.",
    "failure_scenario": "Load spike saturates `work`: chainSubmit's compute returns thenRunAsync on a completed base, executeOrInline runs the cell task inline inside the mapping function; the task is admit() -> publishEvent(AdmissionDecided) -> onInboundEvent -> chainSubmit SAME key -> CHM throws 'Recursive update' -> caught and logged as 'supervisor.handleInbound threw' — the admission decision vanishes and the session hangs until timeout. Exactly under overload — the moment correctness matters most — events are dropped; submitPersist has the same shape and additionally runs blocking JDBC under the CHM bin lock, stalling unrelated cells."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 988,
    "summary": "Terminal-state crash/failure windows resurrect finished sessions: the FINAL-state snapshot is saved before the asynchronously-queued delete, JdbcPersistenceProvider.delete swallows SQLException entirely (delete failures are permanent orphans, not 'benign'), and onInboundEvent's rehydration branch never consults the `terminated` set — a late event between quota release and the async delete restores a finished session from its stale snapshot and re-acquires its quota unchecked.",
    "failure_scenario": "(a) Crash (or one transient DB error at delete time, JdbcPersistenceProvider.java:199) after save(finalState): startup recovery rehydrates the finished session; its matured final-state self-timeout makes rehydrate() call transitionTo(final) which RE-RUNS the final state's entry action — violating the 'entry not replayed' spec and duplicating terminal side effects for any machine whose close-equivalent isn't context-idempotent — while transiently re-acquiring quota and churning the pool. (b) No crash needed: supervisor terminates (quota released, line 1012), delete still queued behind a persist backlog; a late wire event finds supervisorOf==null, restoreAllCellsFor loads the stale pre-final snapshot, reacquireQuotaOnRestore (line 868) bumps the partner counter for a session that already ended — a zombie occupying a quota slot."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/flat/Registry.java",
    "line": 832,
    "summary": "Restore-path holes: the global wall-clock timeout is NEVER rescheduled on any restore path (and is cancelled permanently on offline suspend) so restored sessions have no lifetime cap; a rehydrate failure force-cleans the cell, which DELETES the only recovery snapshot (a bad deploy erases all in-flight persisted state); and restoreOneCell leaks a borrowed machine and throws the raw serializer exception to the wire when context deserialization fails.",
    "failure_scenario": "(a) Node restarts mid-call with .globalTimeout(5, MINUTES, ...) configured: recoverUnfinishedOnStartup resumes the session but only dispatch() ever calls scheduleGlobalTimeout (line 275) and MachineSnapshot carries no dispatch-time field — the session (and its quota slots) can live forever. (b) Deploy a build that renamed a state / changed the context shape while unfinished snapshots exist: Machine.rehydrate throws 'Saved state not found', the catch runs forceCleanupAll (line 882) whose termination ritual queues persistence.delete — the recovery data is destroyed by the recovery path instead of being left for a fixed build. (c) A corrupt snapshot: `SnapshotSerializer.contextFromBase64Json` (line 863) sits outside any try after the pool borrow at line 855 — each inbound event for that id borrows-and-leaks another machine and throws the raw RuntimeException into the channel thread."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/session/SessionSupervisor.java",
    "line": 312,
    "summary": "close() wraps buildSdr(), sdrSink() and the write in one EMPTY catch after ctx.outcome is already stamped — a throw from the domain's buildSdr hook (or the sink) loses the SDR permanently, unloggably and unretryably, contradicting both the 'SDR always' guarantee and the catch comment ('the loss is logged by the sink side' — false when buildSdr itself throws).",
    "failure_scenario": "Domain buildSdr NPEs on a field that is null on the admission-reject path: exception swallowed with zero log lines anywhere; because `ctx.outcome` was set at line 302 BEFORE the try, the idempotence gate `if (ctx.outcome != null) return;` (line 301) blocks any re-entry — that session's billing/audit record is gone forever with no trace. A 2-minute sink outage silently loses the SDR of every session that terminates in the window: undetectable revenue loss on the exact paths the base was built to protect."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/session/SessionSupervisor.java",
    "line": 157,
    "summary": "Subclass hooks run inside side-effecting transition GUARDS, and the framework converts any hook throw into guard-false with at most a debug-sampled log (Machine.java:358), while entry/exit action exceptions are swallowed with NO logging at all (Machine.java:417/466) — a hook crash silently drops the event or flips the session outcome.",
    "failure_scenario": "onSignalingDone throws on a malformed grant: the ADMITTED->ACTIVE guard (lines 157-160) 'fails', SignalingDone is dropped, the signaling child is already terminal so nobody re-sends — a successfully-signaled session dies of ADMITTED timeout as FAILED/'silent'. onSettled throws: the TEARING_DOWN->SUCCEEDED guard (lines 183-187) falls through to `.on(Settled.class, FAILED)` — a delivered session is recorded FAILED and its totals lost. Any entry action that throws half-way (balance reserved, reservation id not yet recorded) is silently 'successful': the half-mutated context is persisted as a clean transition with no log line to debug from."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/registry/internal/QuotaController.java",
    "line": 40,
    "summary": "Quota exactness edges: rebindQuotaKeys' failure rollback re-acquires the old keys UNCHECKED while dispatch's tryAcquire deliberately runs outside quotaLock, so a racing dispatch pushes the partner counter to cap+N; a rejected dispatch/rebind still burns a TPS token from earlier-checked dimensions (under-admission); and the four counter maps never remove zero-count entries — unbounded heap growth for high-cardinality keys (per-user/per-MAC, the wifi rebind use case).",
    "failure_scenario": "Partner P at cap 10: rebind releases P (9), its new-key tryAcquire fails, and in the gap a lock-free dispatch (flat/Registry.java:260) admits an 11th session before the rollback `acquireUnchecked(old)` (line 323) restores P to 11 — repeatable to cap+N. TPS: 100 requests for P all rejected on a full route each consumed a partner TPS token (no rollback, line 96) so request 101 to an EMPTY route is rejected PARTNER_TPS_EXCEEDED despite zero admissions that second. Memory: `partnerActive`/`routeActive`/`partnerTps`/`routeTps` (lines 40-47) have no remove() anywhere — 10M distinct subscriber keys over weeks = 10M dead AtomicInteger/TpsBucket entries."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/channel/Channel.java",
    "line": 29,
    "summary": "The Channel abstraction cannot support correct Kafka/Redis/HTTP implementations: no ack/completion signal in either direction (commit-on-handler-return is wrong because onInboundEvent merely queues), no lifecycle (start/stop/drain — Registry.shutdown never touches channels, and post-shutdown events are silently swallowed after the consumer committed them), no backpressure or threading contract, no typed retrieval (getChannel() returns Channel<?,?> so every send site needs an unchecked cast) — and on the production flat stack the inbound half is simply DEAD: Builder.channel(...) stores the channel but nothing ever calls channel.onInbound(...) (the only wiring in the codebase is the legacy Statewalk.wireInbound, api stack).",
    "failure_scenario": "An author binds a Kafka channel to a flat Registry per the Channel javadoc ('a registry calls this once during initialisation'): outbound send() works, but every inbound event vanishes — TestChannel.inject hits the null-handler guard and does nothing, no error, no log. Even with manual wiring, at-least-once is unimplementable: when the handler returns, the event is only queued on the per-cell chain — a crash before machine.fire loses it after the offset commit — and onInboundEvent both throws (unknown id -> IllegalStateException into the consumer's poll loop) and blocks (synchronous persistence.load on the rehydrate path) on the consumer thread."
  },
  {
    "file": "/home/mustafa/telcobright-projects/statewalk/src/main/java/com/telcobright/statewalk/v2/registry/api/Registry.java",
    "line": 63,
    "summary": "The library ships TWO complete, drifting registry stacks (flat/Registry 1219 lines vs registry/api/Registry 954 lines + MultiRegistry/Statewalk/StatewalkSystem, plus two unrelated classes both named InternalEventResolver) duplicating dispatch gates, chains, global timeout, persistence, rehydration, quota, and the termination ritual — and the fixes have forked: the api stack still blocks on synchronous JDBC inside the machine's transition path (line 650), never re-acquires quota on rehydrate (line 561 — the exact bug commit 1fd4279 fixed in flat only, despite its offline path's comment claiming re-acquire happens), registers quota keys AFTER the machine is visible (line 437 — permanent slot leak on a terminate race), leaks MultiRegistry.activeCells entries for every offline-suspended id, and QuotaController.release() ungated by limits goes negative when api's per-dispatch getQuotaLimits() toggles a cap.",
    "failure_scenario": "Every future fix must land twice or silently miss one stack — flat already gained async persistence, restore re-acquire, and offline cascade that api never received. Concretely on api today: a node restart rehydrates 500 sessions holding zero quota slots (caps over-admit by a full cap per partner); a slow store back-pressures event processing directly because save() runs inside the synchronized transition; and identical-simple-named InternalEventResolver classes with different contracts guarantee wrong-import bugs in downstream call/SMS/HTTP/WiFi code. For v3 one stack must be chosen (flat is the evolved one) and the other deleted or reduced to a thin adapter."
  }
]
```

---

## Assessment beyond the findings (the parts of the brief a bug list can't carry)

**Verdict.** The flat stack's architecture is genuinely good — spec-based machines, pooled cells, mandatory timeouts, one-SDR-with-history is the right foundation shape, and things like `PooledFieldValidator`, `SessionTimings` validation, quota rebind's lock discipline, and the frozen `StateMap` builder invariants are well built. But it is **not yet a rock-solid base**: the failure modes cluster in exactly the areas the brief asked about (teardown vs late work, pooling identity, the serial invariant, crash windows), and the root causes are systemic, not incidental:

1. **No identity/generation token.** Machines, timers, and queued chain tasks are correlated by object reference + state name + resettable flags. Every stale-fire, cross-session, and double-release bug above traces to this. v3's single highest-leverage change: a monotonically increasing per-borrow **epoch** stamped on the machine, captured by every timer/queued task/handle, and checked at the door.
2. **Two cleanup disciplines.** Terminal cleanup dedups by a time-window set; suspend doesn't dedup at all; queued vs inline cleanup race the `active` map. v3 should have ONE structural claim ("the thread that removes the machine from the row owns its cleanup", e.g. via an atomic per-cell state field: LIVE→SUSPENDING/TERMINATING), no 60s TTL set, and the cascade must capture the child list before removing the row (or terminate children synchronously before the supervisor's row removal).
3. **Terminal work must be unconditional.** SDR + teardown must run on *every* exit path — give the registry a `onCellRetired(reason)` hook that the session base uses to emit a FAILED/ABORTED SDR when the FSM never reached terminal (shutdown, failRequest, evict), and log every swallowed exception (entry/exit/guard/SDR) at WARN with the machine id.
4. **Timeouts belong on the cell chain.** Fire timeouts by *submitting* to the cell's chain (with epoch check), not by running on scheduler threads; that alone restores the serial invariant and removes the split-lock windows.

**Reusability of `v2.session.SessionSupervisor` for call/SMS/HTTP/WiFi.** The hook surface (admission/spawn/active/teardown/SDR/retry/settle) is the right vocabulary and the graph fits all four domains. Weak points to fix before migration: hooks inside guards (needs first-class *transition actions* in StateMap so `on(Event, TARGET, action)` exists — that also removes the side-effecting-guard bug class), the retry contract (base should own cleanup-then-respawn ordering with a fresh child instance name or epoch), `endCause` precedence rules, monotonic timings alongside wall-clock, and `settlesAsync`/`sessionSucceeded` are fine as-is. Note today's consumers (routesphere `SmsSupervisor`, wifi-sphere `WifiSessionSupervisor`) still extend flat `Supervisor` directly — migrating them onto the hardened session base should be a v3 acceptance test.

**Channel.** Needs a real contract before Kafka/Redis are attempted: `start/stop/drain` lifecycle owned by the registry; a per-event `CompletionStage`/ack callback so consumers can commit offsets after the cell actually processed (or dead-letter on reject); a documented threading rule ("handler must be non-blocking; the registry never throws into it" — return a `DispatchResult` instead); typed retrieval (`<O,I> Channel<O,I> channel(ChannelKey<O,I>)`); and wire `onInbound` in the flat builder (or delete the parameter until it does something). Redis: worth adding as a *persistence provider* (hash per machineId + a registry index set, MULTI for save+index; much faster restore scans than the JDBC N+1) before adding it as a channel.

**Registry lifecycle.** Collapse `evict/offline/finish` semantics into one documented cell-state machine; make restore atomic per id (single-flight per parentId lock covering probe→borrow→register→rehydrate); persist dispatch-time in `MachineSnapshot` so the global timeout survives restore; batch startup recovery from the already-loaded snapshots instead of N+1 point loads.

## Recommended v3.0 plan (`com.telcobright:statewalk`)

1. **M1 — collapse to one stack.** Delete `registry/api/*` + `MultiRegistry`/`Statewalk`/`StatewalkSystem` or rebuild them as thin adapters over `flat`; rename `flat` → core packages under the new coordinates; rename one `InternalEventResolver`.
2. **M2 — identity & cleanup rewrite (fixes findings 1,2,4,5,6,7):** per-borrow epoch; atomic cell-state claim replacing the 60s set; timeouts via the cell chain; putIfAbsent-style dispatch/restore claims; two-arg removes in TimeoutManager; pool containment/cap.
3. **M3 — terminal guarantees (findings 3,10,11,12):** `onCellRetired` SDR backstop; transition actions in StateMap; log every swallowed exception; SDR write moved off the machine monitor (queue to a sink executor with retry).
4. **M4 — crash-consistency (findings 8,9):** fix chainSubmit to append outside `compute` (compute only swaps the future; build the continuation after), delete-before-release ordering or terminal tombstones, rehydration-failure quarantine (move snapshot to a dead-letter table, never delete), global-timeout persistence, optional sync-persist mode for domains that need write-ahead semantics, an allowlist for `SnapshotSerializer` classes.
5. **M5 — Channel v2 + Redis persistence provider; quota dimension map** (named dimensions instead of hard-coded partner/route; prune zero-count entries).
6. **M6 — migration:** port `SmsSupervisor`/`CallSupervisor`/`WifiSessionSupervisor` onto the hardened `SessionSupervisor`; add the missing test classes this audit exposed (cascade-with-live-children asserting pool return counts, id-reuse within 60s, retry-within-60s, shutdown-emits-SDRs, saturation soak asserting zero dropped admissions).

All 15 findings were verified CONFIRMED or PLAUSIBLE-with-named-trigger against the source (finding 8's recursive-update mechanism reproduced empirically in a standalone test); none were refuted by a guard elsewhere. The single most urgent pre-production fixes are findings 1–3: they fire on *every* multi-cell teardown, *every* fast retry, and *every* shutdown respectively.

---

## Supplementary sweep findings (below top-15 cutoff)

**Refuted (1):** the claim that `SessionSupervisor.defineRoutes` being `final` blocks domain routing is wrong — the base explicitly delegates to the abstract `defineDomainRoutes(r)` hook (SessionSupervisor.java:88, 132), which is the sanctioned way for subclasses to add forward/drop rules.

**Confirmed/plausible, below the top-15 cutoff** (none outranks the 15 already reported; they stand as supplementary findings for the v3 backlog):

| File | Finding |
|---|---|
| `flat/Registry.java:563` + `flat/InternalEventResolver.java:114` | A `forwardTo("typo")` child name is never validated at build time and drops every event at DEBUG — `referencedChildNames()` exists for exactly this check but nothing calls it. Fix: validate route targets against registered types in `Registry.Builder.build()`. (M3) |
| `registry/consumes/EventTypeRegistry.java:84` | The event-type contract is dead on the flat stack: `requireRegistered()` and `returnIfPoolable()` are only called by the legacy api stack, so the documented throw-on-unregistered guarantee is false and poolable events are never returned. Folds into finding 15 (dual-stack drift). |
| `registry/consumes/EventTypeRegistry.java:73` | `registerPoolable` uses `put` (not idempotent) — a duplicate registration silently replaces the pool mid-flight. |
| `session/SessionHistory.java:34` | The 1000-entry cap drops the **newest** records — a chatty session loses exactly its tail (teardown transitions, end cause) from the SDR. Fix in M3: ring buffer keeping head + tail, or evict oldest. |
| `channel/TestChannel.java:42` | `inject()` silently no-ops with no handler and runs handlers synchronously — masks the dead flat inbound wiring (finding 14) and the reentrancy races a real channel would hit. |
| `pipeline/StepTrace.java:15` | Dead code: no references in main or test; the trace-collector API its javadoc promises doesn't exist. Delete or implement in v3. |

The main report stands unchanged: the 15-finding JSON above, the systemic root-cause assessment (missing borrow-epoch identity, split cleanup disciplines, non-unconditional terminal work, timers off the cell chain), and the M1–M6 v3.0 plan. The sweep's confirmations strengthen M3 (add route-target validation and the history ring buffer) and M1 (EventTypeRegistry's dead contract is one more casualty of the dual stack).
---

# STATUS UPDATE (2026-09-01): v3.0 FIX IMPLEMENTED

The fix described by the plan above has been **implemented** in
`/home/mustafa/telcobright-projects/statewalk` (staged in git, not yet committed).
All 15 findings + the 6 supplementary findings are closed. 85 tests green
(`mvn test`), including a new `RegistryHardeningV3Test` regression suite that
reproduces each audit bug class. Coordinates are now
`com.telcobright:statewalk:3.0.0-SNAPSHOT`; see the repo README for the package
map and the v2→v3 migration steps (needed by routesphere CallSupervisor/
SmsSupervisor and wifi-sphere WifiSessionSupervisor).

Key mechanisms another agent should know before touching the code:
- Cell lifecycle claims (LIVE→TERMINATING|SUSPENDING CAS) own all cleanup; the
  60s dedup set is gone.
- Machine carries two identity tokens: borrow `epoch` (checked by every queued
  task/timer) and a state-visit token (checked by state timers).
- State timers are routed through the per-cell serial chain via the registry
  handle — never run on scheduler threads directly.
- Forced failover (`Machine.forceFailover`) drives live machines to their
  current state's timeout target on shutdown/global-timeout/persist-failure —
  that is what guarantees SDR-on-every-exit-path.
- chainSubmit appends OUTSIDE ConcurrentHashMap.compute and always executes on
  the executor (never inline) — do not "optimize" this back.
- Restore: single-flight per id, supervisor-snapshot-first, final-state
  snapshots are purge-only tombstones, failures quarantine to a dead-letter
  area (never delete), the persisted global deadline is re-armed.

---

# STATUS UPDATE #2 (2026-09-01): v3.0 API finalised and pushed

All work is committed and pushed to origin/master (github.com:pialmmh/statewalk),
commits fb2f293..96ac85e. 96 tests green. On top of the core rewrite:

- The registry is now GENERIC and renamed: `StatemachineRegistry<T>`
  (T = the supervisor's context/task type; typed dispatch/first-event/quota
  extractor). Builder-only construction everywhere.
- Builder lambdas per machine type: constructor/factory, `.resetHook(typeName,
  m -> ...)` (pool-return cleanup; registering one legalises mutable props on
  that type), `.volatileLoader`, `.preWarmContextClass(...)` (restored v2
  param). State lambdas: onEntry/onExit, guards + transition actions, stay
  handlers.
- Timeouts: every state mandatory, two modes — `.timeout(d,u,finalTarget)` or
  `.timeoutStay(d,u[,action])` (checkpoint: run action, re-persist context
  with refreshed deadline, re-arm). Global lifetime timeout persisted.
- Rehydration: seats saved state, never replays entry actions; matured target
  deadline → immediate transition; matured stay deadline → immediate
  checkpoint + re-arm; unmatured → remaining slice.

NEXT (other repos, not started): migrate routesphere CallSupervisor/
SmsSupervisor and wifi-sphere WifiSessionSupervisor onto
com.telcobright:statewalk:3.0.0-SNAPSHOT session.SessionSupervisor —
migration steps are in the statewalk README.
