package com.telcobright.statewalk.session;

import com.telcobright.statewalk.registry.InternalEventResolver;
import com.telcobright.statewalk.registry.Supervisor;
import com.telcobright.statewalk.session.events.AdmissionDecided;
import com.telcobright.statewalk.session.events.ServiceEnd;
import com.telcobright.statewalk.session.events.SettleRequest;
import com.telcobright.statewalk.session.events.Settled;
import com.telcobright.statewalk.session.events.SignalingDone;
import com.telcobright.statewalk.session.events.SignalingFailed;
import com.telcobright.statewalk.session.events.SignalingProgress;
import com.telcobright.statewalk.state.StateMap;

import java.util.concurrent.TimeUnit;

/**
 * THE generic session supervisor — the reusable base every protocol
 * implementation extends (wifi, call, sms, http). One session = one call,
 * whatever the domain:
 *
 * <pre>
 *   ADMITTING ──accept──► ADMITTED ──SignalingDone──► ACTIVE ──ServiceEnd──► TEARING_DOWN ──Settled──► SUCCEEDED
 *       │                     │  ▲__retry(nextAttempt)     │                       │
 *       └── reject ───────────┴── SignalingFailed / abort ─┴───(dead-man)──────────┴──────────────────► FAILED
 * </pre>
 *
 * The base owns the generic responsibilities — admission, the signaling
 * window, retry, teardown, settlement — and <strong>guarantees exactly one
 * {@link SdrRecord} per session from BOTH terminal states</strong>, embedding
 * the full {@link SessionHistory} (every transition of every machine of the
 * cell, tapped via {@code Machine.onTransitioned}). A session that never did
 * anything still ends here (ADMITTED timeout → FAILED → SDR) — no orphans.
 * The registry's forced-failover paths (shutdown, global timeout,
 * persistence failure) also land in FAILED, so the SDR ships on EVERY exit
 * path (v3).
 *
 * <p>The domain detail lives in the SIGNALING child (spawned by
 * {@link #spawnChildren}) and an optional budget child; they talk to the base
 * with the {@code session.events} vocabulary ({@link SignalingDone},
 * {@link SignalingFailed}, {@link ServiceEnd}, {@link Settled}).
 *
 * <p><b>Hook discipline (v3):</b> domain payload-copy hooks
 * ({@link #onSignalingDone}, {@link #onSettled}, endCause capture) run as
 * <em>transition actions</em> — after the routing decision, exception-shielded
 * by the framework — never inside guards, so a hook throw can neither drop the
 * event nor flip the session outcome.
 *
 * <p>Subclass rules (enforced by the framework): only {@code final} fields
 * (pool validator); {@link #defineDomainRoutes} runs from the constructor —
 * reference event classes and child names only, never instance fields.
 */
public abstract class SessionSupervisor<C extends SessionContext> extends Supervisor<C> {

    public static final String ADMITTING = "ADMITTING";
    public static final String ADMITTED = "ADMITTED";
    public static final String ACTIVE = "ACTIVE";
    public static final String TEARING_DOWN = "TEARING_DOWN";
    public static final String SUCCEEDED = "SUCCEEDED";
    public static final String FAILED = "FAILED";

    // ─────────────────────────────────────────────────────────────────
    // The override surface
    // ─────────────────────────────────────────────────────────────────

    /** Per-state timeouts, from the domain's policy. Called lazily at start (fields are set). */
    protected abstract SessionTimings timings();

    /**
     * Admission: identify, authorize, route, reserve. Runs synchronously in
     * ADMITTING.entry on the cell's chain; copy accepted data into the context
     * here. Must not throw — a throw is treated as a reject with the
     * exception's toString as cause (original preserved in the history).
     */
    protected abstract AdmissionVerdict runAdmission(C ctx);

    /**
     * Spawn the signaling child (and budget/aux children) via
     * {@code r.spawnChild(name, childCtx)}. Child contexts must share
     * {@code ctx.history} (implement {@link HistoryCarrier} with it) so the
     * cell writes one timeline.
     */
    protected abstract void spawnChildren(InternalEventResolver r, C ctx);

    /** Deliver the service (wifi: gate release + policer + acct-start). ACTIVE.entry. */
    protected abstract void onActive(C ctx);

    /** Stop the service (wifi: garden + policer clear + NAT flush + acct-stop). TEARING_DOWN.entry, and the FAILED backstop. */
    protected abstract void onTeardown(C ctx);

    /** The domain SDR payload; embedded in the generic {@link SdrRecord}. */
    protected abstract Object buildSdr(C ctx, String outcome);

    /** Where SDRs go. Return a final-field sink. */
    protected abstract SdrSink sdrSink();

    /** Child-forward routes ONLY (the base already self-handles its own vocabulary). Constructor-time: no field access. */
    protected abstract void defineDomainRoutes(InternalEventResolver r);

    /** Copy the grant payload of {@link SignalingDone} into the context. Runs as a transition action. */
    protected void onSignalingDone(C ctx, Object grant) { }

    /** Copy the totals payload of {@link Settled} into the context. */
    protected void onSettled(C ctx, Object totals) { }

    /** Informative signaling phase reports while ADMITTED. */
    protected void onSignalingProgress(C ctx, String phase) { }

    /**
     * Retry decision after a signaling failure (sms/http: next route). Return
     * true to retry — the base then runs {@link #cleanupBeforeRetry} (default:
     * retire ALL children of the failed attempt), increments
     * {@code ctx.attempts}, and calls {@link #spawnChildren} again. The v3
     * base owns the cleanup-then-respawn ordering, so respawning the same
     * child type name is always safe. Default: no retry.
     */
    protected boolean nextAttempt(C ctx, String failureCause) { return false; }

    /**
     * Clean up the failed attempt's children before a retry respawn. Default
     * retires every live child; override to keep long-lived children (e.g. a
     * budget child that must survive attempts): call
     * {@code r.cleanupChild(name)} for just the signaling child instead.
     */
    protected void cleanupBeforeRetry(InternalEventResolver r, C ctx) {
        r.cleanupChildren();
    }

    /** Terminal-side domain cleanup BEFORE the SDR is written (wifi: eject the plane row). Never throws upward. */
    protected void onEnded(C ctx, String outcome) { }

    /**
     * true = a budget child answers {@link SettleRequest} with {@link Settled}
     * (async settle, the call/wifi shape). false = no budget child: teardown
     * settles inline and the terminal transition chains immediately.
     */
    protected boolean settlesAsync() { return true; }

    /** Outcome rule: default = the session delivered service at some point. */
    protected boolean sessionSucceeded(C ctx) { return ctx.activatedAtMs > 0; }

    // ─────────────────────────────────────────────────────────────────
    // Routing — base vocabulary self-handled; domain adds forwards
    // ─────────────────────────────────────────────────────────────────

    @Override
    protected final void defineRoutes(InternalEventResolver r) {
        r.selfHandle(AdmissionDecided.class);
        r.selfHandle(SignalingProgress.class);
        r.selfHandle(SignalingDone.class);
        r.selfHandle(SignalingFailed.class);
        r.selfHandle(ServiceEnd.class);
        r.selfHandle(Settled.class);
        defineDomainRoutes(r);
    }

    // ─────────────────────────────────────────────────────────────────
    // The generic graph
    // ─────────────────────────────────────────────────────────────────

    @Override
    protected final StateMap defineStates() {
        SessionTimings t = timings();
        return StateMap.builder()
            .initialState(ADMITTING)

            .state(ADMITTING)
                .interim()
                .timeout(t.admittingSec(), TimeUnit.SECONDS, FAILED)
                .onEntry(self -> ((SessionSupervisor<?>) self).admit())
                .on(AdmissionDecided.class, ADMITTED,
                    (self, e) -> ((AdmissionDecided) e).accepted())
                .on(AdmissionDecided.class, FAILED)

            .state(ADMITTED)
                .interim()
                .timeout(t.admittedSec(), TimeUnit.SECONDS, FAILED)
                .onEntry(self -> ((SessionSupervisor<?>) self).firstSpawn())
                .on(SignalingDone.class, ACTIVE, null, (self, e) ->
                    ((SessionSupervisor<?>) self).grantArrived((SignalingDone) e))
                .stay(SignalingProgress.class, (self, e) ->
                    ((SessionSupervisor<?>) self).progress((SignalingProgress) e))
                .stay(SignalingFailed.class, (self, e) ->
                    ((SessionSupervisor<?>) self).signalingFailed((SignalingFailed) e))
                .stay(ServiceEnd.class, (self, e) ->
                    ((SessionSupervisor<?>) self).abortPreActive((ServiceEnd) e))

            .state(ACTIVE)
                .interim()
                // dead-man backstop only — the REAL caps (minutes/volume/idle/media)
                // live in the domain's budget child and arrive as ServiceEnd
                .timeout(t.activeMaxSec(), TimeUnit.SECONDS, FAILED)
                .onEntry(self -> ((SessionSupervisor<?>) self).activate())
                .on(ServiceEnd.class, TEARING_DOWN, null, (self, e) ->
                    ((SessionSupervisor<?>) self).recordEnd((ServiceEnd) e))

            .state(TEARING_DOWN)
                .interim()
                .timeout(t.tearingDownSec(), TimeUnit.SECONDS, FAILED)
                .onEntry(self -> ((SessionSupervisor<?>) self).teardown())
                // The one deliberately impure guard in the base: the settle
                // decision must SEE the totals, so the shielded decision step
                // applies them first and then answers success/failure —
                // exceptions inside cannot flip the route (they fall back to
                // the activation rule). The FAILED fallback below needs no
                // action: the decision already applied the totals.
                .on(Settled.class, SUCCEEDED, (self, e) ->
                    ((SessionSupervisor<?>) self).settleDecision((Settled) e))
                .on(Settled.class, FAILED)

            .state(SUCCEEDED)
                .finalState()
                .timeout(1, TimeUnit.SECONDS, SUCCEEDED)
                .onEntry(self -> ((SessionSupervisor<?>) self).close(SUCCEEDED))

            .state(FAILED)
                .finalState()
                .timeout(1, TimeUnit.SECONDS, FAILED)
                .onEntry(self -> ((SessionSupervisor<?>) self).close(FAILED))

            .build();
    }

    // ─────────────────────────────────────────────────────────────────
    // History tap — the supervisor's own transitions
    // ─────────────────────────────────────────────────────────────────

    @Override
    protected final void onTransitioned(String fromState, String toState, String causeHint) {
        C ctx = getContext();
        if (ctx == null) return;
        ctx.history.transition(ctx.historyName(), fromState, toState, causeHint);
    }

    /** A registry-forced failover (shutdown, global timeout, persistence
     *  failure) stamps its reason as the end cause, so the SDR says WHY. */
    @Override
    protected final void onForcedFailover(String reason) {
        C ctx = getContext();
        if (ctx == null) return;
        if (ctx.endCause == null) ctx.endCause = reason;
        ctx.history.note(ctx.historyName(), "forced failover: " + reason);
    }

    // ─────────────────────────────────────────────────────────────────
    // Base steps (named orchestration; domain work behind the hooks)
    // ─────────────────────────────────────────────────────────────────

    private void admit() {
        C ctx = getContext();
        if (ctx.createdAtMs == 0) ctx.createdAtMs = System.currentTimeMillis();
        AdmissionVerdict v;
        try {
            v = runAdmission(ctx);
        } catch (RuntimeException e) {
            ctx.history.note(ctx.historyName(), "admission threw: " + e);
            v = AdmissionVerdict.reject("admission-error: " + e);
        }
        if (!v.accepted() && ctx.endCause == null) ctx.endCause = v.rejectCause();
        publishEvent(new AdmissionDecided(v.accepted(), v.rejectCause()));
    }

    private void firstSpawn() {
        C ctx = getContext();
        ctx.attempts = 1;
        spawnChildren(resolver, ctx);
    }

    private void grantArrived(SignalingDone done) {
        onSignalingDone(getContext(), done.grant());
    }

    private void progress(SignalingProgress p) {
        onSignalingProgress(getContext(), p.phase());
    }

    private void signalingFailed(SignalingFailed f) {
        C ctx = getContext();
        boolean retry;
        try { retry = nextAttempt(ctx, f.cause()); }
        catch (RuntimeException e) {
            ctx.history.note(ctx.historyName(), "nextAttempt threw: " + e);
            retry = false;
        }
        if (retry) {
            ctx.attempts++;
            ctx.history.note(ctx.historyName(), "retry attempt " + ctx.attempts + " after: " + f.cause());
            try { cleanupBeforeRetry(resolver, ctx); }
            catch (RuntimeException e) {
                ctx.history.note(ctx.historyName(), "cleanupBeforeRetry threw: " + e);
            }
            spawnChildren(resolver, ctx);
            return;
        }
        if (ctx.endCause == null) ctx.endCause = f.cause();
        transitionTo(FAILED);
    }

    private void abortPreActive(ServiceEnd e) {
        C ctx = getContext();
        if (ctx.endCause == null) ctx.endCause = e.cause();
        transitionTo(FAILED);
    }

    private void activate() {
        C ctx = getContext();
        ctx.activatedAtMs = System.currentTimeMillis();
        onActive(ctx);
    }

    private void recordEnd(ServiceEnd e) {
        C ctx = getContext();
        if (ctx.endCause == null) ctx.endCause = e.cause();
    }

    private void teardown() {
        C ctx = getContext();
        ctx.tornDown = true;
        try { onTeardown(ctx); }
        catch (RuntimeException e) { ctx.history.note(ctx.historyName(), "teardown threw: " + e); }
        if (settlesAsync()) {
            publishEvent(new SettleRequest());
        } else {
            transitionTo(succeededNow() ? SUCCEEDED : FAILED);
        }
    }

    /**
     * The settle decision: apply the totals, then answer success/failure.
     * Both steps are exception-shielded — a domain hook throw is recorded in
     * the history and the outcome falls back to the activation rule, so it
     * can never silently flip a delivered session to FAILED.
     */
    private boolean settleDecision(Settled s) {
        C ctx = getContext();
        try { onSettled(ctx, s.totals()); }
        catch (RuntimeException e) {
            LOG.warn("[{}] onSettled threw: {}", getMachineId(), e.toString());
            ctx.history.note(ctx.historyName(), "onSettled threw: " + e);
        }
        return succeededNow();
    }

    private boolean succeededNow() {
        C ctx = getContext();
        try { return sessionSucceeded(ctx); }
        catch (RuntimeException e) {
            LOG.warn("[{}] sessionSucceeded threw: {} — falling back to activation rule",
                getMachineId(), e.toString());
            ctx.history.note(ctx.historyName(), "sessionSucceeded threw: " + e);
            return ctx.activatedAtMs > 0;
        }
    }

    /**
     * Terminal close — BOTH outcomes land here; the SDR is unconditional.
     * Idempotent (a re-entered terminal writes nothing twice).
     *
     * <p>v3 hardening: a {@link #buildSdr} throw no longer loses the record —
     * a fallback envelope (without the domain payload) still ships, with the
     * failure noted in the history; a sink throw is logged as an ERROR with
     * the session key, never silently swallowed.
     */
    private void close(String outcome) {
        C ctx = getContext();
        if (ctx == null) return;                      // forced failover raced a reset — nothing to record
        if (ctx.outcome != null) return;
        ctx.outcome = outcome;
        ctx.endedAtMs = System.currentTimeMillis();
        if (ctx.endCause == null) ctx.endCause = ctx.activatedAtMs > 0 ? "timeout" : "silent";
        if (ctx.activatedAtMs > 0 && !ctx.tornDown) {   // dead-man path: TEARING_DOWN never ran
            ctx.tornDown = true;
            try { onTeardown(ctx); }
            catch (RuntimeException e) { ctx.history.note(ctx.historyName(), "backstop teardown threw: " + e); }
        }
        try { onEnded(ctx, outcome); }
        catch (RuntimeException e) { ctx.history.note(ctx.historyName(), "onEnded threw: " + e); }

        Object domainPayload;
        try {
            domainPayload = buildSdr(ctx, outcome);
        } catch (RuntimeException e) {
            LOG.error("[{}] buildSdr threw for session {} — shipping fallback SDR without domain payload: {}",
                getMachineId(), ctx.sessionKey, e.toString());
            ctx.history.note(ctx.historyName(), "buildSdr threw: " + e);
            domainPayload = null;
        }
        SdrRecord record = new SdrRecord(
            ctx.sessionKey, outcome, ctx.endCause,
            ctx.createdAtMs, ctx.activatedAtMs, ctx.endedAtMs,
            ctx.attempts, domainPayload,
            ctx.history.snapshot(), ctx.history.droppedCount());
        try {
            sdrSink().write(record);
        } catch (RuntimeException e) {
            // The sink contract says buffer-and-don't-throw; if it throws
            // anyway we must not crash the machine — but the loss is OURS to
            // report, loudly, with enough identity to reconcile from logs.
            LOG.error("[{}] SDR SINK WRITE FAILED for session {} outcome={} endCause={} — record lost: {}",
                getMachineId(), ctx.sessionKey, outcome, ctx.endCause, e.toString());
        }
    }
}
