package com.telcobright.statewalk.v2.session;

import com.telcobright.statewalk.v2.flat.InternalEventResolver;
import com.telcobright.statewalk.v2.flat.Supervisor;
import com.telcobright.statewalk.v2.session.events.AdmissionDecided;
import com.telcobright.statewalk.v2.session.events.ServiceEnd;
import com.telcobright.statewalk.v2.session.events.SettleRequest;
import com.telcobright.statewalk.v2.session.events.Settled;
import com.telcobright.statewalk.v2.session.events.SignalingDone;
import com.telcobright.statewalk.v2.session.events.SignalingFailed;
import com.telcobright.statewalk.v2.session.events.SignalingProgress;
import com.telcobright.statewalk.v2.state.StateMap;

import java.util.concurrent.TimeUnit;

/**
 * THE generic session supervisor — the reusable base every protocol
 * implementation extends (wifi first; call / sms / http migrate later). One
 * session = one call, whatever the domain:
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
 *
 * <p>The domain detail lives in the SIGNALING child (spawned by
 * {@link #spawnChildren}) and an optional budget child; they talk to the base
 * with the {@code session.events} vocabulary ({@link SignalingDone},
 * {@link SignalingFailed}, {@link ServiceEnd}, {@link Settled}).
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

    /** Copy the grant payload of {@link SignalingDone} into the context. */
    protected void onSignalingDone(C ctx, Object grant) { }

    /** Copy the totals payload of {@link Settled} into the context. */
    protected void onSettled(C ctx, Object totals) { }

    /** Informative signaling phase reports while ADMITTED. */
    protected void onSignalingProgress(C ctx, String phase) { }

    /**
     * Retry decision after a signaling failure (sms/http: next route). Return
     * true to retry — the base increments {@code ctx.attempts} and calls
     * {@link #spawnChildren} again (the domain owns child-name reuse/cleanup).
     * Default: no retry.
     */
    protected boolean nextAttempt(C ctx, String failureCause) { return false; }

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
                .on(SignalingDone.class, ACTIVE, (self, e) -> {
                    ((SessionSupervisor<?>) self).grantArrived((SignalingDone) e);
                    return true;
                })
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
                .on(ServiceEnd.class, TEARING_DOWN, (self, e) -> {
                    ((SessionSupervisor<?>) self).recordEnd((ServiceEnd) e);
                    return true;
                })

            .state(TEARING_DOWN)
                .interim()
                .timeout(t.tearingDownSec(), TimeUnit.SECONDS, FAILED)
                .onEntry(self -> ((SessionSupervisor<?>) self).teardown())
                .on(Settled.class, SUCCEEDED, (self, e) -> {
                    SessionSupervisor<?> m = (SessionSupervisor<?>) self;
                    m.settledArrived((Settled) e);
                    return m.succeededNow();
                })
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
        if (nextAttempt(ctx, f.cause())) {
            ctx.attempts++;
            ctx.history.note(ctx.historyName(), "retry attempt " + ctx.attempts + " after: " + f.cause());
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

    private void settledArrived(Settled s) {
        onSettled(getContext(), s.totals());
    }

    private boolean succeededNow() {
        return sessionSucceeded(getContext());
    }

    /**
     * Terminal close — BOTH outcomes land here; the SDR is unconditional.
     * Idempotent (a re-entered terminal writes nothing twice).
     */
    private void close(String outcome) {
        C ctx = getContext();
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
        try {
            sdrSink().write(new SdrRecord(
                ctx.sessionKey, outcome, ctx.endCause,
                ctx.createdAtMs, ctx.activatedAtMs, ctx.endedAtMs,
                ctx.attempts, buildSdr(ctx, outcome),
                ctx.history.snapshot(), ctx.history.droppedCount()));
        } catch (RuntimeException e) {
            // an SDR sink failure must never crash the machine; the loss is logged by the sink side
        }
    }
}
