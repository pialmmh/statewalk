package com.telcobright.paymentgw;

import com.telcobright.paymentgw.PaymentEvents.PaymentCallback;
import com.telcobright.paymentgw.PaymentEvents.PaymentCancelled;
import com.telcobright.paymentgw.PaymentEvents.ProviderSessionCreated;
import com.telcobright.paymentgw.PaymentEvents.ProviderSessionFailed;
import com.telcobright.paymentgw.PaymentEvents.RefundFailed;
import com.telcobright.paymentgw.PaymentEvents.RefundRequested;
import com.telcobright.paymentgw.PaymentEvents.RefundSucceeded;
import com.telcobright.paymentgw.PaymentEvents.Sweep;
import com.telcobright.statewalk.registry.InternalEventResolver;
import com.telcobright.statewalk.registry.Supervisor;
import com.telcobright.statewalk.state.StateMap;

import java.util.concurrent.TimeUnit;

/**
 * The payment state machine — one instance per payment attempt, pooled and
 * persisted by statewalk. The shape of the journey:
 *
 * <pre>
 *  INITIATED ──ProviderSessionCreated──► AWAITING_PAYMENT ──callback ok──► CAPTURED ──RefundRequested──► REFUNDING
 *      │                                  (HIBERNATED:                      (HIBERNATED:                    │ ok
 *      │ fail/timeout                      customer is at the               refund window;    RefundFailed ─┤
 *      ▼                                   provider's site)                 sweep→SETTLED)         ▲        ▼
 *    FAILED ◄───── callback failed ────────┤ │ cancelled                        ▲ ──────────────────┘    REFUNDED
 *                                          │ ▼                                  │
 *                          window expired  │ CANCELLED                     (back to refundable)
 *                                          ▼
 *                                        EXPIRED
 * </pre>
 *
 * <p><b>Hibernation</b>: AWAITING_PAYMENT and CAPTURED are {@code .offline()}
 * states — on entry the machine is snapshotted and EVICTED from the registry
 * (db row only, zero memory). The next inbound event for the payment id (a
 * provider callback, a cancel, a refund request, a {@link Sweep}) rehydrates
 * it; a deadline that matured while hibernated (payment window, refund
 * window) is honoured at wake/startup and settles the machine.
 *
 * <p>The external world is reached through {@link PgwProviderClient}, attached
 * as VOLATILE context (re-attached on every rehydration, never persisted).
 * Every terminal state emits exactly one {@link PaymentRecord}.
 *
 * <p>Pool discipline: the only instance field is the final timings policy —
 * all per-payment state lives in {@link PaymentContext}.
 */
public class PaymentSupervisor extends Supervisor<PaymentContext> {

    public static final String INITIATED = "INITIATED";
    public static final String AWAITING_PAYMENT = "AWAITING_PAYMENT";
    public static final String CAPTURED = "CAPTURED";
    public static final String REFUNDING = "REFUNDING";
    public static final String REFUNDED = "REFUNDED";
    public static final String CANCELLED = "CANCELLED";
    public static final String FAILED = "FAILED";
    public static final String EXPIRED = "EXPIRED";
    public static final String SETTLED = "SETTLED";

    /** The volatile service bundle the gateway's volatileLoader attaches. */
    record Services(PgwProviderClient provider, PaymentRecordSink sink) {}

    private final PaymentTimings timings;

    public PaymentSupervisor(PaymentTimings timings) {
        this.timings = timings;
    }

    @Override
    protected void defineRoutes(InternalEventResolver r) {
        r.selfHandle(ProviderSessionCreated.class);
        r.selfHandle(ProviderSessionFailed.class);
        r.selfHandle(PaymentCallback.class);
        r.selfHandle(PaymentCancelled.class);
        r.selfHandle(RefundRequested.class);
        r.selfHandle(RefundSucceeded.class);
        r.selfHandle(RefundFailed.class);
        r.selfHandle(Sweep.class);
    }

    @Override
    protected StateMap defineStates() {
        return StateMap.builder()
            .initialState(INITIATED)

            .state(INITIATED)
                .interim()
                .timeout(timings.providerTimeoutSec(), TimeUnit.SECONDS, FAILED)
                .onEntry(self -> ((PaymentSupervisor) self).createProviderSession())
                .on(ProviderSessionCreated.class, AWAITING_PAYMENT, null, (self, e) -> {
                    PaymentContext ctx = ((PaymentSupervisor) self).getContext();
                    ProviderSessionCreated ev = (ProviderSessionCreated) e;
                    ctx.providerRef = ev.providerRef();
                    ctx.redirectUrl = ev.redirectUrl();
                    ctx.note("provider session " + ev.providerRef());
                })
                .on(ProviderSessionFailed.class, FAILED, null, (self, e) -> {
                    PaymentContext ctx = ((PaymentSupervisor) self).getContext();
                    if (ctx.endCause == null) ctx.endCause = ((ProviderSessionFailed) e).reason();
                })

            // HIBERNATED: the customer is on the provider's payment page. The
            // machine exists only as a store row until the callback / cancel /
            // sweep arrives; the payment window is enforced at wake.
            .state(AWAITING_PAYMENT)
                .interim().offline()
                .timeout(timings.paymentWindowSec(), TimeUnit.SECONDS, EXPIRED)
                .on(PaymentCallback.class, CAPTURED,
                    (self, e) -> ((PaymentCallback) e).success(),
                    (self, e) -> {
                        PaymentContext ctx = ((PaymentSupervisor) self).getContext();
                        PaymentCallback cb = (PaymentCallback) e;
                        ctx.providerTxnId = cb.providerTxnId();
                        ctx.paidAmountMinor = cb.paidAmountMinor() > 0 ? cb.paidAmountMinor() : ctx.amountMinor;
                        ctx.capturedAtMs = System.currentTimeMillis();
                        ctx.note("captured " + ctx.paidAmountMinor + " " + ctx.currency + " txn " + cb.providerTxnId());
                    })
                .on(PaymentCallback.class, FAILED, null, (self, e) -> {
                    PaymentContext ctx = ((PaymentSupervisor) self).getContext();
                    if (ctx.endCause == null) ctx.endCause = ((PaymentCallback) e).reason();
                })
                .on(PaymentCancelled.class, CANCELLED, null, (self, e) -> {
                    PaymentContext ctx = ((PaymentSupervisor) self).getContext();
                    if (ctx.endCause == null) ctx.endCause = ((PaymentCancelled) e).reason();
                })
                .stay(Sweep.class, (self, e) -> { /* wake only — a matured window settles via its timer */ })

            // HIBERNATED: money captured; the row IS the refundable record
            // until the refund window closes (sweep/startup settles it).
            .state(CAPTURED)
                .interim().offline()
                .timeout(timings.refundWindowSec(), TimeUnit.SECONDS, SETTLED)
                .on(RefundRequested.class, REFUNDING, null, (self, e) -> {
                    PaymentContext ctx = ((PaymentSupervisor) self).getContext();
                    RefundRequested rr = (RefundRequested) e;
                    ctx.refundRequestedMinor = rr.amountMinor();
                    ctx.refundReason = rr.reason();
                    ctx.note("refund requested " + (rr.amountMinor() > 0 ? rr.amountMinor() : "FULL")
                        + " (" + rr.reason() + ")");
                })
                .stay(PaymentCancelled.class, (self, e) ->
                    ((PaymentSupervisor) self).getContext().note("cancel ignored — already captured"))
                .stay(Sweep.class, (self, e) -> { })

            .state(REFUNDING)
                .interim()
                // Safety net for a hung provider call: stay-mode timeout fires
                // RefundFailed, which routes the payment BACK to the refundable
                // CAPTURED hibernation instead of killing the record.
                .timeoutStay(timings.providerTimeoutSec(), TimeUnit.SECONDS,
                    self -> ((PaymentSupervisor) self).publishEvent(
                        new RefundFailed("refund attempt timed out")))
                .onEntry(self -> ((PaymentSupervisor) self).executeRefund())
                .on(RefundSucceeded.class, REFUNDED, null, (self, e) -> {
                    PaymentContext ctx = ((PaymentSupervisor) self).getContext();
                    RefundSucceeded ok = (RefundSucceeded) e;
                    ctx.refundRef = ok.refundRef();
                    ctx.refundedMinor = ok.refundedMinor();
                    ctx.note("refunded " + ok.refundedMinor() + " ref " + ok.refundRef());
                })
                .on(RefundFailed.class, CAPTURED, null, (self, e) -> {
                    PaymentContext ctx = ((PaymentSupervisor) self).getContext();
                    ctx.note("refund attempt " + ctx.refundAttempts + " failed: " + ((RefundFailed) e).reason());
                })

            .state(REFUNDED)
                .finalState().timeout(1, TimeUnit.SECONDS, REFUNDED)
                .onEntry(self -> ((PaymentSupervisor) self).close(REFUNDED))
            .state(CANCELLED)
                .finalState().timeout(1, TimeUnit.SECONDS, CANCELLED)
                .onEntry(self -> ((PaymentSupervisor) self).close(CANCELLED))
            .state(FAILED)
                .finalState().timeout(1, TimeUnit.SECONDS, FAILED)
                .onEntry(self -> ((PaymentSupervisor) self).close(FAILED))
            .state(EXPIRED)
                .finalState().timeout(1, TimeUnit.SECONDS, EXPIRED)
                .onEntry(self -> ((PaymentSupervisor) self).close(EXPIRED))
            .state(SETTLED)
                .finalState().timeout(1, TimeUnit.SECONDS, SETTLED)
                .onEntry(self -> ((PaymentSupervisor) self).close(SETTLED))

            .build();
    }

    @Override
    protected PaymentContext createContext() { return new PaymentContext(); }

    /** Timeline tap — every hop of this machine is auditable in the record. */
    @Override
    protected void onTransitioned(String fromState, String toState, String causeHint) {
        PaymentContext ctx = getContext();
        if (ctx == null) return;
        ctx.timeline.add(System.currentTimeMillis() + "|" + (fromState == null ? "-" : fromState)
            + ">" + toState + (causeHint != null ? "|" + causeHint : ""));
    }

    /** Registry-forced failover (shutdown etc.) stamps its reason into the record. */
    @Override
    protected void onForcedFailover(String reason) {
        PaymentContext ctx = getContext();
        if (ctx == null) return;
        if (ctx.endCause == null) ctx.endCause = reason;
        ctx.note("forced failover: " + reason);
    }

    // ─────────────────────────────────────────────────────────────────
    // Steps
    // ─────────────────────────────────────────────────────────────────

    private Services services() {
        Object v = getVolatileContext();
        if (v instanceof Services s) return s;
        throw new IllegalStateException("PaymentSupervisor has no Services volatile context — "
            + "the gateway must configure the volatileLoader");
    }

    private void createProviderSession() {
        PaymentContext ctx = getContext();
        if (ctx.createdAtMs == 0) ctx.createdAtMs = System.currentTimeMillis();
        try {
            PgwProviderClient.CheckoutSession cs = services().provider().createCheckout(ctx);
            if (cs == null || cs.redirectUrl() == null) {
                publishEvent(new ProviderSessionFailed("provider returned no checkout session"));
                return;
            }
            publishEvent(new ProviderSessionCreated(cs.providerRef(), cs.redirectUrl()));
        } catch (RuntimeException e) {
            ctx.note("createCheckout threw: " + e);
            publishEvent(new ProviderSessionFailed("provider error: " + e.getMessage()));
        }
    }

    private void executeRefund() {
        PaymentContext ctx = getContext();
        ctx.refundAttempts++;
        try {
            PgwProviderClient.RefundResult r = services().provider().refund(ctx);
            if (r != null && r.success()) {
                publishEvent(new RefundSucceeded(r.refundRef(), ctx.effectiveRefundMinor()));
            } else {
                publishEvent(new RefundFailed(r != null ? r.reason() : "provider returned null"));
            }
        } catch (RuntimeException e) {
            ctx.note("refund threw: " + e);
            publishEvent(new RefundFailed("provider error: " + e.getMessage()));
        }
    }

    /** Terminal close — every outcome lands here; exactly one record per payment. */
    private void close(String outcome) {
        PaymentContext ctx = getContext();
        if (ctx == null) return;
        if (ctx.outcome != null) return;                 // idempotent
        ctx.outcome = outcome;
        ctx.endedAtMs = System.currentTimeMillis();
        if (ctx.endCause == null) {
            ctx.endCause = switch (outcome) {
                case EXPIRED -> "payment window expired";
                case SETTLED -> "refund window closed";
                case REFUNDED -> "refunded";
                case CANCELLED -> "cancelled";
                default -> "failed";
            };
        }
        PaymentRecord record = new PaymentRecord(
            ctx.paymentId, outcome, ctx.endCause,
            ctx.orderRef, ctx.customerRef,
            ctx.amountMinor, ctx.paidAmountMinor, ctx.refundedMinor, ctx.currency,
            ctx.providerRef, ctx.providerTxnId, ctx.refundRef,
            ctx.createdAtMs, ctx.capturedAtMs, ctx.endedAtMs,
            ctx.refundAttempts, java.util.List.copyOf(ctx.timeline));
        try {
            services().sink().write(record);
        } catch (RuntimeException e) {
            LOG.error("[{}] PAYMENT RECORD SINK FAILED for {} outcome={} — record lost: {}",
                getMachineId(), ctx.paymentId, outcome, e.toString());
        }
    }
}
