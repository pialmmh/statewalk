package com.telcobright.paymentgw;

import com.telcobright.paymentgw.PaymentEvents.PaymentCallback;
import com.telcobright.paymentgw.PaymentEvents.PaymentCancelled;
import com.telcobright.paymentgw.PaymentEvents.RefundRequested;
import com.telcobright.paymentgw.PaymentEvents.Sweep;
import com.telcobright.statewalk.persistence.MachineSnapshot;
import com.telcobright.statewalk.persistence.PersistenceProvider;
import com.telcobright.statewalk.persistence.SnapshotSerializer;
import com.telcobright.statewalk.registry.DispatchResult;
import com.telcobright.statewalk.registry.StatemachineRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * THE payment-gateway library facade — what a web app talks to.
 *
 * <pre>
 *   PaymentGateway gw = PaymentGateway.builder("shop-pgw")
 *       .provider(mySslCommerzClient)            // port to the real PGW
 *       .recordSink(record -> billingDb.save(record))
 *       .persistence(new JdbcPersistenceProvider(mysqlDs, "shop_payments"))
 *       .timings(PaymentTimings.defaults())
 *       .build();
 *
 *   // 1. checkout button pressed:
 *   String url = gw.initiatePayment("pay-1001", req).join();   // redirect customer here
 *   // 2. machine HIBERNATES (db row only) while the customer pays externally
 *   // 3. provider webhook / return-url handler:
 *   gw.onProviderCallback("pay-1001", PaymentCallback.success("TXN9", 150_00));
 *   // 4. days later:
 *   gw.requestRefund("pay-1001", 0, "customer complaint");
 * </pre>
 *
 * <p>Every payment lives as a statewalk machine: pooled while active,
 * <b>hibernated to the store</b> whenever it waits on the outside world
 * (customer on the provider page; captured money inside its refund window),
 * rehydrated by the next event — across process restarts. Every terminal
 * outcome emits exactly one {@link PaymentRecord}.
 */
public final class PaymentGateway implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PaymentGateway.class);

    public static final String SUPERVISOR_TYPE = "PaymentSupervisor";

    private final String name;
    private final StatemachineRegistry<PaymentContext> registry;
    private final PersistenceProvider store;

    private PaymentGateway(Builder b) {
        this.name = b.name;
        this.store = b.persistence;
        PaymentSupervisor.Services services = new PaymentSupervisor.Services(b.provider, b.recordSink);
        this.registry = StatemachineRegistry.<PaymentContext>builder(b.name)
            .supervisor(SUPERVISOR_TYPE, () -> new PaymentSupervisor(b.timings), b.poolSize)
            .volatileLoader(SUPERVISOR_TYPE, m -> services)
            .persistence(b.persistence)
            .rehydrate(true)
            .preWarmContextClass(PaymentContext.class)
            .maxConcurrent(b.maxConcurrent)
            .threads(b.threads)
            .build();
    }

    public static Builder builder(String name) { return new Builder(name); }

    // ─────────────────────────────────────────────────────────────────
    // The web-app API
    // ─────────────────────────────────────────────────────────────────

    /** What a payment initiation needs from the merchant side. */
    public record PaymentRequest(String orderRef, String customerRef,
                                 long amountMinor, String currency, String description) {}

    /**
     * Start a payment. Dispatches the machine (INITIATED calls the provider,
     * then the machine hibernates in AWAITING_PAYMENT) and completes the
     * returned future with the provider's REDIRECT URL — send the customer
     * there. Completes exceptionally when the dispatch was rejected or the
     * provider refused the checkout.
     */
    public CompletableFuture<String> initiatePayment(String paymentId, PaymentRequest req) {
        PaymentContext ctx = new PaymentContext();
        ctx.paymentId = paymentId;
        ctx.orderRef = req.orderRef();
        ctx.customerRef = req.customerRef();
        ctx.amountMinor = req.amountMinor();
        ctx.currency = req.currency();
        ctx.description = req.description();

        DispatchResult d = registry.dispatch(paymentId, ctx);
        if (!d.accepted()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "payment dispatch rejected: " + d.rejectCause()));
        }
        // The redirect URL is produced by INITIATED's entry action on the
        // machine's chain; poll the (live, then hibernated) context for it.
        return CompletableFuture.supplyAsync(() -> {
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                PaymentStatus st = status(paymentId);
                if (st.context() != null && st.context().redirectUrl != null) {
                    return st.context().redirectUrl;
                }
                if (PaymentStatus.FINISHED.equals(st.state())
                        || PaymentSupervisor.FAILED.equals(st.state())) {
                    throw new IllegalStateException("payment failed before a redirect URL was issued"
                        + (st.context() != null && st.context().endCause != null
                            ? ": " + st.context().endCause : ""));
                }
                try { Thread.sleep(20); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted awaiting redirect URL");
                }
            }
            throw new IllegalStateException("timed out awaiting the provider redirect URL");
        });
    }

    /**
     * The provider's payment result arrived (webhook or verified return-URL).
     * Rehydrates the hibernated machine and drives it to CAPTURED / FAILED.
     * The returned future completes when the machine processed it.
     */
    public CompletableFuture<Void> onProviderCallback(String paymentId, PaymentCallback callback) {
        return registry.submitInbound(paymentId, callback);
    }

    /** The customer cancelled (provider cancel-URL) or the merchant voided the attempt. */
    public CompletableFuture<Void> cancel(String paymentId, String reason) {
        return registry.submitInbound(paymentId, new PaymentCancelled(reason));
    }

    /**
     * Ask for a refund of a captured payment ({@code amountMinor == 0} = full
     * amount). Wakes the hibernated payment; the machine calls the provider
     * and lands in REFUNDED — or back in the refundable CAPTURED hibernation
     * when the provider declines (check {@link #status} / the record).
     */
    public CompletableFuture<Void> requestRefund(String paymentId, long amountMinor, String reason) {
        return registry.submitInbound(paymentId, new RefundRequested(amountMinor, reason));
    }

    /**
     * Point-in-time status — works for LIVE payments (in the registry),
     * HIBERNATED ones (store row only), finished and unknown ids alike.
     */
    public PaymentStatus status(String paymentId) {
        String liveState = registry.supervisorStateOf(paymentId);
        if (liveState != null) {
            return new PaymentStatus(paymentId, liveState, false, registry.supervisorContextOf(paymentId));
        }
        Optional<MachineSnapshot> snap;
        try { snap = store.load(paymentId, name); }
        catch (RuntimeException e) {
            LOG.warn("[{}] status: store load failed for {}: {}", name, paymentId, e.toString());
            snap = Optional.empty();
        }
        if (snap.isEmpty()) {
            // Finished payments keep a short-lived tombstone in the registry;
            // beyond that window the durable answer is the record sink.
            return new PaymentStatus(paymentId,
                registry.wasRecentlyFinished(paymentId) ? PaymentStatus.FINISHED : PaymentStatus.UNKNOWN,
                false, null);
        }
        PaymentContext ctx = null;
        try {
            ctx = (PaymentContext) SnapshotSerializer.contextFromBase64Json(
                snap.get().contextJsonBase64(), snap.get().contextClassName());
        } catch (RuntimeException e) {
            LOG.warn("[{}] status: context decode failed for {}: {}", name, paymentId, e.toString());
        }
        return new PaymentStatus(paymentId, snap.get().currentState(), true, ctx);
    }

    /**
     * Maintenance sweep: wake every hibernated payment whose deadline (payment
     * window / refund window) has matured so its expiry ritual runs and its
     * record ships. Call periodically (cron / scheduler). Returns how many
     * were woken.
     */
    public int sweepExpired() {
        List<MachineSnapshot> matured;
        try { matured = store.loadMatured(name, System.currentTimeMillis()); }
        catch (RuntimeException e) {
            LOG.warn("[{}] sweep: loadMatured failed: {}", name, e.toString());
            return 0;
        }
        int woken = 0;
        for (MachineSnapshot s : matured) {
            if (s.machineId().contains("#")) continue;   // supervisors only
            registry.submitInbound(s.machineId(), new Sweep());
            woken++;
        }
        if (woken > 0) LOG.info("[{}] sweep woke {} matured payment(s)", name, woken);
        return woken;
    }

    /** True while the payment exists only as a store row (customer at the provider, or refundable capture). */
    public boolean isHibernated(String paymentId) {
        return status(paymentId).hibernated();
    }

    /** Live machine count in memory — hibernated payments do NOT count (that is the point). */
    public int liveCount() { return registry.activeIdCount(); }

    /** Drain helper for tests / orderly maintenance. */
    public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        return registry.awaitIdle(timeout, unit);
    }

    /** The underlying registry — escape hatch for advanced wiring (channels, quotas). */
    public StatemachineRegistry<PaymentContext> registry() { return registry; }

    @Override
    public void close() { registry.shutdown(); }

    // ─────────────────────────────────────────────────────────────────
    // Builder — the only way to construct a gateway
    // ─────────────────────────────────────────────────────────────────

    public static final class Builder {
        private final String name;
        private PgwProviderClient provider;
        private PaymentRecordSink recordSink;
        private PersistenceProvider persistence;
        private PaymentTimings timings = PaymentTimings.defaults();
        private int poolSize = 64;
        private int threads = 2;
        private int maxConcurrent = 0;

        Builder(String name) { this.name = name; }

        /** Port to the real provider (SSLCommerz/bKash/Stripe/… client). Required. */
        public Builder provider(PgwProviderClient p) { this.provider = p; return this; }

        /** Where terminal {@link PaymentRecord}s go. Required. */
        public Builder recordSink(PaymentRecordSink s) { this.recordSink = s; return this; }

        /** The store payments hibernate into (JDBC/Redis/in-memory). Required. */
        public Builder persistence(PersistenceProvider p) { this.persistence = p; return this; }

        public Builder timings(PaymentTimings t) { this.timings = t; return this; }
        public Builder poolSize(int n) { this.poolSize = n; return this; }
        public Builder threads(int n) { this.threads = n; return this; }
        /** Hard cap on payments LIVE in memory at once (0 = unlimited). Hibernated rows are unlimited. */
        public Builder maxConcurrent(int n) { this.maxConcurrent = n; return this; }

        public PaymentGateway build() {
            if (provider == null) throw new IllegalStateException("provider(...) is required");
            if (recordSink == null) throw new IllegalStateException("recordSink(...) is required");
            if (persistence == null) {
                throw new IllegalStateException(
                    "persistence(...) is required — hibernation IS the payment store");
            }
            return new PaymentGateway(this);
        }
    }
}
