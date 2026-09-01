package com.telcobright.paymentgw;

import com.telcobright.paymentgw.PaymentEvents.PaymentCallback;
import com.telcobright.statewalk.persistence.InMemoryPersistenceProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The payment-gateway library, end to end on the in-memory store:
 *
 * <ol>
 *   <li>Full journey: initiate → redirect URL → HIBERNATE (zero machines in
 *       memory, one store row) → success callback rehydrates → CAPTURED
 *       hibernation (refundable row) → refund → REFUNDED, exactly one record.</li>
 *   <li>Cancel at the provider site → CANCELLED record.</li>
 *   <li>Declined payment callback → FAILED record with the reason.</li>
 *   <li>Provider checkout crash → FAILED record; initiate future fails.</li>
 *   <li>Refund declined → BACK to refundable CAPTURED hibernation; a retry
 *       succeeds; record counts both attempts.</li>
 *   <li>Restart: hibernated payments STAY db-only on the new node (no memory
 *       flood); a payment whose window matured during downtime settles as
 *       EXPIRED at startup; a hibernated one still answers callbacks.</li>
 *   <li>sweepExpired() wakes a matured refund window → SETTLED record.</li>
 *   <li>Cancel after capture is ignored — the payment stays refundable.</li>
 *   <li>status() answers for live, hibernated, finished and unknown ids.</li>
 * </ol>
 */
class PaymentGatewayTest {

    // ── scriptable fake provider ────────────────────────────────────

    static final class FakePgw implements PgwProviderClient {
        volatile boolean failCheckout;
        volatile boolean declineRefund;
        final AtomicInteger checkouts = new AtomicInteger();
        final AtomicInteger refunds = new AtomicInteger();

        @Override public CheckoutSession createCheckout(PaymentContext p) {
            checkouts.incrementAndGet();
            if (failCheckout) throw new RuntimeException("pgw api 503");
            return new CheckoutSession("PREF-" + p.paymentId, "https://pgw.example/pay/" + p.paymentId);
        }
        @Override public RefundResult refund(PaymentContext p) {
            refunds.incrementAndGet();
            if (declineRefund) return RefundResult.failed("refund declined by provider");
            return RefundResult.ok("RF-" + p.paymentId + "-" + p.refundAttempts);
        }
    }

    private final List<PaymentGateway> open = new ArrayList<>();
    private final List<PaymentRecord> records = new CopyOnWriteArrayList<>();
    private final FakePgw pgw = new FakePgw();

    private PaymentGateway gateway(InMemoryPersistenceProvider store, PaymentTimings t) {
        PaymentGateway gw = PaymentGateway.builder("pgw-test")
            .provider(pgw)
            .recordSink(records::add)
            .persistence(store)
            .timings(t)
            .poolSize(8).threads(2)
            .build();
        open.add(gw);
        return gw;
    }

    @AfterEach
    void tearDown() { for (var g : open) g.close(); }

    private static PaymentGateway.PaymentRequest req(long amountMinor) {
        return new PaymentGateway.PaymentRequest("ORD-1", "cust-7", amountMinor, "BDT", "test order");
    }

    private PaymentRecord awaitRecord(int n, long timeoutMs) throws InterruptedException {
        long until = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < until) {
            if (records.size() >= n) return records.get(n - 1);
            Thread.sleep(25);
        }
        fail("expected " + n + " record(s) within " + timeoutMs + "ms, have " + records.size());
        return null;
    }

    // ─────────────────────────────────────────────────────────────
    // (1) the full happy journey, hibernating twice
    // ─────────────────────────────────────────────────────────────

    @Test
    void full_journey_pay_hibernate_capture_hibernate_refund() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        PaymentGateway gw = gateway(store, new PaymentTimings(5, 3600, 3600));

        // 1. web app initiates; gets the provider redirect URL
        String url = gw.initiatePayment("pay-1", req(150_00)).get(10, TimeUnit.SECONDS);
        assertEquals("https://pgw.example/pay/pay-1", url);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));

        // 2. customer is at the provider — the machine is HIBERNATED
        assertEquals(0, gw.liveCount(), "ZERO machines in memory while the customer pays externally");
        assertTrue(gw.isHibernated("pay-1"));
        PaymentStatus parked = gw.status("pay-1");
        assertEquals(PaymentSupervisor.AWAITING_PAYMENT, parked.state());
        assertEquals("PREF-pay-1", parked.context().providerRef, "status reads the STORE row");
        assertEquals(1, store.size());

        // 3. provider webhook: paid → rehydrate → CAPTURED → hibernate again
        gw.onProviderCallback("pay-1", PaymentCallback.success("TXN-77", 150_00))
            .get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(0, gw.liveCount(), "captured payment re-hibernates as the refundable row");
        PaymentStatus captured = gw.status("pay-1");
        assertEquals(PaymentSupervisor.CAPTURED, captured.state());
        assertTrue(captured.hibernated());
        assertEquals("TXN-77", captured.context().providerTxnId);
        assertEquals(150_00, captured.context().paidAmountMinor);
        assertTrue(records.isEmpty(), "no record yet — the payment is refundable, not finished");

        // 4. refund (full amount)
        gw.requestRefund("pay-1", 0, "customer complaint").get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));

        PaymentRecord rec = awaitRecord(1, 5000);
        assertEquals(1, records.size(), "exactly ONE record per payment");
        assertEquals(PaymentSupervisor.REFUNDED, rec.outcome());
        assertEquals(150_00, rec.paidAmountMinor());
        assertEquals(150_00, rec.refundedMinor());
        assertEquals("RF-pay-1-1", rec.refundRef());
        assertEquals("TXN-77", rec.providerTxnId());
        assertFalse(rec.moneyCollected(), "fully refunded");
        assertTrue(rec.timeline().stream().anyMatch(l -> l.contains("INITIATED>AWAITING_PAYMENT")),
            "timeline carries the full journey");

        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(0, store.size(), "terminal ritual purged the row");
        assertEquals(PaymentStatus.FINISHED, gw.status("pay-1").state());
    }

    // ─────────────────────────────────────────────────────────────
    // (2) cancel at the provider site
    // ─────────────────────────────────────────────────────────────

    @Test
    void cancel_at_provider_site_ends_cancelled_with_record() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        PaymentGateway gw = gateway(store, new PaymentTimings(5, 3600, 3600));

        gw.initiatePayment("pay-c", req(80_00)).get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(gw.isHibernated("pay-c"));

        gw.cancel("pay-c", "customer pressed cancel").get(10, TimeUnit.SECONDS);
        PaymentRecord rec = awaitRecord(1, 5000);
        assertEquals(PaymentSupervisor.CANCELLED, rec.outcome());
        assertEquals("customer pressed cancel", rec.endCause());
        assertEquals(0, rec.paidAmountMinor());
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(0, store.size());
    }

    // ─────────────────────────────────────────────────────────────
    // (3) declined payment
    // ─────────────────────────────────────────────────────────────

    @Test
    void declined_callback_ends_failed_with_reason() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        PaymentGateway gw = gateway(store, new PaymentTimings(5, 3600, 3600));

        gw.initiatePayment("pay-f", req(99_00)).get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));

        gw.onProviderCallback("pay-f", PaymentCallback.failed("insufficient funds"))
            .get(10, TimeUnit.SECONDS);
        PaymentRecord rec = awaitRecord(1, 5000);
        assertEquals(PaymentSupervisor.FAILED, rec.outcome());
        assertEquals("insufficient funds", rec.endCause());
        assertFalse(rec.moneyCollected());
    }

    // ─────────────────────────────────────────────────────────────
    // (4) provider checkout crash
    // ─────────────────────────────────────────────────────────────

    @Test
    void provider_checkout_crash_fails_the_payment_and_the_initiate_future() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        PaymentGateway gw = gateway(store, new PaymentTimings(5, 3600, 3600));
        pgw.failCheckout = true;
        try {
            CompletableFuture<String> f = gw.initiatePayment("pay-x", req(10_00));
            ExecutionException ex = assertThrows(ExecutionException.class,
                () -> f.get(10, TimeUnit.SECONDS));
            assertTrue(ex.getCause().getMessage().contains("failed"),
                "caller learns the initiation failed: " + ex.getCause().getMessage());
            PaymentRecord rec = awaitRecord(1, 5000);
            assertEquals(PaymentSupervisor.FAILED, rec.outcome());
            assertTrue(rec.endCause().contains("pgw api 503"), rec.endCause());
        } finally {
            pgw.failCheckout = false;
        }
    }

    // ─────────────────────────────────────────────────────────────
    // (5) declined refund → back to refundable; retry succeeds
    // ─────────────────────────────────────────────────────────────

    @Test
    void declined_refund_returns_to_refundable_hibernation_and_retry_succeeds() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        PaymentGateway gw = gateway(store, new PaymentTimings(5, 3600, 3600));

        gw.initiatePayment("pay-r", req(200_00)).get(10, TimeUnit.SECONDS);
        gw.onProviderCallback("pay-r", PaymentCallback.success("TXN-r", 200_00)).get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));

        pgw.declineRefund = true;
        gw.requestRefund("pay-r", 0, "first try").get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(records.isEmpty(), "declined refund must NOT finish the payment");
        assertEquals(PaymentSupervisor.CAPTURED, gw.status("pay-r").state(),
            "back in the refundable state");

        pgw.declineRefund = false;
        gw.requestRefund("pay-r", 0, "second try").get(10, TimeUnit.SECONDS);
        PaymentRecord rec = awaitRecord(1, 5000);
        assertEquals(PaymentSupervisor.REFUNDED, rec.outcome());
        assertEquals(2, rec.refundAttempts(), "both attempts audited");
        assertEquals(2, pgw.refunds.get());
    }

    // ─────────────────────────────────────────────────────────────
    // (6) restart: hibernated rows stay db-only; matured window settles
    // ─────────────────────────────────────────────────────────────

    @Test
    void restart_keeps_hibernated_payments_db_only_and_settles_matured_ones() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        // short payment window for the one that must expire during "downtime"
        PaymentGateway a = gateway(store, new PaymentTimings(5, 1, 3600));
        a.initiatePayment("exp-1", req(10_00)).get(10, TimeUnit.SECONDS);
        assertTrue(a.awaitIdle(5, TimeUnit.SECONDS));
        a.close(); open.remove(a);

        PaymentGateway b0 = gateway(store, new PaymentTimings(5, 3600, 3600));
        b0.initiatePayment("wait-1", req(20_00)).get(10, TimeUnit.SECONDS);
        b0.initiatePayment("wait-2", req(30_00)).get(10, TimeUnit.SECONDS);
        assertTrue(b0.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(3, store.size(), "three hibernated rows before the restart");
        b0.close(); open.remove(b0);     // "crash" — hibernated rows survive close by design

        Thread.sleep(1_200);             // exp-1's payment window matures during downtime

        PaymentGateway b = gateway(store, new PaymentTimings(5, 3600, 3600));
        assertTrue(b.awaitIdle(10, TimeUnit.SECONDS));

        // matured one settled at startup…
        PaymentRecord rec = awaitRecord(1, 5000);
        assertEquals("exp-1", rec.paymentId());
        assertEquals(PaymentSupervisor.EXPIRED, rec.outcome());
        // …the unmatured ones did NOT flood memory — still db-only
        assertEquals(0, b.liveCount(), "hibernated payments stay OUT of memory at startup");
        assertTrue(b.isHibernated("wait-1"));
        assertTrue(b.isHibernated("wait-2"));

        // and a hibernated one still answers its callback on the new node
        b.onProviderCallback("wait-1", PaymentCallback.success("TXN-w1", 20_00)).get(10, TimeUnit.SECONDS);
        assertTrue(b.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(PaymentSupervisor.CAPTURED, b.status("wait-1").state());
    }

    // ─────────────────────────────────────────────────────────────
    // (7) sweep settles a matured refund window
    // ─────────────────────────────────────────────────────────────

    @Test
    void sweep_settles_matured_refund_window_as_settled() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        PaymentGateway gw = gateway(store, new PaymentTimings(5, 3600, 1));   // 1s refund window

        gw.initiatePayment("pay-s", req(70_00)).get(10, TimeUnit.SECONDS);
        gw.onProviderCallback("pay-s", PaymentCallback.success("TXN-s", 70_00)).get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(gw.isHibernated("pay-s"));

        Thread.sleep(1_200);             // refund window matures while hibernated
        int woken = gw.sweepExpired();
        assertEquals(1, woken, "the sweep found and woke the matured payment");
        PaymentRecord rec = awaitRecord(1, 5000);
        assertEquals(PaymentSupervisor.SETTLED, rec.outcome());
        assertEquals("refund window closed", rec.endCause());
        assertTrue(rec.moneyCollected(), "money stayed with the merchant");
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(0, store.size());
    }

    // ─────────────────────────────────────────────────────────────
    // (8) cancel after capture is ignored
    // ─────────────────────────────────────────────────────────────

    @Test
    void cancel_after_capture_is_ignored_payment_stays_refundable() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        PaymentGateway gw = gateway(store, new PaymentTimings(5, 3600, 3600));

        gw.initiatePayment("pay-l", req(40_00)).get(10, TimeUnit.SECONDS);
        gw.onProviderCallback("pay-l", PaymentCallback.success("TXN-l", 40_00)).get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));

        gw.cancel("pay-l", "too late").get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(records.isEmpty(), "captured money cannot be cancelled away");
        assertEquals(PaymentSupervisor.CAPTURED, gw.status("pay-l").state());
        assertTrue(gw.status("pay-l").isRefundable());
    }

    // ─────────────────────────────────────────────────────────────
    // (9) status for every lifecycle shape
    // ─────────────────────────────────────────────────────────────

    @Test
    void status_answers_live_hibernated_finished_and_unknown() throws Exception {
        InMemoryPersistenceProvider store = new InMemoryPersistenceProvider();
        PaymentGateway gw = gateway(store, new PaymentTimings(5, 3600, 3600));

        assertEquals(PaymentStatus.UNKNOWN, gw.status("never-seen").state());

        gw.initiatePayment("pay-q", req(5_00)).get(10, TimeUnit.SECONDS);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertTrue(gw.status("pay-q").hibernated());

        gw.onProviderCallback("pay-q", PaymentCallback.failed("declined")).get(10, TimeUnit.SECONDS);
        awaitRecord(1, 5000);
        assertTrue(gw.awaitIdle(5, TimeUnit.SECONDS));
        assertEquals(PaymentStatus.FINISHED, gw.status("pay-q").state());
    }
}
