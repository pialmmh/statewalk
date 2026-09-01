package com.telcobright.paymentgw;

import com.telcobright.paymentgw.PaymentEvents.PaymentCallback;
import com.telcobright.statewalk.persistence.jdbc.JdbcPersistenceProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The payment gateway against the REAL MySQL in the local LXC — the exact
 * production posture: slow external payment flows, every wait hibernated as a
 * MySQL row, an external coordinator activating a SECOND gateway instance
 * after a crash, the transaction continuing there.
 *
 * <p>SELF-SKIPS when no MySQL answers on 127.0.0.1:3306.
 */
class PaymentGatewayMySqlTest {

    private static final String TABLE = "sw_paymentgw_test";
    private static MysqlDataSource ds;

    private final List<PaymentGateway> open = new ArrayList<>();
    private final List<PaymentRecord> records = new CopyOnWriteArrayList<>();
    private final PaymentGatewayTest.FakePgw pgw = new PaymentGatewayTest.FakePgw();

    @BeforeAll
    static void connect() {
        try {
            MysqlDataSource d = new MysqlDataSource();
            d.setUrl("jdbc:mysql://127.0.0.1:3306/statewalk_test"
                + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=2000&serverTimezone=UTC");
            d.setUser("root");
            d.setPassword("123456");
            try (Connection c = d.getConnection(); Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + TABLE);
                s.execute("DROP TABLE IF EXISTS " + TABLE + "_dead");
            }
            ds = d;
        } catch (Exception e) {
            ds = null;
        }
        assumeTrue(ds != null, "no MySQL on 127.0.0.1:3306 — skipping MySQL payment-gateway tests");
    }

    @AfterEach
    void tearDown() { for (var g : open) g.close(); }

    private PaymentGateway gateway(PaymentTimings t) {
        PaymentGateway gw = PaymentGateway.builder("pgw-mysql")
            .provider(pgw)
            .recordSink(records::add)
            .persistence(new JdbcPersistenceProvider(ds, TABLE))
            .timings(t)
            .poolSize(8).threads(2)
            .build();
        open.add(gw);
        return gw;
    }

    /**
     * The scenario from the design brief, end to end on MySQL:
     * initiate on instance A → customer at the provider (hibernated MySQL
     * row) → instance A CRASHES → the coordinator activates instance B on the
     * same database → the provider's callback lands on B → B rehydrates the
     * machine WITH its context, captures, re-hibernates the refundable row →
     * a refund days later (still on B) completes it. One record, on B.
     */
    @Test
    void crash_failover_mid_payment_continues_on_second_instance() throws Exception {
        PaymentGateway a = gateway(new PaymentTimings(10, 3600, 3600));
        String url = a.initiatePayment("mp-1",
            new PaymentGateway.PaymentRequest("ORD-9", "cust-1", 500_00, "BDT", "tv"))
            .get(15, TimeUnit.SECONDS);
        assertEquals("https://pgw.example/pay/mp-1", url);
        assertTrue(a.awaitIdle(10, TimeUnit.SECONDS));
        assertTrue(a.isHibernated("mp-1"), "customer is at the provider — MySQL row only");

        // Instance A crashes. (close() is safe here: hibernated rows are
        // untouched by shutdown — they ARE the session.)
        a.close(); open.remove(a);

        // The external coordinator activates instance B on the same MySQL.
        PaymentGateway b = gateway(new PaymentTimings(10, 3600, 3600));
        assertTrue(b.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals(0, b.liveCount(), "hibernated payment stays db-only at B's startup — no memory flood");
        PaymentStatus st = b.status("mp-1");
        assertTrue(st.hibernated());
        assertEquals(PaymentSupervisor.AWAITING_PAYMENT, st.state());
        assertEquals("PREF-mp-1", st.context().providerRef, "context came back from MySQL");

        // The provider's payment notification arrives — at instance B.
        b.onProviderCallback("mp-1", PaymentCallback.success("TXN-M1", 500_00)).get(15, TimeUnit.SECONDS);
        assertTrue(b.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals(PaymentSupervisor.CAPTURED, b.status("mp-1").state(),
            "B rehydrated the machine and captured the payment");
        assertTrue(b.isHibernated("mp-1"), "refundable capture re-hibernated to MySQL");
        assertTrue(records.isEmpty());

        // Refund later — still instance B.
        b.requestRefund("mp-1", 0, "warranty return").get(15, TimeUnit.SECONDS);
        assertTrue(b.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals(1, records.size(), "exactly one record for the whole cross-instance journey");
        PaymentRecord rec = records.get(0);
        assertEquals(PaymentSupervisor.REFUNDED, rec.outcome());
        assertEquals(500_00, rec.refundedMinor());
        assertEquals("TXN-M1", rec.providerTxnId());
        assertTrue(rec.timeline().stream().anyMatch(l -> l.contains("INITIATED>AWAITING_PAYMENT")),
            "the timeline crossed the crash intact");
        assertEquals(PaymentStatus.FINISHED, b.status("mp-1").state());
    }

    /** A payment window that matured while everything was down settles at the next activation. */
    @Test
    void payment_window_matured_during_outage_expires_at_next_activation() throws Exception {
        PaymentGateway a = gateway(new PaymentTimings(10, 1, 3600));   // 1s payment window
        a.initiatePayment("mp-exp",
            new PaymentGateway.PaymentRequest("ORD-x", "cust-2", 100_00, "BDT", "x"))
            .get(15, TimeUnit.SECONDS);
        assertTrue(a.awaitIdle(10, TimeUnit.SECONDS));
        a.close(); open.remove(a);

        Thread.sleep(1_300);   // outage; window matures inside MySQL

        PaymentGateway b = gateway(new PaymentTimings(10, 3600, 3600));
        assertTrue(b.awaitIdle(10, TimeUnit.SECONDS));
        long until = System.currentTimeMillis() + 5_000;
        while (records.isEmpty() && System.currentTimeMillis() < until) Thread.sleep(25);
        assertEquals(1, records.size(), "the matured payment settled at activation");
        assertEquals(PaymentSupervisor.EXPIRED, records.get(0).outcome());
        assertEquals("payment window expired", records.get(0).endCause());
    }
}
