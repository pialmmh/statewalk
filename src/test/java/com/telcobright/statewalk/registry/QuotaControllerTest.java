package com.telcobright.statewalk.registry;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * QuotaController v3 exactness + hygiene:
 * <ul>
 *   <li>zero-count concurrency entries are pruned (no unbounded heap for
 *       high-cardinality per-user/per-MAC keys);</li>
 *   <li>a rejected acquire rolls back EVERYTHING it took, TPS included;</li>
 *   <li>per-dimension acquire/release (the rebind protocol) is exact under
 *       parallel hammering.</li>
 * </ul>
 */
class QuotaControllerTest {

    @Test
    void concurrency_counters_prune_at_zero() {
        QuotaController qc = new QuotaController();
        QuotaLimits limits = new QuotaLimits(10, 10, 0, 0);

        final int KEYS = 500;
        for (int i = 0; i < KEYS; i++) {
            assertNull(qc.tryAcquire(QuotaKeys.of("user-" + i, "route-" + i), limits));
        }
        assertEquals(2 * KEYS, qc.trackedKeyCount(), "one partner + one route entry per key");

        for (int i = 0; i < KEYS; i++) {
            qc.release(QuotaKeys.of("user-" + i, "route-" + i), limits);
        }
        assertEquals(0, qc.trackedKeyCount(),
            "released-to-zero keys are pruned — v2 kept every dead entry forever");
        assertEquals(0, qc.partnerActive("user-1"));
    }

    @Test
    void rejected_acquire_rolls_back_concurrency_and_tps() {
        QuotaController qc = new QuotaController();
        // partner: conc 10 (never binding) + tps 5; route: conc 1 — TPS is the
        // budget under test, concurrency must not mask it.
        QuotaLimits limits = new QuotaLimits(10, 1, 5, 0);

        assertNull(qc.tryAcquire(QuotaKeys.of("P", "R"), limits));           // holds R
        // Second on the same route: route-conc fails AFTER partner conc+tps acquired.
        assertEquals(RejectCause.ROUTE_CONCURRENCY_EXCEEDED,
            qc.tryAcquire(QuotaKeys.of("P", "R"), limits));

        assertEquals(1, qc.partnerActive("P"), "partner concurrency rolled back to the single holder");
        assertEquals(1, qc.routeActive("R"));
        // TPS budget must be intact: 4 more acquires on fresh routes succeed
        // (1 used by the holder; the reject burned nothing).
        for (int i = 0; i < 4; i++) {
            assertNull(qc.tryAcquire(QuotaKeys.of("P", "R" + i), limits),
                "TPS token " + (i + 2) + "/5 must still be available");
        }
        assertEquals(RejectCause.PARTNER_TPS_EXCEEDED,
            qc.tryAcquire(QuotaKeys.of("P", "R-last"), limits), "budget exactly exhausted at 5");
    }

    @Test
    void per_dimension_ops_stay_exact_under_parallel_hammering() throws Exception {
        QuotaController qc = new QuotaController();
        QuotaLimits limits = new QuotaLimits(32, 0, 0, 0);

        final int THREADS = 16, ROUNDS = 2000;
        CountDownLatch go = new CountDownLatch(1);
        Thread[] ts = new Thread[THREADS];
        for (int t = 0; t < THREADS; t++) {
            ts[t] = new Thread(() -> {
                try { go.await(); } catch (InterruptedException e) { return; }
                for (int i = 0; i < ROUNDS; i++) {
                    if (qc.tryAcquirePartner("hot", limits) == null) {
                        qc.releasePartner("hot", limits);
                    }
                }
            });
            ts[t].start();
        }
        go.countDown();
        for (Thread t : ts) t.join(20_000);

        assertEquals(0, qc.partnerActive("hot"), "every successful acquire was released — exact");
        assertEquals(0, qc.trackedKeyCount(), "and the entry pruned at zero");
    }

    @Test
    void parallel_acquire_release_cycles_never_exceed_cap() throws Exception {
        QuotaController qc = new QuotaController();
        final int CAP = 8;
        QuotaLimits limits = new QuotaLimits(CAP, 0, 0, 0);
        final int[] maxSeen = {0};

        IntStream.range(0, 8).parallel().forEach(t -> {
            for (int i = 0; i < 3000; i++) {
                if (qc.tryAcquirePartner("k", limits) == null) {
                    int seen = qc.partnerActive("k");
                    synchronized (maxSeen) { if (seen > maxSeen[0]) maxSeen[0] = seen; }
                    qc.releasePartner("k", limits);
                }
            }
        });
        assertTrue(maxSeen[0] <= CAP, "cap never exceeded (saw " + maxSeen[0] + ")");
        assertEquals(0, qc.partnerActive("k"));
    }
}
