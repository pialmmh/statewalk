package com.telcobright.statewalk.v2.registry.internal;

import com.telcobright.statewalk.v2.registry.api.QuotaKeys;
import com.telcobright.statewalk.v2.registry.api.QuotaLimits;
import com.telcobright.statewalk.v2.registry.api.RejectCause;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-key concurrent + TPS quota tracker. Used by {@code Registry} to gate
 * dispatch against {@link QuotaLimits} on partner / route dimensions.
 *
 * <h2>Concurrency model</h2>
 * <ul>
 *   <li>Concurrent counters: {@code AtomicInteger} per key. Inc on
 *       {@link #tryAcquire}; dec on {@link #release}.</li>
 *   <li>TPS counters: sliding 1-second window per key, implemented as a
 *       per-key {@code (timestampSecond, count)} tuple. On every check we
 *       reset the count if the second has rolled over. Light-weight; no
 *       allocation per request after warm-up.</li>
 * </ul>
 *
 * <h2>Atomicity</h2>
 * <p>{@link #tryAcquire} performs the four checks (partner concurrent,
 * partner TPS, route concurrent, route TPS) and rolls back any partial
 * acquires if a later check fails. The result is a single atomic accept-or-
 * reject decision per dispatch.
 *
 * <p><b>Not strictly transactional across keys.</b> Under extreme contention
 * a request might be admitted just as another partner's slot opens — for
 * quota that's fine (we err on letting through, not on blocking). The
 * concurrent counters are exact; the TPS check has a one-second window so
 * it's inherently approximate.
 */
public final class QuotaController {

    /** Concurrent-machine count per partner key. */
    private final ConcurrentHashMap<String, AtomicInteger> partnerActive = new ConcurrentHashMap<>();
    /** Concurrent-machine count per route key. */
    private final ConcurrentHashMap<String, AtomicInteger> routeActive   = new ConcurrentHashMap<>();

    /** Per-partner TPS bucket. */
    private final ConcurrentHashMap<String, TpsBucket> partnerTps = new ConcurrentHashMap<>();
    /** Per-route TPS bucket. */
    private final ConcurrentHashMap<String, TpsBucket> routeTps   = new ConcurrentHashMap<>();

    /**
     * Try to acquire all four quotas atomically. Returns {@code null} on
     * success; on failure, returns the {@link RejectCause} of the first
     * dimension that failed and rolls back any partial acquires.
     */
    public RejectCause tryAcquire(QuotaKeys keys, QuotaLimits limits) {
        if (!limits.enforces() || keys == null) return null;

        boolean partnerConcurrentAcquired = false;
        boolean routeConcurrentAcquired = false;
        RejectCause failure = null;

        // Partner concurrency
        if (limits.maxConcurrentPerPartner() > 0 && keys.partnerKey() != null) {
            AtomicInteger c = partnerActive.computeIfAbsent(keys.partnerKey(), k -> new AtomicInteger(0));
            int after = c.incrementAndGet();
            partnerConcurrentAcquired = true;
            if (after > limits.maxConcurrentPerPartner()) {
                failure = RejectCause.PARTNER_CONCURRENCY_EXCEEDED;
            }
        }
        // Partner TPS (only if not already failed)
        if (failure == null && limits.maxTpsPerPartner() > 0 && keys.partnerKey() != null) {
            TpsBucket b = partnerTps.computeIfAbsent(keys.partnerKey(), k -> new TpsBucket());
            if (!b.tryAcquire(limits.maxTpsPerPartner())) {
                failure = RejectCause.PARTNER_TPS_EXCEEDED;
            }
        }
        // Route concurrency
        if (failure == null && limits.maxConcurrentPerRoute() > 0 && keys.routeKey() != null) {
            AtomicInteger c = routeActive.computeIfAbsent(keys.routeKey(), k -> new AtomicInteger(0));
            int after = c.incrementAndGet();
            routeConcurrentAcquired = true;
            if (after > limits.maxConcurrentPerRoute()) {
                failure = RejectCause.ROUTE_CONCURRENCY_EXCEEDED;
            }
        }
        // Route TPS
        if (failure == null && limits.maxTpsPerRoute() > 0 && keys.routeKey() != null) {
            TpsBucket b = routeTps.computeIfAbsent(keys.routeKey(), k -> new TpsBucket());
            if (!b.tryAcquire(limits.maxTpsPerRoute())) {
                failure = RejectCause.ROUTE_TPS_EXCEEDED;
            }
        }

        // Roll back any concurrent acquires if any dimension failed.
        // (TPS is window-based — bucket count self-corrects, no rollback.)
        if (failure != null) {
            if (partnerConcurrentAcquired) partnerActive.get(keys.partnerKey()).decrementAndGet();
            if (routeConcurrentAcquired)   routeActive.get(keys.routeKey()).decrementAndGet();
        }
        return failure;
    }

    /**
     * Release concurrent counts when a machine terminates. TPS doesn't need
     * release — it's window-based.
     */
    public void release(QuotaKeys keys) {
        if (keys == null) return;
        if (keys.partnerKey() != null) {
            AtomicInteger c = partnerActive.get(keys.partnerKey());
            if (c != null) c.decrementAndGet();
        }
        if (keys.routeKey() != null) {
            AtomicInteger c = routeActive.get(keys.routeKey());
            if (c != null) c.decrementAndGet();
        }
    }

    public int partnerActive(String key) {
        AtomicInteger c = partnerActive.get(key);
        return c != null ? c.get() : 0;
    }

    public int routeActive(String key) {
        AtomicInteger c = routeActive.get(key);
        return c != null ? c.get() : 0;
    }

    // ─────────────────────────────────────────────────────────────────
    // Sliding 1-second TPS bucket
    // ─────────────────────────────────────────────────────────────────

    private static final class TpsBucket {
        // Packs (windowSecond, count) into a single AtomicLong:
        //   high 32 bits = window second (epoch seconds, low 32 bits)
        //   low  32 bits = count
        private final AtomicLong state = new AtomicLong(0);

        boolean tryAcquire(int maxPerSecond) {
            long nowSec = System.currentTimeMillis() / 1000L;
            while (true) {
                long s = state.get();
                long windowSec = s >>> 32;
                int count = (int) (s & 0xFFFFFFFFL);
                if (windowSec != nowSec) {
                    long fresh = (nowSec << 32) | 1L;
                    if (state.compareAndSet(s, fresh)) return true;
                } else {
                    if (count >= maxPerSecond) return false;
                    long bumped = (windowSec << 32) | ((long)(count + 1) & 0xFFFFFFFFL);
                    if (state.compareAndSet(s, bumped)) return true;
                }
            }
        }
    }
}
