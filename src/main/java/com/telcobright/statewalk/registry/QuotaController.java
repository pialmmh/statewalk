package com.telcobright.statewalk.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-key concurrent + TPS quota tracker. Used by {@link StatemachineRegistry} to gate
 * dispatch against {@link QuotaLimits} on partner / route dimensions.
 *
 * <h2>Concurrency model</h2>
 * <ul>
 *   <li>Concurrent counters: one {@code AtomicInteger} per key, mutated inside
 *       {@code ConcurrentHashMap.compute} so a zero count can be pruned
 *       atomically — high-cardinality keys (per-user, per-MAC) no longer
 *       accumulate dead entries forever.</li>
 *   <li>TPS counters: sliding 1-second window per key, a packed
 *       {@code (windowSecond, count)} CAS tuple. Acquire AND release are both
 *       supported, so a dispatch rejected by a later dimension can hand its
 *       burned token back — the counters stay exact within the window.</li>
 * </ul>
 *
 * <h2>Atomicity</h2>
 * <p>{@link #tryAcquire} performs the four checks (partner concurrent, route
 * concurrent, partner TPS, route TPS — concurrency dimensions first so a TPS
 * token is only burned when the request could actually run) and rolls back
 * EVERY partial acquire, TPS included, if a later check fails. The result is
 * a single exact accept-or-reject decision per dispatch.
 */
public final class QuotaController {

    private static final Logger LOG = LoggerFactory.getLogger(QuotaController.class);

    /** Concurrent-machine count per partner key. Zero-count entries are pruned. */
    private final ConcurrentHashMap<String, AtomicInteger> partnerActive = new ConcurrentHashMap<>();
    /** Concurrent-machine count per route key. Zero-count entries are pruned. */
    private final ConcurrentHashMap<String, AtomicInteger> routeActive   = new ConcurrentHashMap<>();

    /** Per-partner TPS bucket. */
    private final ConcurrentHashMap<String, TpsBucket> partnerTps = new ConcurrentHashMap<>();
    /** Per-route TPS bucket. */
    private final ConcurrentHashMap<String, TpsBucket> routeTps   = new ConcurrentHashMap<>();

    // ─────────────────────────────────────────────────────────────────
    // Whole-key acquire / release (dispatch + terminal)
    // ─────────────────────────────────────────────────────────────────

    /**
     * Try to acquire all four quotas atomically. Returns {@code null} on
     * success; on failure, returns the {@link RejectCause} of the first
     * dimension that failed and rolls back every partial acquire.
     */
    public RejectCause tryAcquire(QuotaKeys keys, QuotaLimits limits) {
        if (limits == null || !limits.enforces() || keys == null) return null;

        boolean pConc = false, rConc = false, pTps = false;
        RejectCause failure = null;

        // Concurrency dimensions first — cheap, and failing here means no TPS
        // token is ever burned for a request that could not run anyway.
        if (limits.maxConcurrentPerPartner() > 0 && keys.partnerKey() != null) {
            int after = increment(partnerActive, keys.partnerKey());
            pConc = true;
            if (after > limits.maxConcurrentPerPartner()) {
                failure = RejectCause.PARTNER_CONCURRENCY_EXCEEDED;
            }
        }
        if (failure == null && limits.maxConcurrentPerRoute() > 0 && keys.routeKey() != null) {
            int after = increment(routeActive, keys.routeKey());
            rConc = true;
            if (after > limits.maxConcurrentPerRoute()) {
                failure = RejectCause.ROUTE_CONCURRENCY_EXCEEDED;
            }
        }
        // TPS dimensions last.
        if (failure == null && limits.maxTpsPerPartner() > 0 && keys.partnerKey() != null) {
            TpsBucket b = partnerTps.computeIfAbsent(keys.partnerKey(), k -> new TpsBucket());
            if (b.tryAcquire(limits.maxTpsPerPartner())) pTps = true;
            else failure = RejectCause.PARTNER_TPS_EXCEEDED;
        }
        if (failure == null && limits.maxTpsPerRoute() > 0 && keys.routeKey() != null) {
            TpsBucket b = routeTps.computeIfAbsent(keys.routeKey(), k -> new TpsBucket());
            if (!b.tryAcquire(limits.maxTpsPerRoute())) {
                failure = RejectCause.ROUTE_TPS_EXCEEDED;
            }
        }

        // Roll back EVERY partial acquire on failure — TPS included, so a
        // rejected dispatch does not eat this second's budget.
        if (failure != null) {
            if (pConc) decrementAndPrune(partnerActive, keys.partnerKey());
            if (rConc) decrementAndPrune(routeActive, keys.routeKey());
            if (pTps) {
                TpsBucket b = partnerTps.get(keys.partnerKey());
                if (b != null) b.release();
            }
        }
        return failure;
    }

    /**
     * Take the concurrent slots for {@code keys} WITHOUT checking limits or
     * TPS. One caller: restore-path re-acquire — a restored machine already
     * held its slots before the restart, so the counters must reflect it again
     * (never reject a live session because a cap was lowered meanwhile).
     * Mirrors {@link #tryAcquire}'s gating: a dimension whose cap is {@code 0}
     * is not counted (so a later {@link #release} stays balanced).
     */
    public void acquireUnchecked(QuotaKeys keys, QuotaLimits limits) {
        if (keys == null || limits == null || !limits.enforces()) return;
        if (limits.maxConcurrentPerPartner() > 0 && keys.partnerKey() != null) {
            increment(partnerActive, keys.partnerKey());
        }
        if (limits.maxConcurrentPerRoute() > 0 && keys.routeKey() != null) {
            increment(routeActive, keys.routeKey());
        }
    }

    /**
     * Release concurrent counts when a machine terminates. Gated by the same
     * limits config as acquire, so a dimension that was never counted is never
     * decremented. TPS doesn't need release here — it's window-based.
     */
    public void release(QuotaKeys keys, QuotaLimits limits) {
        if (keys == null || limits == null || !limits.enforces()) return;
        if (limits.maxConcurrentPerPartner() > 0 && keys.partnerKey() != null) {
            decrementAndPrune(partnerActive, keys.partnerKey());
        }
        if (limits.maxConcurrentPerRoute() > 0 && keys.routeKey() != null) {
            decrementAndPrune(routeActive, keys.routeKey());
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Per-dimension ops — used by rebind's acquire-new-BEFORE-release-old
    // protocol (no unchecked re-acquire window, counters can never over-cap)
    // ─────────────────────────────────────────────────────────────────

    /** Checked concurrency acquire on the partner dimension only (no TPS — a rebind is not a new transaction). */
    public RejectCause tryAcquirePartner(String partnerKey, QuotaLimits limits) {
        if (limits == null || limits.maxConcurrentPerPartner() <= 0 || partnerKey == null) return null;
        int after = increment(partnerActive, partnerKey);
        if (after > limits.maxConcurrentPerPartner()) {
            decrementAndPrune(partnerActive, partnerKey);
            return RejectCause.PARTNER_CONCURRENCY_EXCEEDED;
        }
        return null;
    }

    /** Checked concurrency acquire on the route dimension only. */
    public RejectCause tryAcquireRoute(String routeKey, QuotaLimits limits) {
        if (limits == null || limits.maxConcurrentPerRoute() <= 0 || routeKey == null) return null;
        int after = increment(routeActive, routeKey);
        if (after > limits.maxConcurrentPerRoute()) {
            decrementAndPrune(routeActive, routeKey);
            return RejectCause.ROUTE_CONCURRENCY_EXCEEDED;
        }
        return null;
    }

    public void releasePartner(String partnerKey, QuotaLimits limits) {
        if (limits == null || limits.maxConcurrentPerPartner() <= 0 || partnerKey == null) return;
        decrementAndPrune(partnerActive, partnerKey);
    }

    public void releaseRoute(String routeKey, QuotaLimits limits) {
        if (limits == null || limits.maxConcurrentPerRoute() <= 0 || routeKey == null) return;
        decrementAndPrune(routeActive, routeKey);
    }

    // ─────────────────────────────────────────────────────────────────
    // Introspection
    // ─────────────────────────────────────────────────────────────────

    public int partnerActive(String key) {
        AtomicInteger c = partnerActive.get(key);
        return c != null ? c.get() : 0;
    }

    public int routeActive(String key) {
        AtomicInteger c = routeActive.get(key);
        return c != null ? c.get() : 0;
    }

    /** Number of live (non-pruned) counter entries across both concurrency dimensions. */
    public int trackedKeyCount() {
        return partnerActive.size() + routeActive.size();
    }

    /**
     * Drop TPS buckets whose window is stale. Concurrency counters prune
     * themselves on release; TPS buckets only prune here. Call periodically
     * (the registry does, on its timeout scheduler).
     */
    public void pruneStaleTpsBuckets() {
        long nowSec = System.currentTimeMillis() / 1000L;
        partnerTps.entrySet().removeIf(e -> e.getValue().isStale(nowSec));
        routeTps.entrySet().removeIf(e -> e.getValue().isStale(nowSec));
    }

    // ─────────────────────────────────────────────────────────────────
    // Internals
    // ─────────────────────────────────────────────────────────────────

    private static int increment(ConcurrentHashMap<String, AtomicInteger> map, String key) {
        // Mutate inside compute so a concurrent prune can't lose our +1.
        AtomicInteger c = map.compute(key, (k, cur) -> {
            if (cur == null) cur = new AtomicInteger(0);
            cur.incrementAndGet();
            return cur;
        });
        return c.get();
    }

    private static void decrementAndPrune(ConcurrentHashMap<String, AtomicInteger> map, String key) {
        map.compute(key, (k, cur) -> {
            if (cur == null) {
                LOG.warn("quota release for untracked key '{}' — over-release bug upstream", k);
                return null;
            }
            int v = cur.decrementAndGet();
            if (v < 0) {
                LOG.warn("quota counter for key '{}' went negative ({}) — over-release bug upstream; pruning", k, v);
                return null;
            }
            return v == 0 ? null : cur;    // prune at zero
        });
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
                    long bumped = (windowSec << 32) | ((long) (count + 1) & 0xFFFFFFFFL);
                    if (state.compareAndSet(s, bumped)) return true;
                }
            }
        }

        /** Hand back a token acquired this second (rollback of a rejected dispatch). */
        void release() {
            long nowSec = System.currentTimeMillis() / 1000L;
            while (true) {
                long s = state.get();
                long windowSec = s >>> 32;
                int count = (int) (s & 0xFFFFFFFFL);
                if (windowSec != nowSec || count <= 0) return;   // window rolled — nothing to give back
                long dec = (windowSec << 32) | ((long) (count - 1) & 0xFFFFFFFFL);
                if (state.compareAndSet(s, dec)) return;
            }
        }

        boolean isStale(long nowSec) {
            return (state.get() >>> 32) < nowSec - 2;
        }
    }
}
