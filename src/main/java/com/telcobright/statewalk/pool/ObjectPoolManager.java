package com.telcobright.statewalk.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Generic, lock-free object pool keyed on a {@link Poolable} type.
 *
 * <p>Behaviour:
 * <ul>
 *   <li>{@link #borrow()} never blocks. If the pool is empty, a fresh instance
 *       is allocated. The pool never holds more than {@code maxPoolSize} idle
 *       instances — surplus returns are dropped for GC.</li>
 *   <li>{@link #returnObject(Poolable)} calls {@link Poolable#resetForReuse()}.
 *       If reset throws, the instance is dropped (not returned).</li>
 *   <li><b>Containment guard (v3):</b> the pool tracks membership by identity;
 *       a double-return of the same instance is rejected with an ERROR log
 *       instead of corrupting the pool (one instance handed to two borrowers
 *       was the v2 failure mode).</li>
 *   <li>Pre-warms 25% of {@code maxPoolSize} on construction (capped at 100).</li>
 * </ul>
 *
 * <p>Pulled forward from the v1 library where it has been in production for SMS
 * sigtran and ESL voice campaigns at 1000s of concurrent machines.
 */
public class ObjectPoolManager<T extends Poolable> {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectPoolManager.class);

    private final ConcurrentLinkedQueue<T> available;
    /** Identity set of instances currently INSIDE the pool ({@code available}). */
    private final Set<T> pooled;
    private final AtomicInteger totalCreated;
    private final AtomicInteger totalBorrowed;
    private final AtomicInteger totalReturned;
    private final AtomicInteger resetFailures;
    private final AtomicInteger doubleReturns;
    private final AtomicInteger capDrops;
    private final int maxPoolSize;
    private final Supplier<T> factory;
    private final String name;

    public ObjectPoolManager(String name, Supplier<T> factory, int maxPoolSize) {
        this.name = name;
        this.factory = factory;
        this.maxPoolSize = maxPoolSize;
        this.available = new ConcurrentLinkedQueue<>();
        // Poolable implementations don't override equals/hashCode, so this is
        // an identity set in practice.
        this.pooled = ConcurrentHashMap.newKeySet();
        this.totalCreated = new AtomicInteger(0);
        this.totalBorrowed = new AtomicInteger(0);
        this.totalReturned = new AtomicInteger(0);
        this.resetFailures = new AtomicInteger(0);
        this.doubleReturns = new AtomicInteger(0);
        this.capDrops = new AtomicInteger(0);

        prewarm(Math.min(maxPoolSize / 4, 100));
    }

    private void prewarm(int initialSize) {
        for (int i = 0; i < initialSize; i++) {
            T obj = factory.get();
            obj.resetForReuse();
            pooled.add(obj);
            available.offer(obj);
            totalCreated.incrementAndGet();
        }
    }

    /**
     * Borrow an instance. Never blocks. Returns a pooled instance if available,
     * otherwise allocates a fresh one.
     */
    public T borrow() {
        T obj = available.poll();
        if (obj == null) {
            obj = factory.get();
            totalCreated.incrementAndGet();
        } else {
            pooled.remove(obj);
        }
        totalBorrowed.incrementAndGet();
        return obj;
    }

    /**
     * Return an instance to the pool after calling {@link Poolable#resetForReuse()}.
     * A double-return (the instance is already inside the pool) is rejected —
     * resetting it again could tear down state a concurrent borrower now owns,
     * and offering it twice would eventually hand ONE instance to TWO
     * borrowers. If reset throws, the instance is dropped. If the pool is at
     * its cap, the instance is dropped for GC.
     */
    public void returnObject(T obj) {
        if (obj == null) return;
        if (!pooled.add(obj)) {
            doubleReturns.incrementAndGet();
            LOG.error("[{}] double return of pooled instance {} rejected — caller bug "
                + "(two owners tried to retire the same machine)", name, System.identityHashCode(obj));
            return;
        }
        try {
            obj.resetForReuse();
        } catch (RuntimeException e) {
            resetFailures.incrementAndGet();
            pooled.remove(obj);
            return;   // instance dropped; pool refills on next borrow
        }
        if (available.size() >= maxPoolSize) {
            pooled.remove(obj);       // cap reached — drop for GC
            capDrops.incrementAndGet();
            return;
        }
        available.offer(obj);
        totalReturned.incrementAndGet();
    }

    public void clear() {
        available.clear();
        pooled.clear();
    }

    public PoolStatistics getStatistics() {
        int borrowed = totalBorrowed.get();
        int created = totalCreated.get();
        double hitRatio = borrowed > 0 ? (double) (borrowed - created) / borrowed : 0.0;
        return new PoolStatistics(
            name,
            available.size(),
            created,
            borrowed,
            totalReturned.get(),
            resetFailures.get(),
            doubleReturns.get(),
            capDrops.get(),
            maxPoolSize,
            hitRatio);
    }

    public String getName() { return name; }
    public int getMaxPoolSize() { return maxPoolSize; }

    public record PoolStatistics(
        String name,
        int available,
        int totalCreated,
        int totalBorrowed,
        int totalReturned,
        int resetFailures,
        int doubleReturns,
        int capDrops,
        int maxSize,
        double hitRatio
    ) {
        /** Every borrow is accounted for: back in the pool, dropped at cap, or dropped on a failed reset. */
        public int reclaimed() { return totalReturned + capDrops + resetFailures; }

        @Override
        public String toString() {
            return String.format(
                "Pool[%s]: available=%d created=%d borrowed=%d returned=%d resetFails=%d doubleReturns=%d capDrops=%d hitRatio=%.2f%%",
                name, available, totalCreated, totalBorrowed, totalReturned, resetFailures, doubleReturns, capDrops, hitRatio * 100);
        }
    }
}
