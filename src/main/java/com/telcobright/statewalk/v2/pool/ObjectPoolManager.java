package com.telcobright.statewalk.v2.pool;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Generic, lock-free object pool keyed on a {@link Poolable} type.
 *
 * <p>Behaviour:
 * <ul>
 *   <li>{@link #borrow()} never blocks. If the pool is empty, a fresh instance
 *       is allocated. The pool can grow above {@code maxPoolSize} (soft cap).</li>
 *   <li>{@link #returnObject(Poolable)} calls {@link Poolable#resetForReuse()}.
 *       If reset throws, the instance is dropped (not returned).</li>
 *   <li>Pre-warms 25% of {@code maxPoolSize} on construction (capped at 100).</li>
 * </ul>
 *
 * <p>Pulled forward from the v1 library where it has been in production for SMS
 * sigtran and ESL voice campaigns at 1000s of concurrent machines.
 */
public class ObjectPoolManager<T extends Poolable> {

    private final ConcurrentLinkedQueue<T> available;
    private final AtomicInteger totalCreated;
    private final AtomicInteger totalBorrowed;
    private final AtomicInteger totalReturned;
    private final AtomicInteger resetFailures;
    private final int maxPoolSize;
    private final Supplier<T> factory;
    private final String name;

    public ObjectPoolManager(String name, Supplier<T> factory, int maxPoolSize) {
        this.name = name;
        this.factory = factory;
        this.maxPoolSize = maxPoolSize;
        this.available = new ConcurrentLinkedQueue<>();
        this.totalCreated = new AtomicInteger(0);
        this.totalBorrowed = new AtomicInteger(0);
        this.totalReturned = new AtomicInteger(0);
        this.resetFailures = new AtomicInteger(0);

        prewarm(Math.min(maxPoolSize / 4, 100));
    }

    private void prewarm(int initialSize) {
        for (int i = 0; i < initialSize; i++) {
            T obj = factory.get();
            obj.resetForReuse();
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
        }
        totalBorrowed.incrementAndGet();
        return obj;
    }

    /**
     * Return an instance to the pool after calling {@link Poolable#resetForReuse()}.
     * If reset throws, the instance is dropped.
     */
    public void returnObject(T obj) {
        if (obj == null) return;
        try {
            obj.resetForReuse();
            available.offer(obj);
            totalReturned.incrementAndGet();
        } catch (RuntimeException e) {
            resetFailures.incrementAndGet();
            // Instance dropped; pool will refill on next borrow.
        }
    }

    public void clear() {
        available.clear();
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
        int maxSize,
        double hitRatio
    ) {
        @Override
        public String toString() {
            return String.format(
                "Pool[%s]: available=%d created=%d borrowed=%d returned=%d resetFails=%d hitRatio=%.2f%%",
                name, available, totalCreated, totalBorrowed, totalReturned, resetFailures, hitRatio * 100);
        }
    }
}
