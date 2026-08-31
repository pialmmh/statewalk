package com.telcobright.statewalk.pool;

/**
 * Contract for any object held in an {@link ObjectPoolManager}.
 *
 * <p>Implementations must:
 * <ul>
 *   <li>Have a no-arg constructor (the pool's factory creates instances via {@code Supplier::get}).</li>
 *   <li>Null every reference field and zero every primitive in {@link #resetForReuse()}.</li>
 *   <li>Be safe to call {@link #resetForReuse()} multiple times (idempotent).</li>
 *   <li>Not perform I/O, blocking calls, or allocation in {@link #resetForReuse()}.</li>
 * </ul>
 *
 * <p>Reset failure (any thrown exception) drops the instance — it is not returned
 * to the pool. The pool refills by allocating a fresh instance on the next borrow.
 */
public interface Poolable {

    /**
     * Reset all instance state so the object is indistinguishable from a fresh allocation.
     */
    void resetForReuse();
}
