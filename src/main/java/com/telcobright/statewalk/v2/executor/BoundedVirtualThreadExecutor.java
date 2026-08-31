package com.telcobright.statewalk.v2.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A virtual-thread executor with a hard upper bound on in-flight tasks.
 *
 * <p><b>Why bounded.</b> JDK virtual threads are cheap individually — but
 * unbounded creation under load (e.g. a runaway feedback loop, or a buggy
 * caller submitting forever) consumes file descriptors / memory
 * unpredictably. The framework consistently rejects unbounded pools —
 * platform OR virtual.
 *
 * <p><b>Semantics:</b>
 * <ul>
 *   <li>{@link #tryExecute(Runnable)} — submit if a permit is available;
 *       returns false otherwise. Caller decides whether to drop, retry,
 *       or run inline.</li>
 *   <li>{@link #executeOrInline(Runnable)} — submit if possible; otherwise
 *       run on the calling thread. Used by the framework to absorb load
 *       spikes without dropping events (channel threads block briefly
 *       instead of losing data).</li>
 *   <li>{@link #awaitIdle(long, TimeUnit)} — block until all in-flight
 *       tasks complete. Used by tests to deterministically synchronise
 *       with the executor.</li>
 * </ul>
 *
 * <p>Thread naming: {@code <name>-vt-N} where N is per-thread sequence.
 * Daemon threads — won't block JVM shutdown.
 */
public class BoundedVirtualThreadExecutor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedVirtualThreadExecutor.class);

    private final String name;
    private final int maxInFlight;
    private final Semaphore permits;
    private final ExecutorService delegate;
    private final AtomicInteger droppedCount = new AtomicInteger(0);
    private final AtomicInteger inlineCount = new AtomicInteger(0);

    public BoundedVirtualThreadExecutor(String name, int maxInFlight) {
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be > 0 (got " + maxInFlight + ")");
        }
        this.name = name;
        this.maxInFlight = maxInFlight;
        this.permits = new Semaphore(maxInFlight);

        AtomicInteger threadSeq = new AtomicInteger(1);
        ThreadFactory factory = r -> {
            Thread t = Thread.ofVirtual()
                .name(name + "-vt-" + threadSeq.getAndIncrement())
                .unstarted(r);
            return t;
        };
        this.delegate = Executors.newThreadPerTaskExecutor(factory);
    }

    /**
     * Submit if a permit is available. Returns true if accepted, false if
     * the executor is at capacity (caller decides what to do).
     */
    public boolean tryExecute(Runnable r) {
        if (!permits.tryAcquire()) {
            droppedCount.incrementAndGet();
            return false;
        }
        delegate.execute(() -> {
            try { r.run(); }
            catch (Throwable t) { LOG.warn("[{}] task threw: {}", name, t.toString()); }
            finally { permits.release(); }
        });
        return true;
    }

    /**
     * Submit if a permit is available; otherwise run on the calling thread.
     * Pragmatic backpressure — caller absorbs the load spike rather than
     * dropping the work. The {@link #inlineCount} counter tracks how often
     * this fallback fires; high counts indicate undersized executor.
     */
    public void executeOrInline(Runnable r) {
        if (tryExecute(r)) return;
        inlineCount.incrementAndGet();
        if (LOG.isDebugEnabled()) {
            LOG.debug("[{}] executor at capacity, running inline (inFlight={}, max={})",
                name, inFlight(), maxInFlight);
        }
        try { r.run(); } catch (Throwable t) { LOG.warn("[{}] inline task threw: {}", name, t.toString()); }
    }

    /**
     * Block until all in-flight tasks have completed.
     *
     * @return true if drained within the timeout, false otherwise
     */
    public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        if (permits.tryAcquire(maxInFlight, timeout, unit)) {
            permits.release(maxInFlight);
            return true;
        }
        return false;
    }

    public int inFlight()      { return maxInFlight - permits.availablePermits(); }
    public int maxInFlight()   { return maxInFlight; }
    public int droppedCount()  { return droppedCount.get(); }
    public int inlineCount()   { return inlineCount.get(); }
    public String getName()    { return name; }

    /**
     * Adapter for {@link CompletableFuture#thenRunAsync(Runnable, java.util.concurrent.Executor)}
     * and similar APIs. Submissions go through {@link #executeOrInline(Runnable)} —
     * caller backpressures when the executor is at capacity.
     */
    public java.util.concurrent.Executor asExecutor() {
        return this::executeOrInline;
    }

    @Override
    public void close() {
        delegate.shutdown();
        try {
            if (!delegate.awaitTermination(5, TimeUnit.SECONDS)) {
                delegate.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            delegate.shutdownNow();
        }
    }
}
