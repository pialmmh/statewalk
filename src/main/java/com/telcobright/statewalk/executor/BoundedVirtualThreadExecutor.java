package com.telcobright.statewalk.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A virtual-thread executor for the registry's per-cell chain work.
 *
 * <h2>Why not a hard submission bound (v3)</h2>
 * v2 bounded submissions with a semaphore and ran overflow tasks inline on the
 * submitting thread. Under saturation that executed cell tasks inside
 * {@code ConcurrentHashMap.compute} (→ "Recursive update" drops) and made
 * {@code awaitIdle} blind to inline tasks. v3 keeps the executor
 * <b>always-accepting</b> — internal chain progress is never blocked or run
 * inline — and moves load shedding to the registry's ENTRY points (dispatch /
 * inbound), which is where backpressure belongs. {@code maxInFlight} remains
 * as an overload <b>watermark</b>: crossing it logs a WARN so undersizing is
 * visible, but nothing is dropped.
 *
 * <p>In-flight tasks are counted exactly (including the rare post-close
 * fallback), so {@link #awaitIdle} means what it says.
 *
 * <p>Thread naming: {@code <name>-vt-N}. Daemon virtual threads — won't block
 * JVM shutdown.
 */
public class BoundedVirtualThreadExecutor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedVirtualThreadExecutor.class);

    private final String name;
    private final int maxInFlight;
    private final ExecutorService delegate;
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicLong overloadWarnedAtMs = new AtomicLong(0);
    private final Object idleLock = new Object();
    private volatile boolean closed;

    public BoundedVirtualThreadExecutor(String name, int maxInFlight) {
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be > 0 (got " + maxInFlight + ")");
        }
        this.name = name;
        this.maxInFlight = maxInFlight;

        AtomicInteger threadSeq = new AtomicInteger(1);
        ThreadFactory factory = r -> Thread.ofVirtual()
            .name(name + "-vt-" + threadSeq.getAndIncrement())
            .unstarted(r);
        this.delegate = Executors.newThreadPerTaskExecutor(factory);
    }

    /**
     * Submit a task. Always accepted: on a virtual thread normally; run on the
     * calling thread only in the shutdown race where the delegate has already
     * closed (so queued terminal work still completes instead of throwing into
     * the caller or leaking). Crossing the {@code maxInFlight} watermark logs
     * a rate-limited WARN.
     */
    public void submit(Runnable r) {
        int n = inFlight.incrementAndGet();
        if (n > maxInFlight) warnOverload(n);
        Runnable wrapped = () -> {
            try { r.run(); }
            catch (Throwable t) { LOG.warn("[{}] task threw: {}", name, t.toString()); }
            finally {
                if (inFlight.decrementAndGet() == 0) {
                    synchronized (idleLock) { idleLock.notifyAll(); }
                }
            }
        };
        try {
            delegate.execute(wrapped);
        } catch (RejectedExecutionException closedRace) {
            // Delegate already shut down (registry close in progress). Run the
            // task here so terminal work still lands and the count stays exact.
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] executor closed — running task on caller thread", name);
            }
            wrapped.run();
        }
    }

    private void warnOverload(int n) {
        long now = System.currentTimeMillis();
        long last = overloadWarnedAtMs.get();
        if (now - last > 5_000 && overloadWarnedAtMs.compareAndSet(last, now)) {
            LOG.warn("[{}] in-flight tasks {} exceed watermark {} — the executor is undersized "
                + "or a downstream stall is backing work up", name, n, maxInFlight);
        }
    }

    /**
     * Block until all in-flight tasks have completed.
     *
     * @return true if drained within the timeout, false otherwise
     */
    public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (idleLock) {
            while (inFlight.get() > 0) {
                long remainingNs = deadline - System.nanoTime();
                if (remainingNs <= 0) return false;
                long ms = remainingNs / 1_000_000;
                idleLock.wait(Math.max(1, ms), (int) (remainingNs % 1_000_000));
            }
        }
        return true;
    }

    public int inFlight()      { return inFlight.get(); }
    public int maxInFlight()   { return maxInFlight; }
    public String getName()    { return name; }
    public boolean isClosed()  { return closed; }

    /**
     * Adapter for {@code CompletableFuture.thenRunAsync(..., executor)} and
     * similar APIs. Submissions go through {@link #submit(Runnable)}.
     */
    public java.util.concurrent.Executor asExecutor() {
        return this::submit;
    }

    @Override
    public void close() {
        closed = true;
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
