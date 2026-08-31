package com.telcobright.statewalk.timeout;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps a {@link ScheduledExecutorService} with the leak-prevention discipline
 * the framework relies on.
 *
 * <p>Critical settings:
 * <ul>
 *   <li>{@code setRemoveOnCancelPolicy(true)} — cancelled tasks are physically
 *       removed from the queue so their captured references can be reclaimed.</li>
 *   <li>Daemon threads — does not prevent JVM shutdown.</li>
 *   <li>Named threads — for debugging.</li>
 * </ul>
 *
 * <p>Tracked timeouts are keyed by id (typically the requestId / machineId).
 * Cancelling a tracked timeout removes its entry from the active map. The
 * fired-task cleanup uses the two-arg {@code remove(id, future)} so a fired
 * timer can never evict a NEWER tracked timer that reused the same id.
 */
public class TimeoutManager {

    private final String name;
    private final int threadPoolSize;
    private final ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<String, ScheduledFuture<?>> activeTimeouts;

    public TimeoutManager(String name, int threadPoolSize) {
        this.name = name;
        this.threadPoolSize = threadPoolSize;
        this.activeTimeouts = new ConcurrentHashMap<>();

        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(
            threadPoolSize,
            new ThreadFactory() {
                private final AtomicInteger n = new AtomicInteger(1);
                @Override public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "Timeout-" + name + "-" + n.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            });
        exec.setRemoveOnCancelPolicy(true);
        exec.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        exec.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.scheduler = exec;
    }

    /**
     * Fire-and-forget timeout — caller does not need to cancel it later.
     * After {@link #shutdown()} the runnable is silently discarded.
     */
    public ScheduledFuture<?> schedule(Runnable action, long delay, TimeUnit unit) {
        try {
            return scheduler.schedule(action, delay, unit);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            return null;    // shutting down — timers are moot
        }
    }

    /**
     * Tracked timeout, cancellable by id. Replaces any existing tracked timeout
     * for the same id.
     */
    public ScheduledFuture<?> scheduleTracked(String id, Runnable action, long delay, TimeUnit unit) {
        cancelTracked(id);
        AtomicReference<ScheduledFuture<?>> self = new AtomicReference<>();
        ScheduledFuture<?> f;
        try {
            f = scheduler.schedule(() -> {
                try {
                    action.run();
                } finally {
                    // Two-arg remove: only evict OUR entry. A newer tracked timer
                    // that reused this id must survive our cleanup.
                    ScheduledFuture<?> mine = self.get();
                    if (mine != null) activeTimeouts.remove(id, mine);
                }
            }, delay, unit);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            return null;    // shutting down
        }
        self.set(f);
        activeTimeouts.put(id, f);
        return f;
    }

    public boolean cancelTracked(String id) {
        ScheduledFuture<?> f = activeTimeouts.remove(id);
        if (f == null) return false;
        return f.cancel(false);
    }

    public int activeCount() {
        return activeTimeouts.size();
    }

    public String getName() { return name; }
    public int getThreadPoolSize() { return threadPoolSize; }

    public void shutdown() {
        for (ScheduledFuture<?> f : activeTimeouts.values()) {
            f.cancel(false);
        }
        activeTimeouts.clear();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
