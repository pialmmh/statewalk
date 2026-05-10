package com.telcobright.statewalk.v2.timeout;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Cancelling a tracked timeout removes its entry from the active map.
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
     */
    public ScheduledFuture<?> schedule(Runnable action, long delay, TimeUnit unit) {
        return scheduler.schedule(action, delay, unit);
    }

    /**
     * Tracked timeout, cancellable by id. Replaces any existing tracked timeout
     * for the same id.
     */
    public ScheduledFuture<?> scheduleTracked(String id, Runnable action, long delay, TimeUnit unit) {
        cancelTracked(id);
        ScheduledFuture<?> f = scheduler.schedule(() -> {
            try {
                action.run();
            } finally {
                activeTimeouts.remove(id);
            }
        }, delay, unit);
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
