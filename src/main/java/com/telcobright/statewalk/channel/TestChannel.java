package com.telcobright.statewalk.channel;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory {@link Channel} for unit tests.
 *
 * <p>Records every {@link #send} and {@link #cancel} call into a public list
 * for assertions. Tests inject inbound events with {@link #inject(String, Object)}
 * and can join the returned stage to synchronise with the cell actually
 * processing the event.
 *
 * <p>Injecting before the registry started the channel, or after it stopped,
 * returns a failed stage — mirroring what a real consumer would experience —
 * instead of silently dropping the event (the v2 behaviour that masked dead
 * inbound wiring).
 */
public class TestChannel<O, I> implements Channel<O, I> {

    private final String name;
    private final AtomicBoolean connected = new AtomicBoolean(true);
    private final AtomicBoolean started = new AtomicBoolean(false);
    public final List<Sent<O>> sends = new CopyOnWriteArrayList<>();
    public final List<String> cancels = new CopyOnWriteArrayList<>();
    private volatile Inbound<I> gateway;

    public TestChannel() { this("test"); }
    public TestChannel(String name) { this.name = name; }

    @Override public void send(String requestId, O command) {
        sends.add(new Sent<>(requestId, command));
    }

    @Override public void cancel(String requestId) {
        cancels.add(requestId);
    }

    @Override public void start(Inbound<I> gateway) {
        this.gateway = gateway;
        this.started.set(true);
    }

    @Override public void stop() {
        this.started.set(false);
        this.gateway = null;
    }

    @Override public boolean isConnected() { return connected.get(); }
    @Override public String getName() { return name; }
    public boolean isStarted() { return started.get(); }

    /**
     * Test helper: simulate an inbound event for the given request id. The
     * returned stage completes when the cell processed the event; join it to
     * make assertions deterministic.
     */
    public CompletionStage<Void> inject(String requestId, I event) {
        Inbound<I> g = gateway;
        if (g == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "TestChannel '" + name + "' is not started — bind it to a Registry via .channel(...) "
                + "or call start(gateway) first (injects must not be silently dropped)"));
        }
        return g.offer(requestId, event);
    }

    public void setConnected(boolean v) { connected.set(v); }

    public record Sent<O>(String requestId, O command) {}
}
