package com.telcobright.statewalk.v2.channel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * In-memory {@link Channel} for unit tests.
 *
 * <p>Records every {@link #send} and {@link #cancel} call into a public list
 * for assertions. Tests inject inbound events with {@link #inject(String, Object)}
 * — the registered handler receives them synchronously.
 */
public class TestChannel<O, I> implements Channel<O, I> {

    private final String name;
    private final AtomicBoolean connected = new AtomicBoolean(true);
    public final List<Sent<O>> sends = new CopyOnWriteArrayList<>();
    public final List<String> cancels = new CopyOnWriteArrayList<>();
    private volatile BiConsumer<String, I> handler;

    public TestChannel() { this("test"); }
    public TestChannel(String name) { this.name = name; }

    @Override public void send(String requestId, O command) {
        sends.add(new Sent<>(requestId, command));
    }

    @Override public void cancel(String requestId) {
        cancels.add(requestId);
    }

    @Override public void onInbound(BiConsumer<String, I> handler) {
        this.handler = handler;
    }

    @Override public boolean isConnected() { return connected.get(); }
    @Override public String getName() { return name; }

    /** Test helper: simulate an inbound event for the given request id. */
    public void inject(String requestId, I event) {
        BiConsumer<String, I> h = handler;
        if (h != null) h.accept(requestId, event);
    }

    public void setConnected(boolean v) { connected.set(v); }

    public record Sent<O>(String requestId, O command) {}
}
