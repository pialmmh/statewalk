package com.telcobright.statewalk.channel;

import java.util.concurrent.CompletionStage;

/**
 * Protocol I/O conduit between a {@link com.telcobright.statewalk.registry.Registry}
 * and an external system (FreeSWITCH ESL, sigtran, HTTP, Kafka, Redis streams, ...).
 *
 * <h2>Send/receive symmetry</h2>
 * A Channel exposes both directions: outbound commands ({@link #send}) and
 * inbound events (delivered to the {@link Inbound} gateway the registry hands
 * to {@link #start}). Mid-flight cancel ({@link #cancel}) is per-protocol —
 * uuid_kill on ESL, BYE on SIP, no-op on HTTP, etc.
 *
 * <h2>Lifecycle (v3)</h2>
 * The registry OWNS the channel lifecycle: it calls {@link #start} once at
 * build (handing over the inbound gateway) and {@link #stop} first thing in
 * shutdown, so no event is consumed-and-lost after the registry stops
 * accepting. Implementations must make both idempotent.
 *
 * <h2>Delivery contract (v3)</h2>
 * {@link Inbound#offer} returns a {@link CompletionStage} that completes when
 * the event has actually been processed by the target cell (not merely
 * queued), or completes exceptionally when it was rejected (unknown id with no
 * recovery path, overload, shutdown). At-least-once consumers (Kafka) commit
 * their offset on completion; at-most-once consumers may ignore the stage.
 *
 * <h2>Threading contract</h2>
 * {@code offer} is non-blocking: the registry resolves the target and queues
 * the event on the cell's serial chain, then returns. The one exception is a
 * cold rehydration probe (a synchronous store read) — consumers that cannot
 * tolerate that should keep rehydration off their hot path via startup
 * recovery. The registry never throws into the channel's consumer thread —
 * failures travel through the stage.
 *
 * <p><b>Machines never import a Channel implementation.</b> They reach
 * external systems only through their registry's channel API. Swapping
 * protocols, or substituting a {@link TestChannel}, is a registry-config
 * change.
 *
 * @param <O> outbound command type (typed; protocol-specific)
 * @param <I> inbound event type — must implement {@code StatemachineEvent}
 *            (or the registry builder must be given a decoder)
 */
public interface Channel<O, I> {

    /**
     * Send a command associated with a request id. Fire-and-forget — delivery
     * confirmation, retries, and protocol-specific error semantics are the
     * implementation's responsibility.
     */
    void send(String requestId, O command);

    /**
     * Cancel any in-flight work for the request id. Implementation-specific:
     * ESL → uuid_kill, sigtran → cancel pending request, HTTP → typically no-op.
     */
    default void cancel(String requestId) {}

    /**
     * Begin consuming: the registry calls this exactly once at build, handing
     * the gateway that inbound events flow through. Implementations that pull
     * (Kafka poll loops) start their consumer here; push implementations
     * (ESL socket) register their listener here.
     */
    void start(Inbound<I> gateway);

    /**
     * Stop consuming and drain in-flight handler calls. The registry calls
     * this first in shutdown. Must be idempotent; after it returns the
     * implementation must not invoke the gateway again.
     */
    void stop();

    /** True if the channel is presently usable for {@link #send}. */
    boolean isConnected();

    /** Channel name — used in logs, metrics, and multi-channel registries. */
    String getName();

    /**
     * The registry-side gateway inbound events are offered to.
     */
    @FunctionalInterface
    interface Inbound<I> {
        /**
         * Offer one inbound event for {@code requestId}. The returned stage
         * completes when the target cell has processed the event, or
         * exceptionally when the event was rejected. Never throws.
         */
        CompletionStage<Void> offer(String requestId, I event);
    }
}
