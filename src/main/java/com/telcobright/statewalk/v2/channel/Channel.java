package com.telcobright.statewalk.v2.channel;

import java.util.function.BiConsumer;

/**
 * Protocol I/O conduit between a {@link com.telcobright.statewalk.v2.registry.api.Registry}
 * and an external system (FreeSWITCH ESL, sigtran, HTTP, Kafka, ...).
 *
 * <p><b>Send/receive symmetry.</b> A Channel exposes both directions:
 * outbound commands ({@link #send}) and inbound events
 * ({@link #onInbound}). Mid-flight cancel ({@link #cancel}) is per-protocol —
 * uuid_kill on ESL, BYE on SIP, no-op on HTTP, etc.
 *
 * <p><b>Machines never import a Channel implementation.</b> They reach
 * external systems only through their registry's channel API. Swapping
 * protocols, or substituting a {@link TestChannel}, is a registry-config
 * change.
 *
 * @param <O> outbound command type (typed; protocol-specific)
 * @param <I> inbound event type
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
    void cancel(String requestId);

    /**
     * Subscribe to inbound events. The handler receives the request id (parsed
     * from the protocol-specific id field, e.g. ESL Channel-Unique-ID) and the
     * typed inbound event.
     *
     * <p>A registry calls this once during initialisation and routes inbound
     * events to active machines by id. There is one inbound handler per channel.
     */
    void onInbound(BiConsumer<String, I> handler);

    /** True if the channel is presently usable for {@link #send}. */
    boolean isConnected();

    /** Channel name — used in logs, metrics, and multi-channel registries. */
    String getName();
}
