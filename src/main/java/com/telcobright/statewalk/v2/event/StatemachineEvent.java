package com.telcobright.statewalk.v2.event;

/**
 * Marker for anything that can be fired into a {@link com.telcobright.statewalk.v2.machine.Machine}.
 *
 * <p><b>Typed dispatch only.</b> Transitions and stay-handlers are keyed on
 * the event class — there are no string event names anywhere in the framework.
 * This is a deliberate performance choice: hash on a {@code Class} object
 * (identity-based) is cheaper than {@code String} hashing, and class-based
 * dispatch is type-safe at compile time.
 *
 * <p><b>Registration mandatory.</b> Every concrete event class must be
 * registered with the framework's {@link EventTypeRegistry} before it can be
 * fired. Firing an unregistered event throws — this catches typos and
 * forgotten events at the first dispatch attempt rather than silently
 * dropping them.
 *
 * <p><b>First-message flag.</b> Override {@link #isFirst()} to return
 * {@code true} on creation / init events (e.g. {@code CHANNEL_PARK} for a
 * call, {@code SubmitSm} for an SMS). The registry uses this signal on
 * inbound dispatch to decide between creating a new machine and rehydrating
 * an existing one — see
 * {@link com.telcobright.statewalk.v2.registry.Registry#onInboundEvent}.
 *
 * <p>Implementations are typically:
 * <ul>
 *   <li><b>Records</b> — small, immutable, no pooling needed.</li>
 *   <li><b>Mutable Poolable classes</b> — when allocation rate is high enough
 *       that GC pressure matters; the framework pools and resets these.</li>
 * </ul>
 */
public interface StatemachineEvent {

    /**
     * Returns {@code true} if this event marks the start of a new request /
     * call / message flow — the framework will create a new machine for it
     * if no machine is currently bound to the request id. Defaults to
     * {@code false} (most events are mid-flow signals).
     */
    default boolean isFirst() { return false; }
}
