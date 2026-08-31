package com.telcobright.statewalk.event;

/**
 * Marker for anything that can be fired into a {@link com.telcobright.statewalk.machine.Machine}.
 *
 * <p><b>Typed dispatch only.</b> Transitions and stay-handlers are keyed on
 * the event class — there are no string event names anywhere in the framework.
 * This is a deliberate performance choice: hash on a {@code Class} object
 * (identity-based) is cheaper than {@code String} hashing, and class-based
 * dispatch is type-safe at compile time.
 *
 * <p><b>No registration step.</b> Routing is declared, not registered: the
 * supervisor's resolver rules and each state's transition table name the
 * event classes they handle, and the registry validates route targets at
 * build. An event no rule mentions is logged (unrouted → WARN at the
 * resolver; unhandled in a state → silently ignored by design).
 *
 * <p><b>First-message flag.</b> Override {@link #isFirst()} to return
 * {@code true} on creation / init events (e.g. {@code CHANNEL_PARK} for a
 * call, {@code SubmitSm} for an SMS). The registry uses this signal on
 * inbound dispatch to decide between creating a new machine and rehydrating
 * an existing one — see
 * {@link com.telcobright.statewalk.registry.StatemachineRegistry#onInboundEvent}.
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
