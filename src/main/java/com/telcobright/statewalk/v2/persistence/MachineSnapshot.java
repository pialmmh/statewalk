package com.telcobright.statewalk.v2.persistence;

/**
 * Persisted state of a single machine, keyed by {@link #machineId}.
 *
 * <p>Written by the {@link com.telcobright.statewalk.v2.registry.api.Registry} after
 * every state transition (when a {@link PersistenceProvider} is configured),
 * and read on rehydration when an inbound event arrives for an unknown id.
 *
 * @param machineId          Primary key for both DB row and Redis key.
 * @param registryName       Identifies which registry produced this snapshot.
 * @param currentState       The state the machine was in when persisted.
 * @param contextClassName   Fully-qualified class name of the context object,
 *                           used for typed JSON deserialization on rehydration.
 *                           {@code null} if the machine had no context.
 * @param contextJsonBase64  Base64-encoded JSON of the context object.
 *                           {@code null} if no context.
 * @param savedAtMs          Wall-clock millis when this snapshot was written.
 * @param timeoutTargetState Target state of the saved state's timeout, if any
 *                           (must be a final state by builder rules). Null if
 *                           the saved state had no timeout active.
 * @param timeoutDeadlineMs  Wall-clock epoch millis when the timeout matures.
 *                           {@code 0} if no timeout active.
 */
public record MachineSnapshot(
    String machineId,
    String registryName,
    String currentState,
    String contextClassName,
    String contextJsonBase64,
    long savedAtMs,
    String timeoutTargetState,
    long timeoutDeadlineMs
) {
    /** True if the timeout deadline has passed at the given wall-clock time. */
    public boolean timeoutFiredBy(long nowMs) {
        return timeoutDeadlineMs > 0 && nowMs >= timeoutDeadlineMs;
    }
}
