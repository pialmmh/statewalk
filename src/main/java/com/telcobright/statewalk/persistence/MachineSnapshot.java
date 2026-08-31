package com.telcobright.statewalk.persistence;

/**
 * Persisted state of a single machine, keyed by {@link #machineId}.
 *
 * <p>Written by the {@link com.telcobright.statewalk.registry.StatemachineRegistry} after
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
 * @param globalDeadlineMs   Wall-clock epoch millis when the request's GLOBAL
 *                           (whole-lifetime) timeout matures; {@code 0} if the
 *                           registry has no global timeout. Carried on the
 *                           supervisor cell's snapshots only, so a restored
 *                           request keeps its original lifetime cap instead of
 *                           living forever (v3).
 */
public record MachineSnapshot(
    String machineId,
    String registryName,
    String currentState,
    String contextClassName,
    String contextJsonBase64,
    long savedAtMs,
    String timeoutTargetState,
    long timeoutDeadlineMs,
    long globalDeadlineMs
) {
    /** Compatibility constructor for snapshots without a global deadline. */
    public MachineSnapshot(String machineId, String registryName, String currentState,
                           String contextClassName, String contextJsonBase64, long savedAtMs,
                           String timeoutTargetState, long timeoutDeadlineMs) {
        this(machineId, registryName, currentState, contextClassName, contextJsonBase64,
             savedAtMs, timeoutTargetState, timeoutDeadlineMs, 0L);
    }

    /** True if the timeout deadline has passed at the given wall-clock time. */
    public boolean timeoutFiredBy(long nowMs) {
        return timeoutDeadlineMs > 0 && nowMs >= timeoutDeadlineMs;
    }

    /** True if the request's global lifetime cap has passed at the given wall-clock time. */
    public boolean globalDeadlinePassedBy(long nowMs) {
        return globalDeadlineMs > 0 && nowMs >= globalDeadlineMs;
    }
}
