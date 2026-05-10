package com.telcobright.statewalk.v2.persistence;

import java.util.Optional;

/**
 * Backend-agnostic persistence interface for {@link MachineSnapshot} rows.
 *
 * <p>Concrete providers (JDBC, Redis, in-memory test) implement these three
 * operations. The framework handles JSON serialization, base64 encoding,
 * timeout-deadline arithmetic, and rehydration logic — the provider only
 * stores and retrieves snapshot records.
 *
 * <h2>Wired in via the builder:</h2>
 * <pre>{@code
 * Statewalk.builder()
 *     .persistence(new InMemoryPersistenceProvider())
 *     .rehydrate(true)
 *     .registry(...)
 *     .build();
 * }</pre>
 *
 * <p><b>Persistence vs rehydration are independent toggles.</b> Calling
 * {@code .persistence(...)} alone causes every state transition to be saved.
 * Adding {@code .rehydrate(true)} additionally allows inbound events for
 * unknown machine ids to load + restore from the provider.
 *
 * <p><b>Throughput contract:</b> {@link #save} is called inline on every
 * state transition. Implementations should make this fast (or implement
 * batching internally if the backend is slow). The framework does not
 * batch on the caller side in v1.
 */
public interface PersistenceProvider {

    /**
     * Persist a snapshot. Replaces any existing row for the same
     * {@link MachineSnapshot#machineId()}.
     */
    void save(MachineSnapshot snapshot);

    /**
     * Load the snapshot for a machine id, or empty if no row exists.
     */
    Optional<MachineSnapshot> load(String machineId);

    /**
     * Delete the snapshot for a machine id. Called by the framework when a
     * machine reaches a terminal state — once terminated there is no value
     * in keeping the row, and rehydrating a terminated machine is a bug.
     *
     * <p>Idempotent — deleting a missing key is a no-op.
     */
    void delete(String machineId);
}
