package com.telcobright.statewalk.v2.persistence;

import java.util.List;
import java.util.Optional;

/**
 * Backend-agnostic persistence interface for {@link MachineSnapshot} rows.
 *
 * <p>Concrete providers (JDBC, Redis, in-memory test) implement these
 * operations. The framework handles JSON serialization, base64 encoding,
 * timeout-deadline arithmetic, and rehydration logic — the provider only
 * stores and retrieves snapshot records.
 *
 * <h2>Compound key</h2>
 *
 * <p>Snapshots are keyed by <strong>(machineId, registryName)</strong>. This
 * matters when one logical request id (e.g. a call UUID) is associated with
 * multiple machine types in a multi-machine registry: every cell saves
 * independently, every cell can be loaded independently, and the framework
 * can ask {@link #loadAll(String)} for every cell of a given request id when
 * restoring after a crash.
 *
 * <h2>Throughput contract</h2>
 *
 * <p>{@link #save} is called inline on every state transition. Implementations
 * should make this fast (or batch internally if the backend is slow). The
 * framework does not batch on the caller side in v1.
 */
public interface PersistenceProvider {

    /**
     * Persist a snapshot. Replaces any existing row keyed by
     * (snapshot.machineId, snapshot.registryName).
     */
    void save(MachineSnapshot snapshot);

    /**
     * Load the snapshot for one specific cell, or empty if no row exists.
     */
    Optional<MachineSnapshot> load(String machineId, String registryName);

    /**
     * Load every snapshot for the given machine id, across all registry
     * names. Used by the framework on cross-cell rehydration: when an
     * inbound event arrives for an unknown id, restore every cell that has
     * a saved state for it.
     *
     * <p>Returned list is empty if the id has no snapshots.
     */
    List<MachineSnapshot> loadAll(String machineId);

    /**
     * Delete the snapshot for one specific cell. Called by the framework
     * when a machine reaches a terminal state.
     *
     * <p>Idempotent — deleting a missing key is a no-op.
     */
    void delete(String machineId, String registryName);

    /**
     * Load every snapshot for {@code registryName} whose state-timeout has
     * already matured at {@code nowMs} — that is, {@code 0 < timeoutDeadlineMs
     * <= nowMs}.
     *
     * <p>Called once at registry build (when rehydration is enabled) so the
     * framework can settle machines whose timeout fell due while the process
     * was down. Without it those snapshots — and any external reservation they
     * hold (e.g. a prepaid balance hold) — would persist until a coincidental
     * inbound event for the same id happened to arrive.
     *
     * <p><b>Default returns empty:</b> a provider that does not implement this
     * opts out of <em>proactive</em> startup recovery only; it still recovers
     * lazily on inbound events via {@link #load}. Durable providers should
     * override and push the deadline filter into the backing store.
     */
    default List<MachineSnapshot> loadMatured(String registryName, long nowMs) {
        return List.of();
    }

    /**
     * Load <b>every</b> snapshot for {@code registryName} — i.e. every
     * unfinished machine, since terminal snapshots are deleted on terminate.
     *
     * <p>Called once at registry build (when rehydration is enabled) so a fresh
     * node can resume all in-flight requests after a failover/restart: matured
     * cells settle, the rest keep running. This is the disaster-recovery entry
     * point — the new node rebuilds state from redis/db and carries on.
     *
     * <p><b>Default returns empty:</b> a provider that does not implement this
     * opts out of proactive startup resume; lazy rehydration on inbound events
     * still works. Durable providers should override.
     */
    default List<MachineSnapshot> loadAllForRegistry(String registryName) {
        return List.of();
    }
}
