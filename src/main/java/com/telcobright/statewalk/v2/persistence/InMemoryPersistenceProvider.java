package com.telcobright.statewalk.v2.persistence;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory implementation. Useful for tests, local dev, and
 * any setup that needs persistence-on-the-API without real durability.
 *
 * <p><b>Not crash-safe</b> — the snapshot map dies with the JVM. For real
 * durability use a JDBC- or Redis-backed provider.
 */
public class InMemoryPersistenceProvider implements PersistenceProvider {

    private final Map<String, MachineSnapshot> store = new ConcurrentHashMap<>();

    @Override
    public void save(MachineSnapshot snapshot) {
        store.put(snapshot.machineId(), snapshot);
    }

    @Override
    public Optional<MachineSnapshot> load(String machineId) {
        return Optional.ofNullable(store.get(machineId));
    }

    @Override
    public void delete(String machineId) {
        store.remove(machineId);
    }

    /** Test helper: how many snapshots are currently stored. */
    public int size() { return store.size(); }

    /** Test helper: clear the store. */
    public void clear() { store.clear(); }
}
