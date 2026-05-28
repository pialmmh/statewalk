package com.telcobright.statewalk.v2.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory implementation. Useful for tests, local dev, and any
 * setup that needs persistence-on-the-API without real durability.
 *
 * <p>Keyed by {@code (machineId, registryName)} so multiple cells of one
 * request id can coexist (e.g. CallSupervisor + CallSignaling + BalanceTracker
 * for the same call UUID).
 *
 * <p><b>Not crash-safe</b> — the snapshot map dies with the JVM. For real
 * durability use a JDBC- or Redis-backed provider.
 */
public class InMemoryPersistenceProvider implements PersistenceProvider {

    /** Outer key = machineId. Inner key = registryName. */
    private final Map<String, Map<String, MachineSnapshot>> store = new ConcurrentHashMap<>();

    @Override
    public void save(MachineSnapshot snapshot) {
        store.compute(snapshot.machineId(), (k, byReg) -> {
            if (byReg == null) byReg = new ConcurrentHashMap<>();
            byReg.put(snapshot.registryName(), snapshot);
            return byReg;
        });
    }

    @Override
    public Optional<MachineSnapshot> load(String machineId, String registryName) {
        Map<String, MachineSnapshot> byReg = store.get(machineId);
        if (byReg == null) return Optional.empty();
        return Optional.ofNullable(byReg.get(registryName));
    }

    @Override
    public List<MachineSnapshot> loadAll(String machineId) {
        Map<String, MachineSnapshot> byReg = store.get(machineId);
        if (byReg == null) return List.of();
        return new ArrayList<>(byReg.values());
    }

    @Override
    public void delete(String machineId, String registryName) {
        store.computeIfPresent(machineId, (k, byReg) -> {
            byReg.remove(registryName);
            return byReg.isEmpty() ? null : byReg;
        });
    }

    @Override
    public List<MachineSnapshot> loadMatured(String registryName, long nowMs) {
        List<MachineSnapshot> out = new ArrayList<>();
        for (Map<String, MachineSnapshot> byReg : store.values()) {
            MachineSnapshot s = byReg.get(registryName);
            if (s != null && s.timeoutFiredBy(nowMs)) out.add(s);
        }
        return out;
    }

    /** Test helper: total snapshot count across all ids and registries. */
    public int size() {
        int total = 0;
        for (var byReg : store.values()) total += byReg.size();
        return total;
    }

    /** Test helper: clear the store. */
    public void clear() { store.clear(); }
}
