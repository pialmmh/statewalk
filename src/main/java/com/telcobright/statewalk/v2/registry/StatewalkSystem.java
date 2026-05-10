package com.telcobright.statewalk.v2.registry;

import com.telcobright.statewalk.v2.event.EventTypeRegistry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Runtime handle returned by {@link Statewalk.Builder#build()}.
 *
 * <p>Public surface: {@link #dispatch(String, String, Object)},
 * {@link #shutdown()}, plus introspection helpers. Everything else
 * (initialising registries, binding channels, validating supervisor
 * children) has already happened inside {@code build()}.
 *
 * <p>The system holds the framework's single {@link EventTypeRegistry};
 * all registered events and event pools are reachable through
 * {@link #getEventTypes()}.
 */
public final class StatewalkSystem {

    private final Map<String, Statewalk.RegistryEntry> registries;
    private final EventTypeRegistry eventTypes;

    StatewalkSystem(Map<String, Statewalk.RegistryEntry> registries,
                    EventTypeRegistry eventTypes) {
        this.registries = Collections.unmodifiableMap(new LinkedHashMap<>(registries));
        this.eventTypes = eventTypes;
    }

    /**
     * Dispatch a new request to the named registry. Equivalent to looking
     * up the registry and calling its {@code dispatch(requestId, task)}.
     *
     * @return true on success, false if registry unknown / at capacity /
     *     shutting down / duplicate id.
     */
    public boolean dispatch(String registryName, String requestId, Object task) {
        Statewalk.RegistryEntry e = registries.get(registryName);
        if (e == null) return false;
        return e.registry().dispatch(requestId, task);
    }

    /** Look up a registry by the name it was registered under. */
    public Registry<?, ?> getRegistry(String name) {
        Statewalk.RegistryEntry e = registries.get(name);
        return e == null ? null : e.registry();
    }

    public EventTypeRegistry getEventTypes() { return eventTypes; }

    /**
     * Aggregate statistics across every registry plus the event-type registry.
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Map<String, Object>> perRegistry = new LinkedHashMap<>();
        for (var entry : registries.values()) {
            perRegistry.put(entry.name(), entry.registry().getStatistics());
        }
        out.put("registries", perRegistry);
        out.put("eventTypes", eventTypes.poolStatistics());
        return out;
    }

    /**
     * Shut down every registry in registration order. Each registry drains
     * active machines and clears its pool.
     */
    public void shutdown() {
        for (var entry : registries.values()) {
            try { entry.registry().shutdown(); } catch (RuntimeException ignored) {}
        }
    }
}
