package com.telcobright.statewalk.v2.registry.api;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Routing table for {@link MultiRegistry}. Maps event class to a target
 * machine type plus an id extractor that pulls the machine id out of the
 * event instance.
 *
 * <p>Used by both {@code onInboundEvent} (channel → registry) and
 * {@code publish} (machine → registry). One mechanism, one lookup table.
 */
public final class InternalEventResolver {

    public record Rule<E extends StatemachineEvent>(
        Class<? extends Machine<?>> targetType,
        Function<E, String> idExtractor) {}

    private final Map<Class<? extends StatemachineEvent>, Rule<?>> rules = new HashMap<>();

    InternalEventResolver() {}

    public <E extends StatemachineEvent> void register(
            Class<E> eventClass,
            Class<? extends Machine<?>> targetType,
            Function<E, String> idExtractor) {
        if (rules.containsKey(eventClass)) {
            throw new IllegalStateException("Duplicate route for " + eventClass.getName());
        }
        rules.put(eventClass, new Rule<>(targetType, idExtractor));
    }

    public Rule<?> lookup(Class<? extends StatemachineEvent> eventClass) {
        return rules.get(eventClass);
    }

    public int size() { return rules.size(); }
}
