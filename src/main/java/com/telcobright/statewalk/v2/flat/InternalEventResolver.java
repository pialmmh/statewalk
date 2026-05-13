package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sole bus between a {@link Supervisor} and the internal machines it
 * orchestrates. Lives on the supervisor instance; concrete supervisor
 * subclasses populate its rules in {@code defineRoutes}.
 *
 * <p>Three event categories the resolver covers:
 * <ul>
 *   <li><b>Self</b> — supervisor's own state graph handles it: fires on the
 *       supervisor.</li>
 *   <li><b>Forward (one)</b> — single child machine type receives it.</li>
 *   <li><b>Forward (many)</b> — fan-out to several child types; selective by
 *       the supervisor's choice.</li>
 *   <li><b>Drop</b> — explicit no-op (silences WARN logs for events the
 *       supervisor intentionally ignores).</li>
 * </ul>
 *
 * <p>Unregistered event classes log at WARN and drop. Use {@link #drop} to
 * silence intentionally.
 *
 * <p>The resolver also owns child spawn/cleanup helpers — child machines are
 * never reachable through public Registry API; the only path is via this
 * class, which the supervisor uses inside its own state actions.
 */
public final class InternalEventResolver {

    private static final Logger LOG = LoggerFactory.getLogger(InternalEventResolver.class);

    /** A routing decision for one event class. */
    public sealed interface Rule {
        record Self() implements Rule {}
        record ForwardOne(Class<? extends Machine<?>> target) implements Rule {}
        record ForwardMany(List<Class<? extends Machine<?>>> targets) implements Rule {}
        record Drop() implements Rule {}
    }

    private final Supervisor<?> owner;
    private final Map<Class<? extends StatemachineEvent>, Rule> rules = new HashMap<>();

    InternalEventResolver(Supervisor<?> owner) {
        this.owner = owner;
    }

    // ── DSL — concrete supervisors populate rules in defineRoutes ─────

    public void selfHandle(Class<? extends StatemachineEvent> eventClass) {
        rules.put(eventClass, new Rule.Self());
    }

    public void forwardTo(Class<? extends Machine<?>> childType,
                          Class<? extends StatemachineEvent> eventClass) {
        rules.put(eventClass, new Rule.ForwardOne(childType));
    }

    public void forwardToAll(List<Class<? extends Machine<?>>> childTypes,
                              Class<? extends StatemachineEvent> eventClass) {
        rules.put(eventClass, new Rule.ForwardMany(List.copyOf(childTypes)));
    }

    public void drop(Class<? extends StatemachineEvent> eventClass) {
        rules.put(eventClass, new Rule.Drop());
    }

    // ── Spawn / cleanup helpers used by the supervisor's state actions ─

    public void spawnChild(Class<? extends Machine<?>> childType, Object task) {
        owner.getOwningRegistry().spawnChildInternal(owner.getMachineId(), childType, task);
    }

    public void cleanupChild(Class<? extends Machine<?>> childType) {
        owner.getOwningRegistry().cleanupChildInternal(owner.getMachineId(), childType);
    }

    // ── Routing called by Supervisor.handleInbound ────────────────────

    void route(StatemachineEvent event) {
        Rule rule = rules.get(event.getClass());
        if (rule == null) {
            LOG.warn("[{}] unrouted event {} — drop (declare .selfHandle/.forwardTo/.drop)",
                owner.getMachineId(), event.getClass().getSimpleName());
            return;
        }
        switch (rule) {
            case Rule.Self ignored -> owner.fire(event);
            case Rule.ForwardOne(var type) ->
                owner.getOwningRegistry()
                     .forwardToChild(owner.getMachineId(), type, event);
            case Rule.ForwardMany(var types) -> {
                for (var type : types) {
                    owner.getOwningRegistry()
                         .forwardToChild(owner.getMachineId(), type, event);
                }
            }
            case Rule.Drop ignored -> { /* explicit no-op */ }
        }
    }

    /** Test helper. */
    public int ruleCount() { return rules.size(); }
}
