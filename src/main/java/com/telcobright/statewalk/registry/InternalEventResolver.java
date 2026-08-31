package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.event.StatemachineEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sole bus between a {@link Supervisor} and the internal machines it
 * orchestrates. Lives on the supervisor instance; routing rules are
 * populated by the supervisor's {@code defineRoutes} (or the routes consumer
 * passed via {@link Supervisor#Supervisor(java.util.function.Consumer)}).
 *
 * <p>Targets are addressed by the child machine type's {@code name} string —
 * matching the {@link MachineSpec#name()} declared in the Registry builder.
 * No {@code Class} references here, so the resolver works uniformly for spec-
 * based and raw-factory child registrations.
 *
 * <p>Three event categories the resolver covers:
 * <ul>
 *   <li><b>Self</b> — supervisor's own state graph handles it: fires on the
 *       supervisor.</li>
 *   <li><b>Forward (one)</b> — single child machine type receives it.</li>
 *   <li><b>Forward (many)</b> — fan-out to several child types.</li>
 *   <li><b>Drop</b> — explicit no-op (silences WARN logs for events the
 *       supervisor intentionally ignores).</li>
 * </ul>
 *
 * <p>Unregistered event classes log at WARN and drop. Use {@link #drop} to
 * silence intentionally.
 */
public final class InternalEventResolver {

    private static final Logger LOG = LoggerFactory.getLogger(InternalEventResolver.class);

    /** A routing decision for one event class. */
    public sealed interface Rule {
        record Self() implements Rule {}
        record ForwardOne(String target) implements Rule {}
        record ForwardMany(List<String> targets) implements Rule {}
        record Drop() implements Rule {}
    }

    private final Supervisor<?> owner;
    private final Map<Class<? extends StatemachineEvent>, Rule> rules = new HashMap<>();

    InternalEventResolver(Supervisor<?> owner) {
        this.owner = owner;
    }

    // ── DSL — populated by Supervisor.defineRoutes or spec.routes ─────

    public void selfHandle(Class<? extends StatemachineEvent> eventClass) {
        rules.put(eventClass, new Rule.Self());
    }

    public void forwardTo(String childTypeName,
                          Class<? extends StatemachineEvent> eventClass) {
        rules.put(eventClass, new Rule.ForwardOne(childTypeName));
    }

    public void forwardToAll(List<String> childTypeNames,
                              Class<? extends StatemachineEvent> eventClass) {
        rules.put(eventClass, new Rule.ForwardMany(List.copyOf(childTypeNames)));
    }

    public void drop(Class<? extends StatemachineEvent> eventClass) {
        rules.put(eventClass, new Rule.Drop());
    }

    // ── Spawn / cleanup helpers used by the supervisor's state actions ─

    public void spawnChild(String childTypeName, Object task) {
        owner.getOwningRegistry().spawnChildInternal(owner.getMachineId(), childTypeName, task);
    }

    public void cleanupChild(String childTypeName) {
        owner.getOwningRegistry().cleanupChildInternal(owner.getMachineId(), childTypeName);
    }

    /**
     * Retire EVERY live child of this request (never the supervisor itself).
     * The claims are immediate, so a respawn of the same child type name may
     * follow synchronously — this is the retry contract: clean up the failed
     * attempt's children, then {@code spawnChild} the next attempt.
     */
    public void cleanupChildren() {
        owner.getOwningRegistry().cleanupAllChildrenInternal(owner.getMachineId());
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
            case Rule.ForwardOne(var name) ->
                owner.getOwningRegistry()
                     .forwardToChild(owner.getMachineId(), name, event);
            case Rule.ForwardMany(var names) -> {
                for (var n : names) {
                    owner.getOwningRegistry()
                         .forwardToChild(owner.getMachineId(), n, event);
                }
            }
            case Rule.Drop ignored -> { /* explicit no-op */ }
        }
    }

    /** Test/validation helpers. */
    public int ruleCount() { return rules.size(); }

    /** All child target names referenced by forwardTo / forwardToAll rules. */
    public Set<String> referencedChildNames() {
        Set<String> out = new java.util.HashSet<>();
        for (Rule r : rules.values()) {
            switch (r) {
                case Rule.ForwardOne(var n) -> out.add(n);
                case Rule.ForwardMany(var ns) -> out.addAll(ns);
                default -> { }
            }
        }
        return out;
    }
}
