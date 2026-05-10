package com.telcobright.statewalk.v2.registry;

import com.telcobright.statewalk.v2.machine.Machine;

import java.util.List;

/**
 * A {@link Registry} whose machines orchestrate child registries' machines.
 *
 * <p>Subclasses declare children via {@link #children()} (data, not code).
 * The framework guarantees:
 * <ul>
 *   <li>{@link #spawnChild(String, String, Object)} delegates to the child
 *       registry's {@code dispatch}, keyed by the same request id.</li>
 *   <li>{@link #toChild(String, String, Object)} sends an inbound directive to
 *       a child machine — the child's registry routes it via
 *       {@link Registry#onInboundEvent(String, com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent)}.</li>
 *   <li>On parent termination, every child registry is asked to force-cleanup
 *       its own machine for the same id, BEFORE the parent's pool return.
 *       Child-before-parent ordering is mechanical, not policy.</li>
 * </ul>
 *
 * <p>Children must hold IS-A {@code Registry} of their own machine type; they
 * are referenced by name (case-sensitive) in {@link #spawnChild} / {@link #toChild}.
 */
public abstract class SupervisorRegistry<M extends Machine<?, C>, C> extends Registry<M, C> {

    /**
     * Declare the child registries this supervisor coordinates. Order is the
     * spawn order; teardown runs in reverse.
     *
     * <p>Returning an empty list makes this behave like a plain {@link Registry}.
     */
    protected abstract List<NamedChild> children();

    /**
     * Spawn a child machine for the same request id. Convenience over locating
     * the child registry and calling its {@code dispatch}.
     *
     * @return true on success, false if no such child or child rejected dispatch.
     */
    public final boolean spawnChild(String requestId, String childName, Object task) {
        NamedChild c = findChild(childName);
        if (c == null) return false;
        return c.registry().dispatch(requestId, task);
    }

    /**
     * Send a directive event to a named child's machine.
     */
    public final void toChild(String requestId, String childName,
                              com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent directive) {
        NamedChild c = findChild(childName);
        if (c == null) return;
        c.registry().onInboundEvent(requestId, directive);
    }

    /**
     * Force-cleanup a named child's machine. Used during parent teardown and
     * by escalation paths (e.g. balance exhausted → tear down signaling).
     */
    public final boolean cleanupChild(String requestId, String childName) {
        NamedChild c = findChild(childName);
        if (c == null) return false;
        return c.registry().forceCleanupMachine(requestId);
    }

    private NamedChild findChild(String name) {
        for (NamedChild c : children()) {
            if (c.name().equals(name)) return c;
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────
    // Termination override — children before parent.
    // ─────────────────────────────────────────────────────────────────

    @Override
    protected final void handleTermination(String requestId, M machine, String finalState) {
        // 1. Force-cleanup children first, in reverse declaration order.
        List<NamedChild> kids = children();
        for (int i = kids.size() - 1; i >= 0; i--) {
            try {
                kids.get(i).registry().forceCleanupMachine(requestId);
            } catch (RuntimeException ignored) {}
        }
        // 2. Subclass-specific parent teardown (settlement, CDR, counters).
        handleSupervisorTermination(requestId, machine, finalState);
    }

    /**
     * Subclass hook for supervisor-level cleanup that must run AFTER children
     * have been torn down (settlement, CDR emission, counter decrement).
     *
     * <p>Default: no-op. Wrapped in try-catch by the framework.
     */
    protected void handleSupervisorTermination(String requestId, M machine, String finalState) {}

    // ─────────────────────────────────────────────────────────────────
    // Child descriptor
    // ─────────────────────────────────────────────────────────────────

    /**
     * Names a child registry the supervisor coordinates.
     */
    public record NamedChild(String name, Registry<?, ?> registry) {}
}
