package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

/**
 * Abstract base class for every controller-supervisor in the flat registry
 * model. Owns an {@link InternalEventResolver} as a member; concrete
 * subclasses (e.g. {@code CallSupervisor}, {@code SmsSupervisor}) declare
 * routing rules in {@link #defineRoutes(InternalEventResolver)}.
 *
 * <p>Every event — wire-inbound or child-published — arrives via
 * {@link #handleInbound(StatemachineEvent)} and the resolver decides:
 * the supervisor fires it on itself, forwards it to one or more children,
 * or drops it explicitly.
 *
 * <p>Children are not reachable from anywhere outside the framework. The
 * resolver is the only path; the resolver is the supervisor's member; the
 * supervisor's class is the user's only handle.
 *
 * @param <E> task (persisting entity) type
 * @param <C> persistent context type — saved to snapshot; volatile context
 *            stays out of persistence per framework rule.
 */
public abstract class Supervisor<E, C> extends Machine<E, C> {

    /** The supervisor's bus. Populated once in constructor. */
    protected final InternalEventResolver resolver;

    protected Supervisor() {
        this.resolver = new InternalEventResolver(this);
        defineRoutes(this.resolver);
    }

    /**
     * Concrete supervisors declare every event class their resolver
     * recognizes. Called once at supervisor construction (so route tables
     * are static for the lifetime of the instance).
     */
    protected abstract void defineRoutes(InternalEventResolver r);

    /**
     * Single entry point for events arriving at this supervisor. Framework
     * calls this on wire-inbound events; the framework also re-routes any
     * child's {@code publishEvent} back through this method. Final — concrete
     * supervisors do not override; they only declare routes.
     */
    public final void handleInbound(StatemachineEvent event) {
        resolver.route(event);
    }

    /**
     * Framework-internal: get the {@link Registry} this supervisor is bound
     * to. The cast assumes the supervisor is in a flat Registry — which is
     * the only legal placement.
     */
    final Registry getOwningRegistry() {
        var handle = getRegistry();
        if (handle instanceof Registry.PerMachineHandle pmh) {
            return pmh.registry();
        }
        throw new IllegalStateException(
            "Supervisor " + getMachineId() + " is not bound to a flat Registry");
    }
}
