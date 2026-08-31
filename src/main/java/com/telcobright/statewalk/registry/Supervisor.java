package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.machine.Machine;
import com.telcobright.statewalk.event.StatemachineEvent;

import java.util.function.Consumer;

/**
 * Abstract base class for every controller-supervisor in the flat registry
 * model. Owns an {@link InternalEventResolver} as a member; concrete
 * supervisors populate routing rules either by overriding
 * {@link #defineRoutes(InternalEventResolver)} (old subclass pattern) or by
 * passing a routes consumer to {@link #Supervisor(Consumer)} (spec pattern,
 * see {@link SpecBackedSupervisor}).
 *
 * <p>Every event — wire-inbound or child-published — arrives via
 * {@link #handleInbound(StatemachineEvent)} and the resolver decides:
 * the supervisor fires it on itself, forwards it to one or more children,
 * or drops it explicitly.
 *
 * <p>Children are not reachable from anywhere outside the framework. The
 * resolver is the only path; the resolver is the supervisor's member; the
 * supervisor's identity is the user's only handle.
 *
 * @param <C> context type — initial context provided at dispatch by the
 *            registry; mutated by state actions and steps; snapshotted on
 *            every transition. Volatile context stays out of persistence.
 */
public abstract class Supervisor<C> extends Machine<C> {

    /** The supervisor's bus. Populated once in constructor. */
    protected final InternalEventResolver resolver;

    /**
     * No-arg constructor used by the subclass-override pattern. Creates the
     * resolver, then calls {@link #defineRoutes(InternalEventResolver)} which
     * the subclass overrides. Subclass instance fields are NOT yet set when
     * {@code defineRoutes} runs — keep that method dependency-free (only event
     * classes and constants).
     */
    protected Supervisor() {
        this.resolver = new InternalEventResolver(this);
        defineRoutes(this.resolver);
    }

    /**
     * Constructor taking a routes consumer directly — used by
     * {@link SpecBackedSupervisor} so the spec (which holds the consumer) does
     * not need to be a subclass field at {@code super()} call time. The
     * consumer runs against the freshly created resolver before this
     * constructor returns; {@link #defineRoutes(InternalEventResolver)} is
     * never invoked along this path.
     */
    protected Supervisor(Consumer<InternalEventResolver> routes) {
        this.resolver = new InternalEventResolver(this);
        if (routes != null) routes.accept(this.resolver);
    }

    /**
     * Subclass-override hook. Default is a no-op so the routes-consumer path
     * does not require an empty override. Subclasses that use the no-arg
     * constructor override this to declare their routes.
     */
    protected void defineRoutes(InternalEventResolver r) { /* no-op default */ }

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
     * Direct access to the resolver — exposed for state actions that need to
     * spawn or clean up children. Equivalent to the {@code protected resolver}
     * field but typed as a method for use from outside this package (e.g.
     * spec-based onEntry lambdas that receive the supervisor as a generic
     * {@code Machine<?>}).
     */
    public final InternalEventResolver resolver() { return resolver; }

    /**
     * Framework-internal: get the {@link StatemachineRegistry} this supervisor is bound
     * to. The cast assumes the supervisor is in a flat StatemachineRegistry — which is
     * the only legal placement.
     */
    final StatemachineRegistry getOwningRegistry() {
        var handle = getRegistry();
        if (handle instanceof StatemachineRegistry.PerMachineHandle pmh) {
            return pmh.registry();
        }
        throw new IllegalStateException(
            "Supervisor " + getMachineId() + " is not bound to a flat StatemachineRegistry");
    }
}
