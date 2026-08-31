package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.state.StateMap;

/**
 * The framework's generic concrete {@link Supervisor} — the runtime type for
 * every supervisor cell registered via a {@link SupervisorSpec}. One Java
 * class for all protocols; the protocol-specific bits (state graph, routes,
 * context type) come from the spec.
 *
 * <p>Users do not subclass this. They author a {@link SupervisorSpec} and the
 * Registry instantiates {@code SpecBackedSupervisor} on every pool borrow.
 *
 * <p>The spec's {@code routes} consumer runs in {@link Supervisor}'s
 * constructor — it is passed up so the resolver is fully wired before the
 * subclass body runs (and before any state action can fire).
 */
public final class SpecBackedSupervisor<C> extends Supervisor<C> {

    private final SupervisorSpec<C> spec;

    public SpecBackedSupervisor(SupervisorSpec<C> spec) {
        super(spec.routes());
        this.spec = spec;
    }

    public SupervisorSpec<C> getSpec() { return spec; }

    @Override
    protected StateMap defineStates() { return spec.stateMap(); }

    @Override
    protected C createContext() { return spec.contextFactory().get(); }
}
