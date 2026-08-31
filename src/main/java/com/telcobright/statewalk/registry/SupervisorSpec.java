package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.state.StateMap;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Declarative specification for the {@link Supervisor} of a {@link StatemachineRegistry}.
 *
 * <p>Builder-only construction: {@link #builder()} is the sole way to obtain
 * an instance.
 *
 * <p>Extends {@link MachineSpec}'s data with a {@code routes} consumer that
 * populates the {@link InternalEventResolver} when the supervisor instance is
 * constructed. The routes consumer references children by their
 * {@code MachineSpec.name()} string; every referenced name is validated
 * against the registered children at {@code StatemachineRegistry.builder().build()}.
 *
 * <p>One supervisor per StatemachineRegistry; the supervisor's {@code name} also names its
 * pool and forms the machine id directly (no {@code #} suffix).
 *
 * <p>Example:
 * <pre>{@code
 * SupervisorSpec<CallSupervisorContext> sup = SupervisorSpec.<CallSupervisorContext>builder()
 *     .name("CallSupervisor")
 *     .contextFactory(CallSupervisorContext::new)
 *     .stateMap(callSupervisorStateMap)
 *     .routes(r -> {
 *         r.selfHandle(AdmissionDecided.class);
 *         r.forwardTo("CallSignaling", CallEvents.CallRinging.class);
 *         r.forwardTo("BalanceTracker", SettleFinal.class);
 *     })
 *     .build();
 * }</pre>
 */
public final class SupervisorSpec<C> {

    private final String name;
    private final Supplier<C> contextFactory;
    private final StateMap stateMap;
    private final Consumer<InternalEventResolver> routes;

    private SupervisorSpec(String name, Supplier<C> contextFactory, StateMap stateMap,
                           Consumer<InternalEventResolver> routes) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("SupervisorSpec.name must be non-blank");
        }
        if (stateMap == null) {
            throw new IllegalArgumentException("SupervisorSpec.stateMap is required");
        }
        if (contextFactory == null) {
            throw new IllegalArgumentException("SupervisorSpec.contextFactory is required");
        }
        if (routes == null) {
            throw new IllegalArgumentException("SupervisorSpec.routes is required");
        }
        this.name = name;
        this.contextFactory = contextFactory;
        this.stateMap = stateMap;
        this.routes = routes;
    }

    public String name()                              { return name; }
    public Supplier<C> contextFactory()               { return contextFactory; }
    public StateMap stateMap()                        { return stateMap; }
    public Consumer<InternalEventResolver> routes()   { return routes; }

    /** A view of this spec as a {@link MachineSpec} (without routes). */
    public MachineSpec<C> asMachineSpec() {
        return MachineSpec.of(name, contextFactory, stateMap);
    }

    @Override public String toString() { return "SupervisorSpec[" + name + "]"; }

    public static <C> Builder<C> builder() { return new Builder<>(); }

    public static final class Builder<C> {
        private String name;
        private Supplier<C> contextFactory;
        private StateMap stateMap;
        private Consumer<InternalEventResolver> routes;

        Builder() {}

        public Builder<C> name(String n)                                 { this.name = n; return this; }
        public Builder<C> contextFactory(Supplier<C> f)                  { this.contextFactory = f; return this; }
        public Builder<C> stateMap(StateMap s)                           { this.stateMap = s; return this; }
        public Builder<C> routes(Consumer<InternalEventResolver> r)      { this.routes = r; return this; }

        public SupervisorSpec<C> build() {
            return new SupervisorSpec<>(name, contextFactory, stateMap, routes);
        }
    }
}
