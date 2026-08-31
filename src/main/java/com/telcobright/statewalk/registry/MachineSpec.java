package com.telcobright.statewalk.registry;

import com.telcobright.statewalk.state.StateMap;

import java.util.function.Supplier;

/**
 * Declarative specification for a non-supervisor {@link com.telcobright.statewalk.machine.Machine}
 * registered in a {@link StatemachineRegistry}. Bundles the three things that vary per
 * machine type — name, context factory, state graph — so a new protocol /
 * machine type can be added without writing a Machine subclass.
 *
 * <p>Builder-only construction: {@link #builder()} is the sole way to obtain
 * an instance (statewalk convention — no public constructors on framework
 * building blocks).
 *
 * <p>The {@code name} is the type's identity inside one StatemachineRegistry: it forms the
 * child portion of machine ids ({@code parentId#name}), keys the pool, and is
 * the target string used by {@link InternalEventResolver#forwardTo}.
 *
 * <p>The {@code stateMap} is shared across every instance of this machine type
 * — built once at registration, immutable thereafter.
 *
 * <p>Example:
 * <pre>{@code
 * MachineSpec<CallSignalingContext> sig = MachineSpec.<CallSignalingContext>builder()
 *     .name("CallSignaling")
 *     .contextFactory(CallSignalingContext::new)
 *     .stateMap(StateMap.builder()
 *         .initialState("TRYING")
 *         .state("TRYING").interim().timeout(30, SECONDS, "TERMINATED")
 *             .on(CallEvents.CallRinging.class, "RINGING")
 *         ...
 *         .build())
 *     .build();
 * }</pre>
 */
public final class MachineSpec<C> {

    private final String name;
    private final Supplier<C> contextFactory;
    private final StateMap stateMap;

    private MachineSpec(String name, Supplier<C> contextFactory, StateMap stateMap) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("MachineSpec.name must be non-blank");
        }
        if (stateMap == null) {
            throw new IllegalArgumentException("MachineSpec.stateMap is required");
        }
        if (contextFactory == null) {
            throw new IllegalArgumentException("MachineSpec.contextFactory is required");
        }
        this.name = name;
        this.contextFactory = contextFactory;
        this.stateMap = stateMap;
    }

    public String name()                 { return name; }
    public Supplier<C> contextFactory()  { return contextFactory; }
    public StateMap stateMap()           { return stateMap; }

    @Override public String toString() { return "MachineSpec[" + name + "]"; }

    public static <C> Builder<C> builder() { return new Builder<>(); }

    static <C> MachineSpec<C> of(String name, Supplier<C> contextFactory, StateMap stateMap) {
        return new MachineSpec<>(name, contextFactory, stateMap);
    }

    public static final class Builder<C> {
        private String name;
        private Supplier<C> contextFactory;
        private StateMap stateMap;

        Builder() {}

        public Builder<C> name(String n)                       { this.name = n; return this; }
        public Builder<C> contextFactory(Supplier<C> f)        { this.contextFactory = f; return this; }
        public Builder<C> stateMap(StateMap s)                 { this.stateMap = s; return this; }

        public MachineSpec<C> build() {
            return new MachineSpec<>(name, contextFactory, stateMap);
        }
    }
}
