package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.state.StateMap;

import java.util.function.Supplier;

/**
 * Declarative specification for a non-supervisor {@link com.telcobright.statewalk.v2.machine.Machine}
 * registered in a {@link Registry}. Bundles the three things that vary per
 * machine type — name, context factory, state graph — so a new protocol /
 * machine type can be added without writing a Machine subclass.
 *
 * <p>The {@code name} is the type's identity inside one Registry: it forms the
 * child portion of machine ids ({@code parentId#name}), keys the pool, and is
 * the target string used by {@link InternalEventResolver#forwardTo}.
 *
 * <p>The {@code stateMap} is shared across every instance of this machine type
 * — built once at registration, immutable thereafter.
 *
 * <p>Use {@link Builder} for ergonomic construction:
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
public record MachineSpec<C>(
    String name,
    Supplier<C> contextFactory,
    StateMap stateMap
) {
    public MachineSpec {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("MachineSpec.name must be non-blank");
        }
        if (stateMap == null) {
            throw new IllegalArgumentException("MachineSpec.stateMap is required");
        }
        if (contextFactory == null) {
            throw new IllegalArgumentException("MachineSpec.contextFactory is required");
        }
    }

    public static <C> Builder<C> builder() { return new Builder<>(); }

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
