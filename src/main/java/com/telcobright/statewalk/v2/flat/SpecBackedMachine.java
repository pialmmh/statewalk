package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.machine.Machine;
import com.telcobright.statewalk.v2.state.StateMap;

/**
 * The framework's generic concrete {@link Machine} — the runtime type for
 * every non-supervisor cell registered via a {@link MachineSpec}. There is
 * one Java class for all machine types; differentiation happens at the
 * {@link MachineSpec} layer (name + context factory + state graph).
 *
 * <p>Users do not subclass this. They author a {@link MachineSpec} and the
 * Registry instantiates {@code SpecBackedMachine} on every pool borrow.
 */
public final class SpecBackedMachine<C> extends Machine<C> {

    private final MachineSpec<C> spec;

    public SpecBackedMachine(MachineSpec<C> spec) {
        this.spec = spec;
    }

    public MachineSpec<C> getSpec() { return spec; }

    @Override
    protected StateMap defineStates() { return spec.stateMap(); }

    @Override
    protected C createContext() { return spec.contextFactory().get(); }
}
