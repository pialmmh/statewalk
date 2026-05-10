package com.telcobright.statewalk.v2.state;

import com.telcobright.statewalk.v2.event.StatemachineEvent;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Frozen, declarative description of one state in a state machine.
 *
 * <p>Built by {@link StateMap.Builder}; consumed by
 * {@link com.telcobright.statewalk.v2.machine.Machine}. The configuration is
 * data — a state machine is the union of its state configs plus an initial
 * state name. Subclasses describe states; the framework wires lifecycle.
 *
 * @param name        The state name (unique within a machine).
 * @param onEntry     Optional entry action; null if none.
 * @param onExit      Optional exit action; null if none.
 * @param transitions Map of {@code event class -> target state name} for
 *                    leave-this-state transitions.
 * @param stayActions Map of {@code event class -> handler} for events handled
 *                    without leaving this state.
 * @param timeout     Optional declarative timeout for this state; null if none.
 * @param finalState  True if this is a terminal state. The framework wires
 *                    {@code finalState onEntry -> registry.onMachineTerminated}
 *                    and {@code reset() -> IDLE}.
 */
public record StateConfig(
    String name,
    Consumer<Object> onEntry,
    Consumer<Object> onExit,
    Map<Class<? extends StatemachineEvent>, String> transitions,
    Map<Class<? extends StatemachineEvent>, BiConsumer<Object, StatemachineEvent>> stayActions,
    Timeout timeout,
    boolean finalState
) {
    public record Timeout(long duration, TimeUnit unit, String targetState) {}
}
