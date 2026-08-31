package com.telcobright.statewalk.state;

import com.telcobright.statewalk.event.StatemachineEvent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * Frozen, declarative description of one state in a state machine.
 *
 * <p>Built by {@link StateMap.Builder}; consumed by
 * {@link com.telcobright.statewalk.machine.Machine}. The configuration is
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
 * @param offline     True if this is an offline (suspend) state. On entry, the
 *                    machine notifies the registry; the registry persists the
 *                    snapshot and removes the machine from the active map +
 *                    returns it to the pool. The machine resumes via the normal
 *                    rehydration path on the next inbound event. Final and
 *                    offline are mutually exclusive — builder rejects the
 *                    combination.
 */
public record StateConfig(
    String name,
    Consumer<Object> onEntry,
    Consumer<Object> onExit,
    Map<Class<? extends StatemachineEvent>, List<GuardedTransition>> transitions,
    Map<Class<? extends StatemachineEvent>, BiConsumer<Object, StatemachineEvent>> stayActions,
    Timeout timeout,
    boolean finalState,
    boolean offline
) {
    /**
     * A state's mandatory timeout — one of two modes:
     * <ul>
     *   <li><b>target</b> ({@code stay == false}): on maturity the machine
     *       transitions to {@code targetState} (a final state, by builder
     *       rule) — the classic fallback that always lands terminal.</li>
     *   <li><b>stay</b> ({@code stay == true}, {@code targetState == null}):
     *       on maturity the machine STAYS in the state — the optional
     *       {@code onTimeoutStay} action runs (heartbeat / checkpoint work),
     *       the context is re-persisted with the refreshed deadline, and the
     *       timer re-arms for the next period. The state waits indefinitely
     *       for events, checkpointing every period; the registry's global
     *       lifetime timeout remains the hard cap.</li>
     * </ul>
     */
    public record Timeout(long duration, TimeUnit unit, String targetState,
                          boolean stay, Consumer<Object> onTimeoutStay) {
        /** Target-mode timeout (the classic shape). */
        public Timeout(long duration, TimeUnit unit, String targetState) {
            this(duration, unit, targetState, false, null);
        }
    }

    /**
     * One transition option for an event class. A list of these is stored
     * per event so a state can resolve the same event to different targets
     * based on context (guard predicates evaluated in declaration order;
     * first match wins). {@code guard == null} means unconditional —
     * effectively an "always true" fallback.
     *
     * <p>{@code action} (optional) runs when the guard passes, BEFORE the
     * transition executes — the domain's copy-the-event-payload step. Guards
     * must stay side-effect free; mutation belongs in the action. An action
     * throw is logged and does not veto the transition.
     */
    public record GuardedTransition(
        BiPredicate<Object, StatemachineEvent> guard,
        String targetState,
        BiConsumer<Object, StatemachineEvent> action
    ) {
        public GuardedTransition(BiPredicate<Object, StatemachineEvent> guard, String targetState) {
            this(guard, targetState, null);
        }
    }
}
