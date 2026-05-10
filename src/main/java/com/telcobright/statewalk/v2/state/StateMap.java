package com.telcobright.statewalk.v2.state;

import com.telcobright.statewalk.v2.event.StatemachineEvent;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Frozen, declarative state graph for a {@link com.telcobright.statewalk.v2.machine.Machine}.
 *
 * <p>The IDLE state is auto-injected by {@link Builder#build()}. Subclasses
 * declare protocol/business states only; IDLE is always present and is always
 * the resting state when a machine is in the pool.
 *
 * <p>Final-state semantics:
 * <ul>
 *   <li>A state declared with {@link Builder.StateBuilder#finalState()} marks
 *       the machine as terminated when entered. The framework calls back into
 *       the registry, which runs the 8-step termination ritual and resets the
 *       machine to IDLE before returning to the pool.</li>
 * </ul>
 */
public final class StateMap {

    /** The reserved name of the resting state every machine has. */
    public static final String IDLE = "IDLE";

    private final String initialState;
    private final Map<String, StateConfig> states;

    private StateMap(String initialState, Map<String, StateConfig> states) {
        this.initialState = initialState;
        this.states = Collections.unmodifiableMap(states);
    }

    public String initialState() { return initialState; }
    public Map<String, StateConfig> states() { return states; }

    public StateConfig get(String name) {
        StateConfig c = states.get(name);
        if (c == null) {
            throw new IllegalStateException("Unknown state: " + name);
        }
        return c;
    }

    public boolean has(String name) { return states.containsKey(name); }

    public static Builder builder() { return new Builder(); }

    // ─────────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────────

    public static final class Builder {
        private String initialState;
        private final Map<String, StateBuilder> stateBuilders = new LinkedHashMap<>();

        public Builder initialState(String name) {
            this.initialState = name;
            return this;
        }

        public StateBuilder state(String name) {
            if (IDLE.equals(name)) {
                throw new IllegalArgumentException("IDLE is reserved and auto-injected");
            }
            StateBuilder sb = new StateBuilder(name, this);
            stateBuilders.put(name, sb);
            return sb;
        }

        public StateMap build() {
            if (initialState == null) {
                throw new IllegalStateException("initialState required");
            }
            if (!stateBuilders.containsKey(initialState)) {
                throw new IllegalStateException(
                    "initialState '" + initialState + "' is not a declared state");
            }

            // Mandatory-timeout invariant: every user-declared state MUST have a timeout.
            // IDLE is auto-injected and exempt; final states must still declare one
            // (the framework intervenes on terminal entry, so the timer never fires,
            // but the discipline of "every state has a fallback" is preserved).
            for (StateBuilder sb : stateBuilders.values()) {
                if (sb.timeoutDuration <= 0 || sb.timeoutUnit == null || sb.timeoutTarget == null) {
                    throw new IllegalStateException(
                        "State '" + sb.name + "' is missing a mandatory timeout. "
                        + "Use .timeout(duration, unit, targetState) on every state.");
                }
            }

            // Validate all transition + timeout targets exist (IDLE is always valid).
            for (StateBuilder sb : stateBuilders.values()) {
                for (String target : sb.transitions.values()) {
                    if (!IDLE.equals(target) && !stateBuilders.containsKey(target)) {
                        throw new IllegalStateException(
                            "State '" + sb.name + "' transitions to unknown state '" + target + "'");
                    }
                }
                if (!IDLE.equals(sb.timeoutTarget)
                    && !stateBuilders.containsKey(sb.timeoutTarget)) {
                    throw new IllegalStateException(
                        "State '" + sb.name + "' timeout targets unknown state '" + sb.timeoutTarget + "'");
                }
            }

            // Timeout-target-final invariant: every state's timeout must point
            // at a state declared with .finalState(). This makes timeout the
            // safety-fallback that always lands the machine in a terminal
            // state — never a half-progressed mid-flow state.
            for (StateBuilder sb : stateBuilders.values()) {
                if (IDLE.equals(sb.timeoutTarget)) {
                    throw new IllegalStateException(
                        "State '" + sb.name + "' timeout targets IDLE — timeout target must be a "
                        + "user-declared final state.");
                }
                StateBuilder targetSb = stateBuilders.get(sb.timeoutTarget);
                if (targetSb != null && !targetSb.finalState) {
                    throw new IllegalStateException(
                        "State '" + sb.name + "' timeout targets '" + sb.timeoutTarget
                        + "' which is not a final state. Mark the target with .finalState().");
                }
            }

            Map<String, StateConfig> built = new LinkedHashMap<>();

            // Auto-inject IDLE (no actions, no transitions, not final).
            built.put(IDLE, new StateConfig(
                IDLE,
                null,
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                false));

            for (StateBuilder sb : stateBuilders.values()) {
                StateConfig.Timeout to = sb.timeoutDuration > 0
                    ? new StateConfig.Timeout(sb.timeoutDuration, sb.timeoutUnit, sb.timeoutTarget)
                    : null;
                built.put(sb.name, new StateConfig(
                    sb.name,
                    sb.onEntry,
                    sb.onExit,
                    Collections.unmodifiableMap(new LinkedHashMap<>(sb.transitions)),
                    Collections.unmodifiableMap(new LinkedHashMap<>(sb.stayActions)),
                    to,
                    sb.finalState));
            }
            return new StateMap(initialState, built);
        }

        // ─────────────────────────────────────────────────────────────
        // StateBuilder
        // ─────────────────────────────────────────────────────────────

        public final class StateBuilder {
            private final String name;
            private final Builder parent;
            private Consumer<Object> onEntry;
            private Consumer<Object> onExit;
            private final Map<Class<? extends StatemachineEvent>, String> transitions = new LinkedHashMap<>();
            private final Map<Class<? extends StatemachineEvent>, BiConsumer<Object, StatemachineEvent>> stayActions = new LinkedHashMap<>();
            private long timeoutDuration;
            private TimeUnit timeoutUnit;
            private String timeoutTarget;
            private boolean finalState;

            StateBuilder(String name, Builder parent) {
                this.name = name;
                this.parent = parent;
            }

            public StateBuilder onEntry(Consumer<Object> action) {
                this.onEntry = action;
                return this;
            }

            public StateBuilder onExit(Consumer<Object> action) {
                this.onExit = action;
                return this;
            }

            public StateBuilder on(Class<? extends StatemachineEvent> eventType, String targetState) {
                transitions.put(eventType, targetState);
                return this;
            }

            public StateBuilder stay(Class<? extends StatemachineEvent> eventType,
                                     BiConsumer<Object, StatemachineEvent> handler) {
                stayActions.put(eventType, handler);
                return this;
            }

            public StateBuilder timeout(long duration, TimeUnit unit, String targetState) {
                this.timeoutDuration = duration;
                this.timeoutUnit = unit;
                this.timeoutTarget = targetState;
                return this;
            }

            public StateBuilder finalState() {
                this.finalState = true;
                return this;
            }

            public StateBuilder state(String nextName) {
                return parent.state(nextName);
            }

            public StateMap build() {
                return parent.build();
            }
        }
    }
}
