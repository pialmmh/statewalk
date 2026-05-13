package com.telcobright.statewalk.v2.state;

import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

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

    /** True if any user-declared state is marked {@code .offline()}. Used by the
     *  framework builder to require persistence when offline is in play. */
    public boolean hasOfflineState() {
        for (StateConfig c : states.values()) {
            if (c.offline()) return true;
        }
        return false;
    }

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
                for (var optList : sb.transitions.values()) {
                    for (var opt : optList) {
                        String target = opt.targetState();
                        if (!IDLE.equals(target) && !stateBuilders.containsKey(target)) {
                            throw new IllegalStateException(
                                "State '" + sb.name + "' transitions to unknown state '" + target + "'");
                        }
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

            // Final ⊕ offline — mutually exclusive. Final states terminate;
            // offline states suspend. Combining them is a contradiction.
            for (StateBuilder sb : stateBuilders.values()) {
                if (sb.finalState && sb.offline) {
                    throw new IllegalStateException(
                        "State '" + sb.name + "' cannot be both final and offline. "
                        + "Final terminates the machine; offline suspends it. Choose one.");
                }
            }

            // Mandatory state kind: every user-declared state must explicitly
            // call either .interim() or .finalState() on the builder. No
            // implicit default — forces every state author to state intent.
            for (StateBuilder sb : stateBuilders.values()) {
                if (!sb.kindDeclared) {
                    throw new IllegalStateException(
                        "State '" + sb.name + "' is missing a mandatory kind declaration. "
                        + "Call either .interim() or .finalState() on every state. "
                        + ".offline() is an additional flag on interim states only.");
                }
            }

            Map<String, StateConfig> built = new LinkedHashMap<>();

            // Auto-inject IDLE (no actions, no transitions, not final, not offline).
            built.put(IDLE, new StateConfig(
                IDLE,
                null,
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                false,
                false));

            for (StateBuilder sb : stateBuilders.values()) {
                StateConfig.Timeout to = sb.timeoutDuration > 0
                    ? new StateConfig.Timeout(sb.timeoutDuration, sb.timeoutUnit, sb.timeoutTarget)
                    : null;
                // Freeze each guarded-transition list so runtime walking is safe.
                java.util.Map<Class<? extends StatemachineEvent>, java.util.List<StateConfig.GuardedTransition>> frozenTransitions =
                    new LinkedHashMap<>();
                for (var e : sb.transitions.entrySet()) {
                    frozenTransitions.put(e.getKey(), java.util.List.copyOf(e.getValue()));
                }
                built.put(sb.name, new StateConfig(
                    sb.name,
                    sb.onEntry,
                    sb.onExit,
                    Collections.unmodifiableMap(frozenTransitions),
                    Collections.unmodifiableMap(new LinkedHashMap<>(sb.stayActions)),
                    to,
                    sb.finalState,
                    sb.offline));
            }
            return new StateMap(initialState, built);
        }

        /** True if any user-declared state is marked offline. Used by the
         *  framework builder to require persistence when offline is in play. */
        public boolean hasOfflineState() {
            for (StateBuilder sb : stateBuilders.values()) {
                if (sb.offline) return true;
            }
            return false;
        }

        // ─────────────────────────────────────────────────────────────
        // StateBuilder
        // ─────────────────────────────────────────────────────────────

        public final class StateBuilder {
            private final String name;
            private final Builder parent;
            private Consumer<Object> onEntry;
            private Consumer<Object> onExit;
            private final Map<Class<? extends StatemachineEvent>, java.util.List<StateConfig.GuardedTransition>> transitions = new LinkedHashMap<>();
            private final Map<Class<? extends StatemachineEvent>, BiConsumer<Object, StatemachineEvent>> stayActions = new LinkedHashMap<>();
            private long timeoutDuration;
            private TimeUnit timeoutUnit;
            private String timeoutTarget;
            private boolean finalState;
            private boolean offline;
            /** True once .interim() or .finalState() has been called. Builder rejects the state otherwise. */
            private boolean kindDeclared;

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

            /**
             * Unconditional transition for {@code eventType} from this state.
             * If this state already has guarded {@code on(...)} entries for the
             * same event, this acts as the fallback — guards are evaluated in
             * declaration order; the first one whose predicate returns true
             * wins, and an unconditional entry counts as "always true".
             */
            public StateBuilder on(Class<? extends StatemachineEvent> eventType, String targetState) {
                return on(eventType, targetState, null);
            }

            /**
             * Guarded transition. The {@code guard} predicate receives the
             * machine instance (as {@code Object}) and the firing event; if
             * it returns true, the machine transitions to {@code targetState}.
             *
             * <p>Multiple guarded {@code on(...)} calls for the same event are
             * legal — they are evaluated in declaration order, first match
             * wins. Conventionally, declare guarded variants first and a
             * final {@code on(eventType, fallback)} last as the catch-all.
             */
            public StateBuilder on(Class<? extends StatemachineEvent> eventType,
                                    String targetState,
                                    java.util.function.BiPredicate<Object, StatemachineEvent> guard) {
                transitions
                    .computeIfAbsent(eventType, k -> new java.util.ArrayList<>())
                    .add(new StateConfig.GuardedTransition(guard, targetState));
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

            /**
             * Declare this state as <strong>final</strong> — entering it
             * terminates the machine. Mutually exclusive with
             * {@link #interim()} and {@link #offline()}.
             */
            public StateBuilder finalState() {
                this.finalState = true;
                this.kindDeclared = true;
                return this;
            }

            /**
             * Declare this state as <strong>interim</strong> — a normal
             * mid-flow state. Mandatory if not calling {@link #finalState()};
             * forces every state author to state intent explicitly.
             *
             * <p>An interim state may additionally be marked {@link #offline()}
             * for suspend semantics.
             */
            public StateBuilder interim() {
                this.kindDeclared = true;
                return this;
            }

            /**
             * Mark this state as <b>offline</b> (suspend). On entry, the
             * machine notifies the registry; the registry persists the
             * snapshot, removes the machine from the active map, and returns
             * it to the pool. The machine resumes via the normal rehydration
             * path on the next inbound event.
             *
             * <p>Offline states cannot also be final — builder rejects the
             * combination at {@link Builder#build()} time. Persistence MUST
             * be configured at the framework builder when any state is
             * offline; the framework builder validates this.
             *
             * <p>The state's exit action is NOT fired during the offline
             * transition (the machine doesn't truly leave the state — it's
             * suspended). When the machine is rehydrated and a later event
             * triggers a transition, the exit action runs normally.
             */
            public StateBuilder offline() {
                this.offline = true;
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
