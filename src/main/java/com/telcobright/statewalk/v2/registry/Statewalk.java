package com.telcobright.statewalk.v2.registry;

import com.telcobright.statewalk.v2.channel.Channel;
import com.telcobright.statewalk.v2.event.EventTypeRegistry;
import com.telcobright.statewalk.v2.event.StatemachineEvent;
import com.telcobright.statewalk.v2.persistence.PersistenceProvider;
import com.telcobright.statewalk.v2.pool.Poolable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * <b>The single entry point for consuming the framework.</b>
 *
 * <p>You do not call {@code new Registry()} directly, you do not call
 * {@code initBase}, you do not bind channels by hand. All wiring happens
 * through this builder. By design — every other path into the framework
 * goes through package-private methods that user code in different
 * packages cannot reach.
 *
 * <h2>Required steps</h2>
 * <ol>
 *   <li><b>Register every event class.</b> Records: {@link Builder#registerEvent(Class)}.
 *       Mutable / poolable: {@link Builder#registerPoolableEvent(Class, Supplier, int)}.</li>
 *   <li><b>Register every registry.</b> {@link Builder#registry(String, Registry, int, int)}.
 *       For supervisors, register them too — the children list is verified
 *       at {@link Builder#build()}.</li>
 *   <li><b>Bind channels (optional).</b> {@link Builder#channel(String, Channel)}.</li>
 *   <li><b>{@link Builder#build()}.</b> Returns a {@link StatewalkSystem} on
 *       which {@code dispatch} / {@code shutdown} can be called.</li>
 * </ol>
 *
 * <h2>Example</h2>
 * <pre>{@code
 * StatewalkSystem system = Statewalk.builder()
 *     .registerEvent(CallSignaling.Ringing.class)
 *     .registerEvent(CallSignaling.Answered.class)
 *     .registerEvent(CallSignaling.Hangup.class)
 *     .registry("signaling", new CallSignalingRegistry(),    5_000, 4)
 *     .registry("balance",   new BalanceReserverRegistry(),  2_000, 4)
 *     .registry("call",      new CallSupervisorRegistry(...), 5_000, 4)
 *     .channel("signaling", new EslChannel(...))
 *     .build();
 *
 * system.dispatch("call", "uuid-1", new CallTask(...));
 * // ...
 * system.shutdown();
 * }</pre>
 */
public final class Statewalk {

    private Statewalk() {}

    public static Builder builder() { return new Builder(); }

    // ─────────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────────

    public static final class Builder {

        private final EventTypeRegistry eventTypes = new EventTypeRegistry();
        private final Map<String, RegistryEntry> registries = new LinkedHashMap<>();
        private final List<ChannelBinding> channelBindings = new ArrayList<>();
        private PersistenceProvider persistenceProvider;
        private boolean rehydrateEnabled;

        Builder() {}

        /**
         * Configure a persistence provider. With a provider set, every state
         * transition is persisted (regardless of {@link #rehydrate(boolean)}).
         * To additionally allow rehydration of unknown ids on inbound events,
         * also call {@link #rehydrate(boolean)}.
         *
         * <p>Pass {@code null} (or skip the call) to disable persistence
         * entirely.
         */
        public Builder persistence(PersistenceProvider provider) {
            this.persistenceProvider = provider;
            return this;
        }

        /**
         * Enable rehydration on inbound events for unknown machine ids.
         * Has no effect unless {@link #persistence(PersistenceProvider)} is
         * also configured. With both set, an inbound non-first event for an
         * unknown id loads the snapshot and restores the machine; without
         * persistence + rehydrate, the same event throws.
         */
        public Builder rehydrate(boolean enabled) {
            this.rehydrateEnabled = enabled;
            return this;
        }

        /** Register an immutable (record) event class. */
        public Builder registerEvent(Class<? extends StatemachineEvent> eventClass) {
            eventTypes.register(eventClass);
            return this;
        }

        /** Register a mutable {@link Poolable} event class. */
        public <E extends StatemachineEvent & Poolable> Builder registerPoolableEvent(
                Class<E> eventClass, Supplier<E> factory, int poolSize) {
            eventTypes.registerPoolable(eventClass, factory, poolSize);
            return this;
        }

        /**
         * Register a registry under {@code name}. Names are caller-chosen and
         * are used by {@link StatewalkSystem#dispatch(String, String, Object)}
         * to route a request to the correct supervisor / registry.
         *
         * <p>No debug sampling — for sampled debug tracing, use the overload
         * that takes {@code debugSampleRate}.
         */
        public Builder registry(String name, Registry<?, ?> registry,
                                int poolSize, int timeoutThreads) {
            return registry(name, registry, poolSize, timeoutThreads, 0);
        }

        /**
         * Register a registry with sampled debug-mode tracing.
         *
         * @param debugSampleRate {@code 0} disables sampling. {@code N > 0}
         *     puts every Nth dispatched machine in debug mode — that machine
         *     emits DEBUG-level state-transition logs (state + context + event
         *     tuple). Common values: {@code 50} for 1-in-50, {@code 1} for
         *     all-on (development).
         */
        public Builder registry(String name, Registry<?, ?> registry,
                                int poolSize, int timeoutThreads, int debugSampleRate) {
            if (registries.containsKey(name)) {
                throw new IllegalStateException("Duplicate registry name: " + name);
            }
            registries.put(name, new RegistryEntry(name, registry, poolSize, timeoutThreads, debugSampleRate));
            return this;
        }

        /**
         * Bind a {@link Channel} to a previously-registered registry. The
         * channel's inbound feed is wired to the registry's
         * {@link Registry#onInboundEvent(String, StatemachineEvent)}.
         */
        public Builder channel(String registryName, Channel<?, ?> channel) {
            channelBindings.add(new ChannelBinding(registryName, channel));
            return this;
        }

        /**
         * Validate, initialise every registered registry, wire channels, and
         * return the runtime handle.
         *
         * @throws IllegalStateException if validation fails (unknown
         *     supervisor child, channel bound to unknown registry, etc.)
         */
        public StatewalkSystem build() {
            validate();

            // Validate: rehydrate without persistence is a misconfiguration.
            if (rehydrateEnabled && persistenceProvider == null) {
                throw new IllegalStateException(
                    "rehydrate(true) requires a persistence provider. "
                    + "Add .persistence(...) to the builder, or remove .rehydrate(true).");
            }

            // Initialise registries — order matters for supervisor cross-refs,
            // but each registry is self-contained at init time.
            for (var entry : registries.values()) {
                entry.registry.initialize(
                    eventTypes, entry.poolSize, entry.timeoutThreads, entry.debugSampleRate,
                    persistenceProvider, rehydrateEnabled);
            }

            // Bind channels and wire inbound.
            for (var binding : channelBindings) {
                RegistryEntry e = registries.get(binding.registryName);
                if (e == null) {
                    throw new IllegalStateException(
                        "Channel bound to unknown registry: " + binding.registryName);
                }
                e.registry.bindChannel(binding.channel.getName(), binding.channel);
                wireInbound(e.registry, binding.channel);
            }

            return new StatewalkSystem(registries, eventTypes);
        }

        private void validate() {
            // Every supervisor's children must also be registered with this builder.
            for (var entry : registries.values()) {
                if (entry.registry instanceof SupervisorRegistry<?, ?> sup) {
                    for (var child : sup.children()) {
                        boolean found = false;
                        for (var e : registries.values()) {
                            if (e.registry == child.registry()) { found = true; break; }
                        }
                        if (!found) {
                            throw new IllegalStateException(
                                "Supervisor '" + entry.name + "' references child registry '"
                                + child.name() + "' that has not been registered with the builder.");
                        }
                    }
                }
            }
            // Every channel binding must reference a known registry.
            for (var binding : channelBindings) {
                if (!registries.containsKey(binding.registryName)) {
                    throw new IllegalStateException(
                        "Channel bound to unknown registry: " + binding.registryName);
                }
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static void wireInbound(Registry<?, ?> registry, Channel<?, ?> channel) {
            ((Channel) channel).onInbound((id, evt) ->
                registry.onInboundEvent((String) id, (StatemachineEvent) evt));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Internal records used by Builder + StatewalkSystem
    // ─────────────────────────────────────────────────────────────────

    record RegistryEntry(String name, Registry<?, ?> registry, int poolSize, int timeoutThreads,
                          int debugSampleRate) {}
    record ChannelBinding(String registryName, Channel<?, ?> channel) {}
}
