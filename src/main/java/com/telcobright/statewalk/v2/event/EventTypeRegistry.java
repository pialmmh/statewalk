package com.telcobright.statewalk.v2.event;

import com.telcobright.statewalk.v2.pool.ObjectPoolManager;
import com.telcobright.statewalk.v2.pool.Poolable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Type registry every event class must be registered with before it can be fired.
 *
 * <p><b>Why a registry instead of free-form events?</b>
 * <ul>
 *   <li><b>Safety</b> — firing an unregistered event throws, catching typos and
 *       forgotten event classes at first dispatch instead of silently dropping.</li>
 *   <li><b>Performance</b> — class-keyed dispatch (no string lookup) plus
 *       optional pooling of mutable events. The framework can borrow an event,
 *       populate it, fire it, and return it on consume.</li>
 *   <li><b>Discoverability</b> — registered events are inspectable; tooling
 *       can list all events a system handles.</li>
 * </ul>
 *
 * <p><b>Two registration flavours:</b>
 * <ul>
 *   <li>{@link #register(Class)} — for immutable event classes (typically records).
 *       No pooling; allocation is cheap.</li>
 *   <li>{@link #registerPoolable(Class, Supplier, int)} — for mutable event
 *       classes implementing {@link Poolable}. The framework borrows / resets /
 *       returns these around dispatch.</li>
 * </ul>
 *
 * <p>The framework auto-registers {@link TimeoutEvent} on construction.
 */
public final class EventTypeRegistry {

    private final ConcurrentHashMap<Class<? extends StatemachineEvent>, EventDescriptor<?>> types =
        new ConcurrentHashMap<>();

    public EventTypeRegistry() {
        // Auto-register framework-internal events.
        register(TimeoutEvent.class);
    }

    /**
     * Register an immutable (typically record-based) event class. Idempotent —
     * registering the same class twice is a no-op.
     *
     * @throws IllegalArgumentException if the class is {@link Poolable} —
     *     poolable events must use {@link #registerPoolable}.
     */
    public <E extends StatemachineEvent> void register(Class<E> eventClass) {
        if (Poolable.class.isAssignableFrom(eventClass)) {
            throw new IllegalArgumentException(
                eventClass.getName() + " implements Poolable — use registerPoolable(class, factory, poolSize)");
        }
        types.putIfAbsent(eventClass, new EventDescriptor<>(eventClass, null));
    }

    /**
     * Register a poolable mutable event class. The framework will borrow /
     * reset / return instances around dispatch. The factory must produce
     * fresh instances; size is the pool's max (soft) cap.
     */
    public <E extends StatemachineEvent & Poolable> void registerPoolable(
            Class<E> eventClass, Supplier<E> factory, int poolSize) {
        if (!Poolable.class.isAssignableFrom(eventClass)) {
            throw new IllegalArgumentException(
                eventClass.getName() + " must implement Poolable to be poolable");
        }
        ObjectPoolManager<E> pool = new ObjectPoolManager<>(
            eventClass.getSimpleName() + "-EventPool", factory, poolSize);
        types.put(eventClass, new EventDescriptor<>(eventClass, pool));
    }

    public boolean isRegistered(Class<? extends StatemachineEvent> eventClass) {
        return types.containsKey(eventClass);
    }

    /**
     * Validate the event class is registered; throw a precise error if not.
     * Called by registries on inbound dispatch as the first guard.
     */
    public void requireRegistered(Class<? extends StatemachineEvent> eventClass) {
        if (!types.containsKey(eventClass)) {
            throw new IllegalStateException(
                "Event type not registered: " + eventClass.getName()
                + ". Register it via Statewalk.builder().registerEvent(...) before dispatch.");
        }
    }

    /**
     * Borrow a poolable event. Caller populates fields then fires it. The
     * framework returns it on consume.
     */
    @SuppressWarnings("unchecked")
    public <E extends StatemachineEvent & Poolable> E borrow(Class<E> eventClass) {
        EventDescriptor<?> d = types.get(eventClass);
        if (d == null) {
            throw new IllegalStateException("Not registered: " + eventClass.getName());
        }
        if (d.pool == null) {
            throw new IllegalStateException(
                eventClass.getName() + " is not poolable (registered via register, not registerPoolable)");
        }
        return ((ObjectPoolManager<E>) d.pool).borrow();
    }

    /**
     * Return an event to its pool if it is poolable. No-op for non-poolable
     * events (records). Called by the framework after dispatch.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void returnIfPoolable(StatemachineEvent event) {
        if (event == null) return;
        if (!(event instanceof Poolable)) return;
        EventDescriptor<?> d = types.get(event.getClass());
        if (d == null || d.pool == null) return;
        ObjectPoolManager pool = (ObjectPoolManager) d.pool;
        pool.returnObject((Poolable) event);
    }

    public int registeredCount() { return types.size(); }

    public Map<Class<? extends StatemachineEvent>, ObjectPoolManager.PoolStatistics> poolStatistics() {
        Map<Class<? extends StatemachineEvent>, ObjectPoolManager.PoolStatistics> out = new java.util.LinkedHashMap<>();
        types.forEach((cls, desc) -> {
            if (desc.pool != null) out.put(cls, desc.pool.getStatistics());
        });
        return out;
    }

    private record EventDescriptor<E>(Class<E> type, ObjectPoolManager<?> pool) {}
}
