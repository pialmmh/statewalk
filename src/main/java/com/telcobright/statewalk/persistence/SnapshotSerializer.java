package com.telcobright.statewalk.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JSON + Base64 helpers for context serialization. Used by the registry on
 * save (to populate {@link MachineSnapshot#contextJsonBase64()}) and on
 * rehydration (to reconstruct the context object).
 *
 * <p><b>Discipline rule for context classes:</b> they must be Jackson-friendly
 * — public no-arg constructor (or a record), accessible fields or
 * getters/setters, no captured outer-class references. Anonymous inner classes
 * will fail to serialize.
 *
 * <h2>No reflection on the hot path</h2>
 *
 * <p>{@link #contextFromBase64Json} historically called {@link Class#forName}
 * once per rehydration. That's now eliminated by caching the resolved class
 * in {@link #CLASS_CACHE}, populated three ways:
 *
 * <ol>
 *   <li><b>Auto-warm on save</b> — every successful
 *       {@link #contextToBase64Json} caches {@code context.getClass()} keyed
 *       by FQN. In steady state, the saving process always populates the
 *       cache before the loading process needs it.</li>
 *   <li><b>Explicit pre-warm at startup</b> via
 *       {@link #registerContextClass(Class)} — adopters can call this from
 *       their app bootstrap, or via
 *       {@code Statewalk.builder().preWarmContextClass(...)}.</li>
 *   <li><b>Cold-path fallback</b> — first rehydrate after a process restart
 *       (cache empty) incurs a single {@code Class.forName} per class type,
 *       then the result is cached for all subsequent loads. This is
 *       <em>startup-amortized</em>, not steady-state hot-path reflection.</li>
 * </ol>
 *
 * <p>Jackson's internal reflection is similarly amortized — its
 * BeanSerializer / BeanDeserializer caches are populated on first save/load
 * per class and reused thereafter.
 */
public final class SnapshotSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Resolved {@code Class<?>} by FQN. Populated on save (auto-warm), on
     * explicit {@link #registerContextClass}, and on cold-path load.
     * Thread-safe; reads are lock-free.
     */
    private static final ConcurrentHashMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

    private SnapshotSerializer() {}

    /**
     * Explicitly register a context class so the first rehydration after a
     * restart avoids any {@code Class.forName} call. Optional — the cache
     * auto-warms on every save anyway, so this only matters for the very
     * first rehydrate after a cold start with prior snapshots in the store.
     */
    public static void registerContextClass(Class<?> cls) {
        if (cls != null) CLASS_CACHE.putIfAbsent(cls.getName(), cls);
    }

    /**
     * Serialize a context object to base64-encoded JSON. {@code null} → {@code null}.
     * Auto-warms the class cache as a side effect.
     */
    public static String contextToBase64Json(Object context) {
        if (context == null) return null;
        try {
            // Auto-warm: the saving process always sees the class first,
            // populating the cache for the loading process.
            CLASS_CACHE.putIfAbsent(context.getClass().getName(), context.getClass());
            String json = MAPPER.writeValueAsString(context);
            return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to JSON-serialize context of type " + context.getClass().getName()
                + ": " + e.getMessage(), e);
        }
    }

    /**
     * Deserialize a base64-encoded JSON string into an instance of
     * {@code contextClassName}. Both arguments {@code null}-safe — returns
     * {@code null} if the snapshot had no context.
     */
    public static Object contextFromBase64Json(String b64Json, String contextClassName) {
        if (b64Json == null || contextClassName == null) return null;
        try {
            Class<?> ctxClass = CLASS_CACHE.get(contextClassName);
            if (ctxClass == null) {
                // Cold path: first load after process restart for this class.
                // One-time Class.forName, then cached for all subsequent loads.
                ctxClass = Class.forName(contextClassName);
                CLASS_CACHE.putIfAbsent(contextClassName, ctxClass);
            }
            byte[] bytes = Base64.getDecoder().decode(b64Json);
            String json = new String(bytes, StandardCharsets.UTF_8);
            return MAPPER.readValue(json, ctxClass);
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to JSON-deserialize context " + contextClassName
                + ": " + e.getMessage(), e);
        }
    }

    /** Test helper — returns the size of the resolved-class cache. */
    public static int classCacheSize() { return CLASS_CACHE.size(); }

    /** Test helper — clears the cache (used by tests that want to exercise the cold path). */
    public static void clearClassCache() { CLASS_CACHE.clear(); }
}
