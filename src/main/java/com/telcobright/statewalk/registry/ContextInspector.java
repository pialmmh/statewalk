package com.telcobright.statewalk.registry;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dev-mode leak smell-test: scans a machine context for {@link Collection} /
 * {@link Map} fields that have grown past a threshold.
 *
 * <p>Complements {@link PooledFieldValidator}. The validator closes the leak
 * surface where a <em>field</em> outlives the request; this catches the other
 * surface where the field is correctly declared but a protocol's own
 * state-action code grows its <em>contents</em> without bound within a single
 * request (e.g. appending every wire event to a list that is never trimmed).
 * The validator cannot see that — the field is fine, the data is not.
 *
 * <p>Invoked only for debug-sampled cells at terminate time, so it never
 * touches the high-TPS hot path. The scan is shallow (direct fields of the
 * context object, not nested graphs) by design — it is a cheap smell-test, not
 * a heap analyzer.
 */
final class ContextInspector {

    private ContextInspector() {}

    /**
     * Returns {@code fieldName → elementCount} for every {@link Collection} /
     * {@link Map} field of {@code context} whose size strictly exceeds
     * {@code threshold}. Empty when the context is {@code null} or clean.
     *
     * <p>Never throws: fields that cannot be read (module access, odd types)
     * are skipped silently — this is diagnostics, not control flow.
     */
    static Map<String, Integer> oversizedFields(Object context, int threshold) {
        Map<String, Integer> out = new LinkedHashMap<>();
        if (context == null) return out;
        for (Class<?> k = context.getClass(); k != null && k != Object.class; k = k.getSuperclass()) {
            for (Field f : k.getDeclaredFields()) {
                if (f.isSynthetic() || Modifier.isStatic(f.getModifiers())) continue;
                Class<?> ft = f.getType();
                boolean isColl = Collection.class.isAssignableFrom(ft);
                boolean isMap  = Map.class.isAssignableFrom(ft);
                if (!isColl && !isMap) continue;
                try {
                    f.setAccessible(true);
                    Object v = f.get(context);
                    int size = (v instanceof Collection<?> c) ? c.size()
                             : (v instanceof Map<?, ?> mp)     ? mp.size()
                             : 0;
                    if (size > threshold) out.put(f.getName(), size);
                } catch (ReflectiveOperationException | RuntimeException ignored) {
                    // inaccessible or odd field — skip; diagnostics must never break teardown
                }
            }
        }
        return out;
    }
}
