package com.telcobright.statewalk.v2.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * JSON + Base64 helpers for context serialization. Used by the registry on
 * save (to populate {@link MachineSnapshot#contextJsonBase64()}) and on
 * rehydration (to reconstruct the context object).
 *
 * <p><b>Discipline rule for context classes:</b> they must be Jackson-friendly
 * — public no-arg constructor (or a record), accessible fields or
 * getters/setters, no captured outer-class references. Anonymous inner classes
 * will fail to serialize.
 */
public final class SnapshotSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SnapshotSerializer() {}

    /**
     * Serialize a context object to base64-encoded JSON. {@code null} → {@code null}.
     */
    public static String contextToBase64Json(Object context) {
        if (context == null) return null;
        try {
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
     * {@code contextClass}. Both arguments {@code null}-safe — returns
     * {@code null} if the snapshot had no context.
     */
    public static Object contextFromBase64Json(String b64Json, String contextClassName) {
        if (b64Json == null || contextClassName == null) return null;
        try {
            Class<?> ctxClass = Class.forName(contextClassName);
            byte[] bytes = Base64.getDecoder().decode(b64Json);
            String json = new String(bytes, StandardCharsets.UTF_8);
            return MAPPER.readValue(json, ctxClass);
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to JSON-deserialize context " + contextClassName
                + ": " + e.getMessage(), e);
        }
    }
}
