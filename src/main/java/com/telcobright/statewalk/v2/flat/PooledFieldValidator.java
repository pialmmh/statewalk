package com.telcobright.statewalk.v2.flat;

import com.telcobright.statewalk.v2.machine.Machine;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Build-time guard against the one memory-leak vector the pooling model cannot
 * clean up on its own: a <strong>mutable instance field on a pooled machine
 * subclass</strong>.
 *
 * <h2>Why this exists</h2>
 * Every machine in a {@link Registry} is borrowed from a pool, used for one
 * request, reset, and returned. {@link Machine#resetForReuse()} nulls
 * everything the framework owns — context, volatile context, ids, current
 * state, timeout future. What it cannot see is a field a subclass declared
 * itself. If that field is non-{@code final}, a state action can stash
 * per-request data in it; reset leaves the reference dangling; the next borrow
 * inherits it. That is simultaneously:
 * <ul>
 *   <li>a <b>correctness bug</b> — request B reads data left by request A, and</li>
 *   <li>a <b>leak</b> — request A's object graph stays reachable through the
 *       pooled (idle) machine and can never be collected.</li>
 * </ul>
 * "Today call works, but tomorrow a new machine with a different definition
 * leaks" is exactly this vector. We close it structurally rather than by
 * convention.
 *
 * <h2>What is rejected</h2>
 * At registry build every registered machine type is scanned from its concrete
 * class up to (but excluding) the framework bases {@link Machine} /
 * {@link Supervisor}. A declared field is an offender when it is none of:
 * <ul>
 *   <li>{@code static} — shared across instances, not per-borrow state;</li>
 *   <li>{@code final} — set once at construction, intended to survive reuse
 *       (immutable config / service handles, e.g. a resolver);</li>
 *   <li>synthetic — compiler- or coverage-agent-generated (e.g. JaCoCo's
 *       {@code $jacocoData}, an enclosing-instance reference).</li>
 * </ul>
 *
 * <h2>Where state belongs instead</h2>
 * <ul>
 *   <li><b>Per-request mutable state</b> → the context object {@code C}. The
 *       framework allocates it per borrow and clears it on reset.</li>
 *   <li><b>Immutable config / service handles</b> → a {@code final} field set
 *       in the constructor, or a {@code volatileLoader} (which also re-attaches
 *       cleanly after crash-rehydration).</li>
 * </ul>
 *
 * <p>Spec-backed machines ({@link SpecBackedMachine} / {@link SpecBackedSupervisor})
 * always pass — their only field is the {@code final} spec. The check exists to
 * keep the raw-factory / custom-subclass path honest and to make every protocol
 * added in the future leak-safe by construction.
 *
 * <p><b>Caveat (documented, not enforced):</b> a {@code final} field may still
 * hold a mutable object whose contents grow per request (e.g.
 * {@code final List<String> log = new ArrayList<>()} appended to on every
 * event). That is a far more deliberate misuse; {@link ContextInspector} is the
 * runtime smell-test for the analogous problem on the context object.
 */
final class PooledFieldValidator {

    private PooledFieldValidator() {}

    /**
     * Throws {@link IllegalStateException} listing every leak-prone instance
     * field on {@code machineClass}. No-op when the type is clean.
     *
     * @param typeName     the type's registered name (for the error message)
     * @param machineClass the concrete pooled class produced by the factory
     */
    static void validate(String typeName, Class<?> machineClass) {
        List<String> offenders = new ArrayList<>();
        for (Class<?> k = machineClass;
             k != null && k != Machine.class && k != Supervisor.class && k != Object.class;
             k = k.getSuperclass()) {
            for (Field f : k.getDeclaredFields()) {
                int mods = f.getModifiers();
                if (f.isSynthetic())          continue;   // compiler / coverage-agent fields
                if (Modifier.isStatic(mods))  continue;   // shared, not per-borrow
                if (Modifier.isFinal(mods))   continue;   // construction-time config
                offenders.add(k.getSimpleName() + "." + f.getName()
                    + " : " + f.getType().getSimpleName());
            }
        }
        if (offenders.isEmpty()) return;

        throw new IllegalStateException(
            "Machine type '" + typeName + "' (" + machineClass.getName() + ") declares mutable "
            + "instance field(s) " + offenders + ". Pooled machines are reused across requests; a "
            + "non-final instance field captures per-request state that survives resetForReuse() "
            + "and bleeds into — and leaks from — the next borrow. Put per-request state in the "
            + "context (C), which the framework clears on reset; for immutable config or service "
            + "handles make the field 'final' (set in the constructor) or supply a volatileLoader.");
    }
}
