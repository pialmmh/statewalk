package com.telcobright.statewalk.v2.pipeline;

import java.util.Map;

/**
 * One captured step execution. Holds the step's name, pass/fail status,
 * duration, summary, and a before/after snapshot of the surrounding context
 * so a debugger or REST endpoint can show exactly which fields the step
 * mutated.
 *
 * <p>Produced by the pipeline wrapper around each step call when a trace
 * collector is attached. Production runs leave the collector null and pay
 * zero cost.
 */
public record StepTrace(
    String stepName,
    String status,                       // "PASS" | "FAIL" | "EXCEPTION"
    long   durationMs,
    String summary,
    Map<String, String> contextBefore,
    Map<String, String> contextAfter,
    Map<String, String> changes          // field -> "newVal (was: oldVal)"
) {}
