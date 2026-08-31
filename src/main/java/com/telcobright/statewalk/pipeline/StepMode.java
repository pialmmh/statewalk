package com.telcobright.statewalk.pipeline;

/**
 * Execution mode for a {@link ProcessingStep}.
 *
 * <ul>
 *   <li><b>LIVE</b> — real execution. Side effects happen (balance reserve,
 *       ESL command, DB write, etc.).</li>
 *   <li><b>SIMULATE</b> — dry-run. Read-only steps run normally. Side-effect
 *       steps override {@code doSimulate} to skip the actual write and
 *       return a mock result so the rest of the pipeline can still flow.</li>
 * </ul>
 *
 * <p>SIMULATE is the foundation of the "would this call admit?" tooling —
 * the same step pipeline that runs in production answers what-if questions
 * without touching FreeSWITCH or the database.
 */
public enum StepMode {
    LIVE,
    SIMULATE
}
