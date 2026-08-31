package com.telcobright.statewalk.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for a single business processing step. Each step is one class
 * with one job. Composes into a pipeline that lives inside a supervisor's
 * entry action (e.g. {@code CallSupervisor.ADMITTING.entry} walks identify
 * → authorize → digit-filter → reserve-balance).
 *
 * <p>Features:
 * <ul>
 *   <li>{@code requestId} logged on every step line for correlation.</li>
 *   <li>{@link StepMode#LIVE} vs {@link StepMode#SIMULATE} — read-only steps
 *       run the same path in both; side-effect steps override
 *       {@link #doSimulate} to mock the side effect.</li>
 *   <li>Failures (null result) always logged at WARN regardless of mode.</li>
 *   <li>{@code debug} flag logs every step's input/output/duration at INFO.</li>
 * </ul>
 *
 * @param <IN>  step input type
 * @param <OUT> step output type ({@code null} = step failed; pipeline short-circuits)
 */
public abstract class ProcessingStep<IN, OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessingStep.class);

    /** Step name for logging (e.g. {@code "IDENTIFY_LEAF_TENANT"}). */
    public abstract String name();

    /** Execute the step. Return {@code null} on failure. */
    protected abstract OUT doExecute(IN input);

    /**
     * Override in steps with side effects (balance reserve, ESL commands).
     * Called instead of {@link #doExecute} when {@code mode == SIMULATE}.
     * Default delegates to {@code doExecute} — safe for read-only steps.
     */
    protected OUT doSimulate(IN input) {
        return doExecute(input);
    }

    /** Human-readable summary of the result for the debug/simulation log. */
    protected String summarize(OUT result) {
        return result != null ? "PASS" : "FAIL";
    }

    public final OUT execute(IN input, String requestId, boolean debug) {
        return execute(input, requestId, debug, StepMode.LIVE);
    }

    public final OUT execute(IN input, String requestId, boolean debug, StepMode mode) {
        long start = (debug || mode == StepMode.SIMULATE) ? System.nanoTime() : 0L;

        OUT result;
        try {
            result = (mode == StepMode.SIMULATE) ? doSimulate(input) : doExecute(input);
        } catch (Exception e) {
            LOG.warn("{} | {} | {} | EXCEPTION | {}", requestId, name(), tag(mode), e.toString());
            return null;
        }

        if (debug || mode == StepMode.SIMULATE) {
            long ms = (System.nanoTime() - start) / 1_000_000L;
            if (result != null) {
                LOG.info("{} | {} | {} | {} | {}ms",
                    requestId, name(), tag(mode), summarize(result), ms);
            } else {
                LOG.warn("{} | {} | {} | FAIL | {}ms", requestId, name(), tag(mode), ms);
            }
        } else if (result == null) {
            LOG.warn("{} | {} | FAIL", requestId, name());
        }
        return result;
    }

    private static String tag(StepMode m) { return m == StepMode.SIMULATE ? "SIM" : "LIVE"; }
}
