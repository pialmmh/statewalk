package com.telcobright.statewalk.event;

/**
 * Fired into a machine when a state's declared timeout matures.
 *
 * <p>Carries the originating state name (so handlers can confirm the timeout
 * is still relevant) and the target state the framework will transition to.
 */
public record TimeoutEvent(String fromState, String targetState) implements StatemachineEvent {
}
