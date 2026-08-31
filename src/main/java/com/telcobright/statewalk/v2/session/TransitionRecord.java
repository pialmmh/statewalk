package com.telcobright.statewalk.v2.session;

/**
 * One line of a session's history: either a state transition of one machine of
 * the cell (supervisor or child), or a free-form note from a domain action.
 *
 * <p>A note is a record whose {@code fromState}/{@code toState} are null and
 * whose {@code cause} carries the note text.
 *
 * @param atMs      wall-clock millis
 * @param machine   history name of the machine ("supervisor", "signaling", …)
 * @param fromState state left, or null for a note / the IDLE→initial hop
 * @param toState   state entered, or null for a note
 * @param cause     driving event's simple class name, null for a chained hop,
 *                  or the note text
 */
public record TransitionRecord(long atMs, String machine, String fromState, String toState, String cause) {

    public boolean isNote() { return fromState == null && toState == null; }
}
