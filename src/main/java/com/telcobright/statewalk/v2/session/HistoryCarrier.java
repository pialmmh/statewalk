package com.telcobright.statewalk.v2.session;

/**
 * Contract for a machine context that participates in the cell's shared
 * {@link SessionHistory}. The supervisor's context owns the history; child
 * contexts carry a reference to the SAME object plus their machine's history
 * name, so the whole cell writes one ordered timeline.
 */
public interface HistoryCarrier {

    SessionHistory history();

    /** Short stable name of this machine in history lines ("signaling", "budget", …). */
    String historyName();
}
