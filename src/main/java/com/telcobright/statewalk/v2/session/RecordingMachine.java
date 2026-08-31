package com.telcobright.statewalk.v2.session;

import com.telcobright.statewalk.v2.machine.Machine;

/**
 * Base for CHILD machines of a {@link SessionSupervisor} cell: taps every
 * transition into the cell's shared {@link SessionHistory} (via the context's
 * {@link HistoryCarrier}). Domain children extend this instead of
 * {@code Machine} to appear in the session timeline; nothing else changes.
 *
 * @param <C> child context type — must implement {@link HistoryCarrier}
 */
public abstract class RecordingMachine<C extends HistoryCarrier> extends Machine<C> {

    @Override
    protected final void onTransitioned(String fromState, String toState, String causeHint) {
        C ctx = getContext();
        if (ctx == null || ctx.history() == null) return;
        ctx.history().transition(ctx.historyName(), fromState, toState, causeHint);
    }
}
