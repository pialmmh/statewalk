package com.telcobright.statewalk.v2.session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Append-only timeline of one session — every transition of every machine in
 * the cell plus domain notes. Lives on the supervisor's context and is shared
 * by reference with the child contexts, so the whole cell writes ONE ordered
 * history; the SDR embeds the snapshot at the end.
 *
 * <p>Thread-safe: transitions run under each machine's own monitor and the
 * timeout scheduler's threads, so appends are synchronized here. Bounded to
 * {@link #MAX_ENTRIES}; overflow is counted, never grows the heap.
 */
public final class SessionHistory {

    public static final int MAX_ENTRIES = 1000;

    private final List<TransitionRecord> entries = new ArrayList<>();
    private int dropped;

    public synchronized void transition(String machine, String from, String to, String cause) {
        append(new TransitionRecord(System.currentTimeMillis(), machine, from, to, cause));
    }

    /** Domain action line ("gate release", "grant copied", …). */
    public synchronized void note(String machine, String text) {
        append(new TransitionRecord(System.currentTimeMillis(), machine, null, null, text));
    }

    private void append(TransitionRecord r) {
        if (entries.size() >= MAX_ENTRIES) { dropped++; return; }
        entries.add(r);
    }

    public synchronized List<TransitionRecord> snapshot() {
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }

    public synchronized int droppedCount() { return dropped; }
}
