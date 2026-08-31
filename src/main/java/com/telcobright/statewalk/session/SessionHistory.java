package com.telcobright.statewalk.session;

import java.util.ArrayDeque;
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
 * cell chains, so appends are synchronized here.
 *
 * <p>Bounded to {@link #MAX_ENTRIES} as head + tail: the FIRST half is kept
 * verbatim (dispatch, admission, activation) and the LAST half rolls (a
 * chatty session drops from the MIDDLE, never its ending) — so the SDR always
 * shows how the session began AND how it ended: the teardown transitions and
 * the end cause survive. Dropped middle records are counted.
 */
public final class SessionHistory {

    public static final int MAX_ENTRIES = 1000;
    private static final int HEAD_KEEP = MAX_ENTRIES / 2;
    private static final int TAIL_KEEP = MAX_ENTRIES - HEAD_KEEP;

    private final List<TransitionRecord> head = new ArrayList<>();
    private final ArrayDeque<TransitionRecord> tail = new ArrayDeque<>();
    private int dropped;

    public synchronized void transition(String machine, String from, String to, String cause) {
        append(new TransitionRecord(System.currentTimeMillis(), machine, from, to, cause));
    }

    /** Domain action line ("gate release", "grant copied", …). */
    public synchronized void note(String machine, String text) {
        append(new TransitionRecord(System.currentTimeMillis(), machine, null, null, text));
    }

    private void append(TransitionRecord r) {
        if (head.size() < HEAD_KEEP) {
            head.add(r);
            return;
        }
        tail.addLast(r);
        if (tail.size() > TAIL_KEEP) {
            tail.removeFirst();          // roll the middle out — the ending always survives
            dropped++;
        }
    }

    public synchronized List<TransitionRecord> snapshot() {
        List<TransitionRecord> out = new ArrayList<>(head.size() + tail.size());
        out.addAll(head);
        out.addAll(tail);
        return Collections.unmodifiableList(out);
    }

    public synchronized int droppedCount() { return dropped; }
}
