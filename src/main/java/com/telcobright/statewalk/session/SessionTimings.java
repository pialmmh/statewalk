package com.telcobright.statewalk.session;

/**
 * Per-state timeouts of the generic graph, in seconds. Domains source these
 * from their own policy (wifi: the document-driven SessionPolicy). Every
 * timeout targets FAILED — the base guarantees the SDR is still written.
 *
 * @param admittingSec   admission verdict must arrive within this
 * @param admittedSec    the whole signaling window (wifi: the captive journey)
 * @param activeMaxSec   dead-man backstop while ACTIVE (real caps live in the
 *                       domain's budget child; this only catches a dead feed)
 * @param tearingDownSec settlement must arrive within this
 */
public record SessionTimings(long admittingSec, long admittedSec, long activeMaxSec, long tearingDownSec) {

    public SessionTimings {
        if (admittingSec <= 0 || admittedSec <= 0 || activeMaxSec <= 0 || tearingDownSec <= 0) {
            throw new IllegalArgumentException("all SessionTimings must be positive");
        }
    }
}
