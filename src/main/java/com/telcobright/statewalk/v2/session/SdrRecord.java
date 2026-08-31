package com.telcobright.statewalk.v2.session;

import java.util.List;

/**
 * The generic SDR envelope — exactly ONE per session, written by the base from
 * BOTH terminal states. {@code domain} is the protocol payload built by
 * {@link SessionSupervisor#buildSdr} (wifi: mac/zone/bytes/probe facts; call:
 * numbers/PDD/charges); {@code history} is the full cell timeline.
 */
public record SdrRecord(
    String sessionKey,
    String outcome,          // SUCCEEDED | FAILED
    String endCause,
    long createdAtMs,
    long activatedAtMs,      // 0 = never delivered service
    long endedAtMs,
    int attempts,
    Object domain,
    List<TransitionRecord> history,
    int historyDropped) {}
