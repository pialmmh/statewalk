package com.telcobright.statewalk.v2.registry.api;

/**
 * Reason a registry rejected a dispatch attempt. The handler (e.g. CCH for
 * calls) maps these to protocol-level cause codes for the wire response.
 *
 * <p>Independent from {@code CallCause} (the call-FSM-level cause vocabulary
 * for hangups). The registry doesn't know about call hangups; it only
 * reports its own rejection conditions. The handler bridges the two.
 */
public enum RejectCause {
    /** Registry-wide {@code maxConcurrent} limit reached. */
    CAPACITY_EXCEEDED,

    /** Per-partner concurrent-machine quota reached. */
    PARTNER_CONCURRENCY_EXCEEDED,

    /** Per-route concurrent-machine quota reached. */
    ROUTE_CONCURRENCY_EXCEEDED,

    /** Per-partner transactions-per-second quota reached. */
    PARTNER_TPS_EXCEEDED,

    /** Per-route transactions-per-second quota reached. */
    ROUTE_TPS_EXCEEDED,

    /** Same machineId already active — likely a producer bug or replay. */
    DUPLICATE_ID,

    /** Registry is in shutdown — refusing new work. */
    SHUTTING_DOWN,

    /** Builder hasn't initialised the registry yet. */
    NOT_INITIALIZED,

    /** Pool integrity check failed (machine returned non-IDLE) — operator-visible. */
    POOL_INTEGRITY_ERROR
}
