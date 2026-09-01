package com.telcobright.paymentgw;

/**
 * Point-in-time view of one payment, answerable at ANY moment of its life:
 * live in the registry, hibernated as a store row, or already finished.
 *
 * @param paymentId  the id asked about
 * @param state      current machine state, or {@code "FINISHED"} when no live
 *                   machine and no store row remains, or {@code "UNKNOWN"}
 *                   when the id was never seen
 * @param hibernated true when the payment exists ONLY as a store row
 * @param context    the last known context (live or deserialized from the
 *                   store); {@code null} for FINISHED/UNKNOWN
 */
public record PaymentStatus(String paymentId, String state, boolean hibernated, PaymentContext context) {

    public static final String FINISHED = "FINISHED";
    public static final String UNKNOWN = "UNKNOWN";

    public boolean isLive()       { return context != null && !hibernated; }
    public boolean isRefundable() { return PaymentSupervisor.CAPTURED.equals(state) || PaymentSupervisor.REFUNDING.equals(state); }
}
