package com.telcobright.paymentgw;

import java.util.List;

/**
 * The payment's terminal record — exactly ONE per payment, emitted from EVERY
 * terminal state (captured-and-settled, refunded, cancelled, failed, expired)
 * to the configured {@link PaymentRecordSink}. The billing/reconciliation
 * analog of statewalk's session SDR.
 *
 * @param paymentId        the machine id
 * @param outcome          terminal state name: SETTLED | REFUNDED | CANCELLED | FAILED | EXPIRED
 * @param endCause         why (decline reason, "cancelled", "payment window expired", …)
 * @param orderRef         merchant order reference
 * @param customerRef      merchant customer reference
 * @param amountMinor      requested amount (minor units)
 * @param paidAmountMinor  captured amount (0 = never paid)
 * @param refundedMinor    refunded amount (0 = no refund)
 * @param currency         ISO currency
 * @param providerRef      provider session id
 * @param providerTxnId    provider transaction id (null = never captured)
 * @param refundRef        provider refund reference (null = no refund)
 * @param createdAtMs      when the payment was initiated
 * @param capturedAtMs     when it was captured (0 = never)
 * @param endedAtMs        when it reached the terminal state
 * @param refundAttempts   how many refund attempts ran
 * @param timeline         the full audit trail
 */
public record PaymentRecord(
    String paymentId,
    String outcome,
    String endCause,
    String orderRef,
    String customerRef,
    long amountMinor,
    long paidAmountMinor,
    long refundedMinor,
    String currency,
    String providerRef,
    String providerTxnId,
    String refundRef,
    long createdAtMs,
    long capturedAtMs,
    long endedAtMs,
    int refundAttempts,
    List<String> timeline
) {
    /** True when the customer's money ultimately stayed with the merchant. */
    public boolean moneyCollected() {
        return capturedAtMs > 0 && refundedMinor < paidAmountMinor;
    }
}
