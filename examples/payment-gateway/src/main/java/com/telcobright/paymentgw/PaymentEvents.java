package com.telcobright.paymentgw;

import com.telcobright.statewalk.event.StatemachineEvent;

/**
 * The payment machine's event vocabulary. Wire-side events arrive through
 * {@link PaymentGateway} (webhook / redirect-return / API handlers call it);
 * provider-side events are published internally by the machine's own entry
 * actions.
 */
public final class PaymentEvents {

    private PaymentEvents() {}

    // ── internal: provider session creation (INITIATED entry publishes) ──

    /** The provider accepted the checkout: reference + customer redirect URL. */
    public record ProviderSessionCreated(String providerRef, String redirectUrl) implements StatemachineEvent {}

    /** The provider rejected / was unreachable at checkout creation. */
    public record ProviderSessionFailed(String reason) implements StatemachineEvent {}

    // ── wire: what comes back after the customer visited the provider ──

    /**
     * The provider's payment result (webhook or redirect-return, verified by
     * the web app before it calls the gateway). {@code success == false}
     * carries the decline reason.
     */
    public record PaymentCallback(boolean success, String providerTxnId,
                                  long paidAmountMinor, String reason) implements StatemachineEvent {
        public static PaymentCallback success(String providerTxnId, long paidAmountMinor) {
            return new PaymentCallback(true, providerTxnId, paidAmountMinor, null);
        }
        public static PaymentCallback failed(String reason) {
            return new PaymentCallback(false, null, 0, reason);
        }
    }

    /** The customer cancelled at the provider site (or the merchant voided the attempt). */
    public record PaymentCancelled(String reason) implements StatemachineEvent {}

    // ── wire: refund API ──

    /** Merchant/API asks for a refund; {@code amountMinor == 0} = full amount. */
    public record RefundRequested(long amountMinor, String reason) implements StatemachineEvent {}

    // ── internal: refund outcome (REFUNDING entry publishes) ──

    public record RefundSucceeded(String refundRef, long refundedMinor) implements StatemachineEvent {}
    public record RefundFailed(String reason) implements StatemachineEvent {}

    // ── maintenance: wakes a hibernated payment so a matured deadline fires ──

    /** No-op wake event used by {@link PaymentGateway#sweepExpired()}. */
    public record Sweep() implements StatemachineEvent {}
}
