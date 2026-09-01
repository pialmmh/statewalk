package com.telcobright.paymentgw;

/**
 * Port to the EXTERNAL payment-gateway provider (SSLCommerz, bKash, Stripe,
 * aamarPay, …). The library calls this from inside the payment state machine;
 * implementations do the HTTP work against the provider's API.
 *
 * <p>Attached as VOLATILE context (a service handle) — never persisted, always
 * re-attached on rehydration — so hibernated payments survive restarts while
 * the client itself can be reconfigured per deploy.
 *
 * <p>Both calls run on a registry worker thread, serialized per payment.
 * Throwing is legal and handled: a {@code createCheckout} throw fails the
 * payment; a {@code refund} throw counts as a failed refund attempt (the
 * payment returns to its refundable state).
 */
public interface PgwProviderClient {

    /**
     * Create a checkout/payment session at the provider. Returns the
     * provider's reference and the URL the web app must redirect the customer
     * to. Called once from the INITIATED state.
     */
    CheckoutSession createCheckout(PaymentContext payment);

    /**
     * Execute a refund at the provider for {@code payment.refundRequestedMinor}
     * (full amount when 0). Called from the REFUNDING state.
     */
    RefundResult refund(PaymentContext payment);

    /** The provider's checkout session: its reference + the customer redirect URL. */
    record CheckoutSession(String providerRef, String redirectUrl) {}

    /** Outcome of a refund attempt at the provider. */
    record RefundResult(boolean success, String refundRef, String reason) {
        public static RefundResult ok(String refundRef) { return new RefundResult(true, refundRef, null); }
        public static RefundResult failed(String reason) { return new RefundResult(false, null, reason); }
    }
}
