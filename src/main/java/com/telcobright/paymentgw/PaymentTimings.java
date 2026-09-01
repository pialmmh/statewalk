package com.telcobright.paymentgw;

/**
 * The payment machine's clock policy, in seconds.
 *
 * @param providerTimeoutSec  max wait for the provider API (checkout create /
 *                            refund call) before the attempt fails
 * @param paymentWindowSec    how long the customer may sit on the provider's
 *                            payment page before the attempt expires
 *                            (hibernated — enforced at wake/startup/sweep)
 * @param refundWindowSec     how long a captured payment stays refundable
 *                            (hibernated) before it settles for good
 */
public record PaymentTimings(long providerTimeoutSec, long paymentWindowSec, long refundWindowSec) {

    public PaymentTimings {
        if (providerTimeoutSec <= 0 || paymentWindowSec <= 0 || refundWindowSec <= 0) {
            throw new IllegalArgumentException("all PaymentTimings must be positive");
        }
    }

    /** Sensible production defaults: 30s provider calls, 30min payment page, 7-day refund window. */
    public static PaymentTimings defaults() {
        return new PaymentTimings(30, 30 * 60, 7 * 24 * 3600);
    }
}
