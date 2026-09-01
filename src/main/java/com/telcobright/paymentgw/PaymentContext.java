package com.telcobright.paymentgw;

import java.util.ArrayList;
import java.util.List;

/**
 * The payment machine's persisted context — snapshotted to the store on every
 * transition, so a hibernated payment IS this object (plus the state name) as
 * a DB row. Jackson-friendly by construction: public fields, no-arg
 * constructor, only JDK types.
 */
public class PaymentContext {

    // ── identity / order ──
    public String paymentId;          // the machine id; the web app's key for callbacks
    public String orderRef;           // merchant order / invoice reference
    public String customerRef;        // merchant's customer id (msisdn, account, …)
    public long amountMinor;          // requested amount in minor units (poisha/cents)
    public String currency;           // "BDT", "USD", …
    public String description;

    // ── provider session ──
    public String providerRef;        // the provider's session/checkout id
    public String redirectUrl;        // where the web app sends the customer

    // ── capture ──
    public String providerTxnId;      // provider's transaction id on success
    public long paidAmountMinor;      // what was actually paid

    // ── refund ──
    public long refundRequestedMinor; // 0 = full amount
    public String refundReason;
    public String refundRef;          // provider's refund reference
    public long refundedMinor;

    // ── lifecycle facts ──
    public String endCause;           // why it ended (decline reason, "cancelled", "expired", …)
    public String outcome;            // terminal outcome name; null while running
    public long createdAtMs;
    public long capturedAtMs;
    public long endedAtMs;
    public int refundAttempts;

    /** Compact audit trail ("ts|from>to|cause" + notes); rides inside the snapshot. */
    public List<String> timeline = new ArrayList<>();

    public PaymentContext() {}

    /** Amount a refund attempt should move: the requested amount, or everything paid. */
    public long effectiveRefundMinor() {
        return refundRequestedMinor > 0 ? refundRequestedMinor : paidAmountMinor;
    }

    void note(String text) {
        timeline.add(System.currentTimeMillis() + "|" + text);
    }
}
