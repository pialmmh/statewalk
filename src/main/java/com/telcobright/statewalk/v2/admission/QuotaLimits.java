package com.telcobright.statewalk.v2.admission;

/**
 * Quota thresholds for a registry. {@code 0} on any field disables that
 * dimension's check. Provided to the registry via the
 * {@code Registry.getQuotaLimits()} subclass hook.
 *
 * @param maxConcurrentPerPartner concurrent-machine ceiling per partner key
 * @param maxConcurrentPerRoute   concurrent-machine ceiling per route key
 * @param maxTpsPerPartner        transactions per second per partner key
 * @param maxTpsPerRoute          transactions per second per route key
 */
public record QuotaLimits(
    int maxConcurrentPerPartner,
    int maxConcurrentPerRoute,
    int maxTpsPerPartner,
    int maxTpsPerRoute
) {

    public static final QuotaLimits UNLIMITED = new QuotaLimits(0, 0, 0, 0);

    public boolean enforces() {
        return maxConcurrentPerPartner > 0 || maxConcurrentPerRoute > 0
            || maxTpsPerPartner > 0 || maxTpsPerRoute > 0;
    }
}
