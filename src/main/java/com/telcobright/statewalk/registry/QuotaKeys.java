package com.telcobright.statewalk.registry;

/**
 * Keys identifying a request for quota-tracking purposes. Subclass registries
 * compute these from the task at dispatch time via
 * {@code Registry.quotaKeysFor(task)}.
 *
 * <p>Either field can be {@code null} — null disables that dimension's check
 * for this request. {@link #NONE} is the convenient "no quotas to enforce"
 * value (default behaviour).
 *
 * @param partnerKey identifies the incoming partner / customer / tenant
 * @param routeKey   identifies the route / dialplan context / outbound trunk
 */
public record QuotaKeys(String partnerKey, String routeKey) {
    public static final QuotaKeys NONE = new QuotaKeys(null, null);

    public static QuotaKeys ofPartner(String partner) { return new QuotaKeys(partner, null); }
    public static QuotaKeys ofRoute(String route)     { return new QuotaKeys(null, route); }
    public static QuotaKeys of(String partner, String route) { return new QuotaKeys(partner, route); }
}
