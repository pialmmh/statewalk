package com.telcobright.statewalk.session;

/**
 * Base context every {@link SessionSupervisor} domain context extends
 * (public-field POJO per the statewalk convention). Holds the generic
 * lifecycle facts + the cell's shared {@link SessionHistory}; the domain
 * subclass adds its own fields (mac/msisdn for wifi, numbers for a call, …).
 */
public class SessionContext implements HistoryCarrier {

    /** Domain session key (wifi: {@code <macNoColons>-<epochSec>}; call: channel uuid). */
    public String sessionKey;

    public long createdAtMs;
    /** 0 until the session reached ACTIVE (service delivered). */
    public long activatedAtMs;
    public long endedAtMs;

    /** Signaling attempt counter (1 on first spawn; retry hook increments). */
    public int attempts;

    /** Why the session ended — domain vocabulary; null until known. */
    public String endCause;

    /** TEARING_DOWN ran its teardown (service stopped, accounting closed). */
    public boolean tornDown;

    /** Terminal outcome: {@code SUCCEEDED} | {@code FAILED}; null while running. */
    public String outcome;

    public final SessionHistory history = new SessionHistory();

    @Override public SessionHistory history() { return history; }
    @Override public String historyName() { return "supervisor"; }
}
