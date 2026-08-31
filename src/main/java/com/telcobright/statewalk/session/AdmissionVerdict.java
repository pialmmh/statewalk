package com.telcobright.statewalk.session;

/**
 * Result of {@link SessionSupervisor#runAdmission}. {@code data} is the
 * domain's admission payload (route/levels/zone…) — the domain copies it into
 * its context inside {@code runAdmission} itself; it rides here only for
 * logging/history.
 */
public record AdmissionVerdict(boolean accepted, String rejectCause, Object data) {

    public static AdmissionVerdict accept(Object data) { return new AdmissionVerdict(true, null, data); }

    public static AdmissionVerdict reject(String cause) { return new AdmissionVerdict(false, cause, null); }
}
