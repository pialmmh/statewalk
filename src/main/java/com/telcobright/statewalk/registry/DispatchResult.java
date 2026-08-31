package com.telcobright.statewalk.registry;

/**
 * Outcome of a {@code Registry.dispatch} call. Successful dispatches return
 * {@link #ok()}; rejections carry a {@link RejectCause} the handler can use
 * to formulate a protocol-level response.
 */
public record DispatchResult(boolean accepted, RejectCause rejectCause) {

    private static final DispatchResult OK = new DispatchResult(true, null);

    public static DispatchResult ok() { return OK; }

    public static DispatchResult rejected(RejectCause cause) {
        if (cause == null) throw new IllegalArgumentException("rejectCause required for a rejection");
        return new DispatchResult(false, cause);
    }
}
