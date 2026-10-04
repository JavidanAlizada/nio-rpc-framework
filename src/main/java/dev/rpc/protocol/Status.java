package dev.rpc.protocol;

/**
 * Outcome code carried by a Response, one unsigned byte on the wire. Codes this version doesn't know are kept as
 * they are, so a newer server can add codes without breaking an older client.
 */
public record Status(int code) {

    public static final Status OK = new Status(0);
    public static final Status APPLICATION_ERROR = new Status(1);
    public static final Status UNKNOWN_METHOD = new Status(2);
    public static final Status BAD_REQUEST = new Status(3);
    public static final Status DEADLINE_EXCEEDED = new Status(4);
    public static final Status CANCELLED = new Status(5);
    public static final Status OVERLOADED = new Status(6);
    public static final Status INTERNAL = new Status(7);

    private static final Status[] KNOWN = {
        OK, APPLICATION_ERROR, UNKNOWN_METHOD, BAD_REQUEST, DEADLINE_EXCEEDED, CANCELLED, OVERLOADED, INTERNAL
    };
    private static final String[] NAMES = {
        "OK", "APPLICATION_ERROR", "UNKNOWN_METHOD", "BAD_REQUEST", "DEADLINE_EXCEEDED", "CANCELLED", "OVERLOADED",
        "INTERNAL"
    };

    public Status {
        if (code < 0 || code > 0xFF) {
            throw new IllegalArgumentException("status code must fit in a byte: " + code);
        }
    }

    /** Returns the shared constant for a known code, or a new unknown status that keeps the number. */
    public static Status of(int code) {
        return code >= 0 && code < KNOWN.length ? KNOWN[code] : new Status(code);
    }

    public boolean isOk() {
        return code == 0;
    }

    /** Whether this code is defined by protocol version 1. */
    public boolean isKnown() {
        return code < KNOWN.length;
    }

    @Override
    public String toString() {
        return isKnown() ? NAMES[code] : "UNKNOWN(" + code + ")";
    }
}
