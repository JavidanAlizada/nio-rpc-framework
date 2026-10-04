package dev.rpc.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * The outcome of a call. An OK response carries the serialized result; any other status carries an error type and
 * message instead. Exceptions never cross the wire as objects.
 */
public record Response(int requestId, Status status, byte[] payload, String errorType, String message)
        implements Frame {

    public Response {
        if (requestId == 0) {
            throw new IllegalArgumentException("requestId 0 is reserved for connection-level frames");
        }
        Objects.requireNonNull(status, "status");
        if (status.isOk()) {
            Objects.requireNonNull(payload, "payload");
            if (errorType != null || message != null) {
                throw new IllegalArgumentException("an OK response has no error fields");
            }
        } else {
            Objects.requireNonNull(errorType, "errorType");
            Objects.requireNonNull(message, "message");
            if (payload != null) {
                throw new IllegalArgumentException("an error response has no payload");
            }
        }
    }

    /** A successful response carrying the serialized result. */
    public static Response ok(int requestId, byte[] payload) {
        return new Response(requestId, Status.OK, payload, null, null);
    }

    /** A failed response; status must not be OK. */
    public static Response error(int requestId, Status status, String errorType, String message) {
        return new Response(requestId, status, null, errorType, message);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Response r
                && requestId == r.requestId
                && status.equals(r.status)
                && Arrays.equals(payload, r.payload)
                && Objects.equals(errorType, r.errorType)
                && Objects.equals(message, r.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, status, Arrays.hashCode(payload), errorType, message);
    }

    @Override
    public String toString() {
        return status.isOk()
                ? "Response[requestId=" + requestId + ", OK, payload=" + payload.length + " bytes]"
                : "Response[requestId=" + requestId + ", " + status + ", " + errorType + ": " + message + "]";
    }
}
