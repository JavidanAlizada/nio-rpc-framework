package dev.rpc.protocol;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A call: which method, how long the caller is still willing to wait, metadata headers, and the serialized
 * arguments. timeoutNanos is time remaining, not a timestamp, because the two machines' clocks aren't comparable;
 * 0 means no deadline.
 */
public record Request(int requestId, int methodId, long timeoutNanos, Map<String, String> headers, byte[] payload)
        implements Frame {

    public Request {
        if (requestId == 0) {
            throw new IllegalArgumentException("requestId 0 is reserved for connection-level frames");
        }
        if (timeoutNanos < 0) {
            throw new IllegalArgumentException("timeoutNanos must be >= 0 (0 = no deadline): " + timeoutNanos);
        }
        Objects.requireNonNull(headers, "headers");
        Objects.requireNonNull(payload, "payload");
        if (headers.size() > FrameFormat.MAX_HEADERS) {
            throw new IllegalArgumentException("too many headers: " + headers.size());
        }
        // Insertion order is kept so the same Request always encodes to the same bytes.
        var copy = new LinkedHashMap<String, String>(headers.size() * 2);
        headers.forEach((k, v) -> copy.put(Objects.requireNonNull(k, "header key"),
                Objects.requireNonNull(v, "header value")));
        headers = Collections.unmodifiableMap(copy);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Request r
                && requestId == r.requestId
                && methodId == r.methodId
                && timeoutNanos == r.timeoutNanos
                && headers.equals(r.headers)
                && Arrays.equals(payload, r.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, methodId, timeoutNanos, headers, Arrays.hashCode(payload));
    }

    @Override
    public String toString() {
        return "Request[requestId=" + requestId + ", methodId=" + methodId + ", timeoutNanos=" + timeoutNanos
                + ", headers=" + headers + ", payload=" + payload.length + " bytes]";
    }
}
