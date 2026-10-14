package dev.rpc.core;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * What a client interceptor sees of a call: the stub method, the headers to send, and the deadline (null for none).
 * Immutable; an interceptor changes a call by passing a modified copy to next.
 */
public record ClientCall(Method method, Map<String, String> headers, Deadline deadline) {

    public ClientCall {
        Objects.requireNonNull(method, "method");
        headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
    }

    public ClientCall withHeader(String key, String value) {
        var copy = new LinkedHashMap<>(headers);
        copy.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
        return new ClientCall(method, copy, deadline);
    }

    /** A call with a different deadline. Interceptors may tighten it; nothing stops them from removing it. */
    public ClientCall withDeadline(Deadline deadline) {
        return new ClientCall(method, headers, deadline);
    }
}
