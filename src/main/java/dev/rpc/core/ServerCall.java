package dev.rpc.core;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** What a server interceptor sees of a call: the service method, the client's headers, and the deadline (or null). */
public record ServerCall(Method method, Map<String, String> headers, Deadline deadline) {

    public ServerCall {
        Objects.requireNonNull(method, "method");
        headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
    }
}
