package dev.rpc.core;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 * One remote method of a service interface. resultTypes is empty for void results and holds the single result type
 * otherwise, so both directions go through Serializer the same way. async means the declared return type is
 * CompletableFuture; the wire doesn't know the difference.
 */
final class MethodDescriptor {

    private final int id;
    private final Method method;
    private final Type[] parameterTypes;
    private final Type[] resultTypes;
    private final boolean async;

    MethodDescriptor(int id, Method method, Type[] parameterTypes, Type[] resultTypes, boolean async) {
        this.id = id;
        this.method = method;
        this.parameterTypes = parameterTypes;
        this.resultTypes = resultTypes;
        this.async = async;
    }

    int id() {
        return id;
    }

    Method method() {
        return method;
    }

    // The arrays are shared, not copied: nothing in this package modifies them.
    Type[] parameterTypes() {
        return parameterTypes;
    }

    Type[] resultTypes() {
        return resultTypes;
    }

    boolean async() {
        return async;
    }

    @Override
    public String toString() {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName() + " (id " + id + ")";
    }
}
