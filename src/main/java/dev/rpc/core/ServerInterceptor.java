package dev.rpc.core;

/**
 * Wraps every call a server runs, on the call's own virtual thread, so it may block. Interceptors run in the order
 * they were added, the first outermost. For a method returning CompletableFuture, the result an interceptor sees is
 * that future. What it throws is answered like an exception from the method.
 */
@FunctionalInterface
public interface ServerInterceptor {

    Object intercept(ServerCall call, Object[] args, Next next) throws Exception;

    @FunctionalInterface
    interface Next {
        Object proceed(Object[] args) throws Exception;
    }
}
