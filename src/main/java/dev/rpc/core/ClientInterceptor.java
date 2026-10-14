package dev.rpc.core;

import java.util.concurrent.CompletableFuture;

/**
 * Wraps every call a client's stubs make. Interceptors run in the order they were added, the first outermost. An
 * interceptor may change the call or arguments, short-circuit, or call next more than once (each proceed is a new
 * request on the wire, under the call's deadline). Throwing fails the call.
 *
 * Runs on the caller's thread; the returned future completes off the event loop.
 */
@FunctionalInterface
public interface ClientInterceptor {

    CompletableFuture<Object> intercept(ClientCall call, Object[] args, Next next);

    @FunctionalInterface
    interface Next {
        CompletableFuture<Object> proceed(ClientCall call, Object[] args);
    }
}
