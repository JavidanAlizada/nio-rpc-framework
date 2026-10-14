package dev.rpc.core;

import dev.rpc.protocol.Status;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Turns a stub's method calls into remote calls. A CompletableFuture method returns the call's future; any other
 * method waits for it and throws what the call failed with. Object methods and default methods run locally.
 */
final class StubHandler implements InvocationHandler {

    private final RpcClient client;
    private final ServiceDescriptor service;
    private final Duration timeout;

    StubHandler(RpcClient client, ServiceDescriptor service, Duration timeout) {
        this.client = client;
        this.service = service;
        this.timeout = timeout;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        MethodDescriptor remote = service.forMethod(method);
        if (remote != null) {
            Deadline own = timeout == null ? null : Deadline.after(timeout);
            var info = new ClientCall(method, Map.of(), Deadline.earliest(own, RpcContext.deadline()));
            CompletableFuture<Object> call = client.invoke(remote, info, args);
            return remote.async() ? call : await(call);
        }
        if (method.isDefault()) {
            return InvocationHandler.invokeDefault(proxy, method, args);
        }
        return switch (method.getName()) {
            case "equals" -> proxy == args[0];
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> service.type().getName() + " stub for " + client;
            default -> throw new UnsupportedOperationException(method.toString());
        };
    }

    private static Object await(CompletableFuture<Object> call) throws Throwable {
        try {
            return call.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        } catch (InterruptedException e) {
            call.cancel(false);
            Thread.currentThread().interrupt();
            throw new RpcException(Status.CANCELLED, "interrupted while waiting for the response");
        }
    }
}
