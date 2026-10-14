package dev.rpc.core;

import dev.rpc.protocol.Cancel;
import dev.rpc.protocol.Frame;
import dev.rpc.protocol.ProtocolException;
import dev.rpc.protocol.Request;
import dev.rpc.protocol.Response;
import dev.rpc.protocol.Status;
import dev.rpc.serialization.Serializer;
import dev.rpc.transport.Connection;
import dev.rpc.transport.ConnectionClosedException;
import dev.rpc.transport.ConnectionHandler;
import java.lang.System.Logger.Level;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The server's connection handler. On the loop it only looks the method up and checks the connection's limit; the
 * call itself (decode, invoke, encode) runs on a virtual thread.
 */
final class Dispatcher implements ConnectionHandler {

    private static final System.Logger LOG = System.getLogger(Dispatcher.class.getName());
    private static final byte[] EMPTY = new byte[0];

    /** A registered method: its descriptor and a handle bound to the implementation, taking Object[]. */
    record Entry(MethodDescriptor method, MethodHandle handle) { }

    private final Map<Integer, Entry> table;
    private final Serializer serializer;
    private final ExecutorService executor;
    private final int maxConcurrent;
    // Running calls per connection. Also the set of connections close() has to close.
    private final ConcurrentHashMap<Connection, AtomicInteger> running = new ConcurrentHashMap<>();

    Dispatcher(Map<Integer, Entry> table, Serializer serializer, ExecutorService executor, int maxConcurrent) {
        this.table = table;
        this.serializer = serializer;
        this.executor = executor;
        this.maxConcurrent = maxConcurrent;
    }

    @Override
    public void onFrame(Connection connection, Frame frame) {
        switch (frame) {
            case Request request -> dispatch(connection, request);
            case Cancel ignored -> { } // M4
            default -> throw new ProtocolException("a server doesn't accept " + frame.getClass().getSimpleName());
        }
    }

    @Override
    public void onClosed(Connection connection, Throwable cause) {
        running.remove(connection);
    }

    void closeConnections() {
        running.keySet().forEach(Connection::close);
    }

    private void dispatch(Connection connection, Request request) {
        AtomicInteger count = running.computeIfAbsent(connection, c -> new AtomicInteger());
        Entry entry = table.get(request.methodId());
        if (entry == null) {
            reply(connection, Response.error(request.requestId(), Status.UNKNOWN_METHOD, "UnknownMethod",
                    "no method with id " + request.methodId()));
            return;
        }
        if (count.get() >= maxConcurrent) {
            reply(connection, Response.error(request.requestId(), Status.OVERLOADED, "Overloaded",
                    maxConcurrent + " calls already running on this connection"));
            return;
        }
        count.incrementAndGet();
        try {
            executor.execute(() -> invoke(connection, count, entry, request));
        } catch (RejectedExecutionException e) {
            // The server is closing.
            count.decrementAndGet();
            connection.close();
        }
    }

    private void invoke(Connection connection, AtomicInteger count, Entry entry, Request request) {
        int id = request.requestId();
        MethodDescriptor method = entry.method();
        Object[] args;
        try {
            args = serializer.deserialize(method.parameterTypes(), request.payload());
        } catch (RuntimeException e) {
            finish(connection, count, failure(id, Status.BAD_REQUEST, e));
            return;
        }
        Object result;
        try {
            result = entry.handle().invokeExact(args);
        } catch (Throwable t) {
            finish(connection, count, thrown(id, method, t));
            rethrowIfFatal(t);
            return;
        }
        if (!method.async()) {
            finish(connection, count, success(id, method, result));
        } else if (result == null) {
            finish(connection, count, Response.error(id, Status.INTERNAL, "NullPointerException",
                    method + " returned null instead of a CompletableFuture"));
        } else {
            ((CompletableFuture<?>) result).whenComplete((value, error) -> {
                if (error == null) {
                    finish(connection, count, success(id, method, value));
                } else {
                    Throwable t = error instanceof CompletionException && error.getCause() != null
                            ? error.getCause() : error;
                    finish(connection, count, thrown(id, method, t));
                }
            });
        }
    }

    private Response success(int id, MethodDescriptor method, Object value) {
        try {
            byte[] payload = method.resultTypes().length == 0
                    ? EMPTY : serializer.serialize(method.resultTypes(), new Object[] {value});
            return Response.ok(id, payload);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "couldn't serialize the result of " + method, e);
            return failure(id, Status.INTERNAL, e);
        }
    }

    private static Response thrown(int id, MethodDescriptor method, Throwable t) {
        if (t instanceof Exception) {
            return failure(id, Status.APPLICATION_ERROR, t);
        }
        LOG.log(Level.WARNING, method + " threw " + t, t);
        return failure(id, Status.INTERNAL, t);
    }

    private static Response failure(int id, Status status, Throwable t) {
        return Response.error(id, status, t.getClass().getName(), Messages.truncate(t.getMessage()));
    }

    private void finish(Connection connection, AtomicInteger count, Response response) {
        count.decrementAndGet();
        reply(connection, response);
    }

    private static void reply(Connection connection, Response response) {
        try {
            connection.write(response);
        } catch (ConnectionClosedException e) {
            // The client has already failed this call.
            LOG.log(Level.DEBUG, "dropping {0}: connection closed", response);
        } catch (RuntimeException e) {
            // Most likely a result over the frame size limit.
            if (response.status().isOk()) {
                LOG.log(Level.WARNING, "couldn't send " + response, e);
                reply(connection, failure(response.requestId(), Status.INTERNAL, e));
            } else {
                throw e;
            }
        }
    }

    private static void rethrowIfFatal(Throwable t) {
        if (t instanceof VirtualMachineError e) {
            throw e;
        }
    }
}
