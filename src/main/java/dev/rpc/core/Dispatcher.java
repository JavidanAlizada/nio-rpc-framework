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
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final ScheduledExecutorService timers;
    private final List<ServerInterceptor> interceptors;
    private final int maxConcurrent;
    // Running calls per connection, by request id. A call leaves its map when its work really ends, so the map's
    // size bounds the threads a connection holds. Also the set of connections close() has to close.
    private final ConcurrentHashMap<Connection, Map<Integer, RunningCall>> running = new ConcurrentHashMap<>();

    Dispatcher(Map<Integer, Entry> table, Serializer serializer, ExecutorService executor,
            ScheduledExecutorService timers, List<ServerInterceptor> interceptors, int maxConcurrent) {
        this.table = table;
        this.serializer = serializer;
        this.executor = executor;
        this.timers = timers;
        this.interceptors = interceptors;
        this.maxConcurrent = maxConcurrent;
    }

    @Override
    public void onFrame(Connection connection, Frame frame) {
        switch (frame) {
            case Request request -> dispatch(connection, request);
            case Cancel cancel -> cancel(connection, cancel.requestId());
            default -> throw new ProtocolException("a server doesn't accept " + frame.getClass().getSimpleName());
        }
    }

    @Override
    public void onClosed(Connection connection, Throwable cause) {
        Map<Integer, RunningCall> calls = running.remove(connection);
        if (calls != null) {
            for (RunningCall call : calls.values()) {
                // Nobody is left to read a reply.
                call.claimReply();
                call.cancel();
            }
        }
    }

    void closeConnections() {
        running.keySet().forEach(Connection::close);
    }

    private void cancel(Connection connection, int requestId) {
        Map<Integer, RunningCall> calls = running.get(connection);
        RunningCall call = calls == null ? null : calls.get(requestId);
        // The client has dropped the call, so it gets no reply. A call already replying is left alone.
        if (call != null && call.claimReply()) {
            call.cancel();
        }
    }

    private void dispatch(Connection connection, Request request) {
        Map<Integer, RunningCall> calls = running.computeIfAbsent(connection, c -> new ConcurrentHashMap<>());
        Entry entry = table.get(request.methodId());
        if (entry == null) {
            reply(connection, Response.error(request.requestId(), Status.UNKNOWN_METHOD, "UnknownMethod",
                    "no method with id " + request.methodId()));
            return;
        }
        if (calls.size() >= maxConcurrent) {
            reply(connection, Response.error(request.requestId(), Status.OVERLOADED, "Overloaded",
                    maxConcurrent + " calls already running on this connection"));
            return;
        }
        // Measured from when the loop read the Request, so waiting for a thread counts against it.
        Deadline deadline = request.timeoutNanos() == 0 ? null : Deadline.afterNanos(request.timeoutNanos());
        var call = new RunningCall(request.requestId(), deadline);
        // Ids only repeat after 2^32 calls, so a put never replaces a call that's still running.
        calls.put(call.requestId(), call);
        try {
            call.started(executor.submit(() -> invoke(connection, calls, call, entry, request)));
            if (deadline != null) {
                call.deadlineTimer(timers.schedule(() -> expire(connection, call, entry.method()),
                        request.timeoutNanos(), TimeUnit.NANOSECONDS));
            }
        } catch (RejectedExecutionException e) {
            // The server is closing.
            calls.remove(call.requestId(), call);
            call.claimReply();
            call.cancel();
            connection.close();
        }
    }

    private static void expire(Connection connection, RunningCall call, MethodDescriptor method) {
        if (call.claimReply()) {
            call.cancel();
            reply(connection, Response.error(call.requestId(), Status.DEADLINE_EXCEEDED, "DeadlineExceeded",
                    "the caller's deadline passed while " + method + " was running"));
        }
    }

    private void invoke(Connection connection, Map<Integer, RunningCall> calls, RunningCall call, Entry entry,
            Request request) {
        int id = request.requestId();
        MethodDescriptor method = entry.method();
        Object[] args;
        try {
            args = serializer.deserialize(method.parameterTypes(), request.payload());
        } catch (RuntimeException e) {
            finish(connection, calls, call, failure(id, Status.BAD_REQUEST, e));
            return;
        }
        Object result;
        var info = new ServerCall(method.method(), request.headers(), call.deadline());
        RpcContext.enter(info);
        try {
            result = proceed(0, entry, info, args);
        } catch (Throwable t) {
            finish(connection, calls, call, thrown(id, method, t));
            rethrowIfFatal(t);
            return;
        } finally {
            RpcContext.exit();
        }
        if (!method.async()) {
            finish(connection, calls, call, success(id, method, result));
        } else if (result == null) {
            finish(connection, calls, call, Response.error(id, Status.INTERNAL, "NullPointerException",
                    method + " returned null instead of a CompletableFuture"));
        } else {
            var future = (CompletableFuture<?>) result;
            call.returnedFuture(future);
            future.whenComplete((value, error) -> {
                if (error == null) {
                    finish(connection, calls, call, success(id, method, value));
                } else {
                    Throwable t = error instanceof CompletionException && error.getCause() != null
                            ? error.getCause() : error;
                    finish(connection, calls, call, thrown(id, method, t));
                }
            });
        }
    }

    private Object proceed(int index, Entry entry, ServerCall call, Object[] args) throws Exception {
        if (index == interceptors.size()) {
            try {
                return entry.handle().invokeExact(args);
            } catch (Exception | Error e) {
                throw e;
            } catch (Throwable t) {
                throw new UndeclaredThrowableException(t);
            }
        }
        return interceptors.get(index).intercept(call, args, a -> proceed(index + 1, entry, call, a));
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

    private static void finish(Connection connection, Map<Integer, RunningCall> calls, RunningCall call,
            Response response) {
        calls.remove(call.requestId(), call);
        call.finished();
        if (call.claimReply()) {
            reply(connection, response);
        }
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
