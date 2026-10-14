package dev.rpc.core;

import dev.rpc.protocol.Frame;
import dev.rpc.protocol.ProtocolException;
import dev.rpc.protocol.Response;
import dev.rpc.protocol.Status;
import dev.rpc.serialization.RecordSerializer;
import dev.rpc.serialization.Serializer;
import dev.rpc.transport.Connection;
import dev.rpc.transport.ConnectionClosedException;
import dev.rpc.transport.ConnectionHandler;
import dev.rpc.transport.NioTransport;
import dev.rpc.transport.ReconnectingConnection;
import java.lang.reflect.Proxy;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Calls one server endpoint through stubs of its service interfaces. Every stub shares one connection, which
 * reconnects by itself; calls made while it is down fail at once with ConnectionClosedException.
 *
 * <pre>
 * RpcClient client = RpcClient.connect(transport, new InetSocketAddress("host", 9000));
 * Greeter greeter = client.stub(Greeter.class);
 * </pre>
 */
public final class RpcClient implements AutoCloseable {

    public static final int DEFAULT_MAX_IN_FLIGHT = 1_024;

    private final Serializer serializer;
    private final int maxInFlight;
    private final Duration defaultTimeout;
    private final List<ClientInterceptor> interceptors;
    private final ScheduledExecutorService timers = Timers.create("rpc-client-deadlines");
    private final ReconnectingConnection connection;
    // One table per physical connection: a call must wait in the table of the connection it was written to.
    private final ConcurrentHashMap<Connection, CallTable> tables = new ConcurrentHashMap<>();
    private final Executor completer;
    private volatile boolean closed;

    private RpcClient(Builder builder) {
        this.serializer = builder.serializer;
        this.maxInFlight = builder.maxInFlight;
        this.defaultTimeout = builder.defaultTimeout;
        this.interceptors = List.copyOf(builder.interceptors);
        ThreadFactory threads = Thread.ofVirtual().name("rpc-complete-", 0).factory();
        this.completer = task -> threads.newThread(task).start();
        this.connection = builder.transport.reconnecting(builder.address, new Handler());
    }

    public static RpcClient connect(NioTransport transport, SocketAddress address) {
        return builder(transport, address).connect();
    }

    public static Builder builder(NioTransport transport, SocketAddress address) {
        return new Builder(transport, address);
    }

    /** A stub for type with the client's default timeout. Fails here, not on its first call, if type can't be used. */
    public <T> T stub(Class<T> type) {
        return stub(type, defaultTimeout);
    }

    /** A stub for type whose calls each get timeout (null for none), or less if the calling server call has less. */
    public <T> T stub(Class<T> type, Duration timeout) {
        if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
        ServiceDescriptor service = ServiceDescriptor.of(type);
        Object proxy = Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                new StubHandler(this, service, timeout));
        return type.cast(proxy);
    }

    public ReconnectingConnection.State state() {
        return connection.state();
    }

    /** Fails every pending call with ConnectionClosedException and closes the connection. Stubs fail afterwards. */
    @Override
    public void close() {
        closed = true;
        connection.close();
        tables.values().forEach(CallTable::close);
        timers.shutdownNow();
    }

    @Override
    public String toString() {
        return "RpcClient[" + connection.remoteAddress() + "]";
    }

    /** Runs one call through the interceptors. Never throws; every failure comes back through the future. */
    CompletableFuture<Object> invoke(MethodDescriptor method, ClientCall call, Object[] args) {
        // The request on the wire right now. An interceptor may retry, so cancelling the call cancels whichever
        // attempt is current, whatever future the chain handed back.
        var attempt = new AtomicReference<CompletableFuture<Object>>();
        CompletableFuture<Object> result = proceed(0, method, call, args, attempt);
        result.whenComplete((r, e) -> {
            CompletableFuture<Object> current = attempt.get();
            if (result.isCancelled() && current != null) {
                current.cancel(false);
            }
        });
        return result;
    }

    private CompletableFuture<Object> proceed(int index, MethodDescriptor method, ClientCall call, Object[] args,
            AtomicReference<CompletableFuture<Object>> attempt) {
        if (index == interceptors.size()) {
            CompletableFuture<Object> sent = send(method, args, call.deadline(), call.headers());
            attempt.set(sent);
            return sent;
        }
        try {
            CompletableFuture<Object> result = interceptors.get(index).intercept(call, args,
                    (c, a) -> proceed(index + 1, method, Objects.requireNonNull(c, "call"), a, attempt));
            return result != null ? result : CompletableFuture.failedFuture(
                    new NullPointerException(interceptors.get(index) + " returned null"));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // One request on the wire; deadline may be null.
    private CompletableFuture<Object> send(MethodDescriptor method, Object[] args, Deadline deadline,
            Map<String, String> headers) {
        if (closed) {
            return CompletableFuture.failedFuture(new ConnectionClosedException("client closed"));
        }
        Connection physical = connection.current();
        if (physical == null) {
            return CompletableFuture.failedFuture(new ConnectionClosedException(
                    "not connected to " + connection.remoteAddress() + " (" + connection.state() + ")"));
        }
        byte[] payload;
        try {
            payload = serializer.serialize(method.parameterTypes(), args == null ? new Object[0] : args);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        long timeoutNanos = 0;
        if (deadline != null) {
            timeoutNanos = deadline.remainingNanos();
            if (timeoutNanos <= 0) {
                return CompletableFuture.failedFuture(deadlineExceeded(method));
            }
        }
        CallTable table = tables.computeIfAbsent(physical, c -> new CallTable(c, maxInFlight, completer));
        CompletableFuture<Response> response = table.call(method.id(), timeoutNanos, headers, payload);
        if (response.isCompletedExceptionally() && connection.current() != physical) {
            // physical closed between current() and computeIfAbsent, so this table may have missed its sweep.
            tables.remove(physical, table);
            table.close();
        }
        if (deadline != null && !response.isDone()) {
            expireAt(response, method, timeoutNanos);
        }
        // Runs on the completer thread that completes response, so decoding stays off the event loop.
        CompletableFuture<Object> result = response.thenApply(r -> decode(method, r));
        result.whenComplete((r, e) -> {
            if (result.isCancelled()) {
                response.cancel(false); // frees the request id; the server isn't told until M4
            }
        });
        return result;
    }

    // Failing the future takes the call out of its table, which sends the server a CANCEL.
    private void expireAt(CompletableFuture<Response> response, MethodDescriptor method, long nanos) {
        try {
            ScheduledFuture<?> timer = timers.schedule(
                    () -> completer.execute(() -> response.completeExceptionally(deadlineExceeded(method))),
                    nanos, TimeUnit.NANOSECONDS);
            response.whenComplete((r, e) -> timer.cancel(false));
        } catch (RejectedExecutionException e) {
            response.completeExceptionally(new ConnectionClosedException("client closed"));
        }
    }

    private static RpcException deadlineExceeded(MethodDescriptor method) {
        return new RpcException(Status.DEADLINE_EXCEEDED, "deadline passed before " + method + " was answered");
    }

    private Object decode(MethodDescriptor method, Response response) {
        Status status = response.status();
        if (status.isOk()) {
            return method.resultTypes().length == 0
                    ? null : serializer.deserialize(method.resultTypes(), response.payload())[0];
        }
        if (status.equals(Status.APPLICATION_ERROR)) {
            throw new RemoteException(response.errorType(), response.message());
        }
        throw new RpcException(status, response.errorType() + ": " + response.message());
    }

    private final class Handler implements ConnectionHandler {

        @Override
        public void onFrame(Connection physical, Frame frame) {
            if (!(frame instanceof Response response)) {
                throw new ProtocolException("a client doesn't accept " + frame.getClass().getSimpleName());
            }
            CallTable table = tables.get(physical);
            if (table != null) {
                table.onResponse(response);
            }
        }

        @Override
        public void onClosed(Connection physical, Throwable cause) {
            CallTable table = tables.remove(physical);
            if (table != null) {
                table.close();
            }
        }
    }

    public static final class Builder {

        private final NioTransport transport;
        private final SocketAddress address;
        private Serializer serializer = new RecordSerializer();
        private int maxInFlight = DEFAULT_MAX_IN_FLIGHT;
        private Duration defaultTimeout;
        private final List<ClientInterceptor> interceptors = new ArrayList<>();

        private Builder(NioTransport transport, SocketAddress address) {
            this.transport = Objects.requireNonNull(transport, "transport");
            this.address = Objects.requireNonNull(address, "address");
        }

        public Builder serializer(Serializer serializer) {
            this.serializer = Objects.requireNonNull(serializer, "serializer");
            return this;
        }

        /** Calls that may wait for a response at once; more fail with OVERLOADED without being sent. */
        public Builder maxInFlight(int max) {
            if (max < 1) {
                throw new IllegalArgumentException("max must be >= 1: " + max);
            }
            this.maxInFlight = max;
            return this;
        }

        /** The timeout of stubs created without one. None by default: such calls wait until answered or cancelled. */
        public Builder defaultTimeout(Duration timeout) {
            if (timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("timeout must be positive: " + timeout);
            }
            this.defaultTimeout = timeout;
            return this;
        }

        /** Adds an interceptor around every call. The first one added is the outermost. */
        public Builder intercept(ClientInterceptor interceptor) {
            interceptors.add(Objects.requireNonNull(interceptor, "interceptor"));
            return this;
        }

        /** Starts connecting and returns at once; state() shows when the connection is up. */
        public RpcClient connect() {
            return new RpcClient(this);
        }
    }
}
