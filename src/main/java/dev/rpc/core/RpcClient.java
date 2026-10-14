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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

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
    private final ReconnectingConnection connection;
    // One table per physical connection: a call must wait in the table of the connection it was written to.
    private final ConcurrentHashMap<Connection, CallTable> tables = new ConcurrentHashMap<>();
    private final Executor completer;
    private volatile boolean closed;

    private RpcClient(NioTransport transport, SocketAddress address, Serializer serializer, int maxInFlight) {
        this.serializer = serializer;
        this.maxInFlight = maxInFlight;
        ThreadFactory threads = Thread.ofVirtual().name("rpc-complete-", 0).factory();
        this.completer = task -> threads.newThread(task).start();
        this.connection = transport.reconnecting(address, new Handler());
    }

    public static RpcClient connect(NioTransport transport, SocketAddress address) {
        return builder(transport, address).connect();
    }

    public static Builder builder(NioTransport transport, SocketAddress address) {
        return new Builder(transport, address);
    }

    /** A stub for type. Fails here, not on the first call, if type can't be called remotely. */
    public <T> T stub(Class<T> type) {
        ServiceDescriptor service = ServiceDescriptor.of(type);
        Object proxy = Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                new StubHandler(this, service));
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
    }

    @Override
    public String toString() {
        return "RpcClient[" + connection.remoteAddress() + "]";
    }

    /** Sends one call. Never throws; every failure comes back through the future. */
    CompletableFuture<Object> call(MethodDescriptor method, Object[] args) {
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
        CallTable table = tables.computeIfAbsent(physical, c -> new CallTable(c, maxInFlight, completer));
        CompletableFuture<Response> response = table.call(method.id(), payload);
        if (response.isCompletedExceptionally() && connection.current() != physical) {
            // physical closed between current() and computeIfAbsent, so this table may have missed its sweep.
            tables.remove(physical, table);
            table.close();
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

        /** Starts connecting and returns at once; state() shows when the connection is up. */
        public RpcClient connect() {
            return new RpcClient(transport, address, serializer, maxInFlight);
        }
    }
}
