package dev.rpc.core;

import dev.rpc.serialization.RecordSerializer;
import dev.rpc.serialization.Serializer;
import dev.rpc.transport.Server;
import dev.rpc.transport.Transport;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Serves implementations of service interfaces. Each call runs on its own virtual thread, so a service method may
 * block. The set of services is fixed once the server is bound.
 *
 * <pre>
 * RpcServer server = RpcServer.builder(transport)
 *         .register(Greeter.class, new GreeterImpl())
 *         .bind(new InetSocketAddress(9000));
 * </pre>
 */
public final class RpcServer implements AutoCloseable {

    public static final int DEFAULT_MAX_CONCURRENT = 1_024;

    private final Server server;
    private final Dispatcher dispatcher;
    private final ExecutorService executor;
    private final ScheduledExecutorService timers;

    private RpcServer(Server server, Dispatcher dispatcher, ExecutorService executor,
            ScheduledExecutorService timers) {
        this.server = server;
        this.dispatcher = dispatcher;
        this.executor = executor;
        this.timers = timers;
    }

    public static Builder builder(Transport transport) {
        return new Builder(Objects.requireNonNull(transport, "transport"));
    }

    public SocketAddress localAddress() {
        return server.localAddress();
    }

    /**
     * Stops accepting, closes the connections that have sent calls, and stops the call threads, interrupting calls
     * still running. Waits up to 10 seconds for them.
     */
    @Override
    public void close() {
        server.close();
        dispatcher.closeConnections();
        timers.shutdownNow();
        executor.shutdownNow();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static final class Builder {

        private final Transport transport;
        private final Map<Integer, Dispatcher.Entry> table = new HashMap<>();
        private Serializer serializer = new RecordSerializer();
        private int maxConcurrent = DEFAULT_MAX_CONCURRENT;
        private boolean bound;

        private Builder(Transport transport) {
            this.transport = transport;
        }

        /** Exposes impl as type. Fails if type can't be served or one of its method ids is already taken. */
        public <T> Builder register(Class<T> type, T impl) {
            Objects.requireNonNull(impl, "impl");
            checkNotBound();
            ServiceDescriptor service = ServiceDescriptor.of(type);
            if (!type.isInstance(impl)) {
                throw new IllegalArgumentException(impl.getClass().getName() + " doesn't implement " + type.getName());
            }
            var added = new HashMap<Integer, Dispatcher.Entry>();
            for (MethodDescriptor method : service.methods()) {
                Dispatcher.Entry existing = table.get(method.id());
                if (existing != null) {
                    throw new IllegalArgumentException("method id " + method.id() + " is shared by "
                            + existing.method().method() + " and " + method.method());
                }
                added.put(method.id(), new Dispatcher.Entry(method, handle(method.method(), impl)));
            }
            table.putAll(added);
            return this;
        }

        public Builder serializer(Serializer serializer) {
            this.serializer = Objects.requireNonNull(serializer, "serializer");
            return this;
        }

        /** Calls one connection may have running at once; more are answered with OVERLOADED. */
        public Builder maxConcurrentCallsPerConnection(int max) {
            if (max < 1) {
                throw new IllegalArgumentException("max must be >= 1: " + max);
            }
            this.maxConcurrent = max;
            return this;
        }

        /** Freezes the services and starts listening. The builder can't be used afterwards. */
        public RpcServer bind(SocketAddress address) {
            checkNotBound();
            if (table.isEmpty()) {
                throw new IllegalStateException("no services registered");
            }
            bound = true;
            ExecutorService executor = Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("rpc-call-", 0).factory());
            ScheduledExecutorService timers = Timers.create("rpc-server-deadlines");
            var dispatcher = new Dispatcher(Map.copyOf(table), serializer, executor, timers, maxConcurrent);
            try {
                return new RpcServer(transport.bind(address, dispatcher), dispatcher, executor, timers);
            } catch (RuntimeException e) {
                executor.shutdown();
                timers.shutdown();
                throw e;
            }
        }

        private void checkNotBound() {
            if (bound) {
                throw new IllegalStateException("already bound; services can't change on a running server");
            }
        }

        // The interface's method, bound to impl and spread over an Object[], returning Object (null for void).
        private static MethodHandle handle(Method method, Object impl) {
            try {
                method.setAccessible(true); // the interface itself may not be public
                MethodHandle h = MethodHandles.lookup().unreflect(method).bindTo(impl);
                return h.asSpreader(Object[].class, method.getParameterCount())
                        .asType(MethodType.methodType(Object.class, Object[].class));
            } catch (IllegalAccessException | RuntimeException e) {
                throw new IllegalArgumentException("can't call " + method + ": " + e.getMessage(), e);
            }
        }
    }
}
