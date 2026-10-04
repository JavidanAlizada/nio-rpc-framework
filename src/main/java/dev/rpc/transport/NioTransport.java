package dev.rpc.transport;

import dev.rpc.protocol.FrameEncoder;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The NIO transport: a fixed group of event loops, an acceptor thread per server, and the connections they own.
 * Everything selector-related stays behind bind() and connect().
 *
 * Threads are named rpc-N-loop-I and rpc-N-acceptor-PORT. They are not daemon threads, so close() the transport
 * when done.
 */
public final class NioTransport implements Transport {

    private static final AtomicInteger IDS = new AtomicInteger();

    private final TransportConfig config;
    private final String name;
    private final FrameEncoder encoder;
    private final EventLoop[] loops;
    private final AtomicInteger nextLoop = new AtomicInteger();
    private final Set<NioConnection> connections = ConcurrentHashMap.newKeySet();
    private final Set<NioServer> servers = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    public NioTransport() {
        this(TransportConfig.defaults());
    }

    public NioTransport(TransportConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.name = "rpc-" + IDS.incrementAndGet();
        this.encoder = new FrameEncoder(config.protocolLimits());
        this.loops = new EventLoop[config.ioThreads()];
        for (int i = 0; i < loops.length; i++) {
            loops[i] = new EventLoop(name + "-loop-" + i);
        }
    }

    @Override
    public Server bind(SocketAddress address, ConnectionHandler handler) {
        Objects.requireNonNull(handler, "handler");
        checkOpen();
        try {
            NioServer server = new NioServer(this, address, handler);
            servers.add(server);
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException("can't bind " + address, e);
        }
    }

    @Override
    public CompletableFuture<Connection> connect(SocketAddress address, ConnectionHandler handler) {
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(handler, "handler");
        checkOpen();
        var future = new CompletableFuture<Connection>();
        SocketChannel channel;
        try {
            channel = SocketChannel.open();
            configure(channel);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
        EventLoop loop = nextLoop();
        var connection = new NioConnection(this, loop, channel, handler, address, () -> { }, future);
        connections.add(connection);
        Duration timeout = config.connectTimeout();
        try {
            loop.execute(() -> connection.startConnect(address, timeout));
        } catch (RejectedExecutionException e) {
            connections.remove(connection);
            closeQuietly(channel);
            return CompletableFuture.failedFuture(new ConnectionClosedException("transport is closed"));
        }
        return future;
    }

    /** Takes over a freshly accepted channel. release runs once the connection is closed. */
    void adopt(SocketChannel channel, ConnectionHandler handler, Runnable release) {
        SocketAddress remote;
        try {
            configure(channel);
            remote = channel.getRemoteAddress();
        } catch (IOException e) {
            release.run();
            closeQuietly(channel);
            return;
        }
        EventLoop loop = nextLoop();
        var connection = new NioConnection(this, loop, channel, handler, remote, release, null);
        connections.add(connection);
        try {
            loop.execute(connection::openAccepted);
        } catch (RejectedExecutionException e) {
            connections.remove(connection);
            release.run();
            closeQuietly(channel);
        }
    }

    /**
     * Stops the servers, closes every connection (each gets onClosed exactly once), then stops the loops. Waits up
     * to 10 seconds per loop.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        servers.forEach(NioServer::close);
        connections.forEach(NioConnection::close);
        for (EventLoop loop : loops) {
            loop.shutdown();
        }
        for (EventLoop loop : loops) {
            loop.close();
        }
        // A connect() racing with close() can open a connection after the sweep above. The loops are dead now, so
        // nothing else touches these connections and closing them from this thread is safe.
        for (NioConnection connection : connections) {
            connection.doClose(null);
        }
    }

    // --- package-private plumbing ---

    TransportConfig config() {
        return config;
    }

    String name() {
        return name;
    }

    FrameEncoder encoder() {
        return encoder;
    }

    void released(NioConnection connection) {
        connections.remove(connection);
    }

    void serverClosed(NioServer server) {
        servers.remove(server);
    }

    private EventLoop nextLoop() {
        return loops[Math.floorMod(nextLoop.getAndIncrement(), loops.length)];
    }

    private void configure(SocketChannel channel) throws IOException {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, config.tcpNoDelay());
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("transport is closed");
        }
    }

    static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            // expected at times: the channel is being discarded either way
        }
    }
}
