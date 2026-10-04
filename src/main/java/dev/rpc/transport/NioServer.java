package dev.rpc.transport;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A listening socket with its own acceptor thread blocking in accept(). Accepted channels are handed to the event
 * loops round robin, so an accept storm never takes time from I/O. Over maxConnections, a new connection is
 * accepted and closed at once: TCP has no way to refuse it earlier.
 */
final class NioServer implements Server {

    private static final System.Logger LOG = System.getLogger(NioServer.class.getName());

    private final NioTransport transport;
    private final ServerSocketChannel channel;
    private final ConnectionHandler handler;
    private final SocketAddress localAddress;
    private final Thread acceptor;
    private final AtomicInteger open = new AtomicInteger();
    private volatile boolean closed;

    NioServer(NioTransport transport, SocketAddress address, ConnectionHandler handler) throws IOException {
        this.transport = transport;
        this.handler = handler;
        this.channel = ServerSocketChannel.open();
        try {
            channel.bind(address, transport.config().acceptBacklog());
            this.localAddress = channel.getLocalAddress();
        } catch (IOException e) {
            channel.close();
            throw e;
        }
        int port = ((InetSocketAddress) localAddress).getPort();
        this.acceptor = Thread.ofPlatform().name(transport.name() + "-acceptor-" + port).unstarted(this::acceptLoop);
        acceptor.start();
    }

    private void acceptLoop() {
        while (!closed) {
            SocketChannel accepted;
            try {
                accepted = channel.accept();
            } catch (ClosedChannelException e) {
                return;
            } catch (IOException e) {
                if (closed) {
                    return;
                }
                // Typically out of file descriptors. Pausing keeps a persistent error from spinning this thread.
                LOG.log(Level.WARNING, "accept failed", e);
                pause();
                continue;
            }
            if (open.incrementAndGet() > transport.config().maxConnections()) {
                open.decrementAndGet();
                NioTransport.closeQuietly(accepted);
                continue;
            }
            transport.adopt(accepted, handler, open::decrementAndGet);
        }
    }

    private static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Connections accepted by this server and not yet closed. */
    int openConnections() {
        return open.get();
    }

    @Override
    public SocketAddress localAddress() {
        return localAddress;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        NioTransport.closeQuietly(channel);
        try {
            acceptor.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        transport.serverClosed(this);
    }
}
