package dev.rpc.transport;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Opens connections that carry frames. The NIO implementation is the only one; the interface exists so another
 * transport (a Netty one, as a comparison) could be swapped in without touching the RPC layer.
 */
public interface Transport extends AutoCloseable {

    /** Starts accepting connections on address; every accepted connection reports to handler. */
    Server bind(SocketAddress address, ConnectionHandler handler);

    /** Connects to address. The future completes once the connection is open, or fails if it can't be. */
    CompletableFuture<Connection> connect(SocketAddress address, ConnectionHandler handler);

    /** Closes every server and connection, then stops all transport threads. */
    @Override
    void close();
}
