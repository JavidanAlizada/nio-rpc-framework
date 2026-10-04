package dev.rpc.transport;

import java.net.SocketAddress;

/** A listening socket. Closing it stops new connections; connections already accepted stay open. */
public interface Server extends AutoCloseable {

    /** The bound address, with the real port when port 0 was requested. */
    SocketAddress localAddress();

    @Override
    void close();
}
