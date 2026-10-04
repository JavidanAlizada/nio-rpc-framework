package dev.rpc.transport;

import dev.rpc.protocol.Frame;
import java.net.SocketAddress;

/**
 * One TCP connection carrying frames. Safe to use from any thread; all real I/O happens on the event loop that owns
 * the connection.
 */
public interface Connection {

    /**
     * Encodes frame on the calling thread and queues it. Frames from one thread go out in the order written, and
     * concurrent writers never interleave inside a frame. Throws ConnectionClosedException once the connection is
     * closing.
     */
    void write(Frame frame);

    /**
     * False while more than the high watermark is queued. New calls should check this and fail fast; responses to
     * calls already running are still written.
     */
    boolean isWritable();

    /** Closes the connection and discards queued writes. Idempotent, safe from any thread. */
    void close();

    SocketAddress remoteAddress();
}
