package dev.rpc.transport;

import dev.rpc.protocol.Frame;

/**
 * Receives a connection's events. Every method runs on the connection's event-loop thread, so implementations must
 * not block: hand the work to another thread instead. An exception thrown here closes that connection.
 *
 * PING and PONG are handled by the transport and never reach onFrame.
 */
public interface ConnectionHandler {

    void onFrame(Connection connection, Frame frame);

    /** The connection crossed a watermark: false above the high one, true again below the low one. */
    default void onWritabilityChanged(Connection connection, boolean writable) {
    }

    /** Called exactly once per connection. cause is null when the connection was closed locally. */
    void onClosed(Connection connection, Throwable cause);
}
