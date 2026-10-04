package dev.rpc.transport;

/** Thrown when writing to a connection that is closing or closed. */
public final class ConnectionClosedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ConnectionClosedException(String message) {
        super(message);
    }
}
