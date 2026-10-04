package dev.rpc.protocol;

/**
 * The peer sent bytes that aren't a valid version 1 frame stream. Connection-fatal: once a header can't be trusted
 * there's no way to find the next frame boundary, so the only recovery is to close the connection.
 */
public final class ProtocolException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
