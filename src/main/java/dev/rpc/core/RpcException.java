package dev.rpc.core;

import dev.rpc.protocol.Status;
import java.util.Objects;

/**
 * A call that ended with a non-OK status, either because the server said so or because the client refused to send
 * it (OVERLOADED). A lost connection is not one of these: it surfaces as the transport's ConnectionClosedException.
 */
public class RpcException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient Status status;

    public RpcException(Status status, String message) {
        super(Objects.requireNonNull(status, "status") + ": " + message);
        this.status = status;
    }

    public Status status() {
        return status;
    }
}
