package dev.rpc.protocol;

/** Asks the server to stop working on a call. It may arrive after the response was already sent. */
public record Cancel(int requestId) implements Frame {

    public Cancel {
        if (requestId == 0) {
            throw new IllegalArgumentException("requestId 0 is reserved for connection-level frames");
        }
    }
}
