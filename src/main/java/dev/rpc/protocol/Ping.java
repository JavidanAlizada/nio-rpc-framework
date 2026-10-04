package dev.rpc.protocol;

/** Connection-level liveness probe. The peer answers with a Pong carrying the same 8 opaque bytes. */
public record Ping(long payload) implements Frame {

    @Override
    public int requestId() {
        return 0;
    }
}
