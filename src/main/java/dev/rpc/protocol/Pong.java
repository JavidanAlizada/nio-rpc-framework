package dev.rpc.protocol;

/** Answer to a Ping, echoing its payload. */
public record Pong(long payload) implements Frame {

    @Override
    public int requestId() {
        return 0;
    }
}
