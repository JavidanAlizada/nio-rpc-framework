package dev.rpc.protocol;

/**
 * One protocol frame. The set of frame types is closed, so encoder and decoder switch over it exhaustively.
 *
 * Frames that carry a byte array take ownership of it: nothing copies the array, and nobody may change it after
 * the frame is built. Final-field semantics then make its contents visible to any thread that sees the frame.
 */
public sealed interface Frame permits Request, Response, Cancel, Ping, Pong {

    /** The call this frame belongs to, or 0 for connection-level frames. */
    int requestId();
}
