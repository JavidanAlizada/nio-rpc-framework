package dev.rpc.protocol;

/**
 * Wire constants for protocol version 1, shared by the encoder and the decoder.
 *
 * Header layout (12 bytes, big-endian): version u8, type u8, flags u8, reserved u8, requestId u32, bodyLength u32.
 */
final class FrameFormat {

    static final byte VERSION = 1;
    static final int HEADER_SIZE = 12;

    static final byte TYPE_REQUEST = 1;
    static final byte TYPE_RESPONSE = 2;
    static final byte TYPE_CANCEL = 3;
    static final byte TYPE_PING = 4;
    static final byte TYPE_PONG = 5;

    // Strings and header counts are prefixed with a u16.
    static final int MAX_STRING_BYTES = 0xFFFF;
    static final int MAX_HEADERS = 0xFFFF;

    // methodId i32 + timeoutNanos i64 + headerCount u16
    static final int REQUEST_FIXED_BYTES = 14;
    static final int PING_BYTES = 8;

    private FrameFormat() {
    }
}
