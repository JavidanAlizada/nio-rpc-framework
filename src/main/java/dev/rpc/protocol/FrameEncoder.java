package dev.rpc.protocol;

import static dev.rpc.protocol.FrameFormat.HEADER_SIZE;
import static dev.rpc.protocol.FrameFormat.MAX_STRING_BYTES;
import static dev.rpc.protocol.FrameFormat.PING_BYTES;
import static dev.rpc.protocol.FrameFormat.REQUEST_FIXED_BYTES;
import static dev.rpc.protocol.FrameFormat.TYPE_CANCEL;
import static dev.rpc.protocol.FrameFormat.TYPE_PING;
import static dev.rpc.protocol.FrameFormat.TYPE_PONG;
import static dev.rpc.protocol.FrameFormat.TYPE_REQUEST;
import static dev.rpc.protocol.FrameFormat.TYPE_RESPONSE;
import static dev.rpc.protocol.FrameFormat.VERSION;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * Turns frames into bytes. Stateless and thread-safe: every call returns a new buffer holding exactly one frame.
 */
public final class FrameEncoder {

    private final ProtocolLimits limits;

    public FrameEncoder() {
        this(ProtocolLimits.defaults());
    }

    public FrameEncoder(ProtocolLimits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    /** Encodes one frame into a new buffer positioned at 0, ready to write. */
    public ByteBuffer encode(Frame frame) {
        Objects.requireNonNull(frame, "frame");
        return switch (frame) {
            case Request r -> encodeRequest(r);
            case Response r -> encodeResponse(r);
            case Cancel c -> start(TYPE_CANCEL, c.requestId(), 0).flip();
            case Ping p -> start(TYPE_PING, 0, PING_BYTES).putLong(p.payload()).flip();
            case Pong p -> start(TYPE_PONG, 0, PING_BYTES).putLong(p.payload()).flip();
        };
    }

    private ByteBuffer encodeRequest(Request r) {
        // Strings are encoded up front because their byte lengths decide the body length.
        byte[][] headerBytes = new byte[r.headers().size() * 2][];
        long bodyLength = REQUEST_FIXED_BYTES + (long) r.payload().length;
        int i = 0;
        for (Map.Entry<String, String> e : r.headers().entrySet()) {
            headerBytes[i] = utf8(e.getKey());
            headerBytes[i + 1] = utf8(e.getValue());
            bodyLength += 4L + headerBytes[i].length + headerBytes[i + 1].length;
            i += 2;
        }
        ByteBuffer buf = start(TYPE_REQUEST, r.requestId(), bodyLength)
                .putInt(r.methodId())
                .putLong(r.timeoutNanos())
                .putShort((short) r.headers().size());
        for (byte[] s : headerBytes) {
            putString(buf, s);
        }
        return buf.put(r.payload()).flip();
    }

    private ByteBuffer encodeResponse(Response r) {
        byte status = (byte) r.status().code();
        if (r.status().isOk()) {
            return start(TYPE_RESPONSE, r.requestId(), 1L + r.payload().length)
                    .put(status)
                    .put(r.payload())
                    .flip();
        }
        byte[] errorType = utf8(r.errorType());
        byte[] message = utf8(r.message());
        ByteBuffer buf = start(TYPE_RESPONSE, r.requestId(), 5L + errorType.length + message.length).put(status);
        putString(buf, errorType);
        putString(buf, message);
        return buf.flip();
    }

    private ByteBuffer start(byte type, int requestId, long bodyLength) {
        if (bodyLength > limits.maxBodySize()) {
            throw new FrameTooLargeException(bodyLength, limits.maxBodySize());
        }
        return ByteBuffer.allocate(HEADER_SIZE + (int) bodyLength)
                .put(VERSION)
                .put(type)
                .put((byte) 0) // flags: none defined in version 1
                .put((byte) 0) // reserved
                .putInt(requestId)
                .putInt((int) bodyLength);
    }

    private static void putString(ByteBuffer buf, byte[] utf8) {
        buf.putShort((short) utf8.length).put(utf8);
    }

    private static byte[] utf8(String s) {
        ByteBuffer encoded;
        try {
            // A fresh CharsetEncoder reports a lone surrogate; String.getBytes would quietly write '?' instead.
            encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(s));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("string can't be encoded as UTF-8 (lone surrogate?)", e);
        }
        if (encoded.remaining() > MAX_STRING_BYTES) {
            throw new IllegalArgumentException(
                    "string is " + encoded.remaining() + " bytes in UTF-8, the limit is " + MAX_STRING_BYTES);
        }
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        return bytes;
    }
}
