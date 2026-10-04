package dev.rpc.protocol;

import static dev.rpc.protocol.FrameFormat.HEADER_SIZE;
import static dev.rpc.protocol.FrameFormat.PING_BYTES;
import static dev.rpc.protocol.FrameFormat.REQUEST_FIXED_BYTES;
import static dev.rpc.protocol.FrameFormat.TYPE_CANCEL;
import static dev.rpc.protocol.FrameFormat.TYPE_PING;
import static dev.rpc.protocol.FrameFormat.TYPE_PONG;
import static dev.rpc.protocol.FrameFormat.TYPE_REQUEST;
import static dev.rpc.protocol.FrameFormat.TYPE_RESPONSE;
import static dev.rpc.protocol.FrameFormat.VERSION;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Turns a byte stream, delivered in chunks of any size, back into frames.
 *
 * Not thread-safe: one decoder per connection, only ever called from that connection's event-loop thread. After a
 * ProtocolException it stays failed, because the stream offset can no longer be trusted.
 */
public final class FrameDecoder {

    private enum State { HEADER, BODY, FAILED }

    private final ProtocolLimits limits;
    private final byte[] header = new byte[HEADER_SIZE];

    private State state = State.HEADER;
    private int headerFilled;

    // The frame whose body is being read. body is null while skipping a frame of unknown type.
    private byte type;
    private int requestId;
    private int bodyLength;
    private byte[] body;
    private int bodyFilled;

    private long skippedFrames;

    public FrameDecoder() {
        this(ProtocolLimits.defaults());
    }

    public FrameDecoder(ProtocolLimits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    /**
     * Consumes all of in and hands every completed frame to out. If out throws, the exception propagates and the
     * rest of in is left unread; the decoder itself stays usable.
     */
    public void decode(ByteBuffer in, Consumer<? super Frame> out) {
        if (state == State.FAILED) {
            throw new ProtocolException("decoder already failed; the connection must be closed");
        }
        try {
            while (in.hasRemaining()) {
                if (state == State.HEADER) {
                    readHeader(in);
                } else {
                    readBody(in);
                }
                if (state == State.BODY && bodyFilled == bodyLength) {
                    finishFrame(out);
                }
            }
        } catch (ProtocolException e) {
            fail();
            throw e;
        }
    }

    /** How many frames of an unknown type were skipped. */
    public long skippedFrames() {
        return skippedFrames;
    }

    private void readHeader(ByteBuffer in) {
        int n = Math.min(HEADER_SIZE - headerFilled, in.remaining());
        in.get(header, headerFilled, n);
        headerFilled += n;
        // Checked as soon as byte 0 arrives, so a client speaking another protocol fails at once rather than
        // after 12 bytes it may never send.
        if (header[0] != VERSION) {
            throw new ProtocolException("unsupported protocol version " + (header[0] & 0xFF));
        }
        if (headerFilled == HEADER_SIZE) {
            startBody();
        }
    }

    private void startBody() {
        ByteBuffer h = ByteBuffer.wrap(header);
        h.get(); // version, already checked
        type = h.get();
        byte flags = h.get();
        h.get(); // reserved: ignored on read
        requestId = h.getInt();
        long length = Integer.toUnsignedLong(h.getInt());

        if (flags != 0) {
            throw new ProtocolException("unknown flags 0x" + Integer.toHexString(flags & 0xFF));
        }
        // Before allocating anything, and for unknown types too: a skipped frame still has to be read.
        if (length > limits.maxBodySize()) {
            throw new ProtocolException("frame body is " + length + " bytes, the limit is " + limits.maxBodySize());
        }
        bodyLength = (int) length;
        boolean known = checkHeader(type, requestId, bodyLength);

        body = known ? new byte[bodyLength] : null;
        bodyFilled = 0;
        headerFilled = 0;
        state = State.BODY;
    }

    // Fails fast on anything the header alone already proves wrong; returns false for an unknown frame type.
    private static boolean checkHeader(byte type, int requestId, int bodyLength) {
        switch (type) {
            case TYPE_REQUEST, TYPE_RESPONSE -> requireCallId(type, requestId);
            case TYPE_CANCEL -> {
                requireCallId(type, requestId);
                requireLength(type, bodyLength, 0);
            }
            case TYPE_PING, TYPE_PONG -> {
                if (requestId != 0) {
                    throw new ProtocolException("frame type " + type + " must use requestId 0, got " + requestId);
                }
                requireLength(type, bodyLength, PING_BYTES);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    private static void requireCallId(byte type, int requestId) {
        if (requestId == 0) {
            throw new ProtocolException("frame type " + type + " needs a nonzero requestId");
        }
    }

    private static void requireLength(byte type, int actual, int expected) {
        if (actual != expected) {
            throw new ProtocolException("frame type " + type + " body must be " + expected + " bytes, got " + actual);
        }
    }

    private void readBody(ByteBuffer in) {
        int n = Math.min(bodyLength - bodyFilled, in.remaining());
        if (body != null) {
            in.get(body, bodyFilled, n);
        } else {
            in.position(in.position() + n);
        }
        bodyFilled += n;
    }

    private void finishFrame(Consumer<? super Frame> out) {
        byte[] completed = body;
        body = null;
        state = State.HEADER;
        if (completed == null) {
            skippedFrames++;
            return;
        }
        // State is reset before out runs, so an exception from out leaves the decoder consistent.
        out.accept(parse(type, requestId, completed));
    }

    private static Frame parse(byte type, int requestId, byte[] body) {
        BodyReader r = new BodyReader(body);
        try {
            return switch (type) {
                case TYPE_REQUEST -> parseRequest(requestId, r);
                case TYPE_RESPONSE -> parseResponse(requestId, r);
                case TYPE_CANCEL -> new Cancel(requestId);
                case TYPE_PING -> new Ping(r.getLong());
                case TYPE_PONG -> new Pong(r.getLong());
                default -> throw new IllegalStateException("unknown type reached parse: " + type);
            };
        } catch (IllegalArgumentException e) {
            // A frame constructor rejected a value the checks above should already have caught.
            throw new ProtocolException("invalid frame: " + e.getMessage(), e);
        }
    }

    private static Request parseRequest(int requestId, BodyReader r) {
        if (r.remaining() < REQUEST_FIXED_BYTES) {
            throw new ProtocolException("request body is too short: " + r.remaining() + " bytes");
        }
        int methodId = r.getInt();
        long timeoutNanos = r.getLong();
        if (timeoutNanos < 0) {
            throw new ProtocolException("negative timeoutNanos: " + timeoutNanos);
        }
        int headerCount = r.getUnsignedShort();
        Map<String, String> headers = new LinkedHashMap<>(headerCount * 2);
        for (int i = 0; i < headerCount; i++) {
            String key = r.getString();
            String value = r.getString();
            if (headers.put(key, value) != null) {
                throw new ProtocolException("duplicate header: " + key);
            }
        }
        return new Request(requestId, methodId, timeoutNanos, headers, r.getRest());
    }

    private static Response parseResponse(int requestId, BodyReader r) {
        Status status = Status.of(r.getUnsignedByte());
        if (status.isOk()) {
            return Response.ok(requestId, r.getRest());
        }
        Response response = Response.error(requestId, status, r.getString(), r.getString());
        r.requireEnd();
        return response;
    }

    private void fail() {
        state = State.FAILED;
        body = null;
    }

    /** Strict reader over one complete body: running past the end is a ProtocolException, not an index error. */
    private static final class BodyReader {

        private final ByteBuffer buf;

        BodyReader(byte[] body) {
            this.buf = ByteBuffer.wrap(body);
        }

        int remaining() {
            return buf.remaining();
        }

        int getInt() {
            need(4);
            return buf.getInt();
        }

        long getLong() {
            need(8);
            return buf.getLong();
        }

        int getUnsignedByte() {
            need(1);
            return buf.get() & 0xFF;
        }

        int getUnsignedShort() {
            need(2);
            return buf.getShort() & 0xFFFF;
        }

        String getString() {
            int length = getUnsignedShort();
            need(length);
            ByteBuffer slice = buf.slice(buf.position(), length);
            buf.position(buf.position() + length);
            try {
                // A fresh CharsetDecoder reports malformed input; new String(bytes, UTF_8) would replace it.
                return StandardCharsets.UTF_8.newDecoder().decode(slice).toString();
            } catch (CharacterCodingException e) {
                throw new ProtocolException("string is not valid UTF-8", e);
            }
        }

        byte[] getRest() {
            byte[] rest = Arrays.copyOfRange(buf.array(), buf.position(), buf.limit());
            buf.position(buf.limit());
            return rest;
        }

        void requireEnd() {
            if (buf.hasRemaining()) {
                throw new ProtocolException(buf.remaining() + " unexpected trailing bytes in frame body");
            }
        }

        private void need(int n) {
            if (buf.remaining() < n) {
                throw new ProtocolException("frame body truncated: needed " + n + " more bytes, "
                        + buf.remaining() + " left");
            }
        }
    }
}
