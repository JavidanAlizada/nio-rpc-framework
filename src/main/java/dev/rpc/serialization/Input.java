package dev.rpc.serialization;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

/** Strict big-endian reader over one payload: running past the end is a SerializationException. */
final class Input {

    final int maxDepth;
    private final ByteBuffer buf;

    Input(byte[] data, int maxDepth) {
        this.buf = ByteBuffer.wrap(data);
        this.maxDepth = maxDepth;
    }

    byte readByte() {
        need(1);
        return buf.get();
    }

    short readShort() {
        need(2);
        return buf.getShort();
    }

    char readChar() {
        need(2);
        return buf.getChar();
    }

    int readInt() {
        need(4);
        return buf.getInt();
    }

    long readLong() {
        need(8);
        return buf.getLong();
    }

    /**
     * Reads an i32 count and checks it against the bytes left, given that every item takes at least minBytesPerItem.
     * This is what stops a hostile count of 2^31 from allocating before a single item is read.
     */
    int readCount(int minBytesPerItem) {
        int n = readInt();
        if (n < 0) {
            throw new SerializationException("negative length " + n);
        }
        if ((long) n * minBytesPerItem > buf.remaining()) {
            throw new SerializationException("length " + n + " can't fit in the " + buf.remaining()
                    + " bytes left");
        }
        return n;
    }

    byte[] readBytes(int n) {
        need(n);
        byte[] bytes = new byte[n];
        buf.get(bytes);
        return bytes;
    }

    String readString() {
        int length = readCount(1);
        ByteBuffer slice = buf.slice(buf.position(), length);
        buf.position(buf.position() + length);
        try {
            // A fresh CharsetDecoder reports malformed input; new String(bytes, UTF_8) would replace it.
            return StandardCharsets.UTF_8.newDecoder().decode(slice).toString();
        } catch (CharacterCodingException e) {
            throw new SerializationException("string is not valid UTF-8", e);
        }
    }

    /** The 1-byte marker in front of every reference-typed value. */
    boolean readPresence() {
        byte b = readByte();
        if (b == 0) {
            return false;
        }
        if (b == 1) {
            return true;
        }
        throw new SerializationException("invalid presence marker " + b);
    }

    void requireEnd() {
        if (buf.hasRemaining()) {
            throw new SerializationException(buf.remaining() + " unexpected trailing bytes");
        }
    }

    private void need(int n) {
        if (buf.remaining() < n) {
            throw new SerializationException("payload truncated: needed " + n + " more bytes, "
                    + buf.remaining() + " left");
        }
    }
}
