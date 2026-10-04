package dev.rpc.serialization;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Growable big-endian byte sink for one serialize call. */
final class Output {

    // Leaves headroom below Integer.MAX_VALUE, as ArrayList does.
    private static final int MAX_SIZE = Integer.MAX_VALUE - 8;

    final int maxDepth;
    private byte[] buf = new byte[64];
    private int size;

    Output(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    void writeByte(int v) {
        ensure(1);
        buf[size++] = (byte) v;
    }

    void writeShort(int v) {
        ensure(2);
        buf[size++] = (byte) (v >> 8);
        buf[size++] = (byte) v;
    }

    void writeInt(int v) {
        ensure(4);
        for (int shift = 24; shift >= 0; shift -= 8) {
            buf[size++] = (byte) (v >> shift);
        }
    }

    void writeLong(long v) {
        ensure(8);
        for (int shift = 56; shift >= 0; shift -= 8) {
            buf[size++] = (byte) (v >> shift);
        }
    }

    void writeBytes(byte[] bytes) {
        ensure(bytes.length);
        System.arraycopy(bytes, 0, buf, size, bytes.length);
        size += bytes.length;
    }

    /** i32 byte length, then UTF-8. */
    void writeString(String s) {
        ByteBuffer encoded;
        try {
            // A fresh CharsetEncoder reports a lone surrogate; String.getBytes would quietly write '?' instead.
            encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(s));
        } catch (CharacterCodingException e) {
            throw new SerializationException("string can't be encoded as UTF-8 (lone surrogate?)", e);
        }
        int length = encoded.remaining();
        writeInt(length);
        ensure(length);
        encoded.get(buf, size, length);
        size += length;
    }

    byte[] toByteArray() {
        return Arrays.copyOf(buf, size);
    }

    private void ensure(int n) {
        if ((long) size + n > MAX_SIZE) {
            throw new SerializationException("payload would exceed " + MAX_SIZE + " bytes");
        }
        if (size + n > buf.length) {
            buf = Arrays.copyOf(buf, (int) Math.min(MAX_SIZE, Math.max(buf.length * 2L, size + n)));
        }
    }
}
