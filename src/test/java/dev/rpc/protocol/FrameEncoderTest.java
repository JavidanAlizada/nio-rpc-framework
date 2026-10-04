package dev.rpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

// Golden bytes: the exact wire format, written out by hand. If one of these fails, the protocol changed.
class FrameEncoderTest {

    private static final HexFormat HEX = HexFormat.of();

    private final FrameEncoder encoder = new FrameEncoder();

    @Test
    void request() {
        var frame = new Request(7, 0x0A0B0C0D, 1_000_000_000L, Map.of("k", "v"), new byte[] {1, 2, 3});
        assertBytes("01 01 00 00 00000007 00000017"
                + " 0A0B0C0D 000000003B9ACA00 0001 0001 6B 0001 76 010203", frame);
    }

    @Test
    void requestHeadersKeepInsertionOrder() {
        var headers = new LinkedHashMap<String, String>();
        headers.put("b", "2");
        headers.put("a", "1");
        var frame = new Request(1, 0, 0, headers, new byte[0]);
        assertBytes("01 01 00 00 00000001 0000001A"
                + " 00000000 0000000000000000 0002 0001 62 0001 32 0001 61 0001 31", frame);
    }

    @Test
    void requestHeaderIsUtf8() {
        var frame = new Request(1, 0, 0, Map.of("é", "€"), new byte[0]);
        assertBytes("01 01 00 00 00000001 00000017"
                + " 00000000 0000000000000000 0001 0002 C3A9 0003 E282AC", frame);
    }

    @Test
    void okResponse() {
        assertBytes("01 02 00 00 00000007 00000002 00 FF", Response.ok(7, new byte[] {(byte) 0xFF}));
    }

    @Test
    void errorResponse() {
        assertBytes("01 02 00 00 00000009 0000000A 01 0001 45 0004 626F6F6D",
                Response.error(9, Status.APPLICATION_ERROR, "E", "boom"));
    }

    @Test
    void unknownStatusCodeIsWrittenAsIs() {
        assertBytes("01 02 00 00 00000001 00000005 C8 0000 0000", Response.error(1, Status.of(200), "", ""));
    }

    @Test
    void cancel() {
        assertBytes("01 03 00 00 00000007 00000000", new Cancel(7));
    }

    @Test
    void ping() {
        assertBytes("01 04 00 00 00000000 00000008 0102030405060708", new Ping(0x0102030405060708L));
    }

    @Test
    void pong() {
        assertBytes("01 05 00 00 00000000 00000008 FFFFFFFFFFFFFFFF", new Pong(-1L));
    }

    @Test
    void largestRequestIdIsUnsigned() {
        assertBytes("01 03 00 00 FFFFFFFF 00000000", new Cancel(-1));
    }

    @Test
    void bodyExactlyAtCapIsAllowed() {
        var small = new FrameEncoder(new ProtocolLimits(4));
        ByteBuffer buf = small.encode(Response.ok(1, new byte[3]));
        assertEquals(FrameFormat.HEADER_SIZE + 4, buf.remaining());
    }

    @Test
    void bodyOverCapThrows() {
        var small = new FrameEncoder(new ProtocolLimits(4));
        var e = assertThrows(FrameTooLargeException.class, () -> small.encode(Response.ok(1, new byte[4])));
        assertEquals(5, e.bodyLength());
        assertEquals(4, e.maxBodySize());
    }

    @Test
    void headersCountTowardsTheCap() {
        var small = new FrameEncoder(new ProtocolLimits(20));
        var frame = new Request(1, 0, 0, Map.of("key", "value"), new byte[0]);
        assertThrows(FrameTooLargeException.class, () -> small.encode(frame));
    }

    @Test
    void stringAtLimitIsAllowed() {
        String max = "x".repeat(FrameFormat.MAX_STRING_BYTES);
        ByteBuffer buf = encoder.encode(Response.error(1, Status.INTERNAL, "E", max));
        assertEquals(FrameFormat.HEADER_SIZE + 5 + 1 + max.length(), buf.remaining());
    }

    @Test
    void stringOverLimitIsRejected() {
        String tooLong = "x".repeat(FrameFormat.MAX_STRING_BYTES + 1);
        var frame = new Request(1, 0, 0, Map.of("k", tooLong), new byte[0]);
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(frame));
    }

    @Test
    void multiByteCharactersCountInBytesNotChars() {
        // 21846 chars of a 3-byte character = 65538 bytes, over the limit despite being under 65535 chars.
        String tooLong = "€".repeat(21_846);
        assertThrows(IllegalArgumentException.class,
                () -> encoder.encode(Response.error(1, Status.INTERNAL, "E", tooLong)));
    }

    @Test
    void loneSurrogateIsRejected() {
        var frame = Response.error(1, Status.INTERNAL, "E", "bad \uD800 surrogate");
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(frame));
    }

    @Test
    void eachCallReturnsAFreshBufferAtPositionZero() {
        var frame = new Cancel(3);
        ByteBuffer first = encoder.encode(frame);
        ByteBuffer second = encoder.encode(frame);
        assertEquals(0, first.position());
        assertEquals(first, second);
        first.put(0, (byte) 9);
        assertEquals(1, second.get(0));
    }

    private void assertBytes(String expectedHex, Frame frame) {
        ByteBuffer buf = encoder.encode(frame);
        byte[] actual = new byte[buf.remaining()];
        buf.get(actual);
        assertEquals(expectedHex.replace(" ", "").toUpperCase(), HEX.withUpperCase().formatHex(actual));
    }
}
