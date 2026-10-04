package dev.rpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class FrameDecoderTest {

    private static final HexFormat HEX = HexFormat.of();
    private static final FrameEncoder ENCODER = new FrameEncoder();

    private static final List<Frame> SAMPLES = List.of(
            new Request(7, 0x0A0B0C0D, 1_000_000_000L, Map.of("k", "v", "trace-id", "abc"), new byte[] {1, 2, 3}),
            new Request(-1, -1, 0, Map.of(), new byte[0]),
            Response.ok(7, new byte[] {(byte) 0xFF, 0}),
            Response.ok(8, new byte[0]),
            Response.error(9, Status.APPLICATION_ERROR, "java.lang.IllegalStateException", "boom é €"),
            Response.error(10, Status.of(200), "", ""),
            new Cancel(7),
            new Ping(0x0102030405060708L),
            new Pong(-1L));

    // --- round trips ---

    @Test
    void everySampleRoundTrips() {
        for (Frame frame : SAMPLES) {
            assertEquals(List.of(frame), decodeAll(new FrameDecoder(), encode(frame)));
        }
    }

    @Test
    void severalFramesInOneBuffer() {
        assertEquals(SAMPLES, decodeAll(new FrameDecoder(), stream(SAMPLES)));
    }

    @Test
    void bodyExactlyAtCapRoundTrips() {
        var limits = new ProtocolLimits(100);
        var frame = Response.ok(1, new byte[99]);
        byte[] bytes = toArray(new FrameEncoder(limits).encode(frame));
        assertEquals(List.of(frame), decodeAll(new FrameDecoder(limits), bytes));
    }

    @Test
    void maximumHeaderCountRoundTrips() {
        var headers = new LinkedHashMap<String, String>();
        for (int i = 0; i < 0xFFFF; i++) {
            headers.put("h" + i, "");
        }
        var frame = new Request(1, 1, 0, headers, new byte[] {9});
        Frame decoded = decodeAll(new FrameDecoder(), encode(frame)).get(0);
        assertEquals(frame, decoded);
        assertEquals(List.copyOf(headers.keySet()), List.copyOf(((Request) decoded).headers().keySet()));
    }

    @Test
    void inputBufferIsFullyConsumed() {
        ByteBuffer in = ByteBuffer.wrap(stream(SAMPLES));
        new FrameDecoder().decode(in, f -> { });
        assertFalse(in.hasRemaining());
    }

    // --- arbitrary byte splits ---

    @Test
    void everySingleSplitPoint() {
        byte[] bytes = stream(SAMPLES);
        for (int split = 0; split <= bytes.length; split++) {
            var decoder = new FrameDecoder();
            var out = new ArrayList<Frame>();
            decoder.decode(ByteBuffer.wrap(bytes, 0, split), out::add);
            decoder.decode(ByteBuffer.wrap(bytes, split, bytes.length - split), out::add);
            assertEquals(SAMPLES, out, "split at " + split);
        }
    }

    @Test
    void everyPairOfSplitPoints() {
        byte[] bytes = stream(SAMPLES.subList(0, 4));
        for (int a = 0; a <= bytes.length; a++) {
            for (int b = a; b <= bytes.length; b++) {
                var decoder = new FrameDecoder();
                var out = new ArrayList<Frame>();
                decoder.decode(ByteBuffer.wrap(bytes, 0, a), out::add);
                decoder.decode(ByteBuffer.wrap(bytes, a, b - a), out::add);
                decoder.decode(ByteBuffer.wrap(bytes, b, bytes.length - b), out::add);
                assertEquals(SAMPLES.subList(0, 4), out, "splits at " + a + " and " + b);
            }
        }
    }

    @Test
    void oneByteAtATime() {
        byte[] bytes = stream(SAMPLES);
        var decoder = new FrameDecoder();
        var out = new ArrayList<Frame>();
        for (byte b : bytes) {
            decoder.decode(ByteBuffer.wrap(new byte[] {b}), out::add);
        }
        assertEquals(SAMPLES, out);
    }

    @Test
    void randomChunkings() {
        for (long seed = 0; seed < 1_000; seed++) {
            var random = new Random(seed);
            var frames = new ArrayList<Frame>();
            for (int i = 0, n = 1 + random.nextInt(12); i < n; i++) {
                frames.add(SAMPLES.get(random.nextInt(SAMPLES.size())));
            }
            byte[] bytes = stream(frames);
            var decoder = new FrameDecoder();
            var out = new ArrayList<Frame>();
            int pos = 0;
            while (pos < bytes.length) {
                int chunk = Math.min(bytes.length - pos, random.nextInt(40));
                decoder.decode(ByteBuffer.wrap(bytes, pos, chunk), out::add);
                pos += chunk;
            }
            assertEquals(frames, out, "seed " + seed);
        }
    }

    // --- hostile and malformed input ---

    @Test
    void bodyOverCapFailsFromTheHeaderAlone() {
        var decoder = new FrameDecoder(new ProtocolLimits(16));
        // Only the 12 header bytes are supplied, so the failure can't have come from reading a body.
        assertFails(decoder, "01 02 00 00 00000001 00000011");
    }

    @Test
    void lengthWithTopBitSetIsOverTheCap() {
        assertFails(new FrameDecoder(), "01 02 00 00 00000001 80000000");
    }

    @Test
    void wrongVersionFailsOnTheFirstByte() {
        // "G" from an HTTP request line: one byte is enough to reject it.
        assertFails(new FrameDecoder(), "47");
        assertFails(new FrameDecoder(), "02 01 00 00 00000001 00000000");
        assertFails(new FrameDecoder(), "00");
    }

    @Test
    void unknownFlagIsFatal() {
        assertFails(new FrameDecoder(), "01 03 01 00 00000001 00000000");
        assertFails(new FrameDecoder(), "01 03 80 00 00000001 00000000");
    }

    @Test
    void callFramesNeedARequestId() {
        assertFails(new FrameDecoder(), "01 03 00 00 00000000 00000000");
        assertFails(new FrameDecoder(), "01 01 00 00 00000000 0000000E 00000000 0000000000000000 0000");
        assertFails(new FrameDecoder(), "01 02 00 00 00000000 00000001 00");
    }

    @Test
    void connectionFramesMustUseRequestIdZero() {
        assertFails(new FrameDecoder(), "01 04 00 00 00000001 00000008 0000000000000000");
        assertFails(new FrameDecoder(), "01 05 00 00 00000001 00000008 0000000000000000");
    }

    @Test
    void fixedSizeBodiesAreCheckedFromTheHeader() {
        assertFails(new FrameDecoder(), "01 03 00 00 00000001 00000001");
        assertFails(new FrameDecoder(), "01 04 00 00 00000000 00000007");
        assertFails(new FrameDecoder(), "01 05 00 00 00000000 00000009");
    }

    @Test
    void truncatedRequestBodies() {
        // Shorter than the fixed part.
        assertFails(new FrameDecoder(), "01 01 00 00 00000001 00000004 00000001");
        // Says one header, has none.
        assertFails(new FrameDecoder(), "01 01 00 00 00000001 0000000E 00000001 0000000000000000 0001");
        // Header key length runs past the body.
        assertFails(new FrameDecoder(), "01 01 00 00 00000001 00000012 00000001 0000000000000000 0001 0005 6162");
    }

    @Test
    void truncatedErrorResponse() {
        assertFails(new FrameDecoder(), "01 02 00 00 00000001 00000004 01 0001 45");
        assertFails(new FrameDecoder(), "01 02 00 00 00000001 00000000");
    }

    @Test
    void trailingBytesInErrorResponse() {
        assertFails(new FrameDecoder(), "01 02 00 00 00000001 00000007 01 0000 0000 FFFF");
    }

    @Test
    void duplicateHeader() {
        assertFails(new FrameDecoder(),
                "01 01 00 00 00000001 0000001A 00000001 0000000000000000 0002 0001 61 0001 31 0001 61 0001 32");
    }

    @Test
    void negativeTimeout() {
        assertFails(new FrameDecoder(), "01 01 00 00 00000001 0000000E 00000001 FFFFFFFFFFFFFFFF 0000");
    }

    @Test
    void invalidUtf8() {
        // C3 28: a two-byte lead followed by a non-continuation byte.
        assertFails(new FrameDecoder(), "01 02 00 00 00000001 00000007 01 0002 C328 0000");
        // An overlong encoding of '/'.
        assertFails(new FrameDecoder(), "01 02 00 00 00000001 00000007 01 0002 C0AF 0000");
    }

    @Test
    void failedDecoderStaysFailed() {
        var decoder = new FrameDecoder();
        assertThrows(ProtocolException.class, () -> decoder.decode(ByteBuffer.wrap(HEX.parseHex("47")), f -> { }));
        assertThrows(ProtocolException.class, () -> decoder.decode(ByteBuffer.wrap(encode(new Cancel(1))), f -> { }));
    }

    // --- tolerance ---

    @Test
    void unknownFrameTypeIsSkipped() {
        var out = new ByteArrayOutputStream();
        out.writeBytes(encode(new Cancel(1)));
        out.writeBytes(HEX.parseHex("01 09 00 00 00000005 00000003 010203".replace(" ", "")));
        out.writeBytes(encode(new Cancel(2)));
        var decoder = new FrameDecoder();
        assertEquals(List.of(new Cancel(1), new Cancel(2)), decodeAll(decoder, out.toByteArray()));
        assertEquals(1, decoder.skippedFrames());
    }

    @Test
    void unknownFrameTypeIsSkippedAcrossSplits() {
        byte[] unknown = HEX.parseHex("01 7F 00 00 00000000 00000004 DEADBEEF".replace(" ", ""));
        var decoder = new FrameDecoder();
        var out = new ArrayList<Frame>();
        for (byte b : unknown) {
            decoder.decode(ByteBuffer.wrap(new byte[] {b}), out::add);
        }
        decoder.decode(ByteBuffer.wrap(encode(new Ping(5))), out::add);
        assertEquals(List.of(new Ping(5)), out);
        assertEquals(1, decoder.skippedFrames());
    }

    @Test
    void unknownFrameTypeStillRespectsTheCap() {
        assertFails(new FrameDecoder(new ProtocolLimits(16)), "01 7F 00 00 00000000 00000011");
    }

    @Test
    void reservedByteIsIgnored() {
        assertEquals(List.of(new Cancel(1)), decodeAll(new FrameDecoder(), HEX.parseHex("010300FF0000000100000000")));
    }

    @Test
    void unknownStatusCodeIsKept() {
        byte[] bytes = HEX.parseHex("01 02 00 00 00000001 00000005 C8 0000 0000".replace(" ", ""));
        Frame frame = decodeAll(new FrameDecoder(), bytes).get(0);
        assertEquals(Status.of(200), ((Response) frame).status());
        assertFalse(((Response) frame).status().isKnown());
    }

    @Test
    void arbitraryHeadersArePassedThrough() {
        var frame = new Request(1, 1, 0, Map.of("x-future-extension", "anything"), new byte[0]);
        assertEquals(List.of(frame), decodeAll(new FrameDecoder(), encode(frame)));
    }

    @Test
    void exceptionFromConsumerLeavesDecoderUsable() {
        ByteBuffer in = ByteBuffer.wrap(stream(List.of(new Cancel(1), new Cancel(2))));
        var decoder = new FrameDecoder();
        var boom = new IllegalStateException("consumer failed");
        assertThrows(IllegalStateException.class, () -> decoder.decode(in, f -> {
            throw boom;
        }));
        assertTrue(in.hasRemaining());
        var out = new ArrayList<Frame>();
        decoder.decode(in, out::add);
        assertEquals(List.of(new Cancel(2)), out);
    }

    // --- helpers ---

    private static void assertFails(FrameDecoder decoder, String hex) {
        byte[] bytes = HEX.parseHex(hex.replace(" ", ""));
        assertThrows(ProtocolException.class, () -> decoder.decode(ByteBuffer.wrap(bytes), f -> { }), hex);
    }

    private static List<Frame> decodeAll(FrameDecoder decoder, byte[] bytes) {
        var out = new ArrayList<Frame>();
        decoder.decode(ByteBuffer.wrap(bytes), out::add);
        return out;
    }

    private static byte[] encode(Frame frame) {
        return toArray(ENCODER.encode(frame));
    }

    private static byte[] stream(List<Frame> frames) {
        var out = new ByteArrayOutputStream();
        frames.forEach(f -> out.writeBytes(encode(f)));
        return out.toByteArray();
    }

    private static byte[] toArray(ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }
}
