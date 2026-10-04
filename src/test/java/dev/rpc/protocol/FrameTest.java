package dev.rpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FrameTest {

    @Test
    void framesCompareByValueIncludingPayloadBytes() {
        var a = new Request(1, 2, 3, Map.of("k", "v"), new byte[] {1, 2});
        var b = new Request(1, 2, 3, Map.of("k", "v"), new byte[] {1, 2});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, new Request(1, 2, 3, Map.of("k", "v"), new byte[] {1, 3}));

        assertEquals(Response.ok(1, new byte[] {5}), Response.ok(1, new byte[] {5}));
        assertEquals(Response.error(1, Status.INTERNAL, "E", "m"), Response.error(1, Status.INTERNAL, "E", "m"));
        assertNotEquals(Response.ok(1, new byte[] {5}), Response.ok(1, new byte[] {6}));
    }

    @Test
    void requestIdZeroIsReservedForConnectionFrames() {
        assertThrows(IllegalArgumentException.class, () -> new Request(0, 1, 0, Map.of(), new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Response.ok(0, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new Cancel(0));
        assertEquals(0, new Ping(1).requestId());
        assertEquals(0, new Pong(1).requestId());
    }

    @Test
    void negativeTimeoutIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Request(1, 1, -1, Map.of(), new byte[0]));
    }

    @Test
    void requestHeadersAreCopiedAndUnmodifiable() {
        var headers = new HashMap<String, String>();
        headers.put("k", "v");
        var request = new Request(1, 1, 0, headers, new byte[0]);
        headers.put("other", "x");
        assertEquals(Map.of("k", "v"), request.headers());
        assertThrows(UnsupportedOperationException.class, () -> request.headers().put("x", "y"));
    }

    @Test
    void nullHeaderKeysAndValuesAreRejected() {
        var nullValue = new HashMap<String, String>();
        nullValue.put("k", null);
        assertThrows(NullPointerException.class, () -> new Request(1, 1, 0, nullValue, new byte[0]));
        var nullKey = new HashMap<String, String>();
        nullKey.put(null, "v");
        assertThrows(NullPointerException.class, () -> new Request(1, 1, 0, nullKey, new byte[0]));
    }

    @Test
    void okResponseHasPayloadAndNoErrorFields() {
        assertThrows(NullPointerException.class, () -> Response.ok(1, null));
        assertThrows(IllegalArgumentException.class, () -> new Response(1, Status.OK, new byte[0], "E", null));
    }

    @Test
    void errorResponseHasErrorFieldsAndNoPayload() {
        assertThrows(NullPointerException.class, () -> Response.error(1, Status.INTERNAL, null, "m"));
        assertThrows(NullPointerException.class, () -> Response.error(1, Status.INTERNAL, "E", null));
        assertThrows(IllegalArgumentException.class,
                () -> new Response(1, Status.INTERNAL, new byte[0], "E", "m"));
    }

    @Test
    void knownStatusCodesResolveToConstants() {
        assertSame(Status.OK, Status.of(0));
        assertSame(Status.INTERNAL, Status.of(7));
        assertTrue(Status.OK.isOk());
        assertEquals("DEADLINE_EXCEEDED", Status.DEADLINE_EXCEEDED.toString());
    }

    @Test
    void unknownStatusCodesAreKept() {
        Status s = Status.of(200);
        assertEquals(200, s.code());
        assertFalse(s.isKnown());
        assertFalse(s.isOk());
        assertEquals("UNKNOWN(200)", s.toString());
        assertEquals(Status.of(200), s);
    }

    @Test
    void statusMustFitInAByte() {
        assertThrows(IllegalArgumentException.class, () -> Status.of(256));
        assertThrows(IllegalArgumentException.class, () -> Status.of(-1));
    }

    @Test
    void limitsAreValidated() {
        assertThrows(IllegalArgumentException.class, () -> new ProtocolLimits(0));
        assertThrows(IllegalArgumentException.class, () -> new ProtocolLimits(Integer.MAX_VALUE));
        assertEquals(4 * 1024 * 1024, ProtocolLimits.defaults().maxBodySize());
        assertEquals(4 * 1024 * 1024 + 12, ProtocolLimits.defaults().maxFrameSize());
    }
}
