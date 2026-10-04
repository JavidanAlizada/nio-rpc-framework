package dev.rpc.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.rpc.protocol.ProtocolLimits;
import java.time.Duration;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class TransportConfigTest {

    @Test
    void defaults() {
        var c = TransportConfig.defaults();
        assertEquals(Runtime.getRuntime().availableProcessors(), c.ioThreads());
        assertEquals(1_000, c.maxConnections());
        assertEquals(256 * 1024, c.lowWatermark());
        assertEquals(1024 * 1024, c.highWatermark());
        assertEquals(16 * 1024 * 1024, c.writeQueueLimit());
        assertEquals(Duration.ofSeconds(30), c.idleTimeout());
        assertEquals(Duration.ofSeconds(15), c.pingAfter());
        assertEquals(Duration.ofSeconds(5), c.connectTimeout());
        assertEquals(Duration.ofMillis(100), c.reconnectBaseDelay());
        assertEquals(Duration.ofSeconds(10), c.reconnectMaxDelay());
        assertTrue(c.tcpNoDelay());
        assertEquals(ProtocolLimits.defaults(), c.protocolLimits());
    }

    @Test
    void builderSetsEveryValue() {
        var c = TransportConfig.builder()
                .ioThreads(2)
                .maxConnections(10)
                .watermarks(10, 20, 64 * 1024)
                .idleTimeout(Duration.ofMillis(400))
                .connectTimeout(Duration.ofMillis(50))
                .reconnectDelays(Duration.ofMillis(5), Duration.ofMillis(50))
                .tcpNoDelay(false)
                .protocolLimits(new ProtocolLimits(1024))
                .build();
        assertEquals(2, c.ioThreads());
        assertEquals(10, c.maxConnections());
        assertEquals(10, c.lowWatermark());
        assertEquals(20, c.highWatermark());
        assertEquals(64 * 1024, c.writeQueueLimit());
        assertEquals(Duration.ofMillis(200), c.pingAfter());
        assertEquals(Duration.ofMillis(50), c.connectTimeout());
        assertEquals(Duration.ofMillis(5), c.reconnectBaseDelay());
        assertEquals(Duration.ofMillis(50), c.reconnectMaxDelay());
        assertEquals(false, c.tcpNoDelay());
        assertEquals(1024, c.protocolLimits().maxBodySize());
    }

    @Test
    void countsMustBePositive() {
        rejects(b -> b.ioThreads(0), "ioThreads");
        rejects(b -> b.maxConnections(-1), "maxConnections");
    }

    @Test
    void watermarksMustBeOrdered() {
        rejects(b -> b.watermarks(0, 10, 32 * 1024 * 1024), "lowWatermark");
        rejects(b -> b.watermarks(10, 10, 32 * 1024 * 1024), "lowWatermark < highWatermark");
        rejects(b -> b.watermarks(20, 10, 32 * 1024 * 1024), "lowWatermark < highWatermark");
        rejects(b -> b.watermarks(10, 20, 20), "lowWatermark < highWatermark < writeQueueLimit");
    }

    @Test
    void writeQueueLimitMustHoldTheLargestFrame() {
        // The default 4 MiB body plus a 12-byte header doesn't fit in a 4 MiB queue.
        rejects(b -> b.watermarks(1, 2, 4 * 1024 * 1024), "largest legal frame");
        TransportConfig.builder()
                .protocolLimits(new ProtocolLimits(100))
                .watermarks(1, 2, 112)
                .build();
    }

    @Test
    void durationsMustBePositive() {
        rejects(b -> b.idleTimeout(Duration.ZERO), "idleTimeout");
        rejects(b -> b.connectTimeout(Duration.ofMillis(-1)), "connectTimeout");
        rejects(b -> b.reconnectDelays(Duration.ZERO, Duration.ofSeconds(1)), "reconnectBaseDelay");
        assertThrows(NullPointerException.class, () -> TransportConfig.builder().idleTimeout(null).build());
    }

    @Test
    void reconnectBaseMustNotExceedMax() {
        rejects(b -> b.reconnectDelays(Duration.ofSeconds(2), Duration.ofSeconds(1)), "larger than reconnectMaxDelay");
    }

    private static void rejects(UnaryOperator<TransportConfig.Builder> change, String messagePart) {
        var e = assertThrows(IllegalArgumentException.class, () -> change.apply(TransportConfig.builder()).build());
        assertTrue(e.getMessage().contains(messagePart), e.getMessage());
    }
}
