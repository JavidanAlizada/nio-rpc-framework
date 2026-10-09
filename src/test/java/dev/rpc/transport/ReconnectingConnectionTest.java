package dev.rpc.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.rpc.protocol.Request;
import dev.rpc.protocol.Response;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ReconnectingConnectionTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = resources.size() - 1; i >= 0; i--) {
            resources.get(i).close();
        }
    }

    private <T extends AutoCloseable> T track(T resource) {
        resources.add(resource);
        return resource;
    }

    /** Short delays so the tests spend little time backing off. */
    private NioTransport client() {
        return track(new NioTransport(TransportConfig.builder()
                .ioThreads(1)
                .reconnectDelays(Duration.ofMillis(20), Duration.ofMillis(200))
                .build()));
    }

    /** A server transport of its own, so closing it drops the client's connection as a real server crash would. */
    private NioTransport serverAt(InetSocketAddress address) {
        NioTransport t = track(new NioTransport(TransportConfig.builder().ioThreads(1).build()));
        t.bind(address, new RecordingHandler((c, f) -> {
            if (f instanceof Request r) {
                c.write(Response.ok(r.requestId(), r.payload()));
            }
        }));
        return t;
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(5);
        }
    }

    private static Request request(int id) {
        return new Request(id, 1, 0, Map.of(), new byte[] {(byte) id});
    }

    @Test
    void connectsAndCarriesFrames() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", freePort());
        serverAt(address);
        var handler = new RecordingHandler();
        ReconnectingConnection c = client().reconnecting(address, handler);

        await("CONNECTED", () -> c.state() == ReconnectingConnection.State.CONNECTED);
        assertTrue(c.isWritable());
        assertEquals(address, c.remoteAddress());
        c.write(request(1));
        assertEquals(1, ((Response) handler.take(WAIT)).requestId());
    }

    @Test
    void serverStopBacksOffAndServerRestartReconnectsOnTheSamePort() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", freePort());
        NioTransport server = serverAt(address);
        var handler = new RecordingHandler();
        ReconnectingConnection c = client().reconnecting(address, handler);
        await("CONNECTED", () -> c.state() == ReconnectingConnection.State.CONNECTED);

        server.close();
        // The first close reports the server's EOF; the state then cycles between BACKING_OFF and CONNECTING.
        assertNotSame(RecordingHandler.LOCAL_CLOSE, handler.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS));
        await("BACKING_OFF", () -> c.state() == ReconnectingConnection.State.BACKING_OFF);
        // Let a few refused attempts go by.
        Thread.sleep(300);
        assertNotEquals(ReconnectingConnection.State.CONNECTED, c.state());

        serverAt(address);
        await("CONNECTED again", () -> c.state() == ReconnectingConnection.State.CONNECTED);
        c.write(request(2));
        assertEquals(2, ((Response) handler.take(WAIT)).requestId());
        assertEquals(1, handler.closeCount.get());
    }

    @Test
    void writesFailFastWhileDisconnected() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", freePort());
        ReconnectingConnection c = client().reconnecting(address, new RecordingHandler());

        await("BACKING_OFF", () -> c.state() == ReconnectingConnection.State.BACKING_OFF);
        assertFalse(c.isWritable());
        long start = System.nanoTime();
        Request r = request(1);
        for (int i = 0; i < 100; i++) {
            assertThrows(ConnectionClosedException.class, () -> c.write(r));
        }
        // Fail fast means no waiting for a connection: 100 rejected writes take nowhere near one backoff.
        assertTrue(System.nanoTime() - start < Duration.ofSeconds(1).toNanos());
    }

    @Test
    void framesWrittenWhileDisconnectedAreNeverDelivered() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", freePort());
        var serverSide = new RecordingHandler();
        NioTransport server = track(new NioTransport(TransportConfig.builder().ioThreads(1).build()));
        ReconnectingConnection c = client().reconnecting(address, new RecordingHandler());
        await("BACKING_OFF", () -> c.state() == ReconnectingConnection.State.BACKING_OFF);
        assertThrows(ConnectionClosedException.class, () -> c.write(request(1)));

        server.bind(address, serverSide);
        await("CONNECTED", () -> c.state() == ReconnectingConnection.State.CONNECTED);
        c.write(request(2));
        assertEquals(2, ((Request) serverSide.take(WAIT)).requestId());
        assertTrue(serverSide.frames.isEmpty());
    }

    @Test
    void closeStopsReconnectingAndIsIdempotent() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", freePort());
        NioTransport server = serverAt(address);
        var handler = new RecordingHandler();
        ReconnectingConnection c = client().reconnecting(address, handler);
        await("CONNECTED", () -> c.state() == ReconnectingConnection.State.CONNECTED);

        c.close();
        c.close();
        assertEquals(ReconnectingConnection.State.CLOSED, c.state());
        assertSame(RecordingHandler.LOCAL_CLOSE, handler.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS));
        assertThrows(ConnectionClosedException.class, () -> c.write(request(1)));

        // Still CLOSED well after the longest backoff: no attempt was started behind our back.
        Thread.sleep(400);
        assertEquals(ReconnectingConnection.State.CLOSED, c.state());
        assertEquals(1, handler.closeCount.get());
        server.close();
    }

    @Test
    void closeWhileBackingOffCancelsTheRetry() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", freePort());
        ReconnectingConnection c = client().reconnecting(address, new RecordingHandler());
        await("BACKING_OFF", () -> c.state() == ReconnectingConnection.State.BACKING_OFF);

        c.close();
        serverAt(address);
        Thread.sleep(400);
        assertEquals(ReconnectingConnection.State.CLOSED, c.state());
    }

    @Test
    void transportCloseClosesItsReconnectingConnections() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", freePort());
        serverAt(address);
        NioTransport t = client();
        ReconnectingConnection connected = t.reconnecting(address, new RecordingHandler());
        ReconnectingConnection backingOff = t.reconnecting(
                new InetSocketAddress("127.0.0.1", freePort()), new RecordingHandler());
        await("CONNECTED", () -> connected.state() == ReconnectingConnection.State.CONNECTED);
        await("BACKING_OFF", () -> backingOff.state() == ReconnectingConnection.State.BACKING_OFF);

        t.close();
        assertEquals(ReconnectingConnection.State.CLOSED, connected.state());
        assertEquals(ReconnectingConnection.State.CLOSED, backingOff.state());
        assertThrows(IllegalStateException.class, () -> t.reconnecting(address, new RecordingHandler()));
    }
}
