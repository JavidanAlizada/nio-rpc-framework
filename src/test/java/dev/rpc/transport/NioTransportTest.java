package dev.rpc.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import dev.rpc.protocol.Frame;
import dev.rpc.protocol.FrameDecoder;
import dev.rpc.protocol.FrameEncoder;
import dev.rpc.protocol.Ping;
import dev.rpc.protocol.Pong;
import dev.rpc.protocol.ProtocolException;
import dev.rpc.protocol.ProtocolLimits;
import dev.rpc.protocol.Request;
import dev.rpc.protocol.Response;
import java.io.EOFException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NioTransportTest {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final InetSocketAddress ANY_PORT = new InetSocketAddress("127.0.0.1", 0);

    private final List<NioTransport> transports = new ArrayList<>();

    @AfterEach
    void closeAll() {
        transports.forEach(NioTransport::close);
    }

    private NioTransport transport(TransportConfig config) {
        var t = new NioTransport(config);
        transports.add(t);
        return t;
    }

    private NioTransport transport() {
        return transport(TransportConfig.builder().ioThreads(2).build());
    }

    /** A server that answers every Request with an OK Response carrying the same payload. */
    private static RecordingHandler echo() {
        return new RecordingHandler((c, f) -> {
            if (f instanceof Request r) {
                c.write(Response.ok(r.requestId(), r.payload()));
            }
        });
    }

    private static Request request(int id, byte[] payload) {
        return new Request(id, 1, 0, Map.of(), payload);
    }

    private static Connection connect(NioTransport t, Server server, ConnectionHandler h) throws Exception {
        return t.connect(server.localAddress(), h).get(WAIT.toSeconds(), TimeUnit.SECONDS);
    }

    // --- echo ---

    @Test
    void echoKeepsOrderOnOneConnection() throws Exception {
        NioTransport t = transport();
        Server server = t.bind(ANY_PORT, echo());
        assertNotEquals(0, ((InetSocketAddress) server.localAddress()).getPort());

        var client = new RecordingHandler();
        Connection c = connect(t, server, client);
        int n = 1_000;
        for (int i = 1; i <= n; i++) {
            c.write(request(i, ("payload " + i).getBytes(StandardCharsets.UTF_8)));
        }
        for (int i = 1; i <= n; i++) {
            Response r = (Response) client.take(WAIT);
            assertEquals(i, r.requestId());
            assertEquals("payload " + i, new String(r.payload(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void framesNearTheCapAreWrittenAcrossManyPartialWrites() throws Exception {
        NioTransport t = transport();
        Server server = t.bind(ANY_PORT, echo());
        var client = new RecordingHandler();
        Connection c = connect(t, server, client);

        byte[] big = new byte[ProtocolLimits.DEFAULT_MAX_BODY_SIZE - 100];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 31 + (i >>> 8)); // a pattern that would expose shifted or dropped bytes
        }
        for (int i = 1; i <= 3; i++) {
            c.write(request(i, big));
        }
        for (int i = 1; i <= 3; i++) {
            Response r = (Response) client.take(WAIT);
            assertEquals(i, r.requestId());
            assertArrayEquals(big, r.payload());
        }
    }

    @Test
    void manyConnectionsAcrossAllLoops() throws Exception {
        NioTransport t = transport(TransportConfig.builder().ioThreads(4).build());
        Server server = t.bind(ANY_PORT, echo());
        int connections = 200;
        List<RecordingHandler> handlers = new ArrayList<>();
        List<CompletableFuture<Connection>> futures = new ArrayList<>();
        // Batches of 50 keep the burst under the kernel's accept queue on any OS (macOS caps it at 128); this test
        // is about 200 live connections spread over the loops, not about surviving a connect storm.
        for (int i = 0; i < connections; i++) {
            var h = new RecordingHandler();
            handlers.add(h);
            futures.add(t.connect(server.localAddress(), h));
            if (futures.size() % 50 == 0) {
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                        .get(WAIT.toSeconds(), TimeUnit.SECONDS);
            }
        }
        for (int i = 0; i < connections; i++) {
            Connection c = futures.get(i).get(WAIT.toSeconds(), TimeUnit.SECONDS);
            for (int j = 1; j <= 10; j++) {
                c.write(request(j, new byte[] {(byte) i, (byte) j}));
            }
        }
        for (int i = 0; i < connections; i++) {
            for (int j = 1; j <= 10; j++) {
                Response r = (Response) handlers.get(i).take(WAIT);
                assertEquals(j, r.requestId());
                assertArrayEquals(new byte[] {(byte) i, (byte) j}, r.payload());
            }
        }
    }

    // --- PING / PONG ---

    @Test
    void pingIsAnsweredByTheTransportAndNeverReachesTheHandler() throws Exception {
        NioTransport t = transport();
        var serverHandler = new RecordingHandler();
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket raw = rawSocket(server)) {
            raw.getOutputStream().write(bytes(new Ping(0x1122334455667788L)));
            Frame reply = readFrame(raw);
            assertEquals(new Pong(0x1122334455667788L), reply);
        }
        assertTrue(serverHandler.frames.isEmpty());
    }

    // --- closing ---

    @Test
    void peerEofClosesTheConnectionOnce() throws Exception {
        NioTransport t = transport();
        var serverHandler = new RecordingHandler();
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket raw = rawSocket(server)) {
            raw.getOutputStream().write(bytes(request(1, new byte[0])));
            serverHandler.take(WAIT);
        }
        assertInstanceOf(EOFException.class, serverHandler.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS));
        assertEquals(1, serverHandler.closeCount.get());
    }

    @Test
    void garbageClosesTheConnectionWithAProtocolError() throws Exception {
        NioTransport t = transport();
        var serverHandler = new RecordingHandler();
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket raw = rawSocket(server)) {
            raw.getOutputStream().write("GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            // The server closes its end, so our read sees end of stream.
            assertEquals(-1, raw.getInputStream().read());
        }
        // No frame was ever delivered, so the handler never saw this connection open; the close still reports.
        Throwable cause = serverHandler.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS);
        assertInstanceOf(ProtocolException.class, cause);
    }

    @Test
    void localCloseReportsANullCauseAndLaterWritesFail() throws Exception {
        NioTransport t = transport();
        Server server = t.bind(ANY_PORT, echo());
        var client = new RecordingHandler();
        Connection c = connect(t, server, client);
        c.close();
        c.close();
        assertSame(RecordingHandler.LOCAL_CLOSE, client.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS));
        assertThrows(ConnectionClosedException.class, () -> c.write(request(1, new byte[0])));
        assertEquals(1, client.closeCount.get());
    }

    @Test
    void connectionsOverTheLimitAreClosedAtOnce() throws Exception {
        NioTransport t = transport(TransportConfig.builder().ioThreads(1).maxConnections(2).build());
        var serverHandler = new RecordingHandler(echo()::onFrame);
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket a = rawSocket(server); Socket b = rawSocket(server)) {
            // Make sure both are fully adopted before the third arrives.
            roundTrip(a);
            roundTrip(b);
            try (Socket extra = rawSocket(server)) {
                assertEquals(-1, extra.getInputStream().read(), "the extra connection should be closed");
            }
            roundTrip(a);
            roundTrip(b);
        }
    }

    // --- connecting ---

    @Test
    void connectionRefusedFailsTheFuture() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        NioTransport t = transport();
        var future = t.connect(new InetSocketAddress("127.0.0.1", port), new RecordingHandler());
        var e = assertThrows(ExecutionException.class, () -> future.get(WAIT.toSeconds(), TimeUnit.SECONDS));
        assertInstanceOf(ConnectException.class, e.getCause());
    }

    @Test
    void connectTimeoutFailsTheFuture() throws Exception {
        NioTransport t = transport(TransportConfig.builder()
                .ioThreads(1)
                .connectTimeout(Duration.ofMillis(300))
                .build());
        // 192.0.2.1 is TEST-NET-1: never routed, so the SYN usually goes unanswered. Some networks reject it at once
        // instead; then this test can't observe a timeout and is skipped rather than failing.
        var future = t.connect(new InetSocketAddress("192.0.2.1", 9), new RecordingHandler());
        var e = assertThrows(ExecutionException.class, () -> future.get(WAIT.toSeconds(), TimeUnit.SECONDS));
        String message = String.valueOf(e.getCause().getMessage());
        assumeTrue(message.contains("timed out"), "network rejected TEST-NET at once: " + e.getCause());
        assertInstanceOf(ConnectException.class, e.getCause());
    }

    @Test
    void connectFutureCompletesOffTheEventLoop() throws Exception {
        NioTransport t = transport();
        Server server = t.bind(ANY_PORT, echo());
        var threadName = t.connect(server.localAddress(), new RecordingHandler())
                .thenApply(c -> Thread.currentThread().getName())
                .get(WAIT.toSeconds(), TimeUnit.SECONDS);
        assertTrue(!threadName.contains("-loop-"), "ran on " + threadName);
    }

    // --- shutdown ---

    @Test
    void transportCloseClosesEveryConnectionOnce() throws Exception {
        NioTransport t = transport();
        var serverHandler = new RecordingHandler(echo()::onFrame);
        Server server = t.bind(ANY_PORT, serverHandler);
        var client = new RecordingHandler();
        Connection c = connect(t, server, client);
        c.write(request(1, new byte[0]));
        client.take(WAIT);

        t.close();
        assertSame(RecordingHandler.LOCAL_CLOSE, client.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS));
        serverHandler.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS);
        assertEquals(1, client.closeCount.get());
        assertEquals(1, serverHandler.closeCount.get());
        assertThrows(IllegalStateException.class, () -> t.connect(server.localAddress(), new RecordingHandler()));
    }

    // --- helpers ---

    private static Socket rawSocket(Server server) throws Exception {
        var s = new Socket();
        s.connect(server.localAddress(), 5_000);
        s.setSoTimeout(10_000);
        return s;
    }

    private static void roundTrip(Socket raw) throws Exception {
        raw.getOutputStream().write(bytes(request(7, new byte[] {7})));
        Response r = (Response) readFrame(raw);
        assertEquals(7, r.requestId());
    }

    private static byte[] bytes(Frame frame) {
        ByteBuffer buf = new FrameEncoder().encode(frame);
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }

    private static Frame readFrame(Socket raw) throws Exception {
        InputStream in = raw.getInputStream();
        var decoder = new FrameDecoder();
        var result = new CompletableFuture<Frame>();
        byte[] chunk = new byte[256];
        while (!result.isDone()) {
            int n = in.read(chunk);
            if (n < 0) {
                throw new EOFException("socket closed before a whole frame arrived");
            }
            decoder.decode(ByteBuffer.wrap(chunk, 0, n), result::complete);
        }
        return result.get();
    }
}
