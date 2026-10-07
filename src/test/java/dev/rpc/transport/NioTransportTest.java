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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
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

    // --- idle detection ---

    private static final Duration IDLE = Duration.ofMillis(400);

    private NioTransport idleTransport() {
        return transport(TransportConfig.builder().ioThreads(1).idleTimeout(IDLE).build());
    }

    private static void assertIdleTimeout(RecordingHandler handler) throws Exception {
        Throwable cause = handler.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS);
        assertInstanceOf(SocketTimeoutException.class, cause);
        assertTrue(cause.getMessage().startsWith("idle timeout"), cause.getMessage());
        assertEquals(1, handler.closeCount.get());
    }

    @Test
    void aSilentClientIsPingedThenClosed() throws Exception {
        NioTransport t = idleTransport();
        var serverHandler = new RecordingHandler();
        Server server = t.bind(ANY_PORT, serverHandler);
        long start = System.nanoTime();
        try (Socket raw = rawSocket(server)) {
            // The PING carries the server's nanoTime; same JVM, so it shows the server waited before pinging.
            Ping ping = (Ping) readFrame(raw);
            assertTrue(ping.payload() - start >= IDLE.dividedBy(2).toNanos(), "pinged too early");
            assertEquals(-1, raw.getInputStream().read());
            assertTrue(System.nanoTime() - start >= IDLE.toNanos(), "closed too early");
        }
        assertIdleTimeout(serverHandler);
    }

    @Test
    void aSilentServerIsPingedThenClosed() throws Exception {
        NioTransport t = idleTransport();
        try (ServerSocket rawServer = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            var client = new RecordingHandler();
            t.connect(rawServer.getLocalSocketAddress(), client).get(WAIT.toSeconds(), TimeUnit.SECONDS);
            try (Socket raw = rawServer.accept()) {
                raw.setSoTimeout(10_000);
                assertInstanceOf(Ping.class, readFrame(raw));
                assertEquals(-1, raw.getInputStream().read());
            }
            assertIdleTimeout(client);
        }
    }

    @Test
    void aPeerThatAnswersPingsStaysOpen() throws Exception {
        NioTransport t = idleTransport();
        var serverHandler = new RecordingHandler();
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket raw = rawSocket(server)) {
            // Five pings at least idle/2 apart span 2.5 idle timeouts; without the PONGs it'd be closed after one.
            for (int i = 0; i < 5; i++) {
                Ping ping = (Ping) readFrame(raw);
                raw.getOutputStream().write(bytes(new Pong(ping.payload())));
            }
            assertTrue(!serverHandler.closed.isDone(), "closed a peer that answered every PING");
        }
    }

    @Test
    void aPeerThatKeepsSendingIsNeverPinged() throws Exception {
        NioTransport t = idleTransport();
        var serverHandler = new RecordingHandler();
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket raw = rawSocket(server)) {
            long lastSend = 0;
            // Paced at a quarter of the ping interval, for three idle timeouts. The sleep is the traffic pattern
            // under test, not a wait for something to happen.
            for (int i = 1; i <= 24; i++) {
                lastSend = System.nanoTime();
                raw.getOutputStream().write(bytes(request(i, new byte[0])));
                Thread.sleep(IDLE.toMillis() / 8);
            }
            // The handler doesn't reply, so the first frame back must be a PING sent only once we went quiet.
            Ping ping = (Ping) readFrame(raw);
            assertTrue(ping.payload() - lastSend >= IDLE.dividedBy(2).toNanos(), "pinged while the peer was sending");
        }
        assertEquals(24, serverHandler.frames.size());
    }

    // --- writing and backpressure ---

    @Test
    void concurrentWritersNeverInterleaveInsideAFrame() throws Exception {
        NioTransport t = transport();
        Server server = t.bind(ANY_PORT, echo());
        var client = new RecordingHandler();
        Connection c = connect(t, server, client);

        int writers = 64;
        int perWriter = 200;
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            int writer = w;
            threads.add(Thread.ofVirtual().start(() -> {
                for (int seq = 0; seq < perWriter; seq++) {
                    // requestId 0 is reserved, hence writer + 1.
                    c.write(request((writer + 1) * 1_000 + seq, payloadFor(writer, seq)));
                }
            }));
        }
        for (Thread thread : threads) {
            thread.join(WAIT.toMillis());
        }

        int[] nextSeq = new int[writers];
        for (int i = 0; i < writers * perWriter; i++) {
            Response r = (Response) client.take(WAIT);
            int writer = r.requestId() / 1_000 - 1;
            int seq = r.requestId() % 1_000;
            assertEquals(nextSeq[writer]++, seq, "writer " + writer + "'s frames out of order");
            assertArrayEquals(payloadFor(writer, seq), r.payload(), "frame " + r.requestId() + " corrupted");
        }
    }

    @Test
    void aPeerWithATinyReceiveWindowGetsEveryByte() throws Exception {
        NioTransport t = transport();
        Server server = t.bind(ANY_PORT, echo());
        // A 4 KiB receive window lets the server's socket take only a sliver of each 1 MiB frame per write, so
        // every frame goes out through many partial writes resumed via OP_WRITE.
        try (Socket raw = new Socket()) {
            raw.setReceiveBufferSize(4 * 1024);
            raw.connect(server.localAddress(), 5_000);
            raw.setSoTimeout(10_000);
            byte[] big = payloadFor(7, 1 << 20);
            // Sent from another thread: the server stops reading while its replies back up, so a peer that
            // wrote everything before reading would block forever, which is backpressure doing its job.
            var sender = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 1; i <= 3; i++) {
                        raw.getOutputStream().write(bytes(request(i, big)));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            List<Frame> replies = readFrames(raw, 3);
            sender.join(WAIT.toMillis());
            for (int i = 1; i <= 3; i++) {
                Response r = (Response) replies.get(i - 1);
                assertEquals(i, r.requestId());
                assertArrayEquals(big, r.payload());
            }
        }
    }

    @Test
    void aPeerThatStopsReadingStopsBeingReadFromUntilItCatchesUp() throws Exception {
        NioTransport t = transport(TransportConfig.builder()
                .ioThreads(1)
                .watermarks(16 * 1024, 64 * 1024, 16 * 1024 * 1024)
                .build());
        var serverHandler = new RecordingHandler(echo()::onFrame);
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket raw = new Socket()) {
            raw.setReceiveBufferSize(16 * 1024);
            raw.connect(server.localAddress(), 5_000);
            raw.setSoTimeout(10_000);

            // 32 MiB of requests: far more than the kernel buffers on both sides can hold, so this thread blocks
            // once the server stops reading, and finishes only after the reader below catches up.
            int requests = 4_000;
            byte[] payload = new byte[8 * 1024];
            var sender = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 1; i <= requests; i++) {
                        raw.getOutputStream().write(bytes(request(i, payload)));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            assertEquals(false, serverHandler.nextWritability(WAIT));
            Connection serverSide = serverHandler.firstConnection.get();
            assertTrue(!serverSide.isWritable());
            // Reading is paused: once the read pass in flight is over, no more requests reach the handler.
            Thread.sleep(200);
            int seen = serverHandler.frames.size();
            Thread.sleep(500);
            assertEquals(seen, serverHandler.frames.size(), "server kept reading from a peer that doesn't read");
            assertTrue(seen < requests);

            // Now the peer reads: the server drains below the low watermark and reads again, so every request
            // is eventually answered.
            List<Frame> replies = readFrames(raw, requests);
            sender.join(WAIT.toMillis());
            assertEquals(requests, ((Response) replies.get(requests - 1)).requestId());
            assertEquals(true, serverHandler.nextWritability(WAIT));
            for (String thread : serverHandler.writabilityThreads) {
                assertTrue(thread.contains("-loop-"), "writability event on " + thread);
            }
        }
    }

    @Test
    void theHardLimitClosesTheConnection() throws Exception {
        int maxBody = 64 * 1024;
        NioTransport t = transport(TransportConfig.builder()
                .ioThreads(1)
                .protocolLimits(new ProtocolLimits(maxBody))
                .watermarks(16 * 1024, 64 * 1024, 1024 * 1024)
                .build());
        // On the first request, write responses without ever checking isWritable(), as a buggy caller would.
        var writerFailure = new CompletableFuture<Throwable>();
        var serverHandler = new RecordingHandler((c, f) -> Thread.ofVirtual().start(() -> {
            try {
                for (int i = 1; i <= 100_000; i++) {
                    c.write(Response.ok(i, new byte[maxBody - 100]));
                }
                writerFailure.complete(null);
            } catch (Throwable e) {
                writerFailure.complete(e);
            }
        }));
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket raw = new Socket()) {
            raw.setReceiveBufferSize(16 * 1024);
            raw.connect(server.localAddress(), 5_000);
            raw.getOutputStream().write(bytes(request(1, new byte[0])));

            assertInstanceOf(ConnectionClosedException.class, writerFailure.get(WAIT.toSeconds(), TimeUnit.SECONDS));
            Throwable cause = serverHandler.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS);
            assertInstanceOf(IOException.class, cause);
            assertTrue(cause.getMessage().startsWith("write queue overflow"), cause.getMessage());
            assertEquals(1, serverHandler.closeCount.get());
        }
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

    @Test
    void transportCloseReportsALocalCloseOnBothEndsEvenWhenThePeerClosesFirst() throws Exception {
        // Both ends live in the same transport, so one side's close sends a FIN the other side may read before
        // its own close task runs. CI on Linux hit exactly that; this repeats the race.
        for (int round = 0; round < 50; round++) {
            NioTransport t = new NioTransport(TransportConfig.builder().ioThreads(2).build());
            var serverHandler = new RecordingHandler(echo()::onFrame);
            Server server = t.bind(ANY_PORT, serverHandler);
            var client = new RecordingHandler();
            Connection c = connect(t, server, client);
            c.write(request(1, new byte[0]));
            client.take(WAIT);
            t.close();
            assertSame(RecordingHandler.LOCAL_CLOSE, client.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS),
                    "client, round " + round);
            assertSame(RecordingHandler.LOCAL_CLOSE, serverHandler.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS),
                    "server, round " + round);
        }
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

    /** Bytes that depend on both values, so a shifted, dropped or misrouted byte shows up as a mismatch. */
    private static byte[] payloadFor(int writer, int seq) {
        byte[] out = new byte[seq < 1_000 ? (seq * 37) % 1_024 : seq];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) (writer * 31 + seq * 7 + i);
        }
        return out;
    }

    private static List<Frame> readFrames(Socket raw, int n) throws Exception {
        InputStream in = raw.getInputStream();
        var decoder = new FrameDecoder();
        List<Frame> frames = new ArrayList<>(n);
        byte[] chunk = new byte[64 * 1024];
        while (frames.size() < n) {
            int read = in.read(chunk);
            if (read < 0) {
                throw new EOFException("socket closed after " + frames.size() + " of " + n + " frames");
            }
            decoder.decode(ByteBuffer.wrap(chunk, 0, read), frames::add);
        }
        return frames;
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
