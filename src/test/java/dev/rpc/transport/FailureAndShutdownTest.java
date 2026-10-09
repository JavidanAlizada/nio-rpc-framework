package dev.rpc.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.rpc.protocol.Frame;
import dev.rpc.protocol.FrameDecoder;
import dev.rpc.protocol.FrameEncoder;
import dev.rpc.protocol.Request;
import dev.rpc.protocol.Response;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** How the transport fails and shuts down, on real localhost sockets. No fixed sleeps: every wait is on an event. */
class FailureAndShutdownTest {

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

    private static TransportConfig.Builder config() {
        return TransportConfig.builder().ioThreads(2);
    }

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

    private static Throwable closeCause(RecordingHandler h) throws Exception {
        return h.closed.get(WAIT.toSeconds(), TimeUnit.SECONDS);
    }

    // --- a peer that goes away mid-frame ---

    @Test
    void peerFinInTheMiddleOfAFrameClosesOnceWithEof() throws Exception {
        NioTransport t = transport(config().build());
        var serverHandler = new RecordingHandler();
        Server server = t.bind(ANY_PORT, serverHandler);
        byte[] frame = bytes(request(1, new byte[10_000]));
        try (Socket raw = rawSocket(server)) {
            // The whole header and part of the body: the decoder is left holding a half-built frame.
            raw.getOutputStream().write(frame, 0, frame.length / 2);
            raw.getOutputStream().flush();
        }
        assertInstanceOf(EOFException.class, closeCause(serverHandler));
        assertTrue(serverHandler.frames.isEmpty(), "a truncated frame must never be delivered");
        assertClosedOnceAfterShutdown(t, serverHandler);
    }

    @Test
    void peerResetInTheMiddleOfAFrameClosesOnceWithAnIoError() throws Exception {
        NioTransport t = transport(config().build());
        var serverHandler = new RecordingHandler();
        Server server = t.bind(ANY_PORT, serverHandler);
        byte[] frame = bytes(request(1, new byte[10_000]));
        try (Socket raw = rawSocket(server)) {
            raw.getOutputStream().write(frame, 0, 20);
            raw.getOutputStream().flush();
            // Linger 0 turns close() into an RST instead of a FIN.
            raw.setSoLinger(true, 0);
        }
        // An RST shows up as a "connection reset" IOException, or as EOF if the reset raced the read; either
        // way it's one close.
        assertInstanceOf(IOException.class, closeCause(serverHandler));
        assertTrue(serverHandler.frames.isEmpty());
        assertClosedOnceAfterShutdown(t, serverHandler);
    }

    // --- a failing handler is contained to its own connection ---

    @Test
    void aThrowingHandlerClosesOnlyItsOwnConnection() throws Exception {
        // One loop, so both connections share the thread the handler throws on.
        NioTransport t = transport(TransportConfig.builder().ioThreads(1).build());
        var boom = new IllegalStateException("handler bug");
        Server server = t.bind(ANY_PORT, echo());
        var bad = new RecordingHandler((c, f) -> {
            throw boom;
        });
        var good = new RecordingHandler();
        Connection badConnection = t.connect(server.localAddress(), bad).get(WAIT.toSeconds(), TimeUnit.SECONDS);
        Connection goodConnection = t.connect(server.localAddress(), good).get(WAIT.toSeconds(), TimeUnit.SECONDS);

        badConnection.write(request(1, new byte[0]));
        assertSame(boom, closeCause(bad));

        for (int i = 1; i <= 10; i++) {
            goodConnection.write(request(i, new byte[] {(byte) i}));
            assertEquals(i, ((Response) good.take(WAIT)).requestId());
        }
        assertEquals(0, good.closeCount.get());
    }

    // --- maxConnections ---

    @Test
    void aClosedConnectionFreesItsSlotUnderMaxConnections() throws Exception {
        NioTransport t = transport(TransportConfig.builder().ioThreads(1).maxConnections(1).build());
        var serverHandler = new RecordingHandler(echo()::onFrame);
        Server server = t.bind(ANY_PORT, serverHandler);
        try (Socket first = rawSocket(server)) {
            roundTrip(first);
            try (Socket extra = rawSocket(server)) {
                assertEquals(-1, extra.getInputStream().read(), "over the limit: closed at once");
            }
            roundTrip(first);
        }
        // The slot is given back when the server side sees the close; wait for that, then a new client fits.
        closeCause(serverHandler);
        try (Socket next = rawSocket(server)) {
            roundTrip(next);
        }
    }

    // --- the exactly-once race ---

    @Test
    void manyLocalClosesRacingThePeersCloseFireOnClosedExactlyOnce() throws Exception {
        NioTransport t = transport(config().build());
        List<RecordingHandler> clients = new ArrayList<>();
        List<RecordingHandler> servers = new ArrayList<>();
        var serverSides = new LinkedBlockingQueue<RecordingHandler>();
        Server server = t.bind(ANY_PORT, perConnection(serverSides));

        for (int round = 0; round < 30; round++) {
            var client = new RecordingHandler();
            Connection c = t.connect(server.localAddress(), client).get(WAIT.toSeconds(), TimeUnit.SECONDS);
            c.write(request(1, new byte[0]));
            RecordingHandler serverSide = serverSides.poll(WAIT.toSeconds(), TimeUnit.SECONDS);
            Connection peer = serverSide.firstConnection.get(WAIT.toSeconds(), TimeUnit.SECONDS);

            var start = new CountDownLatch(1);
            List<Thread> closers = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                closers.add(Thread.ofPlatform().start(() -> {
                    awaitQuietly(start);
                    c.close();
                }));
            }
            closers.add(Thread.ofPlatform().start(() -> {
                awaitQuietly(start);
                peer.close();
            }));
            start.countDown();
            for (Thread closer : closers) {
                closer.join(WAIT.toMillis());
            }
            closeCause(client);
            closeCause(serverSide);
            clients.add(client);
            servers.add(serverSide);
        }

        // Stopping the loops runs anything still queued, so a late second onClosed would show up here.
        t.close();
        for (int i = 0; i < clients.size(); i++) {
            assertEquals(1, clients.get(i).closeCount.get(), "client, round " + i);
            assertEquals(1, servers.get(i).closeCount.get(), "server, round " + i);
        }
    }

    // --- shutdown ---

    @Test
    void transportCloseClosesEverythingOnceAndLeavesNoThreadsBehind() throws Exception {
        NioTransport t = transport(TransportConfig.builder()
                .ioThreads(4)
                .reconnectDelays(Duration.ofMillis(20), Duration.ofMillis(200))
                .build());
        var serverHandlers = new LinkedBlockingQueue<RecordingHandler>();
        Server first = t.bind(ANY_PORT, perConnection(serverHandlers));
        Server second = t.bind(ANY_PORT, perConnection(serverHandlers));

        List<RecordingHandler> clients = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            var client = new RecordingHandler();
            Server target = i % 2 == 0 ? first : second;
            Connection c = t.connect(target.localAddress(), client).get(WAIT.toSeconds(), TimeUnit.SECONDS);
            c.write(request(i + 1, new byte[0]));
            client.take(WAIT);
            clients.add(client);
        }
        // One reconnecting connection up, one backing off against a dead port.
        ReconnectingConnection up = t.reconnecting(first.localAddress(), new RecordingHandler());
        ReconnectingConnection down = t.reconnecting(new InetSocketAddress("127.0.0.1", freePort()),
                new RecordingHandler());
        awaitState(up, ReconnectingConnection.State.CONNECTED);
        awaitState(down, ReconnectingConnection.State.BACKING_OFF);

        t.close();

        for (RecordingHandler client : clients) {
            assertSame(RecordingHandler.LOCAL_CLOSE, closeCause(client));
            assertEquals(1, client.closeCount.get());
        }
        // 20 plain connections plus the reconnecting one.
        assertEquals(21, serverHandlers.size());
        for (RecordingHandler h : serverHandlers) {
            assertSame(RecordingHandler.LOCAL_CLOSE, closeCause(h));
            assertEquals(1, h.closeCount.get());
        }
        assertEquals(ReconnectingConnection.State.CLOSED, up.state());
        assertEquals(ReconnectingConnection.State.CLOSED, down.state());
        // close() joins the acceptors and the loops, so their threads are already gone, not just stopping.
        List<String> leftover = Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith(t.name() + "-"))
                .toList();
        assertEquals(List.of(), leftover);
    }

    // --- helpers ---

    /**
     * Gives each accepted connection its own echoing recorder, added to out on that connection's first event, so
     * every server side is counted on its own.
     */
    private static ConnectionHandler perConnection(Queue<RecordingHandler> out) {
        Map<Connection, RecordingHandler> byConnection = new ConcurrentHashMap<>();
        return new ConnectionHandler() {
            private RecordingHandler of(Connection c) {
                return byConnection.computeIfAbsent(c, k -> {
                    var h = echo();
                    out.add(h);
                    return h;
                });
            }

            @Override
            public void onFrame(Connection c, Frame f) {
                of(c).onFrame(c, f);
            }

            @Override
            public void onClosed(Connection c, Throwable cause) {
                of(c).onClosed(c, cause);
            }
        };
    }

    private static void assertClosedOnceAfterShutdown(NioTransport t, RecordingHandler h) {
        t.close();
        assertEquals(1, h.closeCount.get());
    }

    private static void awaitState(ReconnectingConnection c, ReconnectingConnection.State state)
            throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (c.state() != state) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("still " + c.state() + ", expected " + state);
            }
            Thread.sleep(1);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static Socket rawSocket(Server server) throws IOException {
        var s = new Socket();
        s.connect(server.localAddress(), 5_000);
        s.setSoTimeout(10_000);
        return s;
    }

    private static void roundTrip(Socket raw) throws Exception {
        raw.getOutputStream().write(bytes(request(7, new byte[] {7})));
        assertEquals(7, ((Response) readFrame(raw)).requestId());
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
