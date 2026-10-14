package dev.rpc.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.rpc.protocol.Frame;
import dev.rpc.protocol.Request;
import dev.rpc.protocol.Response;
import dev.rpc.protocol.Status;
import dev.rpc.transport.Connection;
import dev.rpc.transport.ConnectionClosedException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CallTableTest {

    private static final byte[] ARGS = {1, 2, 3};
    private static final Executor DIRECT = Runnable::run;

    static final class FakeConnection implements Connection {
        final ConcurrentLinkedQueue<Frame> written = new ConcurrentLinkedQueue<>();
        volatile boolean writable = true;
        volatile RuntimeException failWrites;

        @Override
        public void write(Frame frame) {
            if (failWrites != null) {
                throw failWrites;
            }
            written.add(frame);
        }

        @Override
        public boolean isWritable() {
            return writable;
        }

        @Override
        public void close() {
        }

        @Override
        public SocketAddress remoteAddress() {
            return InetSocketAddress.createUnresolved("fake", 1);
        }

        Request lastRequest() {
            Request last = null;
            for (Frame f : written) {
                last = (Request) f;
            }
            return last;
        }
    }

    private static Throwable failure(CompletableFuture<?> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        throw new AssertionError("expected the call to fail");
    }

    @Test
    void writesARequestAndCompletesItWithTheMatchingResponse() throws Exception {
        var conn = new FakeConnection();
        var table = new CallTable(conn, 8, DIRECT);

        CompletableFuture<Response> a = table.call(42, ARGS);
        CompletableFuture<Response> b = table.call(43, ARGS);
        var requests = new ArrayList<Request>();
        conn.written.forEach(f -> requests.add((Request) f));
        assertEquals(List.of(42, 43), requests.stream().map(Request::methodId).toList());
        assertEquals(0, requests.get(0).timeoutNanos());

        Response second = Response.ok(requests.get(1).requestId(), new byte[] {9});
        table.onResponse(second);
        assertSame(second, b.get(5, TimeUnit.SECONDS));
        assertFalse(a.isDone());
        assertEquals(1, table.inFlight());
    }

    @Test
    void completesOnTheCompleterNotTheCallingThread() throws Exception {
        var conn = new FakeConnection();
        try (ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor()) {
            var table = new CallTable(conn, 8, vt);
            CompletableFuture<Response> f = table.call(1, ARGS);
            CompletableFuture<Boolean> onVirtual = f.thenApply(r -> Thread.currentThread().isVirtual());
            table.onResponse(Response.ok(conn.lastRequest().requestId(), new byte[0]));
            assertTrue(onVirtual.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void idsSkipZeroAndWrap() {
        var conn = new FakeConnection();
        var table = new CallTable(conn, 8, DIRECT);

        table.restartIdsAfter(Integer.MAX_VALUE - 1);
        table.call(1, ARGS);
        table.call(1, ARGS);
        table.restartIdsAfter(-2);
        table.call(1, ARGS);
        table.call(1, ARGS);

        List<Integer> ids = conn.written.stream().map(f -> ((Request) f).requestId()).toList();
        assertEquals(List.of(Integer.MAX_VALUE, Integer.MIN_VALUE, -1, 1), ids);
    }

    @Test
    void anIdStillInUseIsSkipped() {
        var conn = new FakeConnection();
        var table = new CallTable(conn, 8, DIRECT);

        table.call(1, ARGS); // id 1, still pending
        table.call(1, ARGS); // id 2, still pending
        table.restartIdsAfter(0);
        table.call(1, ARGS);

        assertEquals(3, conn.lastRequest().requestId());
    }

    @Test
    void overTheLimitFailsWithOverloadedAndWritesNothing() throws Exception {
        var conn = new FakeConnection();
        var table = new CallTable(conn, 2, DIRECT);
        table.call(1, ARGS);
        table.call(1, ARGS);

        Throwable t = failure(table.call(1, ARGS));
        assertEquals(Status.OVERLOADED, assertInstanceOf(RpcException.class, t).status());
        assertEquals(2, conn.written.size());

        // A completed call frees its slot.
        table.onResponse(Response.ok(conn.lastRequest().requestId(), new byte[0]));
        table.call(1, ARGS);
        assertEquals(3, conn.written.size());
    }

    @Test
    void anUnwritableConnectionFailsFastWithOverloaded() throws Exception {
        var conn = new FakeConnection();
        conn.writable = false;
        var table = new CallTable(conn, 8, DIRECT);

        Throwable t = failure(table.call(1, ARGS));
        assertEquals(Status.OVERLOADED, assertInstanceOf(RpcException.class, t).status());
        assertTrue(conn.written.isEmpty());
        assertEquals(0, table.inFlight());
    }

    @Test
    void aFailedWriteFailsTheCallAndLeavesNothingPending() throws Exception {
        var conn = new FakeConnection();
        conn.failWrites = new ConnectionClosedException("closing");
        var table = new CallTable(conn, 8, DIRECT);

        assertSame(conn.failWrites, failure(table.call(1, ARGS)));
        assertEquals(0, table.inFlight());
    }

    @Test
    void closeFailsPendingCallsAndRefusesNewOnes() throws Exception {
        var conn = new FakeConnection();
        var table = new CallTable(conn, 8, DIRECT);
        CompletableFuture<Response> a = table.call(1, ARGS);
        CompletableFuture<Response> b = table.call(1, ARGS);

        table.close();
        table.close();

        assertInstanceOf(ConnectionClosedException.class, failure(a));
        assertInstanceOf(ConnectionClosedException.class, failure(b));
        assertInstanceOf(ConnectionClosedException.class, failure(table.call(1, ARGS)));
        assertEquals(2, conn.written.size());
        assertEquals(0, table.inFlight());
    }

    @Test
    void cancelFreesTheIdAndALateResponseIsDropped() {
        var conn = new FakeConnection();
        var table = new CallTable(conn, 8, DIRECT);
        CompletableFuture<Response> f = table.call(1, ARGS);
        int id = conn.lastRequest().requestId();

        f.cancel(false);
        assertEquals(0, table.inFlight());
        table.onResponse(Response.ok(id, new byte[0])); // late: dropped, not an error
        assertTrue(f.isCancelled());
    }

    @Test
    void responseCloseAndCancelRacingCompleteEachCallOnce() throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(3)) {
            for (int round = 0; round < 2_000; round++) {
                var conn = new FakeConnection();
                var table = new CallTable(conn, 8, DIRECT);
                CompletableFuture<Response> f = table.call(1, ARGS);
                var completions = new AtomicInteger();
                f.whenComplete((r, e) -> completions.incrementAndGet());
                int id = conn.lastRequest().requestId();

                var start = new CountDownLatch(1);
                List<Runnable> racers = List.of(
                        () -> table.onResponse(Response.ok(id, new byte[0])),
                        table::close,
                        () -> f.cancel(false));
                var done = new ArrayList<Future<?>>();
                for (Runnable r : racers) {
                    done.add(pool.submit(() -> {
                        start.await();
                        r.run();
                        return null;
                    }));
                }
                start.countDown();
                for (var d : done) {
                    d.get(5, TimeUnit.SECONDS);
                }

                assertTrue(f.isDone());
                assertEquals(1, completions.get(), "round " + round);
                assertEquals(0, table.inFlight(), "round " + round);
            }
        }
    }

    @Test
    void registerRacingCloseLeavesNothingPending() throws Exception {
        try (ExecutorService vt = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int round = 0; round < 200; round++) {
                var conn = new FakeConnection();
                var table = new CallTable(conn, 1_024, vt);
                var start = new CountDownLatch(1);
                var futures = new ConcurrentLinkedQueue<CompletableFuture<Response>>();
                var callers = new ArrayList<Future<?>>();
                for (int t = 0; t < 8; t++) {
                    callers.add(vt.submit(() -> {
                        start.await();
                        for (int i = 0; i < 20; i++) {
                            futures.add(table.call(1, ARGS));
                        }
                        return null;
                    }));
                }
                start.countDown();
                table.close();
                for (var c : callers) {
                    c.get(5, TimeUnit.SECONDS);
                }

                for (CompletableFuture<Response> f : futures) {
                    try {
                        f.get(5, TimeUnit.SECONDS);
                        throw new AssertionError("nothing was answered, so nothing can succeed");
                    } catch (ExecutionException e) {
                        assertInstanceOf(ConnectionClosedException.class, e.getCause());
                    } catch (TimeoutException e) {
                        throw new AssertionError("a call was left pending in round " + round, e);
                    }
                }
                assertEquals(160, futures.size());
                assertEquals(0, table.inFlight());
            }
        }
    }
}
