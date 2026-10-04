package dev.rpc.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EventLoopTest {

    private final EventLoop loop = new EventLoop("rpc-test-loop");

    @AfterEach
    void stop() {
        loop.close();
    }

    @Test
    void tasksFromManyThreadsAllRunOnTheLoopThread() throws Exception {
        int threads = 8;
        int perThread = 1_000;
        var ran = new AtomicInteger();
        var offLoop = new AtomicInteger();
        var done = new CountDownLatch(threads * perThread);
        List<Thread> writers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            writers.add(Thread.ofVirtual().start(() -> {
                for (int i = 0; i < perThread; i++) {
                    loop.execute(() -> {
                        if (!loop.inEventLoop()) {
                            offLoop.incrementAndGet();
                        }
                        ran.incrementAndGet();
                        done.countDown();
                    });
                }
            }));
        }
        for (Thread w : writers) {
            w.join();
        }
        assertTrue(done.await(10, TimeUnit.SECONDS), "only " + ran.get() + " tasks ran");
        assertEquals(0, offLoop.get());
    }

    @Test
    void aTaskSubmittedWhileSelectIsBlockedRunsPromptly() throws Exception {
        // With no timers and no channels the loop is blocked in select() with no timeout. Only the wakeup in
        // execute() can get this task run; without it the latch would time out.
        Thread.sleep(50);
        var ran = new CountDownLatch(1);
        loop.execute(ran::countDown);
        assertTrue(ran.await(5, TimeUnit.SECONDS));
    }

    @Test
    void timersFireInDeadlineOrderAndTiesInScheduleOrder() throws Exception {
        var order = Collections.synchronizedList(new ArrayList<String>());
        var done = new CountDownLatch(4);
        loop.execute(() -> {
            loop.schedule(() -> record(order, done, "30ms"), Duration.ofMillis(30));
            loop.schedule(() -> record(order, done, "10ms"), Duration.ofMillis(10));
            loop.schedule(() -> record(order, done, "20ms-a"), Duration.ofMillis(20));
            loop.schedule(() -> record(order, done, "20ms-b"), Duration.ofMillis(20));
        });
        assertTrue(done.await(5, TimeUnit.SECONDS));
        // 20ms-a and 20ms-b are scheduled in the same task, so their deadlines may differ by nanoseconds or tie;
        // either way a was scheduled first and must run first.
        assertEquals(List.of("10ms", "20ms-a", "20ms-b", "30ms"), order);
    }

    @Test
    void timersCanBeScheduledFromAnyThreadAndRunOnTheLoop() throws Exception {
        var onLoop = new CompletableFuture<Boolean>();
        loop.schedule(() -> onLoop.complete(loop.inEventLoop()), Duration.ofMillis(5));
        assertTrue(onLoop.get(5, TimeUnit.SECONDS));
    }

    @Test
    void aCancelledTimerDoesNotRun() throws Exception {
        var cancelledRan = new AtomicBoolean();
        var later = new CountDownLatch(1);
        EventLoop.Timer timer = loop.schedule(() -> cancelledRan.set(true), Duration.ofMillis(10));
        timer.cancel();
        loop.schedule(later::countDown, Duration.ofMillis(40));
        assertTrue(later.await(5, TimeUnit.SECONDS));
        assertFalse(cancelledRan.get());
    }

    @Test
    void aFailingTaskDoesNotStopTheLoop() throws Exception {
        var after = new CountDownLatch(1);
        loop.execute(() -> {
            throw new IllegalStateException("expected by the test");
        });
        loop.execute(after::countDown);
        assertTrue(after.await(5, TimeUnit.SECONDS));
    }

    @Test
    void readyKeysAreDispatchedToTheAttachedHandlerOnTheLoop() throws Exception {
        Pipe pipe = Pipe.open();
        pipe.source().configureBlocking(false);
        var received = new CompletableFuture<String>();
        var onLoop = new AtomicReference<Boolean>();
        loop.execute(() -> {
            try {
                loop.register(pipe.source(), SelectionKey.OP_READ, readyOps -> {
                    onLoop.set(loop.inEventLoop());
                    var buf = ByteBuffer.allocate(16);
                    try {
                        pipe.source().read(buf);
                    } catch (IOException e) {
                        received.completeExceptionally(e);
                    }
                    received.complete(new String(buf.array(), 0, buf.position(), StandardCharsets.US_ASCII));
                });
            } catch (IOException e) {
                received.completeExceptionally(e);
            }
        });
        pipe.sink().write(ByteBuffer.wrap("ping".getBytes(StandardCharsets.US_ASCII)));
        assertEquals("ping", received.get(5, TimeUnit.SECONDS));
        assertTrue(onLoop.get());
        pipe.sink().close();
        pipe.source().close();
    }

    @Test
    void registerOffTheLoopIsRejected() throws Exception {
        Pipe pipe = Pipe.open();
        pipe.source().configureBlocking(false);
        assertThrows(IllegalStateException.class, () -> loop.register(pipe.source(), SelectionKey.OP_READ, ops -> { }));
        pipe.sink().close();
        pipe.source().close();
    }

    @Test
    void tasksQueuedBeforeShutdownStillRunAndLaterOnesAreRejected() throws Exception {
        var ran = new AtomicInteger();
        for (int i = 0; i < 100; i++) {
            loop.execute(ran::incrementAndGet);
        }
        loop.close();
        assertEquals(100, ran.get());
        assertThrows(RejectedExecutionException.class, () -> loop.execute(() -> { }));
    }

    @Test
    void submittingWhileShuttingDownEitherRunsOrRejectsNeverLoses() throws Exception {
        // Race execute() against shutdown(): every task must either run or be rejected.
        for (int round = 0; round < 50; round++) {
            var l = new EventLoop("rpc-race-loop");
            var ran = new AtomicInteger();
            var rejected = new AtomicInteger();
            int submitted = 2_000;
            var start = new CountDownLatch(1);
            Thread submitter = Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < submitted; i++) {
                    try {
                        l.execute(ran::incrementAndGet);
                    } catch (RejectedExecutionException e) {
                        rejected.incrementAndGet();
                    }
                }
            });
            start.countDown();
            l.shutdown();
            submitter.join();
            assertTrue(l.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(submitted, ran.get() + rejected.get(), "round " + round);
        }
    }

    @Test
    void inEventLoopIsFalseElsewhere() throws Exception {
        assertFalse(loop.inEventLoop());
        var inside = new CompletableFuture<Boolean>();
        loop.execute(() -> inside.complete(loop.inEventLoop()));
        assertTrue(inside.get(5, TimeUnit.SECONDS));
    }

    @Test
    void closeStopsTheThread() throws Exception {
        var thread = new CompletableFuture<Thread>();
        loop.execute(() -> thread.complete(Thread.currentThread()));
        Thread t = thread.get(5, TimeUnit.SECONDS);
        loop.close();
        t.join(5_000);
        assertFalse(t.isAlive());
        assertSame("rpc-test-loop", t.getName());
    }

    private static void record(List<String> order, CountDownLatch done, String name) {
        order.add(name);
        done.countDown();
    }
}
