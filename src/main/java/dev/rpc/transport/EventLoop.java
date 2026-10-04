package dev.rpc.transport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * One reactor: a platform thread that owns a Selector, a task queue and a timer queue. Channels registered here,
 * and everything attached to them, are touched only by this thread; other threads reach them through execute().
 *
 * Each iteration selects (with a timeout set by the next timer), dispatches ready keys, runs queued tasks, then
 * runs due timers. A task or handler that throws is logged and the loop carries on.
 */
final class EventLoop implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(EventLoop.class.getName());

    static final int READ_BUFFER_SIZE = 64 * 1024;

    private final Selector selector;
    private final Thread thread;
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final CountDownLatch terminated = new CountDownLatch(1);
    private volatile boolean running = true;

    // Loop thread only.
    private final PriorityQueue<Timer> timers = new PriorityQueue<>(Timer.ORDER);
    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
    private long timerSequence;

    EventLoop(String name) {
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            throw new UncheckedIOException("can't open a selector", e);
        }
        this.thread = Thread.ofPlatform().name(name).unstarted(this::run);
        thread.start();
    }

    boolean inEventLoop() {
        return Thread.currentThread() == thread;
    }

    /** Runs task on the loop thread. Throws RejectedExecutionException once the loop is shutting down. */
    void execute(Runnable task) {
        if (!running) {
            throw new RejectedExecutionException("event loop " + thread.getName() + " is shut down");
        }
        tasks.add(task);
        // If close() won the race, the loop's final drain may already be over. Taking the task back out tells us
        // which: if it's still there nobody will run it, so reject; if it's gone, the drain ran it.
        if (!running && tasks.remove(task)) {
            throw new RejectedExecutionException("event loop " + thread.getName() + " is shut down");
        }
        // Also correct when select() isn't running yet: the next select then returns at once.
        selector.wakeup();
    }

    /** Runs task on the loop thread after delay. Callable from any thread. */
    Timer schedule(Runnable task, Duration delay) {
        Timer timer = new Timer(task, System.nanoTime() + delay.toNanos());
        if (inEventLoop()) {
            addTimer(timer);
        } else {
            execute(() -> addTimer(timer));
        }
        return timer;
    }

    /** Registers channel with this loop's selector. Loop thread only, so the key is never shared with a caller. */
    SelectionKey register(SelectableChannel channel, int ops, IoHandler handler) throws IOException {
        if (!inEventLoop()) {
            throw new IllegalStateException("register must run on the event loop; use execute()");
        }
        return channel.register(selector, ops, handler);
    }

    /** Shared by every connection on this loop: valid only until the read that filled it has been decoded. */
    ByteBuffer readBuffer() {
        assert inEventLoop();
        return readBuffer;
    }

    /** Stops accepting tasks and asks the loop to exit; tasks already queued still run. Doesn't wait. */
    void shutdown() {
        running = false;
        selector.wakeup();
    }

    boolean awaitTermination(Duration timeout) throws InterruptedException {
        return terminated.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Shuts down and waits up to 10 seconds, unless called from the loop itself. */
    @Override
    public void close() {
        shutdown();
        if (inEventLoop()) {
            return;
        }
        try {
            if (!awaitTermination(Duration.ofSeconds(10))) {
                LOG.log(Level.WARNING, "event loop {0} did not stop within 10 seconds", thread.getName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        try {
            while (running) {
                select();
                dispatchReadyKeys();
                runTasks();
                runDueTimers();
            }
        } catch (ClosedSelectorException | IOException e) {
            LOG.log(Level.ERROR, "event loop " + thread.getName() + " failed", e);
        } finally {
            // Tasks queued before shutdown still run; close tasks for connections depend on it.
            runTasks();
            try {
                selector.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "closing selector failed", e);
            }
            terminated.countDown();
        }
    }

    private void select() throws IOException {
        if (!tasks.isEmpty()) {
            selector.selectNow();
            return;
        }
        Timer next = nextLiveTimer();
        if (next == null) {
            selector.select();
            return;
        }
        long waitNanos = next.deadline - System.nanoTime();
        if (waitNanos <= 0) {
            selector.selectNow();
        } else {
            // Round up: select(0) would mean "forever", and an early return would just spin.
            selector.select(Math.max(1, TimeUnit.NANOSECONDS.toMillis(waitNanos + 999_999)));
        }
    }

    private void dispatchReadyKeys() {
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();
            if (!key.isValid()) {
                continue;
            }
            try {
                ((IoHandler) key.attachment()).onReady(key.readyOps());
            } catch (RuntimeException e) {
                LOG.log(Level.ERROR, "I/O handler failed", e);
            }
        }
    }

    private void runTasks() {
        Runnable task;
        while ((task = tasks.poll()) != null) {
            try {
                task.run();
            } catch (RuntimeException e) {
                LOG.log(Level.ERROR, "event loop task failed", e);
            }
        }
    }

    private void runDueTimers() {
        long now = System.nanoTime();
        Timer timer;
        while ((timer = timers.peek()) != null && timer.deadline - now <= 0) {
            timers.poll();
            if (timer.cancelled) {
                continue;
            }
            try {
                timer.task.run();
            } catch (RuntimeException e) {
                LOG.log(Level.ERROR, "event loop timer failed", e);
            }
        }
    }

    private Timer nextLiveTimer() {
        while (!timers.isEmpty() && timers.peek().cancelled) {
            timers.poll();
        }
        return timers.peek();
    }

    private void addTimer(Timer timer) {
        timer.sequence = timerSequence++;
        timers.add(timer);
    }

    /** A scheduled task. Cancelling is safe from any thread; the loop drops cancelled timers lazily. */
    static final class Timer {

        // nanoTime values are compared by difference, which stays correct across numeric overflow. Equal deadlines
        // run in the order they were scheduled.
        static final Comparator<Timer> ORDER = (a, b) -> {
            long diff = a.deadline - b.deadline;
            if (diff != 0) {
                return diff < 0 ? -1 : 1;
            }
            return Long.compare(a.sequence, b.sequence);
        };

        private final Runnable task;
        private final long deadline;
        private long sequence;
        private volatile boolean cancelled;

        private Timer(Runnable task, long deadline) {
            this.task = task;
            this.deadline = deadline;
        }

        void cancel() {
            cancelled = true;
        }
    }
}
