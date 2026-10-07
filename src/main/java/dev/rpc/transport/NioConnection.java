package dev.rpc.transport;

import dev.rpc.protocol.Frame;
import dev.rpc.protocol.FrameDecoder;
import dev.rpc.protocol.FrameEncoder;
import dev.rpc.protocol.Ping;
import dev.rpc.protocol.Pong;
import dev.rpc.protocol.ProtocolException;
import java.io.EOFException;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One TCP connection, owned for life by one event loop. The channel, key, decoder and state are touched only by
 * that loop; other threads reach the connection through the write queue and loop tasks. Only the loop moves the
 * state to CLOSED, which is what makes onClosed fire exactly once.
 */
final class NioConnection implements Connection, IoHandler {

    private static final System.Logger LOG = System.getLogger(NioConnection.class.getName());

    /** Reads per connection per loop iteration, so one busy peer can't starve the others on its loop. */
    static final int MAX_READS_PER_WAKEUP = 16;

    private enum State { CONNECTING, OPEN, CLOSED }

    private final NioTransport transport;
    private final EventLoop loop;
    private final SocketChannel channel;
    private final ConnectionHandler handler;
    private final FrameEncoder encoder;
    private final FrameDecoder decoder;
    private final SocketAddress remoteAddress;
    private final Runnable onRelease;
    private final CompletableFuture<Connection> connectFuture;
    private final WriteWatermarks watermarks;
    private final Duration idleTimeout;
    private final Duration pingAfter;
    // Accepted (server-side) connections only; see writabilityChanged.
    private final boolean pausesReads;

    // Written by any thread, drained by the loop.
    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    // Counted before a buffer is queued, so it may briefly over-report but never under-reports.
    private final AtomicLong pendingBytes = new AtomicLong();
    // True while a flush is guaranteed to run: a flush task is queued, or OP_WRITE is on. Only the writer that
    // flips it schedules one.
    private final AtomicBoolean flushPending = new AtomicBoolean();
    // Written by the loop only; false between crossing the high watermark and dropping below the low one.
    private volatile boolean writable = true;
    private volatile boolean closing;
    // Set once close() is asked for. From then on the connection counts as closed locally, even if the loop
    // happens to see the peer's FIN first: that FIN is usually the peer reacting to our own close.
    private volatile boolean closeRequested;

    // Loop thread only.
    private State state;
    private SelectionKey key;
    private EventLoop.Timer connectTimer;
    private EventLoop.Timer idleTimer;
    // nanoTime of the last sign of life from the peer, and whether we've pinged it since.
    private long lastActivity;
    private boolean pinged;

    NioConnection(NioTransport transport, EventLoop loop, SocketChannel channel, ConnectionHandler handler,
            SocketAddress remoteAddress, Runnable onRelease, CompletableFuture<Connection> connectFuture) {
        this.transport = transport;
        this.loop = loop;
        this.channel = channel;
        this.handler = handler;
        this.encoder = transport.encoder();
        this.decoder = new FrameDecoder(transport.config().protocolLimits());
        this.remoteAddress = remoteAddress;
        this.onRelease = onRelease;
        this.connectFuture = connectFuture;
        this.watermarks = WriteWatermarks.of(transport.config());
        this.idleTimeout = transport.config().idleTimeout();
        this.pingAfter = transport.config().pingAfter();
        this.pausesReads = connectFuture == null;
        this.state = connectFuture == null ? State.OPEN : State.CONNECTING;
    }

    // --- opening (loop thread) ---

    /** For an accepted channel: start reading. */
    void openAccepted() {
        try {
            key = loop.register(channel, SelectionKey.OP_READ, this);
            startIdleTimer();
        } catch (IOException | RuntimeException e) {
            doClose(e);
        }
    }

    /** For a client channel: connect, with a timer that fails the attempt if it takes too long. */
    void startConnect(SocketAddress address, Duration timeout) {
        try {
            if (channel.connect(address)) {
                finishOpen();
                return;
            }
            key = loop.register(channel, SelectionKey.OP_CONNECT, this);
            connectTimer = loop.schedule(
                    () -> doClose(new ConnectException("connect to " + address + " timed out after " + timeout)),
                    timeout);
        } catch (IOException | RuntimeException e) {
            doClose(e);
        }
    }

    private void finishOpen() throws IOException {
        if (key == null) {
            key = loop.register(channel, SelectionKey.OP_READ, this);
        } else {
            key.interestOps(SelectionKey.OP_READ);
        }
        if (connectTimer != null) {
            connectTimer.cancel();
        }
        state = State.OPEN;
        startIdleTimer();
        // Completed off the loop: a caller's thenApply would otherwise run their code on the event loop.
        Thread.ofVirtual().start(() -> connectFuture.complete(this));
    }

    // --- I/O events (loop thread) ---

    @Override
    public void onReady(int readyOps) {
        if (state == State.CONNECTING) {
            if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                try {
                    if (channel.finishConnect()) {
                        finishOpen();
                    }
                } catch (IOException e) {
                    doClose(e);
                }
            }
            return;
        }
        if ((readyOps & SelectionKey.OP_READ) != 0) {
            read();
        }
        if (state == State.OPEN && (readyOps & SelectionKey.OP_WRITE) != 0) {
            flush();
        }
    }

    private void read() {
        ByteBuffer buf = loop.readBuffer();
        try {
            // A handler writing responses can make the connection unwritable mid-loop; then stop reading at once.
            for (int i = 0; i < MAX_READS_PER_WAKEUP && state == State.OPEN && !readsPaused(); i++) {
                buf.clear();
                int n = channel.read(buf);
                if (n < 0) {
                    doClose(new EOFException("peer closed the connection"));
                    return;
                }
                if (n == 0) {
                    return;
                }
                sawLife();
                buf.flip();
                decoder.decode(buf, this::onFrame);
            }
        } catch (IOException | ProtocolException e) {
            doClose(e);
        }
    }

    private void onFrame(Frame frame) {
        if (state != State.OPEN) {
            return;
        }
        switch (frame) {
            case Ping ping -> {
                try {
                    write(new Pong(ping.payload()));
                } catch (ConnectionClosedException e) {
                    // expected while closing: nobody is left to answer
                }
            }
            case Pong pong -> {
                // Liveness only; the read that brought it already counted.
            }
            default -> {
                try {
                    handler.onFrame(this, frame);
                } catch (RuntimeException e) {
                    LOG.log(Level.WARNING, "connection handler failed; closing " + remoteAddress, e);
                    doClose(e);
                }
            }
        }
    }

    // --- liveness (loop thread) ---

    private void startIdleTimer() {
        lastActivity = System.nanoTime();
        idleTimer = loop.schedule(this::checkIdle, pingAfter);
    }

    private void sawLife() {
        lastActivity = System.nanoTime();
        pinged = false;
    }

    /**
     * One timer per connection, re-armed from here rather than on every read: reads only move lastActivity, and
     * the timer, when it fires, works out whether to ping, close, or just look again later.
     */
    private void checkIdle() {
        if (state != State.OPEN) {
            return;
        }
        long now = System.nanoTime();
        long quiet = now - lastActivity;
        if (quiet >= idleTimeout.toNanos()) {
            doClose(new SocketTimeoutException(
                    "idle timeout: nothing heard from the peer for " + idleTimeout.toMillis() + " ms"));
            return;
        }
        if (!pinged && quiet >= pingAfter.toNanos()) {
            pinged = true;
            try {
                write(new Ping(now));
            } catch (ConnectionClosedException e) {
                return;
            }
        }
        long next = lastActivity + (pinged ? idleTimeout : pingAfter).toNanos();
        idleTimer = loop.schedule(this::checkIdle, Duration.ofNanos(next - now));
    }

    // --- writing ---

    @Override
    public void write(Frame frame) {
        Objects.requireNonNull(frame, "frame");
        if (closing) {
            throw closedException();
        }
        ByteBuffer buf = encoder.encode(frame);
        int size = buf.remaining();
        long before = pendingBytes.getAndAdd(size);
        long after = before + size;
        if (watermarks.crossedLimit(before, after)) {
            closeWith(new IOException("write queue overflow: " + after + " bytes pending, limit is "
                    + watermarks.limit()));
            throw closedException();
        }
        writeQueue.add(buf);
        // Queued before the flag is read, so either this CAS wins and schedules a flush, or the flush that already
        // owns the flag clears it before draining and so sees this buffer.
        if (flushPending.compareAndSet(false, true)) {
            runOnLoop(this::flush);
        }
        // Writability changes only on the loop, so a writer crossing the high watermark just asks it to look.
        if (watermarks.crossedHigh(before, after)) {
            runOnLoop(this::checkWatermarks);
        }
    }

    private void runOnLoop(Runnable task) {
        if (loop.inEventLoop()) {
            task.run();
            return;
        }
        try {
            loop.execute(task);
        } catch (RejectedExecutionException e) {
            throw closedException();
        }
    }

    /** Writes queued frames until the socket is full; resumes inside a partly written frame on the next call. */
    private void flush() {
        flushPending.set(false);
        if (state != State.OPEN) {
            return;
        }
        try {
            ByteBuffer buf;
            boolean wrote = false;
            while ((buf = writeQueue.peek()) != null) {
                wrote |= channel.write(buf) > 0;
                if (buf.hasRemaining()) {
                    // OP_WRITE calls flush again, so writers needn't schedule one until then.
                    flushPending.set(true);
                    setInterest(SelectionKey.OP_WRITE, true);
                    break;
                }
                writeQueue.poll();
                pendingBytes.addAndGet(-buf.limit());
            }
            if (readsPaused() && wrote) {
                // We aren't reading, so inbound bytes can't prove the peer is alive. Its kernel taking more of
                // our backlog can: a stuck peer's receive window stays full and our writes stop moving.
                sawLife();
            }
            if (buf == null) {
                setInterest(SelectionKey.OP_WRITE, false);
            }
        } catch (IOException e) {
            doClose(e);
            return;
        }
        checkWatermarks();
    }

    /** Loop thread: applies a watermark crossing, if pendingBytes has made one since the last look. */
    private void checkWatermarks() {
        if (state != State.OPEN) {
            return;
        }
        switch (watermarks.update(pendingBytes.get())) {
            case UNWRITABLE -> writabilityChanged(false);
            case WRITABLE -> writabilityChanged(true);
            case NONE -> { }
        }
    }

    private void writabilityChanged(boolean nowWritable) {
        writable = nowWritable;
        // On a server, every request read produces a response to the same peer, so a peer that doesn't read our
        // writes doesn't get its requests read either; TCP then slows its sends down. A client must keep reading:
        // its backlog is new calls, not answers, and if both ends stopped reading neither would ever drain.
        if (pausesReads) {
            setInterest(SelectionKey.OP_READ, nowWritable);
        }
        try {
            handler.onWritabilityChanged(this, nowWritable);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "connection handler failed; closing " + remoteAddress, e);
            doClose(e);
        }
    }

    private boolean readsPaused() {
        return pausesReads && !writable;
    }

    private void setInterest(int op, boolean on) {
        int ops = key.interestOps();
        int wanted = on ? ops | op : ops & ~op;
        if (wanted != ops) {
            key.interestOps(wanted);
        }
    }

    @Override
    public boolean isWritable() {
        return writable && !closing;
    }

    // --- closing ---

    @Override
    public void close() {
        markClosing();
        closeWith(null);
    }

    /** Marks the connection as closed locally without closing it yet; NioTransport.close() marks all first. */
    void markClosing() {
        closeRequested = true;
        closing = true;
    }

    void closeWith(Throwable cause) {
        closing = true;
        if (loop.inEventLoop()) {
            doClose(cause);
            return;
        }
        try {
            loop.execute(() -> doClose(cause));
        } catch (RejectedExecutionException e) {
            // expected during shutdown: the loop is gone, and NioTransport.close() sweeps what it left behind.
        }
    }

    /**
     * The only place a connection becomes CLOSED. Runs on the loop thread, or on the closing thread once every loop
     * has terminated (NioTransport.close()), so it is never run by two threads at once.
     */
    void doClose(Throwable cause) {
        if (state == State.CLOSED) {
            return;
        }
        State was = state;
        state = State.CLOSED;
        Throwable reported = closeRequested ? null : cause;
        closing = true;
        if (connectTimer != null) {
            connectTimer.cancel();
        }
        if (idleTimer != null) {
            idleTimer.cancel();
        }
        if (key != null) {
            key.cancel();
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOG.log(Level.DEBUG, "closing channel failed", e);
        }
        writeQueue.clear();
        pendingBytes.set(0);
        transport.released(this);
        onRelease.run();
        if (was == State.CONNECTING) {
            Throwable failure = reported != null ? reported : new ConnectionClosedException("closed while connecting");
            Thread.ofVirtual().start(() -> connectFuture.completeExceptionally(failure));
            return;
        }
        try {
            handler.onClosed(this, reported);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "onClosed handler failed", e);
        }
    }

    private ConnectionClosedException closedException() {
        return new ConnectionClosedException("connection to " + remoteAddress + " is closed");
    }

    @Override
    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public String toString() {
        return "NioConnection[" + remoteAddress + "]";
    }
}
