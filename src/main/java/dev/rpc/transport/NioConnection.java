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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
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

    // Written by any thread, drained by the loop.
    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong pendingBytes = new AtomicLong();
    private volatile boolean closing;
    // Set once close() is asked for. From then on the connection counts as closed locally, even if the loop
    // happens to see the peer's FIN first: that FIN is usually the peer reacting to our own close.
    private volatile boolean closeRequested;

    // Loop thread only.
    private State state;
    private SelectionKey key;
    private EventLoop.Timer connectTimer;

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
        this.state = connectFuture == null ? State.OPEN : State.CONNECTING;
    }

    // --- opening (loop thread) ---

    /** For an accepted channel: start reading. */
    void openAccepted() {
        try {
            key = loop.register(channel, SelectionKey.OP_READ, this);
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
            for (int i = 0; i < MAX_READS_PER_WAKEUP && state == State.OPEN; i++) {
                buf.clear();
                int n = channel.read(buf);
                if (n < 0) {
                    doClose(new EOFException("peer closed the connection"));
                    return;
                }
                if (n == 0) {
                    return;
                }
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
                // Liveness only; any inbound byte already counts.
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

    // --- writing ---

    @Override
    public void write(Frame frame) {
        Objects.requireNonNull(frame, "frame");
        if (closing) {
            throw closedException();
        }
        ByteBuffer buf = encoder.encode(frame);
        writeQueue.add(buf);
        // Only the write that finds the queue empty schedules a flush; any later one knows a flush is pending.
        // The buffer is queued before the count goes up, so whoever sees a non-zero count also sees the buffer.
        if (pendingBytes.getAndAdd(buf.remaining()) == 0) {
            if (loop.inEventLoop()) {
                flush();
            } else {
                try {
                    loop.execute(this::flush);
                } catch (RejectedExecutionException e) {
                    throw closedException();
                }
            }
        }
    }

    /** Writes queued frames until the socket is full; resumes inside a partly written frame on the next call. */
    private void flush() {
        if (state != State.OPEN) {
            return;
        }
        try {
            ByteBuffer buf;
            while ((buf = writeQueue.peek()) != null) {
                channel.write(buf);
                if (buf.hasRemaining()) {
                    setWriteInterest(true);
                    return;
                }
                writeQueue.poll();
                pendingBytes.addAndGet(-buf.limit());
            }
            setWriteInterest(false);
        } catch (IOException e) {
            doClose(e);
        }
    }

    private void setWriteInterest(boolean on) {
        int ops = key.interestOps();
        int wanted = on ? ops | SelectionKey.OP_WRITE : ops & ~SelectionKey.OP_WRITE;
        if (wanted != ops) {
            key.interestOps(wanted);
        }
    }

    @Override
    public boolean isWritable() {
        // Backpressure watermarks come with the write-path milestone step; until then only closing matters.
        return !closing;
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
