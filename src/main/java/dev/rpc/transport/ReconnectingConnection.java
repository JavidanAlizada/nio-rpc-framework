package dev.rpc.transport;

import dev.rpc.protocol.Frame;
import java.lang.System.Logger.Level;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Keeps one connection open to an endpoint. When it closes, a new one is attempted after an exponential backoff
 * with full jitter; the attempt count resets once a connect succeeds. Nothing is queued while disconnected: write()
 * fails fast, so a dead endpoint can't build up a backlog.
 *
 * The handler sees every underlying connection's events, with that connection as the argument, so onClosed fires
 * once per connection that opened, not once for this object. Create one with NioTransport.reconnecting().
 */
public final class ReconnectingConnection implements Connection, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ReconnectingConnection.class.getName());

    public enum State { CONNECTING, CONNECTED, BACKING_OFF, CLOSED }

    private final NioTransport transport;
    private final SocketAddress address;
    private final ConnectionHandler handler;
    private final Backoff backoff;

    // Guarded by this. Callbacks arrive from the event loops, connect-future threads and close() callers; a lock
    // keeps the transitions simple, and nothing slow runs while it is held.
    private State state = State.CONNECTING;
    private Attempt attempt;
    private EventLoop.Timer retryTimer;
    private int failures;

    // The open connection, or null. Read without the lock by write() and isWritable().
    private volatile Connection current;
    private volatile State observed = State.CONNECTING;

    ReconnectingConnection(NioTransport transport, SocketAddress address, ConnectionHandler handler, Backoff backoff) {
        this.transport = transport;
        this.address = address;
        this.handler = handler;
        this.backoff = backoff;
    }

    public State state() {
        return observed;
    }

    /** Throws ConnectionClosedException at once unless CONNECTED. */
    @Override
    public void write(Frame frame) {
        Connection c = current;
        if (c == null) {
            throw new ConnectionClosedException("not connected to " + address + " (" + observed + ")");
        }
        c.write(frame);
    }

    @Override
    public boolean isWritable() {
        Connection c = current;
        return c != null && c.isWritable();
    }

    /** Closes the open connection, if any, and stops reconnecting. Idempotent. */
    @Override
    public void close() {
        Connection open;
        synchronized (this) {
            if (state == State.CLOSED) {
                return;
            }
            moveTo(State.CLOSED);
            attempt = null;
            if (retryTimer != null) {
                retryTimer.cancel();
                retryTimer = null;
            }
            open = current;
            current = null;
        }
        transport.reconnectingClosed(this);
        if (open != null) {
            open.close();
        }
    }

    @Override
    public SocketAddress remoteAddress() {
        return address;
    }

    @Override
    public String toString() {
        return "ReconnectingConnection[" + address + ", " + observed + "]";
    }

    // --- lifecycle ---

    void start() {
        connect();
    }

    private void connect() {
        Attempt a;
        synchronized (this) {
            if (state == State.CLOSED) {
                return;
            }
            retryTimer = null;
            moveTo(State.CONNECTING);
            a = new Attempt();
            attempt = a;
        }
        try {
            transport.connect(address, a).whenComplete((c, error) -> {
                if (error != null) {
                    disconnected(a);
                } else {
                    connected(a, c);
                }
            });
        } catch (IllegalStateException e) {
            // the transport is closing; it closes this object too
            close();
        }
    }

    private void connected(Attempt a, Connection c) {
        synchronized (this) {
            // a == attempt fails if close() won, or if the connection already closed and a retry is under way.
            if (a == attempt && !a.closed) {
                moveTo(State.CONNECTED);
                failures = 0;
                current = c;
                return;
            }
        }
        // Either we're CLOSED, or c has closed already; close() is idempotent.
        c.close();
    }

    private void disconnected(Attempt a) {
        synchronized (this) {
            if (a != attempt) {
                return;
            }
            attempt = null;
            current = null;
            moveTo(State.BACKING_OFF);
            Duration delay = backoff.delay(failures++, ThreadLocalRandom.current());
            try {
                retryTimer = transport.schedule(this::connect, delay);
                return;
            } catch (RejectedExecutionException e) {
                LOG.log(Level.DEBUG, "transport closing, not reconnecting to {0}", address);
            }
        }
        close();
    }

    private void moveTo(State next) {
        state = next;
        observed = next;
        LOG.log(Level.DEBUG, "{0} -> {1}", address, next);
    }

    /** One connect attempt; forwards its connection's events to the handler. */
    private final class Attempt implements ConnectionHandler {

        // Loop thread writes it in onClosed; read under the lock.
        private volatile boolean closed;

        @Override
        public void onFrame(Connection connection, Frame frame) {
            handler.onFrame(connection, frame);
        }

        @Override
        public void onWritabilityChanged(Connection connection, boolean writable) {
            handler.onWritabilityChanged(connection, writable);
        }

        @Override
        public void onClosed(Connection connection, Throwable cause) {
            closed = true;
            try {
                handler.onClosed(connection, cause);
            } finally {
                disconnected(this);
            }
        }
    }
}
