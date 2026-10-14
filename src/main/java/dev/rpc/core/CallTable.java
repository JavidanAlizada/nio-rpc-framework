package dev.rpc.core;

import dev.rpc.protocol.Cancel;
import dev.rpc.protocol.Request;
import dev.rpc.protocol.Response;
import dev.rpc.protocol.Status;
import dev.rpc.transport.Connection;
import dev.rpc.transport.ConnectionClosedException;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * The calls waiting for a response on one physical connection.
 *
 * Whoever removes a request id from pending owns that call, so a response, the close sweep and a caller's cancel()
 * can race and the call still completes once. register and close share a lock, so a call either registers before the
 * sweep (and the sweep fails it) or is refused. Responses never take the lock.
 *
 * Futures are completed on the completer executor, never on the event loop, so callbacks a user chains onto them
 * can't stall the loop.
 */
final class CallTable {

    private static final System.Logger LOG = System.getLogger(CallTable.class.getName());
    private static final Map<String, String> NO_HEADERS = Map.of();

    private final Connection connection;
    private final int maxInFlight;
    private final Executor completer;
    private final ConcurrentHashMap<Integer, CompletableFuture<Response>> pending = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private int nextId; // guarded by lock
    private boolean closed; // guarded by lock

    CallTable(Connection connection, int maxInFlight, Executor completer) {
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight must be >= 1: " + maxInFlight);
        }
        this.connection = connection;
        this.maxInFlight = maxInFlight;
        this.completer = completer;
    }

    /**
     * Registers a call and writes its Request. Never throws: a call that can't be sent comes back as a future that
     * has already failed, and nothing is left pending.
     */
    CompletableFuture<Response> call(int methodId, byte[] payload) {
        return call(methodId, 0, NO_HEADERS, payload);
    }

    /** As call(methodId, payload), with the time the caller still waits (0 for no deadline) and headers. */
    CompletableFuture<Response> call(int methodId, long timeoutNanos, Map<String, String> headers, byte[] payload) {
        if (!connection.isWritable()) {
            return CompletableFuture.failedFuture(new RpcException(Status.OVERLOADED,
                    "connection to " + connection.remoteAddress() + " is backed up"));
        }
        var future = new CompletableFuture<Response>();
        int id;
        synchronized (lock) {
            if (closed) {
                return CompletableFuture.failedFuture(new ConnectionClosedException("connection closed"));
            }
            // Checked under the lock: registrations are serialized and removals only shrink the map, so the limit
            // is exact.
            if (pending.size() >= maxInFlight) {
                return CompletableFuture.failedFuture(
                        new RpcException(Status.OVERLOADED, maxInFlight + " calls already in flight"));
            }
            do {
                nextId++;
            } while (nextId == 0 || pending.putIfAbsent(nextId, future) != null);
            id = nextId;
        }
        // Registered before the write: the response can arrive before write() returns.
        // Every other way a call ends removes it first, so this removal succeeds only when the caller gave up on it.
        future.whenComplete((r, e) -> {
            if (pending.remove(id, future)) {
                sendCancel(id);
            }
        });
        try {
            connection.write(new Request(id, methodId, timeoutNanos, headers, payload));
        } catch (RuntimeException e) {
            if (pending.remove(id, future)) {
                future.completeExceptionally(e);
            }
        }
        return future;
    }

    private void sendCancel(int id) {
        try {
            connection.write(new Cancel(id));
        } catch (ConnectionClosedException e) {
            // The server cancels a closed connection's calls by itself.
            LOG.log(Level.DEBUG, "not sending CANCEL for request {0}: connection closed", id);
        }
    }

    /** Called on the event loop for each Response that arrives. */
    void onResponse(Response response) {
        CompletableFuture<Response> future = pending.remove(response.requestId());
        if (future == null) {
            // Cancelled by the caller, duplicated, or never issued. Not fatal: with deadlines, late answers are normal.
            LOG.log(Level.DEBUG, "dropping response for request {0}: not pending", response.requestId());
            return;
        }
        completer.execute(() -> future.complete(response));
    }

    /** Refuses new calls and fails every pending one with ConnectionClosedException. Idempotent. */
    void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        var cause = new ConnectionClosedException("connection to " + connection.remoteAddress() + " closed");
        for (Integer id : pending.keySet()) {
            CompletableFuture<Response> future = pending.remove(id);
            if (future != null) {
                completer.execute(() -> future.completeExceptionally(cause));
            }
        }
    }

    // For tests: the next call takes the first free id after lastId.
    void restartIdsAfter(int lastId) {
        synchronized (lock) {
            nextId = lastId;
        }
    }

    int inFlight() {
        return pending.size();
    }
}
