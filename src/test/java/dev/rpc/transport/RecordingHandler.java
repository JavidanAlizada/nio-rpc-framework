package dev.rpc.transport;

import dev.rpc.protocol.Frame;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/** Test handler: records frames and closes, and can react to frames (e.g. echo them back). */
final class RecordingHandler implements ConnectionHandler {

    final BlockingQueue<Frame> frames = new LinkedBlockingQueue<>();
    final AtomicInteger closeCount = new AtomicInteger();
    final CompletableFuture<Throwable> closed = new CompletableFuture<>();
    final CompletableFuture<Connection> firstConnection = new CompletableFuture<>();
    private final BiConsumer<Connection, Frame> onFrame;

    RecordingHandler() {
        this((c, f) -> { });
    }

    RecordingHandler(BiConsumer<Connection, Frame> onFrame) {
        this.onFrame = onFrame;
    }

    @Override
    public void onFrame(Connection connection, Frame frame) {
        firstConnection.complete(connection);
        frames.add(frame);
        onFrame.accept(connection, frame);
    }

    @Override
    public void onClosed(Connection connection, Throwable cause) {
        closeCount.incrementAndGet();
        // CompletableFuture can't hold null, so a local close is recorded as this marker.
        closed.complete(cause == null ? LOCAL_CLOSE : cause);
    }

    Frame take(Duration timeout) throws InterruptedException {
        Frame f = frames.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (f == null) {
            throw new AssertionError("no frame within " + timeout);
        }
        return f;
    }

    List<Frame> take(int n, Duration timeout) throws InterruptedException {
        List<Frame> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(take(timeout));
        }
        return out;
    }

    static final Throwable LOCAL_CLOSE = new Throwable("closed locally");
}
