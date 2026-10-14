package dev.rpc.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A call the server is working on. It ends in exactly one reply, or none if it was cancelled: whoever claims the
 * reply first (the result, a CANCEL, the deadline, the connection closing) decides.
 *
 * cancel() and the virtual thread publishing an async method's future each write their own volatile field before
 * reading the other's, so at least one of them sees both and the future gets cancelled.
 */
final class RunningCall {

    private final int requestId;
    private final AtomicBoolean replied = new AtomicBoolean();
    private volatile Future<?> task;
    private volatile CompletableFuture<?> async;
    private volatile boolean cancelled;

    RunningCall(int requestId) {
        this.requestId = requestId;
    }

    int requestId() {
        return requestId;
    }

    void started(Future<?> task) {
        this.task = task;
        if (cancelled) {
            task.cancel(true);
        }
    }

    /** Called on the call's thread when the method returned a future instead of a value. */
    void returnedFuture(CompletableFuture<?> future) {
        this.async = future;
        if (cancelled) {
            future.cancel(false);
        }
    }

    /** True for exactly one caller; only that caller may reply. */
    boolean claimReply() {
        return replied.compareAndSet(false, true);
    }

    /** Interrupts the call's thread and cancels its future. Cooperative: a method that ignores both runs on. */
    void cancel() {
        cancelled = true;
        Future<?> t = task;
        if (t != null) {
            t.cancel(true);
        }
        CompletableFuture<?> a = async;
        if (a != null) {
            a.cancel(false);
        }
    }
}
