package dev.rpc.core;

/**
 * The server call the current thread is running, if any. Calls a service method makes through an RpcClient inherit
 * its deadline from here, so a chain of calls shares the first caller's deadline.
 *
 * Each server call has its own virtual thread, which makes a ThreadLocal exact. Work the method hands to another
 * thread doesn't see the context; pass the deadline along explicitly there. (ScopedValue would fit better, but it's
 * a preview API in Java 21.)
 */
public final class RpcContext {

    private static final ThreadLocal<Deadline> DEADLINE = new ThreadLocal<>();

    private RpcContext() {
    }

    /** The deadline of the server call running on this thread, or null if there is none. */
    public static Deadline deadline() {
        return DEADLINE.get();
    }

    static void enter(Deadline deadline) {
        DEADLINE.set(deadline);
    }

    static void exit() {
        DEADLINE.remove();
    }
}
