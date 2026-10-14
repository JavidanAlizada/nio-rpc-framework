package dev.rpc.core;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * A point in time on this JVM's System.nanoTime() clock. It never crosses the wire as a timestamp: a Request carries
 * the time left, because two machines' clocks aren't comparable. Immutable.
 */
public final class Deadline {

    private final long nanoTime;

    private Deadline(long nanoTime) {
        this.nanoTime = nanoTime;
    }

    public static Deadline after(Duration timeout) {
        return afterNanos(timeout.toNanos());
    }

    public static Deadline afterNanos(long nanos) {
        return new Deadline(System.nanoTime() + nanos);
    }

    /** The earlier of two deadlines, where null means none. */
    public static Deadline earliest(Deadline a, Deadline b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        // Compared by difference, which stays right when nanoTime wraps.
        return a.nanoTime - b.nanoTime <= 0 ? a : b;
    }

    /** Time left; zero or negative once it has passed. */
    public long remainingNanos() {
        return nanoTime - System.nanoTime();
    }

    public boolean isExpired() {
        return remainingNanos() <= 0;
    }

    @Override
    public String toString() {
        return "Deadline[" + TimeUnit.NANOSECONDS.toMillis(remainingNanos()) + " ms left]";
    }
}
