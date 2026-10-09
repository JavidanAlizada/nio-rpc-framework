package dev.rpc.transport;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with full jitter: attempt n (from 0) waits a uniformly random time in [0, min(max, base * 2^n)].
 * Full jitter spreads out clients that lost the same server at the same moment, so they don't all come back at once.
 */
record Backoff(Duration base, Duration max) {

    Duration delay(int attempt, RandomGenerator random) {
        long baseNanos = base.toNanos();
        long maxNanos = max.toNanos();
        int shift = Math.min(Math.max(attempt, 0), 62);
        // base << shift would overflow long before it mattered; once it passes max, max is the ceiling anyway.
        long ceiling = baseNanos > (maxNanos >> shift) ? maxNanos : baseNanos << shift;
        return Duration.ofNanos(random.nextLong(ceiling + 1));
    }
}
