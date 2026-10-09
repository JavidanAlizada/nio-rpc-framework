package dev.rpc.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class BackoffTest {

    private final Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofSeconds(10));

    /** Always picks the top of the range it's given, so delay() returns its ceiling. */
    private static final RandomGenerator HIGHEST = new RandomGenerator() {
        @Override
        public long nextLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long nextLong(long bound) {
            return bound - 1;
        }
    };

    private static final RandomGenerator LOWEST = new RandomGenerator() {
        @Override
        public long nextLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long nextLong(long bound) {
            return 0;
        }
    };

    @Test
    void ceilingDoublesFromBaseUntilItHitsMax() {
        assertEquals(Duration.ofMillis(100), backoff.delay(0, HIGHEST));
        assertEquals(Duration.ofMillis(200), backoff.delay(1, HIGHEST));
        assertEquals(Duration.ofMillis(400), backoff.delay(2, HIGHEST));
        assertEquals(Duration.ofMillis(6_400), backoff.delay(6, HIGHEST));
        assertEquals(Duration.ofSeconds(10), backoff.delay(7, HIGHEST));
        assertEquals(Duration.ofSeconds(10), backoff.delay(8, HIGHEST));
    }

    @Test
    void fullJitterCanWaitNothingAtAll() {
        assertEquals(Duration.ZERO, backoff.delay(0, LOWEST));
        assertEquals(Duration.ZERO, backoff.delay(20, LOWEST));
    }

    @Test
    void hugeAttemptCountsStayAtMaxWithoutOverflowing() {
        assertEquals(Duration.ofSeconds(10), backoff.delay(63, HIGHEST));
        assertEquals(Duration.ofSeconds(10), backoff.delay(1_000, HIGHEST));
        assertEquals(Duration.ofSeconds(10), backoff.delay(Integer.MAX_VALUE, HIGHEST));
    }

    @Test
    void randomDelaysStayWithinTheirCeiling() {
        var random = new SplittableRandom(42);
        for (int attempt = 0; attempt < 12; attempt++) {
            long ceiling = Math.min(100L << attempt, 10_000L);
            for (int i = 0; i < 1_000; i++) {
                Duration d = backoff.delay(attempt, random);
                assertTrue(!d.isNegative() && d.toMillis() <= ceiling, attempt + ": " + d);
            }
        }
    }

    @Test
    void baseEqualToMaxIsAFlatRange() {
        var flat = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(1));
        assertEquals(Duration.ofSeconds(1), flat.delay(0, HIGHEST));
        assertEquals(Duration.ofSeconds(1), flat.delay(30, HIGHEST));
    }
}
