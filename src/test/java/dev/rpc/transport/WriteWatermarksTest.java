package dev.rpc.transport;

import static dev.rpc.transport.WriteWatermarks.Change.NONE;
import static dev.rpc.transport.WriteWatermarks.Change.UNWRITABLE;
import static dev.rpc.transport.WriteWatermarks.Change.WRITABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class WriteWatermarksTest {

    private final WriteWatermarks w = new WriteWatermarks(100, 1_000, 10_000);

    @Test
    void staysWritableUpToAndIncludingHigh() {
        assertEquals(NONE, w.update(0));
        assertEquals(NONE, w.update(999));
        assertEquals(NONE, w.update(1_000));
        assertEquals(UNWRITABLE, w.update(1_001));
    }

    @Test
    void becomesWritableOnlyBelowLow() {
        assertEquals(UNWRITABLE, w.update(5_000));
        // Anywhere in the band between low and high keeps the current state: no flapping around one threshold.
        assertEquals(NONE, w.update(999));
        assertEquals(NONE, w.update(1_001));
        assertEquals(NONE, w.update(100));
        assertEquals(WRITABLE, w.update(99));
        assertEquals(NONE, w.update(500));
    }

    @Test
    void eachCrossingIsReportedOnce() {
        assertEquals(UNWRITABLE, w.update(2_000));
        assertEquals(NONE, w.update(3_000));
        assertEquals(WRITABLE, w.update(0));
        assertEquals(NONE, w.update(0));
        assertEquals(UNWRITABLE, w.update(1_001));
    }

    @Test
    void crossingHighNeedsToStartAtOrBelowIt() {
        assertTrue(w.crossedHigh(0, 1_001));
        assertTrue(w.crossedHigh(1_000, 1_001));
        assertFalse(w.crossedHigh(0, 1_000));
        assertFalse(w.crossedHigh(1_001, 2_000), "already above: some earlier write crossed");
    }

    @Test
    void crossingTheLimit() {
        assertTrue(w.crossedLimit(9_000, 10_001));
        assertFalse(w.crossedLimit(9_000, 10_000));
        assertFalse(w.crossedLimit(10_001, 12_000));
    }

    @Test
    void consecutiveAddsCrossEachThresholdExactlyOnce() {
        // Any order of adds: exactly one add crosses high and exactly one crosses the limit.
        long pending = 0;
        int highCrossings = 0;
        int limitCrossings = 0;
        for (int size : new int[] {300, 1, 699, 1, 4_000, 4_999, 1, 1, 50_000}) {
            long before = pending;
            pending += size;
            highCrossings += w.crossedHigh(before, pending) ? 1 : 0;
            limitCrossings += w.crossedLimit(before, pending) ? 1 : 0;
        }
        assertEquals(1, highCrossings);
        assertEquals(1, limitCrossings);
    }

    @Test
    void rejectsUnorderedThresholds() {
        assertThrows(IllegalArgumentException.class, () -> new WriteWatermarks(0, 10, 100));
        assertThrows(IllegalArgumentException.class, () -> new WriteWatermarks(10, 10, 100));
        assertThrows(IllegalArgumentException.class, () -> new WriteWatermarks(10, 100, 100));
    }
}
