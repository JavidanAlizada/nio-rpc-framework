package dev.rpc.transport;

/**
 * The backpressure arithmetic for one connection's write queue, kept apart from the sockets so it can be tested on
 * its own. Above high the connection stops being writable; it becomes writable again only once below low, so a
 * queue hovering around one threshold doesn't flap. Above limit the connection is closed.
 *
 * The crossing checks are pure and safe from any thread. update() keeps state and is called by the loop only.
 */
final class WriteWatermarks {

    enum Change { NONE, UNWRITABLE, WRITABLE }

    private final long low;
    private final long high;
    private final long limit;

    // Loop thread only.
    private boolean writable = true;

    WriteWatermarks(long low, long high, long limit) {
        if (!(0 < low && low < high && high < limit)) {
            throw new IllegalArgumentException("need 0 < low < high < limit, got " + low + ", " + high + ", " + limit);
        }
        this.low = low;
        this.high = high;
        this.limit = limit;
    }

    static WriteWatermarks of(TransportConfig config) {
        return new WriteWatermarks(config.lowWatermark(), config.highWatermark(), config.writeQueueLimit());
    }

    /**
     * Whether going from before to after pending bytes crossed the high watermark upwards. pendingBytes is changed
     * with getAndAdd, so each upward crossing belongs to exactly one write.
     */
    boolean crossedHigh(long before, long after) {
        return before <= high && after > high;
    }

    boolean crossedLimit(long before, long after) {
        return before <= limit && after > limit;
    }

    /** Re-evaluates writability for the current pending bytes and says whether it changed. */
    Change update(long pending) {
        if (writable && pending > high) {
            writable = false;
            return Change.UNWRITABLE;
        }
        if (!writable && pending < low) {
            writable = true;
            return Change.WRITABLE;
        }
        return Change.NONE;
    }

    long limit() {
        return limit;
    }
}
