package dev.rpc.core;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/** Deadline timers: one daemon thread that only hands work off, and calls answered in time leave nothing behind. */
final class Timers {

    private Timers() {
    }

    static ScheduledExecutorService create(String name) {
        var timers = new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().name(name).daemon().factory());
        timers.setRemoveOnCancelPolicy(true);
        return timers;
    }
}
