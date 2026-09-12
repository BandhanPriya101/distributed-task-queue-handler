package com.taskqueue.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe counters describing the health of the queue over its lifetime.
 * Every counter is an {@link AtomicInteger} so workers can update it
 * concurrently without external locking.
 */
public class QueueMetrics {

    private final AtomicInteger submitted = new AtomicInteger(0);
    private final AtomicInteger completed = new AtomicInteger(0);
    private final AtomicInteger failed = new AtomicInteger(0);
    private final AtomicInteger retried = new AtomicInteger(0);
    private final AtomicInteger deadLettered = new AtomicInteger(0);
    private final AtomicInteger cancelled = new AtomicInteger(0);

    void incrementSubmitted() { submitted.incrementAndGet(); }
    void incrementCompleted() { completed.incrementAndGet(); }
    void incrementFailed() { failed.incrementAndGet(); }
    void incrementRetried() { retried.incrementAndGet(); }
    void incrementDeadLettered() { deadLettered.incrementAndGet(); }
    void incrementCancelled() { cancelled.incrementAndGet(); }

    public int getSubmitted() { return submitted.get(); }
    public int getCompleted() { return completed.get(); }
    public int getFailed() { return failed.get(); }
    public int getRetried() { return retried.get(); }
    public int getDeadLettered() { return deadLettered.get(); }
    public int getCancelled() { return cancelled.get(); }

    public String snapshot() {
        return String.format(
            "submitted=%d, completed=%d, failed=%d, retried=%d, deadLettered=%d, cancelled=%d",
            submitted.get(), completed.get(), failed.get(), retried.get(),
            deadLettered.get(), cancelled.get()
        );
    }
}
