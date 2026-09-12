package com.taskqueue.model;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An immutable-ish unit of work submitted to the queue.
 *
 * Ordering (via {@link #compareTo(Task)}) is by descending priority first
 * (higher priority value = served first), then by ascending sequence number
 * so that tasks of equal priority are processed FIFO. This is what allows the
 * task to sit correctly inside a {@link java.util.concurrent.PriorityBlockingQueue}.
 */
public class Task implements Comparable<Task> {

    private final String id;
    private final String type;
    private final Object payload;
    private final int priority;
    private final long sequence;
    private final int maxRetries;
    private final long createdAt;

    private volatile long scheduledAt;
    private volatile TaskStatus status;
    private volatile String lastError;
    private final AtomicInteger attempts = new AtomicInteger(0);

    public Task(String id, String type, Object payload, int priority,
                long sequence, int maxRetries, long scheduledAt) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.priority = priority;
        this.sequence = sequence;
        this.maxRetries = maxRetries;
        this.createdAt = System.currentTimeMillis();
        this.scheduledAt = scheduledAt;
        this.status = TaskStatus.PENDING;
    }

    public String getId() { return id; }
    public String getType() { return type; }
    public Object getPayload() { return payload; }
    public int getPriority() { return priority; }
    public long getSequence() { return sequence; }
    public int getMaxRetries() { return maxRetries; }
    public long getCreatedAt() { return createdAt; }
    public long getScheduledAt() { return scheduledAt; }
    public void setScheduledAt(long scheduledAt) { this.scheduledAt = scheduledAt; }

    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public int getAttempts() { return attempts.get(); }
    public int incrementAttempts() { return attempts.incrementAndGet(); }

    @Override
    public int compareTo(Task other) {
        int byPriority = Integer.compare(other.priority, this.priority); // higher priority wins
        if (byPriority != 0) return byPriority;
        return Long.compare(this.sequence, other.sequence); // FIFO tie-break
    }

    @Override
    public String toString() {
        return String.format(
            "Task{id=%s, type=%s, priority=%d, status=%s, attempts=%d/%d}",
            id, type, priority, status, attempts.get(), maxRetries
        );
    }
}
