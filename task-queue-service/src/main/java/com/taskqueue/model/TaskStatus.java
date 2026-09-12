package com.taskqueue.model;

/**
 * Lifecycle states a {@link Task} can move through.
 *
 * PENDING -> SCHEDULED (if delayed) -> QUEUED -> PROCESSING -> COMPLETED
 *                                                        \-> RETRY_SCHEDULED -> QUEUED (loop)
 *                                                        \-> DEAD_LETTER (retries exhausted)
 * Any non-terminal state can transition to CANCELLED.
 */
public enum TaskStatus {
    PENDING,
    SCHEDULED,
    QUEUED,
    PROCESSING,
    RETRY_SCHEDULED,
    COMPLETED,
    FAILED,
    DEAD_LETTER,
    CANCELLED
}
