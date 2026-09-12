package com.taskqueue.handler;

/**
 * Business logic that processes the payload of a task of a given type.
 * Registered against a "type" string on the {@code TaskQueueService}.
 *
 * Throwing any exception signals failure and triggers the retry / dead-letter
 * pipeline in the queue service.
 */
@FunctionalInterface
public interface TaskHandler {
    void handle(Object payload) throws Exception;
}
