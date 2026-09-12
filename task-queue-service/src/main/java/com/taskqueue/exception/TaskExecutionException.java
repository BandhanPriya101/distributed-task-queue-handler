package com.taskqueue.exception;

/** Wraps a failure that occurred while a handler was processing a task. */
public class TaskExecutionException extends RuntimeException {
    public TaskExecutionException(String message) {
        super(message);
    }

    public TaskExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
