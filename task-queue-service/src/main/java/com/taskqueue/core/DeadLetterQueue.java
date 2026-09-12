package com.taskqueue.core;

import com.taskqueue.model.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Holds tasks that exhausted their retry budget (or had no registered
 * handler). Backed by a {@link ConcurrentLinkedQueue} since it only needs
 * simple thread-safe FIFO storage and inspection, not priority ordering.
 */
public class DeadLetterQueue {

    private final ConcurrentLinkedQueue<Task> tasks = new ConcurrentLinkedQueue<>();

    public void add(Task task) {
        tasks.add(task);
    }

    public List<Task> snapshot() {
        return new ArrayList<>(tasks);
    }

    public int size() {
        return tasks.size();
    }

    /** Removes and returns a task for manual re-submission/inspection, or null if empty. */
    public Task poll() {
        return tasks.poll();
    }
}
