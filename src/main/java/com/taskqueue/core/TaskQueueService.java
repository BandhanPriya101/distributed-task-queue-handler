package com.taskqueue.core;

import com.taskqueue.handler.TaskHandler;
import com.taskqueue.model.Task;
import com.taskqueue.model.TaskStatus;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A self-contained, in-memory, multithreaded task queue that mimics the core
 * mechanics of production message queues (RabbitMQ / SQS / Celery):
 *
 *  - Priority ordering            -> {@link PriorityBlockingQueue}
 *  - O(1) task lookup / status    -> {@link ConcurrentHashMap}
 *  - Lock-free counters / ids     -> {@link java.util.concurrent.atomic.AtomicInteger} / {@link AtomicLong}
 *  - Concurrent worker pool       -> {@link ExecutorService}
 *  - Delayed & retry scheduling   -> {@link ScheduledExecutorService}
 *
 * Usage:
 * <pre>{@code
 * TaskQueueService queue = new TaskQueueService(4);
 * queue.registerHandler("EMAIL", payload -> send((String) payload));
 * queue.start();
 * String id = queue.submit("EMAIL", "hello@example.com", 5);
 * ...
 * queue.shutdown(10, TimeUnit.SECONDS);
 * }</pre>
 */
public class TaskQueueService {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long BASE_BACKOFF_MILLIS = 500L;
    private static final long MAX_BACKOFF_MILLIS = 15_000L;

    private final PriorityBlockingQueue<Task> readyQueue = new PriorityBlockingQueue<>();
    private final ConcurrentHashMap<String, Task> registry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    private final AtomicLong sequenceGenerator = new AtomicLong(0);
    private final QueueMetrics metrics = new QueueMetrics();
    private final DeadLetterQueue deadLetterQueue = new DeadLetterQueue();

    private final int workerCount;
    private final ExecutorService workerPool;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    public TaskQueueService(int workerCount) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be >= 1");
        }
        this.workerCount = workerCount;
        this.workerPool = Executors.newFixedThreadPool(workerCount, new NamedThreadFactory("tq-worker"));
        this.scheduler = Executors.newScheduledThreadPool(
                Math.max(2, workerCount / 2), new NamedThreadFactory("tq-scheduler"));
    }

    // ------------------------------------------------------------------
    // Setup
    // ------------------------------------------------------------------

    /** Registers (or replaces) the handler invoked for tasks of the given type. */
    public void registerHandler(String type, TaskHandler handler) {
        handlers.put(type, handler);
    }

    /** Starts the worker threads. Safe to call once after handlers are registered. */
    public synchronized void start() {
        if (running) return;
        running = true;
        for (int i = 0; i < workerCount; i++) {
            workerPool.submit(this::workerLoop);
        }
    }

    // ------------------------------------------------------------------
    // Submission
    // ------------------------------------------------------------------

    public String submit(String type, Object payload, int priority) {
        return submit(type, payload, priority, DEFAULT_MAX_RETRIES, 0L);
    }

    public String submit(String type, Object payload, int priority, int maxRetries) {
        return submit(type, payload, priority, maxRetries, 0L);
    }

    /**
     * Submits a task for execution.
     *
     * @param type       routing key matched against a registered handler
     * @param payload    arbitrary data passed to the handler
     * @param priority   higher value = served earlier
     * @param maxRetries number of retries permitted after the first attempt fails
     * @param delayMillis if > 0, the task only becomes eligible for execution after this delay
     * @return the generated task id, usable for status lookups / cancellation
     */
    public String submit(String type, Object payload, int priority, int maxRetries, long delayMillis) {
        String id = UUID.randomUUID().toString();
        long sequence = sequenceGenerator.incrementAndGet();
        long scheduledAt = System.currentTimeMillis() + Math.max(0, delayMillis);

        Task task = new Task(id, type, payload, priority, sequence, maxRetries, scheduledAt);
        registry.put(id, task);
        metrics.incrementSubmitted();

        if (delayMillis > 0) {
            task.setStatus(TaskStatus.SCHEDULED);
            scheduler.schedule(() -> enqueue(task), delayMillis, TimeUnit.MILLISECONDS);
        } else {
            enqueue(task);
        }
        return id;
    }

    private void enqueue(Task task) {
        if (task.getStatus() == TaskStatus.CANCELLED) return;
        task.setStatus(TaskStatus.QUEUED);
        readyQueue.offer(task);
    }

    // ------------------------------------------------------------------
    // Worker loop
    // ------------------------------------------------------------------

    private void workerLoop() {
        while (running) {
            try {
                Task task = readyQueue.poll(500, TimeUnit.MILLISECONDS);
                if (task == null) continue;
                if (task.getStatus() == TaskStatus.CANCELLED) continue;
                process(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception unexpected) {
                // Defensive: a worker thread must never die from an unhandled error.
                System.err.println("[tq-worker] unexpected error: " + unexpected);
            }
        }
    }

    private void process(Task task) {
        task.setStatus(TaskStatus.PROCESSING);
        int attempt = task.incrementAttempts();

        TaskHandler handler = handlers.get(task.getType());
        if (handler == null) {
            task.setLastError("No handler registered for type '" + task.getType() + "'");
            moveToDeadLetter(task);
            return;
        }

        try {
            handler.handle(task.getPayload());
            task.setStatus(TaskStatus.COMPLETED);
            metrics.incrementCompleted();
        } catch (Exception e) {
            task.setLastError(e.getMessage() == null ? e.toString() : e.getMessage());
            if (attempt < task.getMaxRetries()) {
                scheduleRetry(task, attempt);
            } else {
                moveToDeadLetter(task);
            }
        }
    }

    private void scheduleRetry(Task task, int attempt) {
        task.setStatus(TaskStatus.RETRY_SCHEDULED);
        metrics.incrementRetried();
        long backoff = computeBackoffMillis(attempt);
        scheduler.schedule(() -> enqueue(task), backoff, TimeUnit.MILLISECONDS);
    }

    /** Exponential backoff with a cap, e.g. 500ms, 1s, 2s, 4s ... up to MAX_BACKOFF_MILLIS. */
    private long computeBackoffMillis(int attempt) {
        long delay = BASE_BACKOFF_MILLIS * (1L << Math.min(attempt, 10));
        return Math.min(delay, MAX_BACKOFF_MILLIS);
    }

    private void moveToDeadLetter(Task task) {
        task.setStatus(TaskStatus.DEAD_LETTER);
        metrics.incrementFailed();
        metrics.incrementDeadLettered();
        deadLetterQueue.add(task);
    }

    // ------------------------------------------------------------------
    // Inspection / control
    // ------------------------------------------------------------------

    public Optional<Task> getTask(String id) {
        return Optional.ofNullable(registry.get(id));
    }

    public Optional<TaskStatus> getStatus(String id) {
        return getTask(id).map(Task::getStatus);
    }

    /** Attempts to cancel a task that has not started processing yet. */
    public boolean cancel(String id) {
        Task task = registry.get(id);
        if (task == null) return false;
        TaskStatus status = task.getStatus();
        if (status == TaskStatus.PENDING || status == TaskStatus.SCHEDULED || status == TaskStatus.QUEUED
                || status == TaskStatus.RETRY_SCHEDULED) {
            task.setStatus(TaskStatus.CANCELLED);
            readyQueue.remove(task);
            metrics.incrementCancelled();
            return true;
        }
        return false;
    }

    public int pendingCount() {
        return readyQueue.size();
    }

    public QueueMetrics getMetrics() {
        return metrics;
    }

    public DeadLetterQueue getDeadLetterQueue() {
        return deadLetterQueue;
    }

    public List<Task> deadLetterSnapshot() {
        return deadLetterQueue.snapshot();
    }

    /** Re-queues a task pulled from the dead-letter queue, resetting its attempt counter is not done on purpose:
     *  callers can inspect lastError before deciding to resubmit fresh via {@link #submit}. */
    public boolean resubmitFromDeadLetter(String id) {
        for (Task task : deadLetterQueue.snapshot()) {
            if (task.getId().equals(id)) {
                task.setStatus(TaskStatus.QUEUED);
                readyQueue.offer(task);
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------
    // Shutdown
    // ------------------------------------------------------------------

    /** Stops accepting new work from the internal loops and drains gracefully. */
    public void shutdown(long timeout, TimeUnit unit) {
        running = false;
        scheduler.shutdown();
        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(timeout, unit)) {
                workerPool.shutdownNow();
            }
            if (!scheduler.awaitTermination(timeout, unit)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** Simple named-thread factory so stack traces/thread dumps are readable. */
    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicLong counter = new AtomicLong(0);

        NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
