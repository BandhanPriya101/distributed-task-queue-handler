package com.taskqueue;

import com.taskqueue.core.TaskQueueService;
import com.taskqueue.model.Task;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runnable demo that exercises every feature of the queue:
 * priority ordering, delayed scheduling, automatic retries with backoff,
 * a permanently-failing task that lands in the dead-letter queue,
 * cancellation, and graceful shutdown.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        TaskQueueService queue = new TaskQueueService(4);

        AtomicInteger emailsSent = new AtomicInteger(0);

        // A well-behaved handler.
        queue.registerHandler("EMAIL", payload -> {
            Thread.sleep(150);
            emailsSent.incrementAndGet();
            log("EMAIL sent -> " + payload);
        });

        // A handler that is flaky the first couple of times, then succeeds.
        queue.registerHandler("IMAGE_RESIZE", payload -> {
            Thread.sleep(200);
            if (ThreadLocalRandom.current().nextInt(100) < 60) {
                throw new RuntimeException("transient resize failure for " + payload);
            }
            log("IMAGE_RESIZE done -> " + payload);
        });

        // A handler that always fails, to demonstrate the dead-letter queue.
        queue.registerHandler("REPORT", payload -> {
            Thread.sleep(100);
            throw new RuntimeException("report generator unavailable for " + payload);
        });

        queue.start();

        log("Submitting tasks...");

        // Priority demo: higher number = served first. These are submitted
        // low-to-high so we can visually confirm re-ordering in the output.
        queue.submit("EMAIL", "low-priority-newsletter@example.com", 1);
        queue.submit("EMAIL", "normal-receipt@example.com", 5);
        queue.submit("EMAIL", "urgent-password-reset@example.com", 10);

        // Retry demo.
        for (int i = 1; i <= 5; i++) {
            queue.submit("IMAGE_RESIZE", "photo-" + i + ".jpg", 3, 4, 0);
        }

        // Dead-letter demo: only 1 retry allowed, handler always throws.
        String reportTaskId = queue.submit("REPORT", "monthly-sales-report", 4, 1, 0);

        // Delayed task demo: scheduled 2 seconds into the future.
        queue.submit("EMAIL", "delayed-digest@example.com", 5, 3, 2000);

        // Cancellation demo: submitted with a delay, then cancelled before it fires.
        String toCancel = queue.submit("EMAIL", "should-never-send@example.com", 2, 3, 3000);
        boolean cancelled = queue.cancel(toCancel);
        log("Cancellation requested for delayed task, success=" + cancelled);

        // Let the queue drain. In a real service this would run indefinitely.
        Thread.sleep(6000);

        log("---- Metrics ----");
        log(queue.getMetrics().snapshot());

        log("---- Dead-letter queue ----");
        for (Task t : queue.deadLetterSnapshot()) {
            log(t + " lastError=" + t.getLastError());
        }
        System.out.println("Report task final status: " + queue.getStatus(reportTaskId).orElse(null));

        queue.shutdown(5, TimeUnit.SECONDS);
        log("Shutdown complete. Emails sent this run: " + emailsSent.get());
    }

    private static void log(String message) {
        System.out.printf("[%s] %s%n", Thread.currentThread().getName(), message);
    }
}
