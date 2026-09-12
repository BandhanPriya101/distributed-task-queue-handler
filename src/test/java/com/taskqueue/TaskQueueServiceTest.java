package com.taskqueue;

import com.taskqueue.core.TaskQueueService;
import com.taskqueue.model.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TaskQueueServiceTest {

    private TaskQueueService queue;

    @BeforeEach
    void setUp() {
        queue = new TaskQueueService(2);
    }

    @AfterEach
    void tearDown() {
        queue.shutdown(5, TimeUnit.SECONDS);
    }

    @Test
    void higherPriorityTasksAreProcessedFirst() throws InterruptedException {
        // Use a single worker so ordering is deterministic.
        TaskQueueService singleWorkerQueue = new TaskQueueService(1);
        List<Integer> executionOrder = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        singleWorkerQueue.registerHandler("ORDERED", payload -> {
            executionOrder.add((Integer) payload);
            latch.countDown();
        });

        // Submit low priority first, with no delay, so all three land in the
        // queue before the single worker starts draining it. Starting the
        // worker only afterward guarantees priority (not arrival order)
        // decides execution order.
        singleWorkerQueue.submit("ORDERED", 1, 1, 3, 0);
        singleWorkerQueue.submit("ORDERED", 2, 5, 3, 0);
        singleWorkerQueue.submit("ORDERED", 3, 10, 3, 0);
        singleWorkerQueue.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(3, 2, 1), executionOrder);
        singleWorkerQueue.shutdown(5, TimeUnit.SECONDS);
    }

    @Test
    void taskSucceedsAfterTransientFailures() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);

        queue.registerHandler("FLAKY", payload -> {
            int call = callCount.incrementAndGet();
            if (call < 3) {
                throw new RuntimeException("fail on purpose, call " + call);
            }
        });

        String id = queue.submit("FLAKY", "payload", 1, 5, 0);
        queue.start();

        // Poll for terminal status rather than racing a latch fired from
        // inside the handler, since the handler returns slightly before the
        // worker thread flips the task's status to COMPLETED.
        long deadline = System.currentTimeMillis() + 10_000;
        TaskStatus status;
        do {
            status = queue.getStatus(id).orElseThrow();
            if (status == TaskStatus.COMPLETED || status == TaskStatus.DEAD_LETTER) break;
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);

        assertEquals(TaskStatus.COMPLETED, status);
        assertEquals(3, callCount.get());
    }

    @Test
    void taskExceedingMaxRetriesLandsInDeadLetterQueue() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        queue.registerHandler("ALWAYS_FAILS", payload -> {
            throw new RuntimeException("always fails");
        });

        String id = queue.submit("ALWAYS_FAILS", "payload", 1, 2, 0);
        queue.start();

        // Poll until the task reaches a terminal state.
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (queue.getStatus(id).orElseThrow() == TaskStatus.DEAD_LETTER) {
                latch.countDown();
                break;
            }
            Thread.sleep(50);
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(1, queue.getDeadLetterQueue().size());
    }

    @Test
    void cancelledTaskNeverExecutes() throws InterruptedException {
        AtomicInteger executed = new AtomicInteger(0);
        queue.registerHandler("CANCELLABLE", payload -> executed.incrementAndGet());

        String id = queue.submit("CANCELLABLE", "payload", 1, 3, 2000);
        boolean cancelled = queue.cancel(id);
        queue.start();

        Thread.sleep(3000);

        assertTrue(cancelled);
        assertEquals(0, executed.get());
        assertEquals(TaskStatus.CANCELLED, queue.getStatus(id).orElseThrow());
    }

    @Test
    void unknownTaskTypeGoesStraightToDeadLetter() throws InterruptedException {
        String id = queue.submit("NO_HANDLER_REGISTERED", "payload", 1, 3, 0);
        queue.start();

        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline
                && queue.getStatus(id).orElseThrow() != TaskStatus.DEAD_LETTER) {
            Thread.sleep(50);
        }

        assertEquals(TaskStatus.DEAD_LETTER, queue.getStatus(id).orElseThrow());
    }
}
