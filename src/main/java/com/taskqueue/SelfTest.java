package com.taskqueue;

import com.taskqueue.core.TaskQueueService;
import com.taskqueue.model.TaskStatus;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Plain-Java assertion suite (no JUnit / Maven / network access required).
 * Mirrors {@code TaskQueueServiceTest} so the behavior can be verified with
 * nothing more than a JDK:
 *
 *   javac -d out $(find src/main/java -name "*.java")
 *   java -cp out com.taskqueue.SelfTest
 */
public class SelfTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws InterruptedException {
        test("higherPriorityTasksAreProcessedFirst", SelfTest::higherPriorityTasksAreProcessedFirst);
        test("taskSucceedsAfterTransientFailures", SelfTest::taskSucceedsAfterTransientFailures);
        test("taskExceedingMaxRetriesLandsInDeadLetterQueue", SelfTest::taskExceedingMaxRetriesLandsInDeadLetterQueue);
        test("cancelledTaskNeverExecutes", SelfTest::cancelledTaskNeverExecutes);
        test("unknownTaskTypeGoesStraightToDeadLetter", SelfTest::unknownTaskTypeGoesStraightToDeadLetter);

        System.out.println();
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private interface Check {
        void run() throws Exception;
    }

    private static void test(String name, Check check) {
        try {
            check.run();
            System.out.println("[PASS] " + name);
            passed++;
        } catch (Throwable t) {
            System.out.println("[FAIL] " + name + " -> " + t);
            t.printStackTrace(System.out);
            failed++;
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void assertEquals(Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError("expected=" + expected + " actual=" + actual);
        }
    }

    // ------------------------------------------------------------------

    private static void higherPriorityTasksAreProcessedFirst() throws Exception {
        TaskQueueService queue = new TaskQueueService(1); // single worker for determinism
        List<Integer> order = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        queue.registerHandler("ORDERED", payload -> {
            order.add((Integer) payload);
            latch.countDown();
        });

        // Submit with no delay so all three land in the PriorityBlockingQueue
        // BEFORE the single worker starts draining it. Starting the worker
        // only after all offers guarantees priority (not arrival order or
        // scheduler-thread timing) decides execution order.
        queue.submit("ORDERED", 1, 1, 3, 0);
        queue.submit("ORDERED", 2, 5, 3, 0);
        queue.submit("ORDERED", 3, 10, 3, 0);
        queue.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "tasks did not complete in time");
        assertEquals(List.of(3, 2, 1), order);
        queue.shutdown(5, TimeUnit.SECONDS);
    }

    private static void taskSucceedsAfterTransientFailures() throws Exception {
        TaskQueueService queue = new TaskQueueService(2);
        AtomicInteger callCount = new AtomicInteger(0);

        queue.registerHandler("FLAKY", payload -> {
            int call = callCount.incrementAndGet();
            if (call < 3) throw new RuntimeException("fail on purpose, call " + call);
        });

        String id = queue.submit("FLAKY", "payload", 1, 5, 0);
        queue.start();

        // Poll for the terminal status rather than racing a latch fired
        // from inside the handler (the handler returns slightly before the
        // worker thread flips the task's status to COMPLETED).
        long deadline = System.currentTimeMillis() + 10_000;
        TaskStatus status;
        do {
            status = queue.getStatus(id).orElseThrow();
            if (status == TaskStatus.COMPLETED || status == TaskStatus.DEAD_LETTER) break;
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);

        assertEquals(TaskStatus.COMPLETED, status);
        assertEquals(3, callCount.get());
        queue.shutdown(5, TimeUnit.SECONDS);
    }

    private static void taskExceedingMaxRetriesLandsInDeadLetterQueue() throws Exception {
        TaskQueueService queue = new TaskQueueService(2);
        queue.registerHandler("ALWAYS_FAILS", payload -> {
            throw new RuntimeException("always fails");
        });

        String id = queue.submit("ALWAYS_FAILS", "payload", 1, 2, 0);
        queue.start();

        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && queue.getStatus(id).orElseThrow() != TaskStatus.DEAD_LETTER) {
            Thread.sleep(50);
        }

        assertEquals(TaskStatus.DEAD_LETTER, queue.getStatus(id).orElseThrow());
        assertEquals(1, queue.getDeadLetterQueue().size());
        queue.shutdown(5, TimeUnit.SECONDS);
    }

    private static void cancelledTaskNeverExecutes() throws Exception {
        TaskQueueService queue = new TaskQueueService(2);
        AtomicInteger executed = new AtomicInteger(0);
        queue.registerHandler("CANCELLABLE", payload -> executed.incrementAndGet());

        String id = queue.submit("CANCELLABLE", "payload", 1, 3, 2000);
        boolean cancelled = queue.cancel(id);
        queue.start();

        Thread.sleep(3000);

        assertTrue(cancelled, "cancel() should have returned true");
        assertEquals(0, executed.get());
        assertEquals(TaskStatus.CANCELLED, queue.getStatus(id).orElseThrow());
        queue.shutdown(5, TimeUnit.SECONDS);
    }

    private static void unknownTaskTypeGoesStraightToDeadLetter() throws Exception {
        TaskQueueService queue = new TaskQueueService(2);
        String id = queue.submit("NO_HANDLER_REGISTERED", "payload", 1, 3, 0);
        queue.start();

        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline
                && queue.getStatus(id).orElseThrow() != TaskStatus.DEAD_LETTER) {
            Thread.sleep(50);
        }

        assertEquals(TaskStatus.DEAD_LETTER, queue.getStatus(id).orElseThrow());
        queue.shutdown(5, TimeUnit.SECONDS);
    }
}
