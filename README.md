# Distributed Task Queue Handler

A lightweight, multithreaded task queue built from scratch in Java. It
simulates core features found in production-grade message queue systems such
as **RabbitMQ**, **Amazon SQS**, and **Celery** — priority scheduling,
delayed execution, automatic retries with exponential backoff, and a
dead-letter queue — using nothing but the JDK's `java.util.concurrent`
package.

## Technologies used

| Concern                         | JDK class used                                    |
|----------------------------------|----------------------------------------------------|
| Priority-ordered ready queue      | `PriorityBlockingQueue<Task>`                      |
| O(1) task lookup / status table   | `ConcurrentHashMap<String, Task>`                  |
| Lock-free counters & IDs          | `AtomicInteger`, `AtomicLong`                      |
| Concurrent worker threads         | `ExecutorService` (fixed thread pool)              |
| Delayed & retry scheduling        | `ScheduledExecutorService`                         |
| Dead-letter storage               | `ConcurrentLinkedQueue<Task>`                      |

Built and tested on **Java 21**.

## Features

- **Priority scheduling** — tasks are ordered by a numeric priority (higher
  value = served first); equal priorities are served FIFO.
- **Delayed execution** — submit a task that only becomes eligible to run
  after a given delay (like SQS delay queues / Celery ETA tasks).
- **Automatic retries with exponential backoff** — a failing handler is
  retried up to `maxRetries` times, with the delay between attempts doubling
  each time (capped), before...
- **Dead-letter queue** — tasks that exhaust their retries (or have no
  registered handler) land here for later inspection or manual resubmission.
- **Cancellation** — pending, scheduled, queued, or retry-scheduled tasks can
  be cancelled before they execute.
- **Live metrics** — submitted / completed / failed / retried / dead-lettered
  / cancelled counters, safe to read from any thread.
- **Graceful shutdown** — stops accepting new dispatches and drains in-flight
  work within a timeout before force-stopping.

## Project layout

```
task-queue-service/
├── pom.xml
├── README.md
└── src
    ├── main/java/com/taskqueue
    │   ├── Main.java                  # runnable demo
    │   ├── SelfTest.java              # dependency-free assertion suite (plain `java`, no Maven needed)
    │   ├── core/
    │   │   ├── TaskQueueService.java  # the engine
    │   │   ├── QueueMetrics.java
    │   │   └── DeadLetterQueue.java
    │   ├── handler/
    │   │   └── TaskHandler.java       # functional interface for business logic
    │   ├── model/
    │   │   ├── Task.java
    │   │   └── TaskStatus.java
    │   └── exception/
    │       └── TaskExecutionException.java
    └── test/java/com/taskqueue
        └── TaskQueueServiceTest.java  # JUnit 5 test suite
```

## Architecture

```
submit(type, payload, priority, maxRetries, delayMillis)
        │
        ├─ delayMillis > 0 ──► ScheduledExecutorService.schedule(...) ──┐
        │                                                                │
        └─ delayMillis == 0 ───────────────────────────────────────────►│
                                                                         ▼
                                                     PriorityBlockingQueue<Task>
                                                          (readyQueue)
                                                                         │
                                     N worker threads (ExecutorService) │ poll()
                                                                         ▼
                                                                    process(task)
                                                                         │
                                        success ─────────────────────► COMPLETED
                                                                         │
                                        failure, attempts < maxRetries ─┤
                                              │                         │
                                              ▼                         │
                                   scheduler.schedule(backoff) ─────────┘ (re-enqueue)
                                              │
                                        attempts == maxRetries
                                              ▼
                                        DeadLetterQueue
```

Every `Task` is tracked in a `ConcurrentHashMap<String, Task>` from the
moment it's submitted, so `getStatus(id)` / `cancel(id)` work in O(1)
regardless of which internal queue the task currently lives in.

## Running the demo

### Option A — plain JDK (no Maven / no internet required)

```bash
cd task-queue-service
mkdir -p out
javac -d out $(find src/main/java -name "*.java")

# run the demo
java -cp out com.taskqueue.Main

# run the dependency-free self-test suite
java -cp out com.taskqueue.SelfTest
```

### Option B — Maven

```bash
cd task-queue-service
mvn compile exec:java -Dexec.mainClass=com.taskqueue.Main
# or, to build a runnable jar:
mvn package
java -jar target/task-queue-service.jar

# run the JUnit 5 test suite
mvn test
```

## Example output

```
[main] Submitting tasks...
[main] Cancellation requested for delayed task, success=true
[tq-worker-2] EMAIL sent -> urgent-password-reset@example.com
[tq-worker-3] EMAIL sent -> normal-receipt@example.com
[tq-worker-1] EMAIL sent -> low-priority-newsletter@example.com
[tq-worker-4] IMAGE_RESIZE done -> photo-1.jpg
[tq-worker-1] IMAGE_RESIZE done -> photo-3.jpg
...
[main] ---- Metrics ----
[main] submitted=11, completed=9, failed=1, retried=1, deadLettered=1, cancelled=1
[main] ---- Dead-letter queue ----
[main] Task{id=..., type=REPORT, priority=4, status=DEAD_LETTER, attempts=1/1} lastError=report generator unavailable for monthly-sales-report
Report task final status: DEAD_LETTER
[main] Shutdown complete. Emails sent this run: 4
```

Notice the `urgent-password-reset` email (priority 10) is sent before the
`normal-receipt` (priority 5) and `low-priority-newsletter` (priority 1)
emails, even though they were submitted in the opposite order — that's the
`PriorityBlockingQueue` in action.

## Using it in your own code

```java
TaskQueueService queue = new TaskQueueService(4); // 4 worker threads

queue.registerHandler("SEND_EMAIL", payload -> emailClient.send((String) payload));
queue.start();

// priority 10, default 3 retries, no delay
String id = queue.submit("SEND_EMAIL", "user@example.com", 10);

queue.getStatus(id);      // Optional<TaskStatus>
queue.cancel(id);         // true if it hadn't started yet
queue.getMetrics();       // live counters
queue.deadLetterSnapshot(); // tasks that permanently failed

queue.shutdown(10, TimeUnit.SECONDS); // graceful drain
```

## Design notes / possible extensions

- This is an **in-memory, single-JVM** queue — despite the "distributed" in
  the title, there's no network layer here. To make it genuinely
  distributed you'd swap the in-memory structures for a shared backing
  store (e.g. Redis sorted sets for priority, a database row per task for
  the registry) and add a lightweight RPC/HTTP layer for producers and
  consumers running in separate processes.
- Retry backoff is exponential with a cap (`500ms, 1s, 2s, 4s, ... up to 15s`)
  — tune `BASE_BACKOFF_MILLIS` / `MAX_BACKOFF_MILLIS` in
  `TaskQueueService` to taste.
- `resubmitFromDeadLetter(id)` lets an operator manually retry a
  permanently-failed task after fixing the underlying issue, without
  losing its history.
