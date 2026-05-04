# Background task concepts

This page explains the ideas behind task queues in plain language. If you've already worked with systems like Celery, Sidekiq, or AWS SQS, you can skip straight to the [developer guide](developer-guide.md).

---

## Why run work in the background?

Imagine a user clicks "Generate monthly report" on your website. Generating the report takes 20 seconds. If your server does that work while the user waits, two things go wrong:

1. The user stares at a loading spinner for 20 seconds.
2. Your server is tied up the whole time, unable to respond to anyone else.

The better approach: your server immediately responds "got it, working on it", and the report generation happens *in the background* — on a separate process, separate machine, or whenever resources are free. When it's done, the user can come back for the result (or be notified automatically).

Work that follows this pattern — offloaded, asynchronous, happening outside the request/response cycle — is called a **background task**.

---

## What is a task queue?

A task queue is a list that sits between the code that *requests* work and the code that *does* work.

```
Your app  →  [queue]  →  Worker
(submits)                (executes)
```

When your app has something to do, it writes a description of the job (the task name, plus any input parameters) to the queue and moves on. One or more *workers* are watching that queue; when a new entry appears, a worker picks it up and runs it.

This decoupling is what makes the pattern powerful:

- Your app stays fast — submitting a task takes milliseconds.
- Workers can run on separate machines, so heavy work doesn't slow down your web server.
- You can add more workers when work piles up, without changing your app.

---

## Workers

A worker is a long-running process whose only job is to pull tasks from a queue and execute them. Jobbers workers are *concurrent* — each worker can run several tasks at the same time, and you can run as many workers as you need.

Workers don't need to know who submitted a task or why. They just pull the next item off the queue, call the corresponding function, record the result, and move on.

---

## Task states

Every task in Jobbers has a *state* that describes where it is in its journey:

| State | Meaning |
| ----------- | ------- |
| `submitted` | The task has been accepted and is waiting in the queue |
| `started` | A worker has picked it up and is running it |
| `completed` | The task finished successfully |
| `failed` | The task failed and has no retries left |
| `scheduled` | The task is waiting for a future time before being retried |
| `cancelled` | A user requested cancellation before the task finished |
| `stalled` | The task stopped sending heartbeats — possibly hung |

See [task lifecycle](task-lifecycle.md) for the full state diagram.

---

## Retries and backoff

Networks hiccup. Third-party APIs go down for a minute. Database connections drop. These are *transient* failures — if you just try again a little later, the task will probably succeed.

A task queue handles this automatically. When a task fails for a transient reason, Jobbers can retry it automatically — up to however many times you configure. Between attempts, it waits a delay that can grow with each retry (this is called *backoff*), so a struggling service isn't hammered immediately.

For example, with exponential backoff:
- Attempt 1 fails → wait 10 seconds
- Attempt 2 fails → wait 20 seconds
- Attempt 3 fails → wait 40 seconds
- ...and so on, up to a configured maximum

Not every failure should be retried. A bug in your code will fail every time no matter how many retries you allow. Jobbers lets you specify which exception types count as transient (and should be retried) versus which should cause the task to fail immediately.

---

## Dead letter queue

When a task exhausts all its retries, where does it go? In many systems, it just disappears — you have no record of what failed or why.

Jobbers can instead move permanently failed tasks to a **dead letter queue** (DLQ) — a separate holding area where you can:

- Browse failed tasks and see the full error details
- Understand which inputs caused the failure
- Fix the underlying problem in your code
- Resubmit the tasks when you're ready

The dead letter queue is opt-in per task. Tasks configured with `dead_letter_policy=SAVE` are preserved there on permanent failure; others are discarded.

---

## Heartbeats and stall detection

Some tasks run for a long time — minutes or even hours. How do you know if such a task is still making progress, or if it has silently hung?

Jobbers uses a **heartbeat** mechanism. A long-running task is expected to periodically send a signal saying "still alive and working". If a task stops sending heartbeats for longer than the configured interval, the Cleaner process marks it as *stalled* — giving you an alert to investigate without relying on a blunt process-level timeout.

---

## Queues, roles, and resource limits

Not all background work is equal. A queue that processes payment webhooks should probably not be starved by a queue full of low-priority report generation jobs. Jobbers solves this with named queues and worker roles:

- **Queues** are named buckets for related tasks. Each queue has a concurrency limit (how many tasks can run at once from that queue) and an optional rate limit (how many tasks can start per second or minute).
- **Roles** are named sets of queues assigned to a worker. A worker assigned to the `payments` role only pulls from the queues in that role — keeping payment processing isolated from lower-priority work.

See [resource management](resource-management.md) for guidance on sizing and configuration.

---

## Observability

Knowing that a task succeeded or failed is the minimum. Jobbers goes further: every task records timestamps for each state transition, and all four Jobbers processes emit structured metrics and logs via OpenTelemetry. This means you can connect Jobbers to your existing monitoring stack and set up dashboards, alerts, and traces without custom instrumentation.

See [operations guide](operations.md) for configuration details.
