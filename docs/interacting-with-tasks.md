# Interacting with Tasks

This document covers the full lifecycle of interacting with a single, standalone task in Jobbers: submitting new work, scheduling it, cancelling it in-flight, and recovering it from the dead letter queue. Submission and scheduling get work into the system; cancellation and DLQ resubmit manage tasks that are already there. Each operation is available both programmatically (Python) and over the HTTP API.

If your task is one node in a DAG rather than a standalone submission, see [interacting-with-dags.md](interacting-with-dags.md) instead — DAG runs are cancelled and recovered as a group (`POST /dags/{dag_run_id}/cancel` / `.../resume`), not one task at a time, and recovery there normally does **not** go through the DLQ (see [§4](#4-resubmitting-from-the-dead-letter-queue) below).

## Quick reference

| Operation | Use when |
| --- | --- |
| **Submit to queue** | Work needs to happen now |
| **Schedule a task** | Work must happen at a specific future datetime (one-shot) |
| **Cancel** | A task needs to be stopped before or during execution |
| **DLQ resubmit** | A permanently-failed task should be retried after fixing the underlying cause |

---

## 1. Submit a task to a queue

The simplest trigger. The task is placed on a queue immediately and a worker picks it up as soon as a slot is free.

### Programmatic

Use the `TaskWrapper` returned by `@register_task`:

```python
from myapp.tasks import process_order  # a @register_task-decorated function

task = await process_order.submit(queue="orders", order_id=42)
print(task.id, task.status)  # SUBMITTED
```

Or build the `Task` object yourself and call `StateManager.submit_task`:

```python
from ulid import ULID
from jobbers.models.task import Task
from jobbers.db import get_state_manager

task = Task(id=ULID(), name="process_order", version=1, queue="orders", parameters={"order_id": 42})
await get_state_manager().submit_task(task)
```

### HTTP

```http
POST /submit-task
Content-Type: application/json

{
  "id": "01HZ...",
  "name": "process_order",
  "version": 1,
  "queue": "orders",
  "parameters": {"order_id": 42}
}
```

Returns the task summary including the assigned `id` and `status: "submitted"`.

---

## 2. Submit a task to the scheduler

Use the scheduler when a task should run at a specific future time. The task is persisted in the scheduler store with a `run_at` timestamp and a `SCHEDULED` status. The Scheduler process polls for due tasks and promotes them back to `SUBMITTED` on their target queue when the time arrives.

### Programmatic

```python
import datetime as dt
from myapp.tasks import send_reminder

run_at = dt.datetime(2026, 6, 13, 9, 0, tzinfo=dt.UTC)
task = await send_reminder.schedule(run_at, queue="notifications", user_id=99)
```

Or using `StateManager.schedule_new_task` directly:

```python
from ulid import ULID
from jobbers.models.task import Task
from jobbers.db import get_state_manager
import datetime as dt

task = Task(id=ULID(), name="send_reminder", version=1, queue="notifications", parameters={"user_id": 99})
run_at = dt.datetime(2026, 6, 13, 9, 0, tzinfo=dt.UTC)
await get_state_manager().schedule_new_task(task, run_at)
```

### HTTP

```http
POST /schedule-task
Content-Type: application/json

{
  "task": {
    "id": "01HZ...",
    "name": "send_reminder",
    "version": 1,
    "queue": "notifications",
    "parameters": {"user_id": 99}
  },
  "run_at": "2026-06-13T09:00:00Z"
}
```

Returns the task summary and the confirmed `run_at`.

### Checking scheduled tasks

```http
GET /scheduled-tasks?queue=notifications
```

Returns tasks currently waiting in the scheduler with their `scheduled_at` timestamps.

> **Retries vs. one-shot scheduling:** `schedule_new_task` / `POST /schedule-task` is for tasks you want to run once at a specific time. Tasks that fail and have a `retry_delay` configured are also moved through the scheduler automatically by the worker — you do not need to call the schedule API for that.
>
> A DAG (including a single-task DAG) can be scheduled to fire repeatedly on a cron expression — see [Cron DAGs in interacting-with-dags.md](interacting-with-dags.md#2-cron-dags).

---

## 3. Cancelling tasks

Cancellation is best-effort. The outcome depends on the task's current status when the request arrives:

| Status at cancel time | What happens |
| --- | --- |
| `submitted` | Removed from the queue; marked `cancelled` immediately. |
| `started` | Cancellation signal sent via Redis pub/sub; worker interrupts the coroutine at the next `await`. |
| `scheduled` | Removed from the scheduler; marked `cancelled` immediately. |
| anything else | Returns `409 Conflict`. |

### Cancel a single task

```http
POST /task/{task_id}/cancel
```

Returns the updated task summary, or `404` if the task does not exist.

### Cancel multiple tasks

```http
POST /tasks/cancel
Content-Type: application/json

{"task_ids": ["01ABC...", "01DEF..."]}
```

Processes all IDs concurrently. The response lists the outcome for each:

```json
{
  "results": [
    {"task_id": "01ABC...", "status": "cancellation_requested"},
    {"task_id": "01DEF...", "status": "error", "detail": "Task is not in a cancellable state"}
  ]
}
```

Individual errors do not abort the rest of the batch.

---

## 4. Resubmitting from the dead letter queue

Tasks with `dead_letter_policy=DeadLetterPolicy.SAVE` are written to the dead letter queue (DLQ) when they permanently fail (all retries exhausted). You can inspect them, resubmit them back onto their original queue, or discard them.

> **DAG tasks:** this endpoint resubmits a task in isolation — for a task that's part of a DAG run, that skips the run's fan-in tracking and aggregate counters, which can leave the run's status inconsistent with what actually happened. Prefer [resuming the run](interacting-with-dags.md#4-resuming-dag-runs) (`POST /dags/{dag_run_id}/resume`), which resubmits every stuck task in the run and keeps that bookkeeping intact. DLQ resubmit here is for standalone tasks that aren't part of a DAG.

### Browse the DLQ

```http
GET /dead-letter-queue?task_name=process_order&queue=orders&limit=25
```

All query parameters are optional. Omitting them returns up to the default limit across all task types and queues. Returns task summaries including the most recent error message.

### Full failure history for one task

```http
GET /dead-letter-queue/{task_id}/history
```

Returns every recorded failure event in chronological order — retry attempt number, timestamp, and error message for each.

### Resubmit tasks

Resubmitted tasks are placed back on their original queue with `status: submitted`. The request body selects tasks either by explicit ID list or by filter:

```http
POST /dead-letter-queue/resubmit
Content-Type: application/json

{"task_name": "process_order", "queue": "orders", "reset_retry_count": true, "limit": 50}
```

Or by explicit IDs:

```http
POST /dead-letter-queue/resubmit
Content-Type: application/json

{"task_ids": ["01ABC...", "01DEF..."], "reset_retry_count": true}
```

| Field | Default | Description |
| --- | --- | --- |
| `task_ids` | `null` | Explicit list of task IDs. Mutually exclusive with filter fields. |
| `queue` | `null` | Filter by queue name. |
| `task_name` | `null` | Filter by task name. |
| `task_version` | `null` | Filter by task version. |
| `reset_retry_count` | `true` | Reset `retry_attempt` to `0` before resubmitting. |
| `limit` | `100` | Max tasks to resubmit when using filter mode (max `1000`). |

At least one of `task_ids`, `queue`, `task_name`, or `task_version` must be provided. Returns the list of resubmitted task summaries.

### Remove from the DLQ without resubmitting

```http
DELETE /dead-letter-queue
Content-Type: application/json

{"task_ids": ["01ABC...", "01DEF..."]}
```

Removes the entries permanently from the DLQ. The task state record is left in place (it will be pruned by the Cleaner according to `--completed-task-age`).
