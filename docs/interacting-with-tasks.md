# Interacting with Tasks

This document covers the full lifecycle of interacting with tasks in Jobbers: submitting new work, scheduling it, cancelling in-flight tasks, and recovering work from the dead letter queue. Submission and scheduling get work into the system; cancellation and DLQ resubmit manage tasks that are already there. Each operation is available both programmatically (Python) and over the HTTP API.

## Quick reference

| Operation | Use when |
| --- | --- |
| **Submit to queue** | Work needs to happen now |
| **Submit a DAG** | Work is structured as dependent steps or parallel branches |
| **Schedule a task** | Work must happen at a specific future datetime (one-shot) |
| **Cron** | Work repeats on a schedule (daily jobs, periodic pipelines) |
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

## 2. Submit a DAG to a queue

A DAG submits a graph of tasks where each node runs after its predecessors complete. Fan-out, fan-in, and error-callback patterns are all supported. See [dag-composition.md](dag-composition.md) for the full reference.

### Programmatic

```python
from jobbers.models.dag import DAGNode
from jobbers.db import get_state_manager

root   = DAGNode("ingest_data")
middle = DAGNode("transform_data")
end    = DAGNode("publish_results")

root.then(middle)
middle.then(end)

dag_run_id, submitted_roots = await get_state_manager().submit_dag(root)
```

`submit_dag` accepts multiple root nodes for multi-root DAGs. It returns the shared `dag_run_id` (a ULID) and the list of root `Task` objects that were enqueued.

Using `@register_task` wrappers:

```python
root = ingest_data.node(queue="etl")
middle = transform_data.node(queue="etl")
end = publish_results.node(queue="etl")
root.then(middle)
middle.then(end)
dag_run_id, _ = await get_state_manager().submit_dag(root)
```

### HTTP

The API accepts a [Mermaid flowchart](mermaid-dag-spec.md) and assigns ULIDs automatically:

```http
POST /submit-dag
Content-Type: application/json

{
  "diagram": "graph LR\n  A[ingest_data] --> B[transform_data] --> C[publish_results]"
}
```

Returns `dag_run_id` and the IDs of the submitted root tasks. All task names in the diagram must already be registered on the workers that will receive them.

---

## 3. Submit a task to the scheduler

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

---

## 4. Cron configuration

A `CronDAGEntry` wraps any DAG (including a single-task DAG) with a cron expression and fires it repeatedly. The Scheduler process computes the next `run_at` after each fire and re-adds the entry to the schedule. `ConcurrencyPolicy` controls overlap between runs.

### Programmatic

```python
import datetime as dt
from croniter import croniter
from jobbers.models.cron_dag import CronDAGEntry, ConcurrencyPolicy
from jobbers.models.dag import DAGNode
from jobbers.db import get_state_manager

# Single-task cron: wrap a one-node DAG
root = DAGNode("generate_report", parameters={"format": "pdf"})
entry = CronDAGEntry(
    name="daily_report",
    cron_expr="0 6 * * *",             # 06:00 UTC every day
    dag_spec=root.to_spec(),
    concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING,
)

sm = get_state_manager()
await sm.add_cron_dag(entry)
```

For a multi-task DAG, build the graph first then pass the root spec:

```python
ingest  = DAGNode("nightly_ingest")
process = DAGNode("nightly_process")
ingest.then(process)

entry = CronDAGEntry(
    name="nightly_pipeline",
    cron_expr="0 2 * * *",
    dag_spec=ingest.to_spec(),
    concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING,
)
await sm.add_cron_dag(entry)
```

`CronDAGEntry` is stored in the `CronDAGScheduler` backend. The entry persists across restarts — call `sm.remove_cron_dag(entry.id)` to cancel it permanently.

### HTTP

```http
POST /cron-dags
Content-Type: application/json

{
  "name": "nightly_pipeline",
  "cron_expr": "0 2 * * *",
  "diagram": "graph LR\n  A[nightly_ingest] --> B[nightly_process]",
  "enabled": true,
  "concurrency_policy": "skip_if_running"
}
```

Returns the entry including its assigned `id` and `next_run_at`.

Other lifecycle endpoints:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/cron-dags` | List all entries with next run times |
| `GET` | `/cron-dags/{id}` | Retrieve a single entry |
| `PUT` | `/cron-dags/{id}` | Replace diagram/settings; resets the schedule |
| `DELETE` | `/cron-dags/{id}` | Remove permanently |

### `ConcurrencyPolicy`

| Value | Behaviour |
| --- | --- |
| `always` (default) | Fire even if the previous run is still active |
| `skip_if_running` | Skip this fire if any task from the previous run is still active |

---

## 5. Cancelling tasks

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

## 6. Resubmitting from the dead letter queue

Tasks with `dead_letter_policy=DeadLetterPolicy.SAVE` are written to the dead letter queue (DLQ) when they permanently fail (all retries exhausted). You can inspect them, resubmit them back onto their original queue, or discard them.

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
