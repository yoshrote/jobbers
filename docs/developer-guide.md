# Developer Guide

## Defining Tasks

Tasks are plain `async def` functions decorated with `@register_task`. Collect all your task definitions in a single module (or package) and pass that module path to `jobbers_manager` and `jobbers_worker` at startup.

### Minimal task

```python
from jobbers.registry import register_task

@register_task(name="send_email", version=1)
async def send_email(**kwargs: object) -> dict[str, object]:
    to = kwargs["to"]
    body = kwargs["body"]
    await _send(to, body)
    return {"status": "sent"}
```

- The function must be `async`.
- `**kwargs` receives the `params` dict submitted with the task.
- The return value is stored in `task.results` and returned by the status endpoint.

### Full configuration

```python
import datetime as dt
import httpx
from jobbers.registry import register_task
from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy

@register_task(
    name="generate_report",
    version=1,

    # Retry behaviour
    max_retries=5,                                       # max retry attempts (default: 3)
    retry_delay=30,                                      # base delay in seconds; omit for immediate retry
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER, # default: EXPONENTIAL
    max_retry_delay=300,                                 # cap on computed delay, seconds (default: 3600)
    expected_exceptions=(httpx.TimeoutException, ConnectionError),

    # Timeout
    timeout=120,                                         # seconds; treated as an expected exception

    # Dead letter queue
    dead_letter_policy=DeadLetterPolicy.SAVE,            # default: NONE

    # Heartbeat monitoring
    max_heartbeat_interval=dt.timedelta(minutes=5),

    # Concurrency
    max_concurrent=2,                                    # default: 1

    # Shutdown behaviour
    # on_shutdown=TaskShutdownPolicy.STOP,               # default; other options: RESUBMIT, CONTINUE
)
async def generate_report(**kwargs: object) -> dict[str, object]:
    report_id = kwargs["report_id"]

    for section in get_sections(report_id):
        await process_section(section)
        await task.heartbeat()   # reset the stall clock; call regularly for long tasks

    return {"url": f"/reports/{report_id}.pdf"}
```

### `@register_task` parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | `str` | required | Task name. Must be unique per `(name, version)` pair. |
| `version` | `int` | required | Task version. Increment when making breaking changes to parameters or behaviour. |
| `max_retries` | `int` | `3` | Maximum retry attempts after the first failure. |
| `retry_delay` | `int \| None` | `None` | Base delay in seconds before a retry. `None` = re-queue immediately. |
| `backoff_strategy` | `BackoffStrategy` | `EXPONENTIAL` | How the delay grows per attempt. |
| `max_retry_delay` | `int` | `3600` | Upper bound on computed delay, in seconds. |
| `expected_exceptions` | `tuple[type[Exception]]` | `None` | Exception types that trigger the retry/backoff path. All others go straight to `failed`. |
| `timeout` | `int \| None` | `None` | Task timeout in seconds. Treated like an expected exception when exceeded. |
| `dead_letter_policy` | `DeadLetterPolicy` | `NONE` | `SAVE` copies permanently failed tasks to the DLQ. |
| `max_heartbeat_interval` | `timedelta \| None` | `None` | If set, the Cleaner marks the task `stalled` when this interval passes without a heartbeat. |
| `max_concurrent` | `int \| None` | `1` | Max simultaneous executions of this task per worker. `None` = unlimited. |
| `on_shutdown` | `TaskShutdownPolicy` | `STOP` | What happens to the task when the worker receives SIGTERM. |

### Heartbeats

Call `await task.heartbeat()` periodically inside long-running tasks. This updates the `heartbeat_at` timestamp, which the Cleaner checks against `max_heartbeat_interval`.

```python
@register_task(name="bulk_import", version=1, max_heartbeat_interval=dt.timedelta(minutes=2))
async def bulk_import(**kwargs: object) -> None:
    for batch in get_batches(kwargs["file_id"]):
        await import_batch(batch)
        await task.heartbeat()   # must be called at least once every 2 minutes
```

### Versioning

Each `(name, version)` pair maps to exactly one function. When you make a breaking change to a task's parameters or behaviour:

1. Register a new version: `@register_task(name="my_task", version=2, ...)`.
2. Keep the old version registered until all in-flight and scheduled tasks of the old version have drained.
3. Workers running the old code will `drop` tasks submitted with the new version number and vice versa.

---

## Submitting Tasks

### Via the HTTP API

```bash
curl -X POST http://localhost:8000/submit-task \
  -H "Content-Type: application/json" \
  -d '{
    "id": "01JBKR2E5F3G4H5J6K7L8M9N0P",
    "name": "generate_report",
    "version": 1,
    "queue": "reports",
    "parameters": {"report_id": 42}
  }'
```

The `id` field must be a valid [ULID](https://github.com/ulid/spec) string. Generate one client-side to get a stable reference before the response arrives.

Response:

```json
{
  "message": "Task submitted successfully",
  "task": {
    "id": "01JBKR2E5F3G4H5J6K7L8M9N0P",
    "name": "generate_report",
    "status": "submitted",
    "queue": "reports",
    "submitted_at": "2026-03-12T10:00:00Z",
    "retry_attempt": 0
  }
}
```

### Via Python (direct)

If you're running inside the same process as the Manager, or integrating via a shared Redis connection:

```python
from ulid import ULID
from jobbers.models.task import Task
from jobbers.state_manager import StateManager

task = Task(
    id=ULID(),
    name="generate_report",
    version=1,
    queue="reports",
    parameters={"report_id": 42},
)
await state_manager.submit_task(task)
```

---

## Checking Task Status

```bash
curl http://localhost:8000/task-status/01JBKR2E5F3G4H5J6K7L8M9N0P
```

```json
{
  "id": "01JBKR2E5F3G4H5J6K7L8M9N0P",
  "name": "generate_report",
  "status": "completed",
  "queue": "reports",
  "retry_attempt": 0,
  "submitted_at": "2026-03-12T10:00:00Z",
  "last_error": null
}
```

Poll this endpoint to wait for completion, or query task lists by status:

```bash
# All running tasks in the "reports" queue
curl "http://localhost:8000/task-list?status=started&queue=reports"
```

---

## Cancelling Tasks

```bash
# Cancel one task
curl -X POST http://localhost:8000/task/01JBKR2E5F3G4H5J6K7L8M9N0P/cancel

# Cancel multiple
curl -X POST http://localhost:8000/tasks/cancel \
  -H "Content-Type: application/json" \
  -d '{"task_ids": ["01ABC...", "01DEF..."]}'
```

| Task status when cancel is requested | What happens |
| --- | --- |
| `submitted` | Removed from the queue; marked `cancelled` immediately. |
| `started` | Cancel signal sent via Redis pub/sub; worker interrupts at the next `await`. |
| `scheduled` | Removed from the delay queue; marked `cancelled` immediately. |
| anything else | Returns `409 Conflict`. |

---

## Queue and Role Setup

Tasks are submitted to a **queue**. Workers are assigned a **role** that maps to a set of queues. A worker consumes all tasks from all queues in its role.

### Create a queue

```bash
curl -X POST http://localhost:8000/queues \
  -H "Content-Type: application/json" \
  -d '{
    "name": "reports",
    "max_concurrent": 5,
    "rate_numerator": 100,
    "rate_denominator": 1,
    "rate_period": "hour"
  }'
```

| Field | Description |
| --- | --- |
| `name` | Queue name. |
| `max_concurrent` | Max tasks per worker running simultaneously from this queue. |
| `rate_numerator` | Numerator of the rate limit (e.g. `100` tasks). |
| `rate_denominator` | Denominator of the rate limit (e.g. `1` period). |
| `rate_period` | Period unit: `second`, `minute`, `hour`, or `day`. |

### Create a role

```bash
curl -X POST http://localhost:8000/roles \
  -H "Content-Type: application/json" \
  -d '{"name": "heavy-workers", "queues": ["reports", "exports"]}'
```

Start a worker consuming this role:

```bash
WORKER_ROLE=heavy-workers jobbers_worker myapp.tasks
```

Workers pick up queue and role changes automatically without restart.

### Queue API reference

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/queues` | List all queues |
| `POST` | `/queues` | Create a queue |
| `GET` | `/queues/{name}/config` | Get queue config |
| `PUT` | `/queues/{name}` | Create or update a queue |
| `DELETE` | `/queues/{name}` | Delete a queue |

### Role API reference

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/roles` | List all roles |
| `POST` | `/roles` | Create a role |
| `GET` | `/roles/{role_name}` | Get queues for a role |
| `PUT` | `/roles/{role_name}` | Replace a role's queue list |
| `DELETE` | `/roles/{role_name}` | Delete a role |

---

## Dead Letter Queue

Tasks with `dead_letter_policy=DeadLetterPolicy.SAVE` are written to the DLQ when they permanently fail.

```bash
# Browse the DLQ
curl "http://localhost:8000/dead-letter-queue?task_name=generate_report&limit=25"

# Full failure history for one task
curl http://localhost:8000/dead-letter-queue/01JBKR2E5F3G4H5J6K7L8M9N0P/history

# Bulk resubmit by task name (resets retry counter)
curl -X POST http://localhost:8000/dead-letter-queue/resubmit \
  -H "Content-Type: application/json" \
  -d '{"task_name": "generate_report", "reset_retry_count": true, "limit": 50}'

# Resubmit explicit task IDs
curl -X POST http://localhost:8000/dead-letter-queue/resubmit \
  -H "Content-Type: application/json" \
  -d '{"task_ids": ["01ABC...", "01DEF..."], "reset_retry_count": true}'

# Remove tasks from the DLQ without resubmitting
curl -X DELETE http://localhost:8000/dead-letter-queue \
  -H "Content-Type: application/json" \
  -d '{"task_ids": ["01ABC..."]}'
```
