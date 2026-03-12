# Jobbers

A lightweight distributed task execution framework for Python, built on asyncio. If you've used Celery, Jobbers will feel familiar — but with first-class support for task introspection, cancellation, stall detection, and OpenTelemetry observability out of the box.

---

## Why Jobbers?

| | Celery | Jobbers |
|---|---|---|
| asyncio-native tasks | Limited | Yes |
| Task introspection API | No | Yes |
| Cancel running/queued/scheduled tasks | No | Yes |
| Stall detection via heartbeats | No | Yes |
| Dead letter queue with history | No | Yes |
| Built-in OpenTelemetry metrics | No | Yes |
| Per-queue rate limiting | No | Yes |

---

## How It Works

Jobbers runs as four independent processes:

| Process | Role |
|---------|------|
| **Manager** | FastAPI web server (port 8000): task submission, status, cancellation, DLQ, queue/role management |
| **Worker** | Pulls tasks from Redis queues and executes them; handles retries, heartbeats, cancellation |
| **Cleaner** | Prunes stale state, rate-limit entries, and detects stalled tasks |
| **Scheduler** | Polls for due retry-delayed tasks and re-enqueues them |

Tasks move through the following states:

```
UNSUBMITTED → SUBMITTED → STARTED → COMPLETED
                                   → FAILED → [DLQ if policy=save]
                                   → SCHEDULED (retry delay, re-queued by Scheduler)
                                   → CANCELLED
                                   → STALLED (heartbeat timeout or SIGTERM + stop policy)
                                   → DROPPED (unknown task type)
```

---

## Quick Start

### 1. Define a task

Tasks are plain async functions decorated with `@register_task`. No class inheritance, no special return types.

```python
import datetime as dt
from jobbers.registry import register_task
from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy

@register_task(
    name="generate_report",
    version=1,
    max_retries=3,
    retry_delay=30,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    max_retry_delay=300,
    dead_letter_policy=DeadLetterPolicy.SAVE,
    max_heartbeat_interval=dt.timedelta(minutes=5),
)
async def generate_report(**kwargs: object) -> dict[str, object]:
    report_id = kwargs["report_id"]

    for section in get_sections(report_id):
        await process_section(section)
        task.heartbeat()  # tell Jobbers this task is still alive

    return {"url": f"/reports/{report_id}.pdf"}
```

### 2. Submit a task

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"name": "generate_report", "version": 1, "queue": "reports", "params": {"report_id": 42}}'
```

```json
{
  "id": "01JBKR2E5F3G4H5J6K7L8M9N0P",
  "name": "generate_report",
  "status": "submitted",
  "queue": "reports",
  "submitted_at": "2026-03-12T10:00:00Z"
}
```

### 3. Inspect a task

```bash
curl http://localhost:8000/task-status/01JBKR2E5F3G4H5J6K7L8M9N0P
```

```json
{
  "id": "01JBKR2E5F3G4H5J6K7L8M9N0P",
  "name": "generate_report",
  "status": "started",
  "queue": "reports",
  "submitted_at": "2026-03-12T10:00:00Z",
  "started_at": "2026-03-12T10:00:01Z",
  "last_heartbeat_at": "2026-03-12T10:03:45Z",
  "retry_attempt": 0
}
```

You can also list all tasks by status, queue, or task name:

```bash
curl "http://localhost:8000/tasks?status=started&queue=reports"
```

### 4. Retrieve results or errors

When a task completes, its result is stored and available via the status endpoint:

```json
{
  "id": "01JBKR2E5F3G4H5J6K7L8M9N0P",
  "status": "completed",
  "result": {"url": "/reports/42.pdf"},
  "completed_at": "2026-03-12T10:04:10Z"
}
```

If the task exhausts its retries, it moves to `failed` (and to the DLQ if configured):

```json
{
  "id": "01JBKR2E5F3G4H5J6K7L8M9N0P",
  "status": "failed",
  "error": "ConnectionError: upstream timed out",
  "retry_attempt": 3
}
```

Inspect the dead letter queue for full failure history:

```bash
curl "http://localhost:8000/dead-letter-queue?task_name=generate_report"
curl "http://localhost:8000/dead-letter-queue/01JBKR2E5F3G4H5J6K7L8M9N0P/history"
```

Resubmit failed tasks in bulk:

```bash
curl -X POST http://localhost:8000/dead-letter-queue/resubmit \
  -H "Content-Type: application/json" \
  -d '{"task_name": "generate_report", "reset_retry_count": true, "limit": 50}'
```

### 5. Cancel a task

Cancellation works regardless of whether the task is queued, running, or waiting on a retry delay.

```bash
curl -X POST http://localhost:8000/task/01JBKR2E5F3G4H5J6K7L8M9N0P/cancel
```

| Task status | What happens |
|-------------|-------------|
| `submitted` / `started` | Cancel signal sent via Redis pubsub; worker interrupts the task at the next `await`. Poll status to confirm. |
| `scheduled` | Removed from the delay queue and marked `cancelled` synchronously. |
| anything else | Returns `409 Conflict`. |

Cancel multiple tasks at once:

```bash
curl -X POST http://localhost:8000/tasks/cancel \
  -H "Content-Type: application/json" \
  -d '{"task_ids": ["01ABC...", "01DEF..."]}'
```

---

## Key Features

### asyncio Native

Every task function is a plain `async def`. You get the full asyncio ecosystem — `aiohttp`, `asyncpg`, `anyio`, etc. — without wrappers or thread-pool workarounds.

### Task Introspection

Query live task state at any time. Jobbers stores the last known state of every task in Redis so you can answer questions Celery can't:

- Which tasks are currently running, on which worker, and for how long?
- How many tasks are queued per queue?
- Which tasks are waiting on a retry delay?
- What failed, when, and why?

```bash
# Active tasks
curl "http://localhost:8000/tasks?status=started"

# Tasks waiting on retry delay
curl http://localhost:8000/scheduled-tasks

# Dead letter queue
curl "http://localhost:8000/dead-letter-queue?queue=reports&limit=25"
```

### Stall Detection via Heartbeats

Long-running tasks call `task.heartbeat()` periodically. If the heartbeat goes silent for longer than `max_heartbeat_interval`, the Cleaner marks the task as `stalled`. You can then alert on it, resubmit it, or investigate.

```python
@register_task(name="bulk_import", version=1, max_heartbeat_interval=dt.timedelta(minutes=2))
async def bulk_import(**kwargs: object) -> None:
    for batch in get_batches(kwargs["file_id"]):
        await import_batch(batch)
        task.heartbeat()  # reset the stall clock
```

### Retry Policies

Per-task retry configuration with four backoff strategies:

| Strategy | Delay per attempt |
|----------|------------------|
| `constant` | Fixed `retry_delay` seconds |
| `linear` | `retry_delay × attempt` |
| `exponential` | `retry_delay × 2^attempt` |
| `exponential_jitter` | Random in `[0, retry_delay × 2^attempt]` — avoids thundering herd |

All strategies are capped at `max_retry_delay`. Tasks that exhaust retries can be saved to the dead letter queue (`dead_letter_policy=DeadLetterPolicy.SAVE`) for later inspection and bulk resubmission.

#### Expected vs. unexpected exceptions

Not every exception should trigger a retry. Jobbers distinguishes between the two via the `expected_exceptions` parameter:

- **Expected exceptions** — transient failures you anticipate and want to retry (e.g. `httpx.TimeoutException`, `ConnectionError`). List them in `expected_exceptions`. When one of these is raised, Jobbers applies the backoff strategy and re-enqueues the task (up to `max_retries`).
- **Unexpected exceptions** — bugs or unrecoverable errors not in `expected_exceptions`. These cause the task to move immediately to `FAILED` with no retry attempt, regardless of how many retries remain.
- **Timeouts** — if a `timeout` is configured and the task exceeds it, it is treated like an expected exception and will retry with backoff.

```python
@register_task(
    name="send_webhook",
    version=1,
    max_retries=5,
    retry_delay=10,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    max_retry_delay=300,
    dead_letter_policy=DeadLetterPolicy.SAVE,
    timeout=30,                                    # fail the attempt after 30s
    expected_exceptions=(httpx.TimeoutException, ConnectionError),
)
async def send_webhook(**kwargs: object) -> None: ...
```

### Cancellation

Tasks can be cancelled at any point in their lifecycle — queued, running, or waiting on a retry delay. Running tasks are interrupted at the next `await` point. The `on_shutdown` policy controls what happens next:

| Policy | Behaviour |
|--------|-----------|
| `stop` (default) | Interrupted and marked `stalled` |
| `resubmit` | Interrupted and re-enqueued for another worker |
| `continue` | Shielded from cancellation; runs to completion |

### Rate Limiting and Concurrency Controls

Each queue has independent concurrency and rate-limit settings, enforced per worker:

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

This creates a `reports` queue that runs at most 5 tasks concurrently per worker and processes no more than 100 tasks per hour.

### OpenTelemetry Instrumentation

Jobbers emits OTLP metrics automatically — no instrumentation code required in your tasks.

Metrics aggregated by task, version, and queue:

| Metric | Type | Description |
|--------|------|-------------|
| `tasks_retried` | Count | Retry events |
| `tasks_dead_lettered` | Count | Tasks moved to DLQ |
| `cancellations_requested` | Count | Cancel signals sent |

Metrics aggregated by task, queue, and status:

| Metric | Type | Description |
|--------|------|-------------|
| `tasks_processed` | Count | Tasks completed (any terminal status) |
| `execution_time` | Histogram | Time from start to finish (ms) |
| `end_to_end_latency` | Histogram | Time from submission to finish (ms) |

Metrics aggregated by task, queue, and role:

| Metric | Type | Description |
|--------|------|-------------|
| `time_in_queue` | Histogram | Time from submission to start |
| `tasks_selected` | Count | Tasks pulled by workers |

Use `time_in_queue` to decide whether to scale workers or split queues. Use `tasks_processed[status=dropped]` to detect workers running stale task versions.

---

## Queue and Role Management

**Queues** are named buckets with their own concurrency limit and optional rate limit. **Roles** are named sets of queues. Workers are assigned a role and consume all queues in that role.

This lets you partition work by resource profile — for example, CPU-intensive tasks and I/O-bound tasks can share workers or be isolated based on how you group queues into roles.

```
Role: "heavy-workers"   → queues: ["reports", "exports"]
Role: "light-workers"   → queues: ["notifications", "webhooks", "default"]
```

Workers detect role and queue changes automatically via a TTL-based refresh tag — no restart required.

### Queue API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/queues` | List all queues |
| `POST` | `/queues` | Create a queue |
| `GET` | `/queues/{name}/config` | Get queue config |
| `PUT` | `/queues/{name}` | Create or update a queue |
| `DELETE` | `/queues/{name}` | Delete a queue |

### Role API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/roles` | List all roles |
| `POST` | `/roles` | Create a role |
| `GET` | `/roles/{name}` | Get queues for a role |
| `PUT` | `/roles/{name}` | Replace a role's queue list |
| `DELETE` | `/roles/{name}` | Delete a role |

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_ROLE` | `default` | Role assigned to this worker |
| `WORKER_TTL` | `50` | Max tasks before worker restarts (memory leak protection) |
| `WORKER_CONCURRENT_TASKS` | `5` | Max tasks running simultaneously per worker |
| `SCHEDULER_POLL_INTERVAL` | `5.0` | Seconds between scheduler polls |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `SQLITE_PATH` | `jobbers.db` | SQLite path for queue/role config and DLQ |

---

## Running Jobbers

After installing the package (`pip install -e .`), five CLI commands are available. All of them emit OpenTelemetry logs and metrics automatically.

---

### `jobbers_manager <task_module>`

Starts the FastAPI web server on port 8000. This is the process clients talk to — it handles task submission, status queries, cancellation, DLQ inspection, and queue/role management.

The `<task_module>` argument tells the manager which tasks exist so it can validate submitted task names and signatures. It accepts either a dotted Python module name or a file path:

```bash
# Dotted module name
jobbers_manager myapp.tasks

# Absolute or relative file path
jobbers_manager /srv/myapp/tasks.py
```

Run one or more manager instances. Because all state lives in Redis and SQLite, any number of managers can run concurrently behind a load balancer.

---

### `jobbers_worker <task_module>`

Starts a worker process that pulls tasks from Redis queues and executes them. Like the manager, it loads your task module so the registered task functions are available to run.

```bash
WORKER_ROLE=heavy-workers \
WORKER_CONCURRENT_TASKS=10 \
WORKER_TTL=100 \
jobbers_worker myapp.tasks
```

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `WORKER_ROLE` | `default` | The role this worker consumes. Workers pull from all queues assigned to this role. |
| `WORKER_CONCURRENT_TASKS` | `5` | Maximum number of tasks running concurrently within this process. |
| `WORKER_TTL` | `50` | Worker exits after processing this many tasks and is expected to be restarted by a process supervisor (e.g. Docker, systemd). Protects against memory leaks in long-running task code. Set to `0` to run indefinitely. |

Scale workers horizontally by running more instances. Each worker is fully independent — they share no state except through Redis and SQLite.

On `SIGTERM`, each in-flight task is handled according to its `on_shutdown` policy (`stop`, `resubmit`, or `continue`) before the process exits.

---

### `jobbers_cleaner`

A one-shot maintenance command. Run it on a cron schedule (e.g. hourly or nightly) to prune stale Redis entries, detect stalled tasks, and trim the dead letter queue.

```bash
jobbers_cleaner \
  --stale-time 600 \
  --completed-task-age 86400 \
  --dlq-age 2592000 \
  --rate-limit-age 604800
```

All arguments are in **seconds** and are optional — omit any you don't need.

| Argument | Description |
|----------|-------------|
| `--stale-time <seconds>` | Mark tasks whose heartbeat is older than this as `stalled`. Use this to surface tasks that have silently frozen. |
| `--completed-task-age <seconds>` | Delete stored state and heartbeat entries for tasks that reached a terminal status (`completed`, `failed`, `cancelled`) longer than this ago. Keeps Redis lean. |
| `--dlq-age <seconds>` | Remove dead letter queue entries older than this. |
| `--rate-limit-age <seconds>` | Prune rate-limit tracking entries older than this. |
| `--min-queue-age <seconds>` | Lower bound (epoch seconds) for queue entries to consider during cleanup. |
| `--max-queue-age <seconds>` | Upper bound (epoch seconds) for queue entries to consider during cleanup. |

A typical cron setup runs the cleaner frequently with a short `--stale-time` for stall detection, and less frequently with longer ages for general pruning:

```bash
# Every 5 minutes: detect stalled tasks
*/5 * * * * jobbers_cleaner --stale-time 300

# Nightly: prune old state
0 2 * * * jobbers_cleaner --completed-task-age 86400 --dlq-age 2592000 --rate-limit-age 604800
```

---

### `jobbers_scheduler`

A long-running process that polls for tasks waiting on a retry delay and re-enqueues them when their scheduled time arrives. Run exactly one scheduler per Redis instance.

```bash
SCHEDULER_POLL_INTERVAL=5.0 \
SCHEDULER_BATCH_SIZE=10 \
SCHEDULER_ROLE=default \
jobbers_scheduler
```

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `SCHEDULER_POLL_INTERVAL` | `5.0` | Seconds to sleep between polls when no tasks are due. |
| `SCHEDULER_BATCH_SIZE` | `1` | Maximum tasks to dispatch per poll iteration. Increase if your workload produces many simultaneous retry delays. |
| `SCHEDULER_ROLE` | `default` | Limits the scheduler to queues belonging to this role. |

The scheduler has no persistent state of its own — it is safe to restart at any time. Tasks whose `run_at` time has passed while the scheduler was down will be dispatched on the next poll.

---

### `jobbers_openapi [output_path]`

Writes the OpenAPI specification to a JSON file without starting a real server. Useful for generating client SDKs or keeping the spec in version control.

```bash
# Write to openapi.json (default)
jobbers_openapi

# Write to a custom path
jobbers_openapi docs/api/openapi.json
```

---

## Running Locally with Docker

```bash
docker compose up
```

This starts all four processes (manager, worker, cleaner, scheduler) along with Redis. The manager API is available at `http://localhost:8000`.
