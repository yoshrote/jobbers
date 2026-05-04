# Jobbers

A distributed task queue and workflow engine for Python — asyncio-native, backed by Redis, with first-class task introspection, cancellation, stall detection, and OpenTelemetry observability built in.

---

## Why Jobbers?

Most task queues are fire-and-forget: you submit a job and hope it worked. Jobbers treats every task as a first-class object you can inspect, cancel, or resubmit at any point in its lifecycle.

| | Celery | TaskIQ | Jobbers |
| --- | --- | --- | --- |
| asyncio-native tasks | Limited | Yes | Yes |
| Live task introspection API | Flower (add-on) | No | Yes (built-in) |
| Cancel queued / running / scheduled tasks | No | No | Yes |
| Stall detection via heartbeats | No | No | Yes |
| Dead letter queue with resubmit | No | RabbitMQ only | Yes |
| Built-in OpenTelemetry metrics | No | Middleware (opt-in) | Yes |
| Per-queue rate limiting | No | No | Yes |
| Live queue/role routing (no restart) | No | No | Yes |
| DAG workflows | Canvas API | None | Yes |
| Dependency injection | No | Yes (separate package) | Yes (built-in) |
| Broker flexibility | High | High | Redis only |

See [docs/celery-comparison.md](docs/celery-comparison.md) and [docs/taskiq-comparison.md](docs/taskiq-comparison.md) for detailed breakdowns.

---

## Key features

### Task introspection

Every task's full state is stored in Redis and queryable at any time via the HTTP API or admin dashboard — status, timestamps, input parameters, return value, error history, retry count, and last heartbeat.

### Retries and backoff

Per-task retry limits, base delay, and backoff strategy (`CONSTANT`, `LINEAR`, `EXPONENTIAL`, `EXPONENTIAL_JITTER`). Declare which exception types are retryable; anything else bypasses the retry budget and fails immediately. See [task lifecycle](docs/task-lifecycle.md).

### Dead letter queue

Tasks that exhaust their retry budget move to the DLQ when `dead_letter_policy=SAVE`, preserving the full error history. Browse, filter, and bulk-resubmit via the API.

### Heartbeat monitoring

Tasks call `await task.heartbeat()` on a configurable interval. The Cleaner marks tasks `stalled` when `max_heartbeat_interval` lapses — independent of process-level timeouts.

### Task cancellation

Cancel any task in `submitted`, `started`, or `scheduled` state via the API. Running tasks receive the cancellation signal at the next `await` checkpoint.

### Composable workflows

Break multi-step workflows into individual tasks connected as a **DAG** (directed acyclic graph). Each node is an independently tracked, retriable task — when one step fails, only that step retries, and steps that can run concurrently are distributed across available workers.

```text
[fetch_data]  →  [normalize]  →  [write_to_warehouse]
                                          ↓
                                  [send_report_email]
```

Because each DAG node is a standard registered task, the same task can be shared across multiple workflows — write `send_notification` once and reuse it as the terminal step in any pipeline.

Workflows can also fan out dynamically: a task can return a variable-length list of parallel child tasks at runtime, with an optional collector that runs once all children complete.

See [DAGs and task chaining](docs/dags.md) for the full API and examples.

### Dependency injection

Task functions declare shared resources — database sessions, HTTP clients, configuration — as typed parameters annotated with `Depends()`. The worker resolves each dependency before calling the task and guarantees cleanup (including generator teardown) after every execution, whether the task succeeded or raised.

```python
from typing import Annotated
from jobbers.di import Depends
from jobbers.registry import register_task

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with session_factory() as session:
        try:
            yield session
        finally:
            pass  # teardown always runs

@register_task(name="import_records", version=1)
async def import_records(
    file_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],   # injected; not part of the submitted payload
) -> dict:
    ...
```

Dependency graphs are validated for cycles at decoration time, shared deps are instantiated once per task execution, and test-time overrides are supported via a `dependency_overrides` context manager. See [dependency injection](docs/developer-guide.md#dependency-injection).

### Recurring scheduled tasks

Task groups can run on a cron schedule, defined once and promoted into the queue automatically at the configured interval. See [scheduled tasks](docs/cron-dags.md).

### Queue and role system

Named queues with per-queue concurrency limits and optional rate limits, assigned to workers via named roles. Workers detect queue and role changes at runtime — no restart required. See [resource management](docs/resource-management.md).

### OpenTelemetry

All four processes emit OTLP logs and metrics, compatible with any OTLP-capable backend — OpenObserve, Grafana, Datadog, etc. See [operations guide](docs/operations.md).

---

## How it works

Jobbers runs as four independent processes:

| Process | Role |
| ------- | ---- |
| **Manager** | FastAPI server (port 8000): task submission, status, cancellation, DLQ, queue/role management |
| **Worker** | Pulls tasks from Redis queues and executes them; handles retries, heartbeats, cancellation |
| **Scheduler** | Polls for due tasks — delayed retries and cron jobs — and re-enqueues them |
| **Cleaner** | Prunes stale state, enforces rate limits, detects stalled tasks |

Tasks move through well-defined states: `submitted → started → completed` on the happy path, with branches to `failed`, `stalled`, `scheduled` (retry delay), `cancelled`, and `dropped`. See [task lifecycle](docs/task-lifecycle.md) for the full state diagram.

---

## Getting started

```bash
git clone <repo-url>
cd jobbers
docker compose up
```

Admin dashboard: `http://localhost:3000` — API: `http://localhost:8000`

See [operations guide](docs/operations.md) for installation, environment variables, and production deployment, and [developer guide](docs/developer-guide.md) to define your first task.

---

## Documentation

| Document | Contents |
| -------- | -------- |
| [Developer guide](docs/developer-guide.md) | Task registration, dependency injection, submitting tasks, queue/role setup, DLQ management, full API reference |
| [Adapter selection](docs/adapter-selection.md) | Choosing between the msgpack (plain Redis) and JSON (Redis Stack) storage adapters |
| [Operations guide](docs/operations.md) | Installation, environment variables, Docker, monitoring |
| [Task lifecycle](docs/task-lifecycle.md) | State machine diagram, transition rules, shutdown policies |
| [DAGs and task chaining](docs/dags.md) | Static and dynamic DAG patterns |
| [Scheduled tasks](docs/cron-dags.md) | Cron-scheduled task groups |
| [Resource management](docs/resource-management.md) | Concurrency limits, rate limiting, worker sizing |
| [Background task concepts](docs/concepts.md) | Primer on task queues, retries, DLQs, and DAGs |
