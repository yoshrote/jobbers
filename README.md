# Jobbers

A lightweight distributed task execution framework for Python, built on asyncio. If you've used Celery, Jobbers will feel familiar — but with first-class support for task introspection, cancellation, stall detection, and OpenTelemetry observability out of the box.

---

## Why Jobbers?

| | Celery | Jobbers |
| --- | --- | --- |
| asyncio-native tasks | Limited | Yes |
| Task introspection API | No | Yes |
| Cancel running/queued/scheduled tasks | No | Yes |
| Stall detection via heartbeats | No | Yes |
| Dead letter queue with history | No | Yes |
| Built-in OpenTelemetry metrics | No | Yes |
| Per-queue rate limiting | No | Yes |

See [docs/celery-comparison.md](docs/celery-comparison.md) for a detailed breakdown.

---

## How It Works

Jobbers runs as four independent processes:

| Process | Role |
| --------- | ------ |
| **Manager** | FastAPI web server (port 8000): task submission, status, cancellation, DLQ, queue/role management |
| **Worker** | Pulls tasks from Redis queues and executes them; handles retries, heartbeats, cancellation |
| **Cleaner** | Prunes stale state, rate-limit entries, and detects stalled tasks |
| **Scheduler** | Polls for due retry-delayed tasks and re-enqueues them |

Tasks move through the following states:

```text
UNSUBMITTED → SUBMITTED → STARTED → COMPLETED
                                   → FAILED → [DLQ if policy=save]
                                   → SCHEDULED (retry delay, re-queued by Scheduler)
                                   → CANCELLED
                                   → STALLED (heartbeat timeout or SIGTERM + stop policy)
                                   → DROPPED (unknown task type)
```

See [docs/task-lifecycle.md](docs/task-lifecycle.md) for a full state diagram and details on how `TaskConfig` settings influence each transition.

---

## Key Features

### asyncio Native

Every task function is a plain `async def`. You get the full asyncio ecosystem — `aiohttp`, `asyncpg`, `anyio`, etc. — without wrappers or thread-pool workarounds.

### Task Introspection

Jobbers stores the live state of every task in Redis. Query what's running, what's queued, what's waiting on a retry delay, and what failed — at any time, via the HTTP API or the admin UI.

### Retries and Backoff

Configure per-task retry limits, base delay, and backoff strategy (`LINEAR`, `EXPONENTIAL`, `EXPONENTIAL_JITTER`). Expected exception types are retried; unexpected exceptions go straight to `failed`.

### Dead Letter Queue

Tasks configured with `dead_letter_policy=SAVE` are written to the DLQ on permanent failure, preserving the full error history. Failed tasks can be browsed, inspected, and bulk-resubmitted via the API.

### Heartbeat Monitoring

Long-running tasks call `await task.heartbeat()` periodically. The Cleaner marks tasks `stalled` when the heartbeat interval lapses, giving you visibility into hung workers without relying on process-level timeouts alone.

### Queue and Role System

**Queues** are named buckets with per-queue concurrency limits and optional rate limits. **Roles** are named sets of queues assigned to workers. Workers detect queue and role changes automatically — no restart required.

### Cancellation

Any task in `submitted`, `started`, or `scheduled` state can be cancelled via the API. Running tasks receive the signal at the next `await` checkpoint.

### Scheduled Tasks (DAGs)

Tasks can be scheduled as cron jobs or chained into directed acyclic graphs (DAGs). See [docs/dags.md](docs/dags.md) and [docs/cron-dags.md](docs/cron-dags.md).

### OpenTelemetry

All four processes emit OTLP logs and metrics automatically. See [docs/operations.md](docs/operations.md) for configuration.

---

## Docs

| Document | Contents |
| --- | --- |
| [docs/developer-guide.md](docs/developer-guide.md) | Task registration, submitting tasks, queue/role setup, DLQ management, full API reference |
| [docs/operations.md](docs/operations.md) | Installation, environment variables, Docker, monitoring |
| [docs/task-lifecycle.md](docs/task-lifecycle.md) | State machine diagram, transition rules, shutdown policies |
| [docs/dags.md](docs/dags.md) | DAG task chaining |
| [docs/cron-dags.md](docs/cron-dags.md) | Cron-scheduled tasks |
| [docs/celery-comparison.md](docs/celery-comparison.md) | Detailed Celery vs Jobbers comparison |
| [docs/resource-management.md](docs/resource-management.md) | Concurrency limits, rate limiting, worker sizing |
