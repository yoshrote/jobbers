# Jobbers — Developer Guide

Jobbers is a lightweight distributed task execution framework (similar to Celery) focused on task tracking, retry policies, heartbeat monitoring, and dead letter queues.

## Directory Structure

```
jobbers/
├── jobbers/                   # Python backend package
│   ├── runners/               # Entry points for each process
│   ├── adapters/              # Task storage backends (Redis JSON vs msgpack)
│   ├── models/                # Pydantic models + enums
│   ├── utils/                 # OpenTelemetry, serialization
│   ├── state_manager.py       # Central Redis/SQLite state management
│   ├── task_processor.py      # Single-task execution lifecycle
│   ├── task_generator.py      # Async iterator: yields tasks from queues
│   ├── task_routes.py         # FastAPI route definitions
│   ├── registry.py            # @register_task() decorator + global task map
│   ├── validation.py          # Task parameter validation
│   └── db.py                  # Redis + SQLite connection singletons
├── frontend/                  # React 19 + Vite 7 admin UI
│   └── src/
│       ├── api/client.js      # All API calls (no direct fetch in components)
│       ├── pages/             # One component per route
│       └── components/        # Reusable UI (StatusBadge, TaskNameSelect)
├── tests/                     # pytest suite
│   ├── adapters/              # Adapter-specific tests
│   ├── models/                # Model unit tests
│   └── runners/               # Runner integration tests
├── docker-compose.yaml
└── openapi.json               # API contract (source of truth for frontend)
```

## The Four Jobber Processes

| Process | Entry Point | Role |
|---------|-------------|------|
| **Manager** | `runners/manager_proc.py` | FastAPI web server (port 8000): task submission, status, cancellation, DLQ, queue/role CRUD |
| **Worker** | `runners/worker_proc.py` | Pulls tasks from Redis queues and executes them; handles retries, heartbeats, cancellation |
| **Cleaner** | `runners/cleaner_proc.py` | Maintenance: prunes stale Redis state, rate-limit entries, DLQ entries, detects stalled tasks |
| **Scheduler** | `runners/scheduler_proc.py` | Polls SQLite for due scheduled tasks and promotes them back into Redis queues |

All four run as separate processes (separate Docker containers in production).

## Tech Stack

- **Python 3.11+**, FastAPI, asyncio, aiosqlite
- **Redis** — task queues and live task state
- **SQLite** — queue/role config, task scheduler, dead letter queue
- **Two interchangeable adapters:** `JsonTaskAdapter` (Redis JSON + RediSearch) and `MsgpackTaskAdapter` (plain Redis + msgpack + sorted sets)
- **React 19 + Vite 7 + React Router 6** (no TypeScript, plain CSS)
- **OpenTelemetry** (OTLP → OpenObserve)

## Task Lifecycle

```
UNSUBMITTED → SUBMITTED → STARTED → COMPLETED
                                  → FAILED (no retries left) → [DLQ if policy=SAVE]
                                  → SCHEDULED (retry delay) → re-queued by scheduler
                                  → CANCELLED (user request)
                                  → STALLED (heartbeat timeout or SIGTERM + STOP policy)
                                  → DROPPED (unknown task type)
```

Workers apply the `on_shutdown` policy on SIGTERM: `STOP` (cancel → STALLED), `RESUBMIT` (re-enqueue), `CONTINUE` (shield to completion).

## Task Registration

```python
from jobbers.registry import register_task
from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy

@register_task(
    name="my_task",
    version=1,
    max_retries=5,
    retry_delay=10,
    backoff_strategy=BackoffStrategy.EXPONENTIAL,
    dead_letter_policy=DeadLetterPolicy.SAVE,
    max_heartbeat_interval=dt.timedelta(minutes=5),
)
async def my_task(**kwargs):
    task.heartbeat()  # call periodically for long-running tasks
    return {"result": "value"}
```

## Queue & Role System

- **Queues**: named buckets with per-queue concurrency limit and optional rate limiting. Stored in SQLite `queues` table.
- **Roles**: named sets of queues assigned to workers. Workers consume all queues in their role. Stored in SQLite `roles` table.
- Workers detect role/queue config changes via a refresh tag TTL in `TaskGenerator`.

## Key Environment Variables

| Variable | Default | Used by |
|----------|---------|---------|
| `WORKER_ROLE` | `"default"` | Worker |
| `WORKER_TTL` | `50` | Worker (max tasks before restart) |
| `WORKER_CONCURRENT_TASKS` | `5` | Worker |
| `SCHEDULER_POLL_INTERVAL` | `5.0` | Scheduler |
| `SCHEDULER_BATCH_SIZE` | `1` | Scheduler |
| `REDIS_URL` | `redis://localhost:6379` | All |
| `SQLITE_PATH` | `jobbers.db` | All |

## Testing

- Uses `fakeredis` (in-memory, supports Lua + JSON modules) — no real Redis needed
- `task_adapter` fixture is parametrized over both `JsonTaskAdapter` and `MsgpackTaskAdapter`
- Key test files: `test_state_manager.py`, `test_task_processor.py`, `test_task_routes.py`, `test_task_generator.py`
- Run with: `pytest` (coverage configured in `pyproject.toml`, excludes `otel.py`)

## Docker

```
docker compose up
```

Services: `redis`, `manager` (8000), `worker`, `collector` (OTLP 4317), `openobserve` (5080), `frontend` (3000).
Vite proxies `/api/*` → `http://manager:8000`.
