# Jobbers — Developer Guide

Jobbers is a lightweight distributed task execution framework (similar to Celery) focused on task tracking, retry policies, heartbeat monitoring, and dead letter queues.

## Directory Structure

```text
jobbers/
├── jobbers/                   # Python backend package
│   ├── runners/               # Entry points for each process
│   ├── adapters/              # Task storage backends (Redis JSON vs msgpack)
│   ├── models/                # Pydantic models + enums
│   ├── utils/                 # OpenTelemetry, serialization
│   ├── di.py                  # FastAPI-styled dependency injection resolver
│   ├── state_manager.py       # Central Redis/SQL state management
│   ├── task_processor.py      # Single-task execution lifecycle
│   ├── task_generator.py      # Async iterator: yields tasks from queues
│   ├── task_routes.py         # FastAPI route definitions
│   ├── registry.py            # @register_task() decorator + global task map
│   ├── validation.py          # Task parameter validation
│   └── db.py                  # Redis + SQL connection singletons
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
| --------- | ------------- | ------ |
| **Manager** | `runners/manager_proc.py` | FastAPI web server (port 8000): task submission, status, cancellation, DLQ, queue/role CRUD |
| **Worker** | `runners/worker_proc.py` | Pulls tasks from Redis queues and executes them; handles retries, heartbeats, cancellation |
| **Cleaner** | `runners/cleaner_proc.py` | Maintenance: prunes stale Redis state, rate-limit entries, DLQ entries, detects stalled tasks |
| **Scheduler** | `runners/scheduler_proc.py` | Polls SQL for due scheduled tasks and promotes them back into Redis queues |

All four run as separate processes (separate Docker containers in production).

## Tech Stack

- **Python 3.11+**, FastAPI, asyncio, sqlalchemy[asyncio]
- **Redis** — task queues, task scheduler, dead letter queue, task state
- **SQLAlchemy** — queue/role config
- **Two interchangeable task adapters:** `JsonTaskAdapter` (Redis JSON + RediSearch) and `MsgpackTaskAdapter` (plain Redis + msgpack + sorted sets)
- **Two interchangeable dead letter adapters:** `JsonDeadQueue` (Redis JSON + RediSearch) and `DeadQueue` (plain Redis + sorted sets)
- **React 19 + Vite 7 + React Router 6** (no TypeScript, plain CSS)
- **OpenTelemetry** (OTLP → OpenObserve)

## Task Lifecycle

```mermaid
UNSUBMITTED → SUBMITTED → STARTED → COMPLETED → [DAG callbacks / fan-out children]
           → SCHEDULED (task.schedule() / POST /schedule-task) → re-queued by scheduler → SUBMITTED
                                  → FAILED (no retries left) → [DLQ if policy=SAVE]
                                  → SCHEDULED (retry delay set) → re-queued by scheduler → SUBMITTED
                                  → UNSUBMITTED (immediate retry, no retry_delay) → SUBMITTED
                                  → CANCELLED (user request)
                                  → STALLED (heartbeat timeout or SIGTERM + STOP policy)
                                  → DROPPED (unknown task type)
```

Workers apply the `on_shutdown` policy on SIGTERM: `STOP` (→ STALLED), `RESUBMIT` (→ UNSUBMITTED, re-enqueued), `CONTINUE` (shield to completion).

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

- **Queues**: named buckets with per-queue concurrency limit and optional rate limiting. Stored in SQL `queues` table.
- **Roles**: named sets of queues assigned to workers. Workers consume all queues in their role. Stored in SQL `roles` table.
- Workers detect role/queue config changes via a refresh tag TTL in `TaskGenerator`.

## Key Environment Variables

| Variable | Default | Used by |
| ---------- | --------- | --------- |
| `WORKER_ROLE` | `"default"` | Worker |
| `WORKER_TTL` | `50` | Worker (max tasks before restart) |
| `WORKER_CONCURRENT_TASKS` | `5` | Worker |
| `SCHEDULER_POLL_INTERVAL` | `5.0` | Scheduler |
| `SCHEDULER_BATCH_SIZE` | `1` | Scheduler |
| `REDIS_URL` | `redis://localhost:6379` | All |
| `SQL_PATH` | `sqlite+aiosqlite:///jobbers.db` | All |
| `CONFIG_CACHE_TTL` | `30` | All (queue/routing config cache TTL in seconds) |

## Testing

- Uses `fakeredis` (in-memory, supports Lua + JSON modules) — no real Redis needed
- `task_adapter` fixture is parametrized over both `JsonTaskAdapter` and `MsgpackTaskAdapter`
- Key test files: `test_state_manager.py`, `test_task_processor.py`, `test_task_routes.py`, `test_task_generator.py`
- Run with: `pytest` (coverage configured in `pyproject.toml`, excludes `otel.py`)

## Code Quality

Install dev dependencies first:

```bash
pip install -e ".[test]"
```

### pytest — run tests with coverage

```bash
pytest
# or with explicit coverage report:
pytest --cov=jobbers --cov-report=term-missing
```

### ruff — lint and format

```bash
# Check for lint errors
ruff check .

# Auto-fix lint errors
ruff check --fix .

# Check formatting (non-destructive)
ruff format --check .

# Apply formatting
ruff format .
```

### mypy — static type checking

```bash
mypy jobbers
```

mypy is configured with `strict = true` and runs only on the `jobbers/` package (tests are excluded).

### Test Architecture

The test suite follows a layered approach designed for speed and systematic protocol coverage:

#### Three tiers of tests

| Tier | Where | Fixture | Purpose |
| ------ | ------- | --------- | --------- |
| **Protocol contract** | `tests/adapters/test_task_adapter_common.py`, `test_dead_queue_common.py` | `task_adapter` / `dead_queue` (parametrized over all implementations) | Verify every implementation satisfies the protocol. Adding a new adapter means adding a fixture variant — all contract tests run automatically. |
| **Implementation edge cases** | `tests/adapters/test_msgpack_adapter.py`, `test_json_adapter.py` | `msgpack_adapter` / `json_adapter` | Cover implementation-specific behaviour that is not a protocol requirement (e.g., sorting limitations, null JSON blobs). |
| **Orchestration** | `test_state_manager.py`, `test_task_processor.py`, `test_task_routes.py`, `test_task_generator.py` | `DummyTaskAdapter` or `Mock(spec=StateManager)` | Test coordination logic without touching real adapters; fast, no Redis Stack required. |

#### Key rules

- **Prefer common tests.** If a behaviour belongs to the protocol, put it in the common file and run it against all implementations.
- **xfail, don't skip, for known limitations.** If one implementation cannot satisfy a contract test, mark it `@pytest.mark.xfail(strict=True)` with a reason. This keeps the test discoverable and will alert you when the limitation is fixed.
- **Adapter fixtures**
  - `task_adapter` (parametrized `["raw", "json"]`) — MsgpackTaskAdapter via FakeRedis + JsonTaskAdapter via real Redis Stack (skipped if unavailable).
  - `dead_queue` (parametrized `["raw", "json"]`) — same pattern, yields `(dq, task_adapter)`.
  - `msgpack_adapter` — plain MsgpackTaskAdapter via FakeRedis; for msgpack-specific edge cases.
  - `json_adapter` — JsonTaskAdapter via real Redis Stack; for json-specific edge cases.
  - `json_dead_queue` — JsonDeadQueue backed by `json_adapter`; for JsonDeadQueue-specific edge cases.
  - `DummyTaskAdapter` (in `tests/conftest.py`) — in-memory stub; use for `state_manager` orchestration tests.

#### Known hard-to-cover paths (concurrency guards)

The following lines cannot be covered reliably without concurrent execution, low-level patching, or specially broken inputs:

- `jobbers/adapters/raw_redis.py` line 312 — `if task_data is None: continue` in `MsgpackTaskAdapter.clean_terminal_tasks` (key deleted between `scan_iter` and `GET`)
- `jobbers/task_generator.py` line 156 — the `if task:` requeue branch in `TaskGenerator.__anext__`'s `CancelledError` handler (task is always `None` when `get_next_task` raises, making this branch unreachable in practice)
- `jobbers/di.py` lines 227, 232 — exception handlers in `DependencyResolver.__aexit__` that catch errors thrown by generator cleanup; requires an async/sync generator that raises during `.aclose()` / `.close()`
- `jobbers/task_processor.py` line 80 — `hints = {}` fallback when `get_type_hints()` raises; only reachable with unresolvable forward references in task annotations

These should be addressed with integration tests or by restructuring the code so the guard is testable.
