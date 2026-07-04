# Jobbers — Developer Guide

Jobbers is a lightweight distributed task execution framework (similar to Celery) focused on task tracking, retry policies, heartbeat monitoring, and dead letter queues.

## Directory Structure

```text
jobbers/
├── jobbers/                   # Python backend package
│   ├── runners/               # Entry points for each process
│   ├── adapters/              # Pluggable adapters: task storage, dead-letter, routing, task/cron schedulers
│   │   ├── _shared.py         # SharedTaskAdapterMixin + _SharedRedisTaskSubmitBase (used by redis/ and redis_json/)
│   │   ├── redis/             # Plain-Redis adapter implementations
│   │   │   ├── routing_backend.py    # RedisQueueConfigAdapter, RedisTaskRoutingConfigAdapter, RedisRoutingBackend
│   │   │   ├── task_state.py         # RedisTaskState
│   │   │   ├── task_submit.py        # RedisTaskSubmit
│   │   │   ├── dead_queue.py         # RedisDeadQueue
│   │   │   ├── task_scheduler.py     # RedisTaskScheduler
│   │   │   ├── cron_dag_scheduler.py # RedisCronDAGScheduler
│   │   │   ├── cancellation_bus.py   # RedisCancellationBus
│   │   │   └── routing_notifications.py # RedisRoutingNotifications
│   │   ├── redis_json/        # Redis Stack (RedisJSON + RediSearch) adapter implementations
│   │   │   ├── routing_backend.py    # RedisJSONQueueConfigAdapter, RedisJSONTaskRoutingConfigAdapter, RedisJSONRoutingBackend
│   │   │   ├── task_state.py         # RedisJSONTaskState
│   │   │   ├── task_submit.py        # RedisJSONTaskSubmit
│   │   │   └── dead_queue.py         # RedisJSONDeadQueue
│   │   ├── sql/               # SQLAlchemy adapter implementations
│   │   │   ├── routing_backend.py    # SQLQueueConfigAdapter, SQLTaskRoutingConfigAdapter, SQLRoutingBackend
│   │   │   ├── task_state.py         # SQLTaskState (+ shared helpers imported by task_submit)
│   │   │   ├── task_submit.py        # SQLTaskSubmit
│   │   │   ├── dead_queue.py         # SQLDeadQueue
│   │   │   ├── task_scheduler.py     # SQLTaskScheduler
│   │   │   └── cron_dag_scheduler.py # SQLCronDAGScheduler
│   │   └── static/            # Read-only in-process adapters
│   │       ├── routing_backend.py    # StaticRoutingBackend
│   │       └── cron_dag_scheduler.py # StaticCronDAGScheduler
│   ├── protocols.py           # Protocol definitions
│   ├── models/                # Pydantic models + enums
│   ├── utils/                 # OpenTelemetry, serialization, dependency injection resolver
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
| **Scheduler** | `runners/scheduler_proc.py` | Polls Redis sorted sets for due scheduled tasks and cron DAGs; promotes them back into queues |

All four run as separate processes (separate Docker containers in production).

## Tech Stack

- **Python 3.11+**, FastAPI, asyncio, sqlalchemy[asyncio]
- **Redis** — task queues, task scheduler, dead letter queue, task state
- **SQLAlchemy** — queue/role config only when `ROUTING_BACKEND=sql`
- **Two interchangeable task state adapters:** `RedisJSONTaskState` (Redis JSON + RediSearch) and `RedisTaskState` (plain Redis + msgpack + sorted sets); each paired with a matching `RedisJSONTaskSubmit` / `RedisTaskSubmit`
- **Two interchangeable dead letter adapters:** `RedisJSONDeadQueue` (Redis JSON + RediSearch) and `RedisDeadQueue` (plain Redis + sorted sets)
- **Four interchangeable routing backends:** `sql`, `redis`, `redis_json`, `static` (see Routing Backends below)
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

### Synchronous (CPU-bound) Tasks

`@register_task` auto-detects whether the decorated function is `async def`. For genuinely
CPU-bound, blocking work (parsing, inference, image processing) that can't be written as
`async def`, just register a plain function instead — it runs in a dedicated
`multiprocessing.Process` rather than being awaited in-process, so a thread pool's GIL
contention isn't a concern.

```python
from jobbers.registry import register_task
from jobbers.sync_runner import SyncTaskContext

@register_task(name="crunch_numbers", version=1)
def crunch_numbers(ctx: SyncTaskContext, n: int) -> dict:
    for i in range(n):
        if ctx.cancelled:
            break          # cooperative — see caveat below
        ctx.heartbeat()    # explicit, same convention as the async path's task.heartbeat()
    return {"n": n}
```

- The function receives a `SyncTaskContext` as its first positional argument instead of
  using `get_current_task()`/`task.heartbeat()` — contextvars and the live Redis/SQL adapter
  don't cross a process boundary. `ctx.heartbeat()` pushes onto a queue the parent drains;
  `ctx.cancelled` is a cooperative flag the function must check itself.
- Dependency injection (`Depends()`) still works, but resolves *inside* the subprocess
  (against a dependency graph looked up fresh via the registry after the task module is
  reloaded there) — never across the process boundary by pickling the resolved value. This
  means provider functions can safely open process-local resources (e.g. their own DB
  connection) but must themselves be importable from the loaded task module.
- `WORKER_SYNC_PROCESSES` (default `2`) bounds how many sync-task subprocesses a worker runs
  concurrently — separate from `WORKER_CONCURRENT_TASKS`, which only bounds async tasks. If a
  sync task is pulled while `WORKER_SYNC_PROCESSES` is saturated, the worker requeues it
  immediately (`jobbers/runners/worker_proc.py`) rather than marking it STARTED and parking it
  — this avoids a false STALLED verdict from the Cleaner and avoids holding a
  `WORKER_CONCURRENT_TASKS` slot for the whole wait (which would otherwise starve async tasks
  behind a sync backlog). For deployments that want to eliminate sync/async contention
  entirely rather than just mitigate it, dedicate queues/roles to sync tasks and run those
  workers with `WORKER_CONCURRENT_TASKS == WORKER_SYNC_PROCESSES`.
- **Forced termination isn't transactional.** On timeout or cancellation the worker calls
  `.terminate()`/`.kill()` on the OS process — reliable for stopping further progress and
  freeing the slot, but it can't roll back work the function had already done (e.g. a
  half-finished external write). `on_shutdown=TaskShutdownPolicy.CONTINUE` opts out of forced
  termination, same as it does for async tasks via `asyncio.shield()`.
- A shared `ProcessPoolExecutor` was deliberately avoided: it has no public API to kill one
  in-flight task without tearing down the whole pool, so each sync task gets its own
  short-lived process instead — process-spawn overhead is the trade for genuine
  terminability.

## Queue & Role System

- **Queues**: named buckets with per-queue concurrency limit and optional rate limiting. Storage backend depends on `ROUTING_BACKEND`.
- **Roles**: named sets of queues assigned to workers. Workers consume all queues in their role. Storage backend depends on `ROUTING_BACKEND`.
- Workers detect role/queue config changes via a `refresh_tag` stored per-role by the routing backend. `TaskGenerator.queues()` polls on every iteration; it also subscribes to the Redis pub/sub channel `queue-config-refresh:{role}` for immediate notification when a tag changes.
- The refresh tag is bumped automatically on `POST /queues/{role}` (set queues), `PUT /roles/{role_name}` (update role), `POST /roles` (create role), `PUT /queues/{queue_name}` (update queue config), and `DELETE /queues/{queue_name}` (delete queue). It can also be triggered manually via `POST /roles/{role_name}/refresh`.
- `POST /queues/{role}`, `PUT /roles/{role_name}`, and `POST /roles` validate that all requested queue names exist before saving; unknown queues return 400.

## OpenTelemetry Metrics

All metrics use the OTLP exporter (configured in `jobbers/utils/otel.py`) and are visible in OpenObserve.

| Metric | Type | Unit | Source | Description |
| ------ | ---- | ---- | ------ | ----------- |
| `hit_counter` | UpDownCounter | — | `task_routes.py` | Active in-flight HTTP requests (increments on entry, decrements on exit) |
| `cancellations_requested` | Counter | `1` | `task_routes.py` | Task cancellation requests (tagged with `queue`, `task`) |
| `time_in_queue` | Histogram | `ms` | `task_generator.py` | Time a task spent waiting in the queue before being picked up (tagged with `queue`, `role`, `task`) |
| `tasks_selected` | Counter | `1` | `task_generator.py` | Tasks pulled from a queue by a worker (tagged with `queue`, `role`, `task`) |
| `queue_config_refreshes` | Counter | `1` | `task_generator.py` | Queue-list refreshes triggered on a worker (tagged with `role`); fires each time a worker reloads its queue assignment due to a `refresh_tag` change |
| `refresh_lag_ms` | Histogram | `ms` | `task_generator.py` | Lag between when the `refresh_tag` was bumped (ULID timestamp) and when the worker picked up the change (tagged with `role`) |

## Adapter Architecture

All pluggable storage is expressed as `@runtime_checkable Protocol` classes in `protocols.py`. The protocols and their concrete implementations are:

### Adapter naming convention

All concrete adapter classes follow the `{Backend}{Protocol}` pattern:

- **Backend** prefix: `Redis` (plain Redis), `RedisJSON` (Redis Stack), `SQL` (SQLAlchemy), `Static` (in-process read-only)
- **Protocol** suffix: the protocol name without the `Protocol` suffix (e.g., `TaskAdapter`, `DeadQueue`, `TaskScheduler`, `CronDAGScheduler`, `RoutingBackend`, `QueueConfigAdapter`, `TaskRoutingConfigAdapter`)

Examples: `RedisTaskState`, `RedisTaskSubmit`, `RedisJSONDeadQueue`, `SQLCronDAGScheduler`, `StaticRoutingBackend`.

No aliases — always use the full `{Backend}{Protocol}` name.

### Adapter file layout

Each backend is a package under `adapters/`. Each file holds one protocol class (or a group of sub-protocol classes that compose up to the same parent protocol). The pattern is `adapters/<backend>/<protocol>.py`:

- Sub-protocols that compose into a single parent protocol are colocated in one file. For example, `RedisQueueConfigAdapter` and `RedisTaskRoutingConfigAdapter` (sub-protocols of `RoutingBackendProtocol`) live together with `RedisRoutingBackend` in `redis/routing_backend.py`.
- Each backend `__init__.py` re-exports all public classes, so existing `from jobbers.adapters.redis import X` imports continue to work unchanged.
- Private helpers shared within a backend live in `<backend>/_helpers.py` (e.g. `redis/_helpers.py` for the msgpack `_pack` helper).

### Routing protocols

| Protocol | sql | redis | redis_json | static |
| --- | --- | --- | --- | --- |
| `QueueConfigProtocol` | `SQLQueueConfigAdapter` | `RedisQueueConfigAdapter` | `RedisJSONQueueConfigAdapter` | — |
| `TaskRoutingConfigProtocol` | `SQLTaskRoutingConfigAdapter` | `RedisTaskRoutingConfigAdapter` | `RedisJSONTaskRoutingConfigAdapter` | — |
| `RoutingBackendProtocol` | `SQLRoutingBackend` | `RedisRoutingBackend` | `RedisJSONRoutingBackend` | `StaticRoutingBackend` |

`RoutingBackendProtocol` is a composite of `QueueConfigProtocol` + `TaskRoutingConfigProtocol` (minus `get_queue_limits`). The `sql`, `redis`, and `redis_json` routing backends are thin delegation wrappers that compose a `_qca` (`QueueConfigProtocol`) and `_rca` (`TaskRoutingConfigProtocol`) sub-adapter internally. `StaticRoutingBackend` is a monolith (no sub-adapters) and raises `RoutingBackendReadOnlyError` on all write operations.

`get_queue_limits` exists on `QueueConfigProtocol` and all three dynamic implementations but is absent from `StaticRoutingBackend` (which is not expected to implement `QueueConfigProtocol`).

### Task storage protocols (split-store design)

Task storage is split across three independent protocols so each concern can use a different backend:

| Protocol | Responsibility | Current implementations |
| --- | --- | --- |
| `TaskStateProtocol` | Task blob persistence, heartbeats, fan-in sets, DAG run index | `RedisTaskState`, `RedisJSONTaskState`, `SQLTaskState` |
| `TaskSubmitProtocol` | Composite submit/pop operations that require co-located state and queue (Lua scripts in Redis) | `RedisTaskSubmit`, `RedisJSONTaskSubmit`, `SQLTaskSubmit` |
| `TaskQueueProtocol` | Active queue membership, enqueue/pop, rate limiting | (Redis sorted-set logic embedded in adapters above) |
| `TaskSchedulerProtocol` | Delayed/scheduled task queue | `RedisTaskScheduler`, `SQLTaskScheduler` |
| `DeadQueueProtocol` | Dead letter queue | `RedisDeadQueue`, `RedisJSONDeadQueue`, `SQLDeadQueue` |
| `CronDAGSchedulerProtocol` | Recurring cron-scheduled DAG entries | `RedisCronDAGScheduler`, `SQLCronDAGScheduler`, `StaticCronDAGScheduler` |

Each protocol has an **Atomic sub-protocol** that extends it with pipeline-staging methods (`stage_*`) and a `pipeline()` factory. `StateManager` checks whether the task adapter, scheduler, and DLQ all implement their Atomic sub-protocols at construction time and selects between **atomic pipeline mode** (MULTI/EXEC across all stores) and **saga mode** (sequential writes with Cleaner compensation):

| Atomic sub-protocol | Adds over base |
| --- | --- |
| `AtomicTaskStateProtocol` | `pipeline()`, `stage_save()`, `stage_requeue()`, `stage_submit_task()`, `stage_remove_from_queue()`, `stage_remove_heartbeat()`, `stage_init_fan_in()`, `read_for_watch()`, `atomic_dispatch_scheduled()` |
| `AtomicTaskSchedulerProtocol` | `pipeline()`, `stage_add()`, `stage_remove()` |
| `AtomicDeadQueueProtocol` | `backend_key`, `pipeline()` |
| `AtomicCronDAGSchedulerProtocol` | `backend_key`, `pipeline()`, `stage_reschedule()`, `stage_set_active_run()`, `stage_clear_active_run()` |

`AtomicCronDAGSchedulerProtocol` allows `StateManager` to fold cron reschedule and active-run marker ops into the same pipeline as task-state ops when the cron scheduler and task-state adapter share a backend (`backend_key` match). `StaticCronDAGScheduler` does not implement the atomic sub-protocol (no shared backend); `StateManager` falls back to sequential async calls.

`atomic_dispatch_scheduled(task, stage_extra)` encapsulates the atomic dispatch: read task (under WATCH for Redis, SELECT FOR UPDATE for SQL), check status, set SUBMITTED, stage requeue, call `stage_extra` for additional staged ops (e.g., scheduler removal), commit. Implemented by Redis adapters (WATCH/MULTI, retries on WatchError) and `SQLTaskState` (SELECT FOR UPDATE, no retry needed). `SQLTaskState` now implements the full `AtomicTaskStateProtocol`, enabling atomic pipeline mode for all-SQL deployments.

`stage_remove_heartbeat(pipe, task)` stages a heartbeat removal onto an existing pipeline. For Redis, removes the entry from the heartbeat sorted set; for SQL, sets `heartbeat_at = NULL` in the tasks table.

`StateManager` holds `task_state: TaskStateProtocol` and `task_submit: TaskSubmitProtocol` as two separate constructor parameters. Each is now a distinct class: `RedisTaskState`/`RedisTaskSubmit` share the same Redis client but have separate responsibilities. Both are created by `create_redis_task_adapters()` in `db.py`. See [docs/datastore-architecture.md](docs/datastore-architecture.md) for a full treatment of consistency modes and what alternative datastores (SQL task state, RabbitMQ queuing) would require.

## Routing Backends

The routing backend controls where queue/role/task-routing config is stored. Select via `ROUTING_BACKEND`:

| Value | Storage | SQL needed? | Dynamic CRUD? |
| ------- | --------- | ------------- | --------------- |
| `sql` (default) | SQLAlchemy (SQLite or Postgres) | Yes | Yes |
| `redis` | Plain Redis keys | No | Yes |
| `redis_json` | RedisJSON + RediSearch (Redis Stack) | No | Yes |
| `static` | In-process memory (read-only) | No | No (405 on writes) |

### Static backend configuration

Config is loaded once at startup. Priority (highest to lowest):

1. **Programmatic** — call `db.register_routing_backend(StaticRoutingBackend(...))` before `init_state_manager()`
2. **Config file via env var** — `STATIC_CONFIG_FILE=/path/to/routing.json` (also accepts `.yaml`/`.yml` with `pyyaml` installed)
3. **CLI runners** — pass `--static-config routing.json` to any runner (implies static backend)
4. **Built-in defaults** — one `default` queue, one `default` role

Config file format (`routing.json`):

```json
{
  "queues": [{"name": "default", "max_concurrent": 10}],
  "roles": {"default": ["default"]},
  "routing": []
}
```

## Key Environment Variables

| Variable | Default | Used by |
| ---------- | --------- | --------- |
| `WORKER_ROLE` | `"default"` | Worker |
| `WORKER_TTL` | `50` | Worker (max tasks before restart) |
| `WORKER_CONCURRENT_TASKS` | `5` | Worker |
| `WORKER_SYNC_PROCESSES` | `2` | Worker (max concurrent sync-task subprocesses) |
| `SCHEDULER_POLL_INTERVAL` | `5.0` | Scheduler |
| `SCHEDULER_BATCH_SIZE` | `1` | Scheduler |
| `TASK_BACKEND` | `"redis_json"` | All (`"redis_json"`, `"redis"`, or `"sql"`) |
| `DLQ_BACKEND` | `"redis"` | All (`"redis"`, `"redis_json"`, or `"sql"`) |
| `TASK_SCHEDULER_BACKEND` | `"redis"` | All (`"redis"` or `"sql"`) |
| `ROUTING_BACKEND` | `"sql"` | All (`"sql"`, `"redis"`, `"redis_json"`, or `"static"`) |
| `CRON_DAG_SCHEDULER_BACKEND` | `"redis"` | All (`"redis"`, `"sql"`, or `"static"`); `static` is read-only in-memory (state resets on restart) |
| `REDIS_URL` | `redis://localhost:6379` | All |
| `SQL_PATH` | `sqlite+aiosqlite:///jobbers.db` | All (used when any backend is `"sql"`; use PostgreSQL for multi-worker deployments) |
| `STATIC_CONFIG_FILE` | — | All (path to JSON/YAML routing config; requires `ROUTING_BACKEND=static`) |

## Testing

- Uses `fakeredis` (in-memory, supports Lua + JSON modules) — no real Redis needed for most tests
- `task_adapter` fixture is parametrized over `["redis", "redis_json", "sql"]` — `(RedisTaskState, RedisTaskSubmit)` via FakeRedis, `(RedisJSONTaskState, RedisJSONTaskSubmit)` via real Redis Stack (skipped if unavailable), `(SQLTaskState, SQLTaskSubmit)` via in-memory SQLite
- `queue_config_adapter` fixture is parametrized over `["sql", "redis", "redis_json"]`
- `task_routing_config_adapter` fixture is parametrized over `["sql", "redis", "redis_json"]`
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
| **Protocol contract** | `tests/adapters/test_task_adapter_common.py`, `test_dead_queue_common.py`, `test_queue_config_common.py`, `test_task_routing_config_common.py` | `task_adapter` / `dead_queue` / `queue_config_adapter` / `task_routing_config_adapter` (each parametrized over all implementations) | Verify every implementation satisfies the protocol. Adding a new adapter means adding a fixture variant — all contract tests run automatically. |
| **Implementation edge cases** | `tests/adapters/test_msgpack_adapter.py`, `test_json_adapter.py` | `msgpack_adapter` / `json_adapter` | Cover implementation-specific behaviour that is not a protocol requirement (e.g., `RedisTaskState` sorting limitations, null JSON blobs in `RedisJSONTaskState`). |
| **Orchestration** | `test_state_manager.py`, `test_task_processor.py`, `test_task_routes.py`, `test_task_generator.py` | `DummyTaskAdapter` or `Mock(spec=StateManager)` | Test coordination logic without touching real adapters; fast, no Redis Stack required. |

#### Key rules

- **Prefer common tests.** If a behaviour belongs to the protocol, put it in the common file and run it against all implementations.
- **xfail, don't skip, for known limitations.** If one implementation cannot satisfy a contract test, mark it `@pytest.mark.xfail(strict=True)` with a reason. This keeps the test discoverable and will alert you when the limitation is fixed.
- **Adapter fixtures**
  - `task_adapter` (parametrized `["redis", "redis_json", "sql"]`) — yields `(state, submit)` pairs: `(RedisTaskState, RedisTaskSubmit)` via FakeRedis + `(RedisJSONTaskState, RedisJSONTaskSubmit)` via real Redis Stack (skipped if unavailable) + `(SQLTaskState, SQLTaskSubmit)` via in-memory SQLite.
  - `dead_queue` (parametrized `["redis", "redis_json", "sql"]`) — same pattern, yields `(dq, task_state_adapter)`.
  - `queue_config_adapter` (parametrized `["sql", "redis", "redis_json"]`) — SQL via in-memory SQLite, Redis via FakeRedis, Redis JSON via real Redis Stack (skipped if unavailable).
  - `task_routing_config_adapter` (parametrized `["sql", "redis", "redis_json"]`) — same pattern.
  - `scheduler` (parametrized `["redis", "sql"]`) — `RedisTaskScheduler` via FakeRedis + `SQLTaskScheduler` via in-memory SQLite; in `tests/schedulers/`.
  - `cron_dag_scheduler` (parametrized `["redis", "sql", "static"]`) — all three cron DAG scheduler backends; in `tests/schedulers/`.
  - `mutable_cron_dag_scheduler` (parametrized `["redis", "sql"]`) — mutable backends only (static raises on add/remove); in `tests/schedulers/`.
  - `msgpack_adapter` — `(RedisTaskState, RedisTaskSubmit)` pair via FakeRedis; for Redis-specific edge cases.
  - `json_adapter` — `(RedisJSONTaskState, RedisJSONTaskSubmit)` pair via real Redis Stack; for Redis Stack-specific edge cases.
  - `json_dead_queue` — `RedisJSONDeadQueue` backed by the state adapter from `json_adapter`; for `RedisJSONDeadQueue`-specific edge cases.
  - `DummyTaskAdapter` (in `tests/conftest.py`) — in-memory stub; use for `state_manager` orchestration tests.

#### Known hard-to-cover paths (concurrency guards)

The following lines cannot be covered reliably without concurrent execution, low-level patching, or specially broken inputs:

- `jobbers/adapters/redis.py` — `if task_data is None: continue` in `RedisTaskState.clean_terminal_tasks` (key deleted between `scan_iter` and `GET`)
- `jobbers/adapters/redis_json.py` — `if role_doc is not None:` false branch in `RedisJSONQueueConfigAdapter.delete_queue`: JSON document deleted between the RediSearch result and the `JSON.GET` pipeline call (requires concurrent deletion)
- `jobbers/task_generator.py` line 151 — the `if task:` requeue branch in `TaskGenerator.__anext__`'s `CancelledError` handler (task is always `None` when `get_next_task` raises, making this branch unreachable in practice)
- `jobbers/di.py` lines 227, 232 — exception handlers in `DependencyResolver.__aexit__` that catch errors thrown by generator cleanup; requires an async/sync generator that raises during `.aclose()` / `.close()`
- `jobbers/task_processor.py` line 80 — `hints = {}` fallback when `get_type_hints()` raises; only reachable with unresolvable forward references in task annotations

These should be addressed with integration tests or by restructuring the code so the guard is testable.
