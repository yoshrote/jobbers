# Jobbers — Developer Guide

Jobbers is a lightweight distributed task execution framework (similar to Celery) focused on task tracking, retry policies, heartbeat monitoring, and dead letter queues.

## Directory Structure

```text
jobbers/
├── jobbers/                   # Python backend package
│   ├── runners/               # Entry points for each process
│   ├── adapters/              # Pluggable adapters: task storage, dead-letter, routing, task/cron schedulers
│   │   ├── redis.py           # All plain-Redis adapter implementations
│   │   ├── redis_json.py      # All Redis Stack (RedisJSON + RediSearch) adapter implementations
│   │   ├── sql.py             # All SQLAlchemy adapter implementations
│   │   └── static.py         # Read-only in-process adapters (routing + cron DAG)
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
- **Two interchangeable task adapters:** `RedisJSONTaskAdapter` (Redis JSON + RediSearch) and `RedisTaskAdapter` (plain Redis + msgpack + sorted sets)
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

Examples: `RedisTaskAdapter`, `RedisJSONDeadQueue`, `SQLCronDAGScheduler`, `StaticRoutingBackend`.

No aliases — always use the full `{Backend}{Protocol}` name.

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
| `TaskStateProtocol` | Task blob persistence, heartbeats, fan-in sets, DAG run index | `RedisTaskAdapter`, `RedisJSONTaskAdapter`, `SQLTaskAdapter` |
| `TaskSubmitProtocol` | Composite submit/pop operations that require co-located state and queue (Lua scripts in Redis) | `RedisTaskAdapter`, `RedisJSONTaskAdapter`, `SQLTaskAdapter` |
| `TaskQueueProtocol` | Active queue membership, enqueue/pop, rate limiting | (Redis sorted-set logic embedded in adapters above) |
| `TaskSchedulerProtocol` | Delayed/scheduled task queue | `RedisTaskScheduler`, `SQLTaskScheduler` |
| `DeadQueueProtocol` | Dead letter queue | `RedisDeadQueue`, `RedisJSONDeadQueue`, `SQLDeadQueue` |
| `CronDAGSchedulerProtocol` | Recurring cron-scheduled DAG entries | `RedisCronDAGScheduler`, `SQLCronDAGScheduler`, `StaticCronDAGScheduler` |

Each protocol has an **Atomic sub-protocol** that extends it with pipeline-staging methods (`stage_*`) and a `pipeline()` factory. `StateManager` checks whether the task adapter, scheduler, and DLQ all implement their Atomic sub-protocols at construction time and selects between **atomic pipeline mode** (MULTI/EXEC across all stores) and **saga mode** (sequential writes with Cleaner compensation):

| Atomic sub-protocol | Adds over base |
| --- | --- |
| `AtomicTaskStateProtocol` | `pipeline()`, `stage_save()`, `stage_requeue()`, `stage_submit_task()`, `stage_remove_from_queue()`, `stage_remove_heartbeat()`, `stage_init_fan_in()`, `read_for_watch()`, `optimistic_dispatch_scheduled()` |
| `AtomicTaskSchedulerProtocol` | `pipeline()`, `stage_add()`, `stage_remove()` |
| `AtomicDeadQueueProtocol` | `backend_key`, `pipeline()` |
| `AtomicCronDAGSchedulerProtocol` | `backend_key`, `pipeline()`, `stage_reschedule()`, `stage_set_active_run()`, `stage_clear_active_run()` |

`AtomicCronDAGSchedulerProtocol` allows `StateManager` to fold cron reschedule and active-run marker ops into the same pipeline as task-state ops when the cron scheduler and task-state adapter share a backend (`backend_key` match). `StaticCronDAGScheduler` does not implement the atomic sub-protocol (no shared backend); `StateManager` falls back to sequential async calls.

`optimistic_dispatch_scheduled(task, stage_extra)` encapsulates the optimistic-locking dispatch loop: read under WATCH, check status, set SUBMITTED, stage requeue, call `stage_extra` for additional staged ops (e.g., scheduler removal), commit. Redis implementations use WATCH/MULTI; SQL implementations use `SELECT FOR UPDATE`.

`stage_remove_heartbeat(pipe, task)` stages a heartbeat sorted-set removal onto an existing pipeline, so `StateManager` does not need to reference any Redis key name constants directly.

`StateManager` holds `task_state: TaskStateProtocol` and `task_submit: TaskSubmitProtocol` as two separate constructor parameters. In the common Redis case, both are the same adapter object; the split allows future configurations where state and queue live on different backends. See [docs/datastore-architecture.md](docs/datastore-architecture.md) for a full treatment of consistency modes and what alternative datastores (SQL task state, RabbitMQ queuing) would require.

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
4. **Inline JSON env vars** — `STATIC_QUEUES`, `STATIC_ROLES`, `STATIC_ROUTING`
5. **Built-in defaults** — one `default` queue, one `default` role

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
| `SCHEDULER_POLL_INTERVAL` | `5.0` | Scheduler |
| `SCHEDULER_BATCH_SIZE` | `1` | Scheduler |
| `TASK_ADAPTER` | `"json"` | All (`"json"` or `"msgpack"`) |
| `ROUTING_BACKEND` | `"sql"` | All (`"sql"`, `"redis"`, `"redis_json"`, or `"static"`) |
| `REDIS_URL` | `redis://localhost:6379` | All |
| `SQL_PATH` | `sqlite+aiosqlite:///jobbers.db` | All (only used when `ROUTING_BACKEND=sql`) |
| `STATIC_CONFIG_FILE` | — | All (path to JSON/YAML routing config; requires `ROUTING_BACKEND=static`) |
| `STATIC_QUEUES` | — | All (inline JSON array of queue configs; fallback when no `STATIC_CONFIG_FILE`) |
| `STATIC_ROLES` | — | All (inline JSON object mapping role → queue list) |
| `STATIC_ROUTING` | — | All (inline JSON array of routing configs) |
| `CONFIG_CACHE_TTL` | `30` | All (queue/routing config cache TTL in seconds) |

## Testing

- Uses `fakeredis` (in-memory, supports Lua + JSON modules) — no real Redis needed for most tests
- `task_adapter` fixture is parametrized over `RedisTaskAdapter` (FakeRedis) and `RedisJSONTaskAdapter` (real Redis Stack, skipped if unavailable)
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
| **Implementation edge cases** | `tests/adapters/test_msgpack_adapter.py`, `test_json_adapter.py` | `msgpack_adapter` / `json_adapter` | Cover implementation-specific behaviour that is not a protocol requirement (e.g., `RedisTaskAdapter` sorting limitations, null JSON blobs in `RedisJSONTaskAdapter`). |
| **Orchestration** | `test_state_manager.py`, `test_task_processor.py`, `test_task_routes.py`, `test_task_generator.py` | `DummyTaskAdapter` or `Mock(spec=StateManager)` | Test coordination logic without touching real adapters; fast, no Redis Stack required. |

#### Key rules

- **Prefer common tests.** If a behaviour belongs to the protocol, put it in the common file and run it against all implementations.
- **xfail, don't skip, for known limitations.** If one implementation cannot satisfy a contract test, mark it `@pytest.mark.xfail(strict=True)` with a reason. This keeps the test discoverable and will alert you when the limitation is fixed.
- **Adapter fixtures**
  - `task_adapter` (parametrized `["raw", "json", "sql"]`) — `RedisTaskAdapter` via FakeRedis + `RedisJSONTaskAdapter` via real Redis Stack (skipped if unavailable) + `SQLTaskAdapter` via in-memory SQLite.
  - `dead_queue` (parametrized `["raw", "json", "sql"]`) — same pattern, yields `(dq, task_adapter)`.
  - `queue_config_adapter` (parametrized `["sql", "redis", "redis_json"]`) — SQL via in-memory SQLite, Redis via FakeRedis, Redis JSON via real Redis Stack (skipped if unavailable).
  - `task_routing_config_adapter` (parametrized `["sql", "redis", "redis_json"]`) — same pattern.
  - `scheduler` (parametrized `["redis", "sql"]`) — `RedisTaskScheduler` via FakeRedis + `SQLTaskScheduler` via in-memory SQLite; in `tests/schedulers/`.
  - `cron_dag_scheduler` (parametrized `["redis", "sql", "static"]`) — all three cron DAG scheduler backends; in `tests/schedulers/`.
  - `mutable_cron_dag_scheduler` (parametrized `["redis", "sql"]`) — mutable backends only (static raises on add/remove); in `tests/schedulers/`.
  - `msgpack_adapter` — plain `RedisTaskAdapter` via FakeRedis; for Redis-specific edge cases.
  - `json_adapter` — `RedisJSONTaskAdapter` via real Redis Stack; for Redis Stack-specific edge cases.
  - `json_dead_queue` — `RedisJSONDeadQueue` backed by `json_adapter`; for `RedisJSONDeadQueue`-specific edge cases.
  - `DummyTaskAdapter` (in `tests/conftest.py`) — in-memory stub; use for `state_manager` orchestration tests.

#### Known hard-to-cover paths (concurrency guards)

The following lines cannot be covered reliably without concurrent execution, low-level patching, or specially broken inputs:

- `jobbers/adapters/redis.py` — `if task_data is None: continue` in `RedisTaskAdapter.clean_terminal_tasks` (key deleted between `scan_iter` and `GET`)
- `jobbers/adapters/redis_json.py` — `if role_doc is not None:` false branch in `RedisJSONQueueConfigAdapter.delete_queue`: JSON document deleted between the RediSearch result and the `JSON.GET` pipeline call (requires concurrent deletion)
- `jobbers/task_generator.py` line 151 — the `if task:` requeue branch in `TaskGenerator.__anext__`'s `CancelledError` handler (task is always `None` when `get_next_task` raises, making this branch unreachable in practice)
- `jobbers/di.py` lines 227, 232 — exception handlers in `DependencyResolver.__aexit__` that catch errors thrown by generator cleanup; requires an async/sync generator that raises during `.aclose()` / `.close()`
- `jobbers/task_processor.py` line 80 — `hints = {}` fallback when `get_type_hints()` raises; only reachable with unresolvable forward references in task annotations

These should be addressed with integration tests or by restructuring the code so the guard is testable.
