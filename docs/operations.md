# Operations Guide

## Installation

### Prerequisites

- Python 3.11+
- Redis (plain Redis or Redis Stack — see [Adapters](#adapters))
- Node.js 18+ and npm (for the frontend)

### Backend

```bash
pip install -e .
```

This installs the `jobbers` package and registers five CLI entry points:
`jobbers_manager`, `jobbers_worker`, `jobbers_cleaner`, `jobbers_scheduler`, `jobbers_openapi`.

Run the database migration before starting any process for the first time:

```bash
jobbers_migrate
```

This creates the SQL tables used for queue and role configuration.

### Frontend

```bash
cd frontend
npm install
npm run dev       # dev server at http://localhost:5173
```

The dev server proxies `/api/*` to `http://localhost:8000` (the Manager API). Set the `VITE_PROXY_TARGET` environment variable to override the target:

```bash
VITE_PROXY_TARGET=http://my-manager-host:8000 npm run dev
```

To build for production:

```bash
npm run build     # output in frontend/dist/
```

---

## Running with Docker Compose

```bash
docker compose up
```

This starts:

| Container | Port | Description |
| --- | --- | --- |
| `manager` | 8000 | FastAPI API server |
| `worker` | — | Task execution |
| `redis` | 6379 | Task queues and state |
| `frontend` | 3000 | Admin UI (dev mode with HMR) |
| `collector` | 4317 / 4318 | OpenTelemetry Collector |
| `openobserve` | 5080 | Metrics and logs UI |

The `cleaner` is not included in the default compose file; run it as a cron job or add it as an additional service.

---

## Running Each Process

### Manager

```bash
jobbers_manager <task_module>
```

Starts the FastAPI web server on port 8000. The `<task_module>` argument must point to the Python module that registers your tasks — either a dotted import path or a file path:

```bash
jobbers_manager myapp.tasks
jobbers_manager /srv/myapp/tasks.py
```

Run one or more Manager instances. Because all state lives in Redis and SQL, any number can run concurrently behind a load balancer.

**Environment variables:**

| Variable | Default | Description |
| --- | --- | --- |
| `TASK_ADAPTER` | `json` | Storage backend: `json` (Redis Stack) or `msgpack` (plain Redis). See [adapter selection](adapter-selection.md). |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `SQL_PATH` | `sqlite+aiosqlite:///jobbers.db` | SQLAlchemy URL for queue/role config |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint for metrics |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint for traces |
| `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint for logs |
| `OTEL_LOGS_EXPORTER` | _(unset — logs disabled)_ | Set to `otlp` to enable log export |

---

### Worker

```bash
WORKER_ROLE=default \
WORKER_CONCURRENT_TASKS=5 \
WORKER_TTL=50 \
jobbers_worker <task_module>
```

Starts a worker process that pulls tasks from Redis queues and executes them. Like the manager, it loads `<task module>` to registered task functions.

**Environment variables:**

| Variable | Default | Description |
| --- | --- | --- |
| `WORKER_ROLE` | `default` | Role this worker consumes. Workers pull from all queues assigned to this role. |
| `WORKER_CONCURRENT_TASKS` | `5` | Max tasks running simultaneously in this process. |
| `WORKER_TTL` | `50` | Exit after processing this many tasks (memory leak protection). Set to `0` for indefinite. |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `SQL_PATH` | `sqlite+aiosqlite:///jobbers.db` | SQLAlchemy URL for queue/role config |
| `OTEL_*` | _(same as Manager)_ | OpenTelemetry endpoints |

Scale horizontally by running more worker processes. Workers are fully independent — they coordinate only through Redis and SQL. Workers detect role and queue configuration changes automatically without restart.

On `SIGTERM`, each in-flight task is handled according to its `on_shutdown` policy before the process exits.

#### Queue configuration refresh

Workers do not require a restart to pick up changes to their role's queue list or to individual queue configurations (concurrency limits, rate limits). The mechanism works as follows:

**Refresh tag.** Each role has a `refresh_tag` (a ULID) stored in the SQL `roles` table. `TaskGenerator.queues()` compares the stored tag against its locally cached value on every iteration. A mismatch means the role's queue assignment has changed: the worker reloads its queue list from SQL and invalidates its per-queue configuration cache.

**What bumps the tag.** The tag is updated automatically whenever:

| Operation | API call |
| --- | --- |
| Queue list for a role is replaced | `POST /queues/{role}` or `PUT /roles/{role_name}` |
| A role is created | `POST /roles` |
| A queue's config is updated | `PUT /queues/{queue_name}` |
| A queue is deleted | `DELETE /queues/{queue_name}` |

It can also be bumped manually:

```bash
curl -X POST http://localhost:8000/roles/default/refresh
# {"role": "default", "refresh_tag": "01JT..."}
```

**Immediate notification via pub/sub.** In addition to polling, each worker subscribes to the Redis channel `queue-config-refresh:{role}` when it starts. Whenever a tag is bumped the Manager publishes to that channel, so workers that are idle (sleeping between polls) will receive the notification on their next loop iteration rather than waiting for the next BZPOPMIN timeout.

**Typical propagation latency.** For workers actively pulling tasks the lag is one loop iteration — effectively immediate. For idle workers it is bounded by the BZPOPMIN timeout (default: no timeout, returns on the next task arrival) plus the time to the next `queues()` call. In practice the pub/sub message arrives before the next task, so the effective lag is low.

**Observability.** Two OTel metrics track refresh activity on each worker:

- `queue_config_refreshes{role=...}` — increments each time a worker reloads its queue list.
- `refresh_lag_ms{role=...}` — records the time between when the tag was written and when the worker picked it up, derived from the ULID timestamp embedded in the tag.

---

### Cleaner

A one-shot command designed to be run on a cron schedule. It prunes stale Redis entries and detects stalled tasks.

```bash
jobbers_cleaner \
  --stale-time 600 \
  --completed-task-age 86400 \
  --dlq-age 2592000 \
  --rate-limit-age 604800
```

Duration arguments (`--stale-time`, `--completed-task-age`, `--dlq-age`, `--rate-limit-age`) are in seconds. Range arguments (`--min-queue-age`, `--max-queue-age`) take Unix epoch timestamps in seconds. All arguments are optional.

| Argument | Description |
| --- | --- |
| `--stale-time <s>` | Mark tasks whose heartbeat is older than this as `stalled`. |
| `--completed-task-age <s>` | Delete stored state for terminal tasks older than this. |
| `--dlq-age <s>` | Remove dead letter queue entries older than this. |
| `--rate-limit-age <s>` | Prune rate-limit tracking entries older than this. |
| `--min-queue-age <s>` | Lower bound (epoch seconds) for queue entries to consider. |
| `--max-queue-age <s>` | Upper bound (epoch seconds) for queue entries to consider. |

Recommended cron setup:

```cron
# Every 5 minutes: detect stalled tasks
*/5 * * * * jobbers_cleaner --stale-time 300

# Nightly: prune old state
0 2 * * * jobbers_cleaner --completed-task-age 86400 --dlq-age 2592000 --rate-limit-age 604800
```

**Environment variables:** `REDIS_URL`, `SQL_PATH` (same as Manager).

---

### Scheduler

A long-running process that handles two scheduling concerns on each poll:

1. **Retry delays** — re-enqueues tasks that are waiting out a backoff delay when their `run_at` arrives.
2. **Cron DAGs** — fires recurring `CronDAGEntry` runs when their cron expression comes due; see [docs/cron-dags.md](cron-dags.md).

Run exactly **one** Scheduler per Redis instance.

```bash
SCHEDULER_POLL_INTERVAL=5.0 \
SCHEDULER_BATCH_SIZE=10 \
SCHEDULER_ROLE=default \
jobbers_scheduler
```

| Variable | Default | Description |
| --- | --- | --- |
| `SCHEDULER_POLL_INTERVAL` | `5.0` | Seconds between polls when no tasks are due. |
| `SCHEDULER_BATCH_SIZE` | `1` | Max tasks dispatched per poll iteration. |
| `SCHEDULER_ROLE` | `default` | Limits the scheduler to queues in this role. |
| `SCHEDULER_CONFIG_REFRESH_INTERVAL` | `3` | Minutes between queue configuration refreshes. |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `SQL_PATH` | `sqlite+aiosqlite:///jobbers.db` | SQLAlchemy URL |

The Scheduler has no persistent state. It is safe to restart at any time — tasks whose `run_at` has passed while it was down will be dispatched on the next poll.

---

## Adapters

Jobbers ships with two interchangeable storage backends selected at startup:

| `TASK_ADAPTER` | Adapter | Redis requirement |
| --- | --- | --- |
| `json` (default) | `RedisJSONTaskState` / `RedisJSONTaskSubmit` + `RedisJSONDeadQueue` | Redis Stack (JSON + RediSearch modules) |
| `msgpack` | `RedisTaskState` / `RedisTaskSubmit` + `RedisDeadQueue` | Plain Redis |

See [adapter selection](adapter-selection.md) for guidance on which to choose.

---

## Monitoring

### Admin UI

The frontend (port 3000 in dev, or the build served behind your web server) provides:

- **Tasks** — filterable list of active tasks with status, queue, and timing details.
- **Schedule** — tasks waiting in the retry delay queue, with their scheduled `run_at` time.
- **Dead Letter Queue** — failed tasks with full error history; supports bulk resubmission.
- **Queues** — create, update, and delete queue configs (concurrency and rate limits).
- **Roles** — assign queues to roles; workers refresh automatically when roles change.

All data is pulled from the Manager API at `http://localhost:8000`.

### API Reference

The Manager serves a full interactive API reference at `http://localhost:8000/docs` (Swagger UI). The machine-readable OpenAPI spec is available at `http://localhost:8000/openapi.json` and is also checked into the repo as `openapi.json` (source of truth for the frontend).

### API Endpoints for Monitoring

```bash
# Tasks currently running (with active heartbeat)
curl http://localhost:8000/active-tasks
curl "http://localhost:8000/active-tasks?queue=reports"

# All tasks by status
curl "http://localhost:8000/task-list?status=started&queue=reports"

# Tasks waiting on retry delay
curl http://localhost:8000/scheduled-tasks

# Dead letter queue
curl "http://localhost:8000/dead-letter-queue?task_name=generate_report&limit=25"

# Full failure history for a specific task
curl http://localhost:8000/dead-letter-queue/01JBKR2E5F3G4H5J6K7L8M9N0P/history
```

### OpenTelemetry Metrics

All four processes emit OTLP metrics automatically. No instrumentation code is required in task functions.

**Emitted by the Worker (task_processor):**

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `tasks_processed` | Counter | `queue`, `task`, `status` | Tasks completed (any terminal status) |
| `task_execution_time` | Histogram (ms) | `queue`, `task`, `status` | Time from `started_at` to `completed_at` |
| `task_end_to_end_latency` | Histogram (ms) | `queue`, `task`, `status` | Time from `submitted_at` to `completed_at` |
| `tasks_retried` | Counter | `queue`, `task`, `version` | Retry events |

**Emitted by the Worker (task_generator):**

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `time_in_queue` | Histogram (ms) | `queue`, `role`, `task` | Time from `submitted_at` to worker pickup |
| `tasks_selected` | Counter | `queue`, `role`, `task` | Tasks pulled from queues |
| `queue_config_refreshes` | Counter | `role` | Queue-list reloads triggered when a worker detects a `refresh_tag` change |
| `refresh_lag_ms` | Histogram (ms) | `role` | Lag between when the `refresh_tag` was bumped and when the worker picked up the change |

**Emitted by the Manager:**

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `cancellations_requested` | Counter | `queue`, `task` | Cancel signals sent |
| `tasks_dead_lettered` | Counter | `queue`, `task`, `version` | Tasks moved to DLQ |

**Useful derived signals:**

- `time_in_queue` rising → workers are undersized or queues need splitting
- `tasks_processed{status="dropped"}` > 0 → workers are running a stale task version
- `tasks_retried` high → upstream dependencies are flaky; tune `expected_exceptions` or back-pressure
- `refresh_lag_ms` high → workers are not picking up queue configuration changes promptly; check for long-running tasks blocking `queues()` calls or Redis pub/sub connectivity issues
- `queue_config_refreshes` absent on a worker that should have received a config change → the worker may be stalled or not subscribed to the refresh channel

### OpenTelemetry Setup

The Docker Compose stack includes an **OpenTelemetry Collector** (`otel-config.yaml`) that:

1. Receives OTLP signals from all jobbers processes on port 4317.
2. Receives Redis metrics via the `redis` receiver (scrapes `redis:6379` every 10 s).
3. Forwards everything to **OpenObserve** on port 5081.

OpenObserve is available at `http://localhost:5080` (default credentials: `root@example.com` / `Complexpass#123`).

To point jobbers at a different OTLP endpoint, set the `OTEL_EXPORTER_OTLP_*` environment variables on each process.
