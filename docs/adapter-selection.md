# Choosing a Storage Adapter

Jobbers has six independently configurable adapter dimensions, each controlled by an environment variable:

| Dimension | Env var | Options |
| --- | --- | --- |
| Task state + submit | `TASK_BACKEND` | `redis_json` (default), `redis`, `sql` |
| Dead letter queue | `DLQ_BACKEND` | `redis`, `sql` |
| Task scheduler | `TASK_SCHEDULER_BACKEND` | `redis`, `sql` |
| Cron DAG scheduler | `CRON_DAG_SCHEDULER_BACKEND` | `redis`, `sql`, `static` |
| Routing backend | `ROUTING_BACKEND` | `redis`, `redis_json`, `sql`, `static` |

Most dimensions are independent. The exception is **task state and task submit, which must always use the same backend** (see [Valid pairings](#valid-pairings) below).

---

## Valid pairings

`TaskState` and `TaskSubmit` cannot be mixed across backends. The submit adapters embed the state adapter's serializer directly and their Lua scripts write both the task blob and the queue entry atomically in a single `EVAL` call — the two are inseparable.

| `TASK_BACKEND` | TaskState | TaskSubmit |
| --- | --- | --- |
| `redis_json` (default) | `RedisJSONTaskState` | `RedisJSONTaskSubmit` |
| `redis` | `RedisTaskState` | `RedisTaskSubmit` |
| `sql` | `SQLTaskState` | `SQLTaskSubmit` |

All other dimensions (`DLQ_BACKEND`, `TASK_SCHEDULER_BACKEND`, `CRON_DAG_SCHEDULER_BACKEND`, `ROUTING_BACKEND`) can be set independently of each other and of `TASK_BACKEND`.

For a full breakdown of why each cross-backend combination fails, see [taskstate-tasksubmit-compatibility.md](taskstate-tasksubmit-compatibility.md).

---

## Redis adapters

Use `TASK_BACKEND=redis_json` (Redis Stack) or `TASK_BACKEND=redis` (plain Redis) to choose between two Redis-backed implementations.

| | `RedisTaskState` / `RedisTaskSubmit` + `RedisDeadQueue` | `RedisJSONTaskState` / `RedisJSONTaskSubmit` + `RedisJSONDeadQueue` |
| --- | --- | --- |
| **Redis requirement** | Plain Redis | Redis Stack (JSON + RediSearch modules) |
| **Task encoding** | Binary msgpack | JSON |
| **Filtered task list queries** | O(n) Python-side | O(log m) server-side index |
| **DLQ filtered queries** | Unordered set intersection | Server-side index, sorted |
| **Pagination with filters** | Page sizes may be unpredictable | Exact page sizes |
| **`order_by=TASK_ID` in task list** | Not supported | Supported |
| **Consistency mode** | Atomic pipeline (MULTI/EXEC) | Atomic pipeline (MULTI/EXEC) |

### Worker performance

The adapters are equivalent in the worker hot path. Both use the same atomic Lua scripts for enqueueing and `BZPOPMIN` for dequeueing. The per-task save on state transitions is a single pipeline write in both cases. The worker never calls `get_all_tasks`, so the filtering performance difference below does not apply to it.

The only worker-path difference is serialization: msgpack binary deserialization is slightly faster and produces smaller Redis values than JSON text, but the margin is negligible compared to actual task execution time.

### Space efficiency

Msgpack binary encoding is typically 10–20% smaller than the equivalent JSON for the same task. At high task volumes or with large `parameters` payloads this compounds across the task blob, the DLQ, and any scheduled-task state. If Redis memory is a hard constraint and Redis Stack is not available, msgpack is the natural choice.

### API / task list performance

`GET /task-list` and the admin dashboard's task browser call `get_all_tasks`, which is where the adapters diverge most significantly.

**Msgpack** fetches candidate task IDs from the queue sorted set and then applies `name`, `version`, and `status` filters in Python. It over-fetches by up to 5× the requested page size to compensate. When filters are selective, the returned page can be smaller than the requested limit, and offset-based pagination does not compose correctly with filters — the offset is applied to raw queue positions before filtering, so pages can overlap.

**JSON** issues a single RediSearch query with all filters applied server-side. Pagination uses Redis's native offset/limit on the result set, so page sizes are exact and pagination with filters works correctly.

If you expect to query task lists with status or name filters — for example in an operations dashboard where engineers browse in-flight or failed tasks — the JSON adapter will be noticeably more responsive at scale and will not exhibit the pagination edge cases.

### DAG-heavy workloads

If your primary interaction with Jobbers is submitting and monitoring DAGs rather than browsing individual task lists, the filtering performance difference above is largely irrelevant.

The two DAG-specific endpoints are adapter-neutral in practice:

**Listing DAG runs** (`GET /dag-runs`) uses a plain sorted set (`ZRANGE` with offset/limit) implemented in the shared base class. Both adapters behave identically here.

**Fetching tasks in a run** (`GET /dag-runs/{id}`) retrieves all task IDs associated with a DAG run. The msgpack adapter reads from a per-run sorted set; the JSON adapter issues a RediSearch query. Both return the complete task list without filtering. The performance characteristics are equivalent for typical DAG sizes.

DAG-heavy deployments running on plain Redis should choose the msgpack adapter without hesitation — DAG monitoring is unaffected by the `get_all_tasks` limitations.

### Dead letter queue

If you use `dead_letter_policy=DeadLetterPolicy.SAVE` and need to browse or filter the DLQ:

**`RedisDeadQueue`** indexes by queue and task name using Redis sets. Filtering by either dimension alone returns results sorted by `failed_at`. Filtering by both queue and name simultaneously uses a set intersection, which loses ordering — results are returned in arbitrary order.

**`RedisJSONDeadQueue`** indexes via RediSearch and always returns results sorted by `failed_at` descending, regardless of which filters are applied.

If your operational workflow involves browsing the DLQ by recency while filtering by task name or queue, use the JSON adapter.

---

## SQL adapter

Set `TASK_BACKEND=sql` to store task state and the task queue in a relational database. The SQL adapter uses SQLAlchemy and works with SQLite (development) or PostgreSQL (production).

```sh
TASK_BACKEND=sql
SQL_PATH=postgresql+asyncpg://user:pass@host/dbname
```

`DLQ_BACKEND`, `TASK_SCHEDULER_BACKEND`, and `CRON_DAG_SCHEDULER_BACKEND` each accept `sql` independently — you can use SQL for some dimensions and Redis for others.

| | SQL adapter |
| --- | --- |
| **Task encoding** | Columns in the `tasks` table |
| **Filtered task list queries** | SQL `WHERE` clause — server-side, exact |
| **DLQ filtered queries** | SQL `WHERE` clause — server-side, sorted |
| **Pagination with filters** | Exact page sizes |
| **`order_by=TASK_ID`** | Supported |
| **Consistency mode** | Saga (no atomic sub-protocol) |
| **Rate-limited submission** | Not supported — raises `NotImplementedError` |

### Consistency mode with SQL

`SQLTaskState` does not implement `AtomicTaskStateProtocol` — it is missing `optimistic_dispatch_scheduled` and `stage_remove_heartbeat`, both of which depend on Redis WATCH/MULTI semantics. `StateManager` always uses **saga mode** with a SQL task adapter: writes are sequenced (blob first, then queue entry), and the Cleaner reconciles any orphans left by a crash between the two writes. See [datastore-architecture.md](datastore-architecture.md) for what saga mode guarantees.

### Rate limiting

`SQLTaskSubmit.submit_rate_limited_task` raises `NotImplementedError`. Tasks submitted to rate-limited queues will fail if `TASK_BACKEND=sql`. Either disable rate limiting on all queues or use a Redis task adapter.

### When to use the SQL adapter

- Redis is unavailable or undesirable in your infrastructure.
- You need full-text filtering on task fields without Redis Stack.
- You are running SQLite in a single-process development or test environment and want to inspect task state directly with standard SQL tooling.
- You are evaluating Jobbers without standing up a Redis instance.

---

## Static adapters

Two adapter dimensions support a read-only `static` mode that requires no database: the routing backend and the cron DAG scheduler.

### Static routing backend

Set `ROUTING_BACKEND=static` to load queue and role configuration once at startup from a file or environment variables. All write operations (`POST /queues`, `PUT /roles`, etc.) return `405 Method Not Allowed`.

Configuration priority (highest to lowest):

1. `db.register_routing_backend(StaticRoutingBackend(...))` called before `init_state_manager()`
2. `STATIC_CONFIG_FILE=/path/to/routing.json` (also accepts `.yaml`/`.yml`)
3. `STATIC_QUEUES`, `STATIC_ROLES`, `STATIC_ROUTING` inline JSON env vars
4. Built-in defaults: one `default` queue, one `default` role

```json
{
  "queues": [{"name": "default", "max_concurrent": 10}],
  "roles": {"default": ["default"]},
  "routing": []
}
```

When to use: deployments where queue/role topology is fixed at deploy time and runtime CRUD is not needed. Eliminates the routing backend database dependency entirely.

### Static cron DAG scheduler

Set `CRON_DAG_SCHEDULER_BACKEND=static` for an in-memory cron DAG scheduler with no database dependency. Entries are fixed at construction time; `add` and `remove` raise `RoutingBackendReadOnlyError`. Runtime state (`next_run_at` tracking, active-run markers) lives in in-memory dicts protected by an `asyncio.Lock` and resets on process restart.

`StaticCronDAGScheduler` does not implement `AtomicCronDAGSchedulerProtocol` — there is no shared backend to co-locate with the task-state adapter. `StateManager` falls back to sequential async calls for cron reschedule and active-run marker operations.

When to use: deployments where cron schedules are defined in code or config files rather than managed at runtime, and where losing in-flight scheduling state on restart is acceptable. Eliminates the cron DAG scheduler database dependency.

---

## Consistency modes

Both Redis adapters implement `AtomicTaskStateProtocol`, so `StateManager` operates in **atomic pipeline mode** by default: related state mutations (save blob + enqueue, save blob + add to DLQ, etc.) are grouped into a single Redis MULTI/EXEC transaction.

`StateManager` also supports a **saga mode** that activates automatically when:

- Any of the task adapter, scheduler, or dead letter queue does not implement its Atomic sub-protocol.
- `force_saga=True` is passed to `StateManager` (or `FORCE_SAGA_MODE=true` in the environment).
- `TASK_BACKEND=sql` (SQL task state never implements the atomic sub-protocol).

In saga mode, writes are sequenced with the blob always written first, and the Cleaner reconciles any orphans left by mid-sequence crashes.

See [datastore-architecture.md](datastore-architecture.md) for details on the consistency model, what each mode guarantees, and what would be required to implement task state on SQL or queuing on RabbitMQ.

---

## Summary: which adapter to choose

**Choose the JSON Redis adapter (`TASK_BACKEND=redis_json`, the default) when:**

- Redis Stack is available (self-hosted, Redis Cloud, or Docker in development).
- You use the task list API or admin dashboard to browse tasks by status, name, or queue.
- You use the DLQ and need browsing results sorted by recency when filters are applied.
- You need reliable offset-based pagination across filtered result sets.

**Choose the msgpack Redis adapter (`TASK_BACKEND=redis`) when:**

- You are running plain Redis (AWS ElastiCache, Azure Cache for Redis, or any managed Redis without module support).
- Your workload is primarily DAG-based and you access tasks through DAG run endpoints rather than the task list API.
- Task payloads are large and Redis memory is a concern.
- You do not need filtered pagination or DLQ ordering.

**Choose the SQL adapter (`TASK_BACKEND=sql`) when:**

- Redis is unavailable or you prefer a single database dependency.
- You need filtered task list queries without Redis Stack.
- You are running a development or test environment with SQLite.
- Rate-limited queues are not required.

**Choose static routing (`ROUTING_BACKEND=static`) when:**

- Queue and role topology is fixed at deploy time.
- You want to eliminate the routing backend database dependency.

**Choose static cron DAG scheduling (`CRON_DAG_SCHEDULER_BACKEND=static`) when:**

- Cron schedules are defined at deploy time and do not need runtime CRUD.
- Losing in-flight scheduling state on restart is acceptable.
- You want to eliminate the cron DAG scheduler database dependency.
