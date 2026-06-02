# Choosing a Storage Adapter

Jobbers ships two interchangeable storage adapters. Both implement the same protocol and support the same task lifecycle, but they have different infrastructure requirements and different performance characteristics at the API layer.

| | `RedisTaskState` / `RedisTaskSubmit` + `RedisDeadQueue` | `RedisJSONTaskState` / `RedisJSONTaskSubmit` + `RedisJSONDeadQueue` |
| --- | --- | --- |
| **Redis requirement** | Plain Redis | Redis Stack (JSON + RediSearch modules) |
| **Task encoding** | Binary msgpack | JSON |
| **Filtered task list queries** | O(n) Python-side | O(log m) server-side index |
| **DLQ filtered queries** | Unordered set intersection | Server-side index, sorted |
| **Pagination with filters** | Page sizes may be unpredictable | Exact page sizes |
| **`order_by=TASK_ID` in task list** | Not supported | Supported |

See the [operations guide](operations.md#adapters) for how to configure which adapter is active.

---

## Worker performance

The adapters are equivalent in the worker hot path. Both use the same atomic Lua scripts for enqueueing and `BZPOPMIN` for dequeueing. The per-task save on state transitions is a single pipeline write in both cases. The worker never calls `get_all_tasks`, so the filtering performance difference below does not apply to it.

The only worker-path difference is serialization: msgpack binary deserialization is slightly faster and produces smaller Redis values than JSON text, but the margin is negligible compared to actual task execution time.

---

## Space efficiency

Msgpack binary encoding is typically 10–20% smaller than the equivalent JSON for the same task. At high task volumes or with large `parameters` payloads this compounds across the task blob, the DLQ, and any scheduled-task state. If Redis memory is a hard constraint and Redis Stack is not available, msgpack is the natural choice.

---

## API / task list performance

`GET /task-list` and the admin dashboard's task browser call `get_all_tasks`, which is where the adapters diverge most significantly.

**Msgpack** fetches candidate task IDs from the queue sorted set and then applies `name`, `version`, and `status` filters in Python. It over-fetches by up to 5× the requested page size to compensate. When filters are selective, the returned page can be smaller than the requested limit, and offset-based pagination does not compose correctly with filters — the offset is applied to raw queue positions before filtering, so pages can overlap.

**JSON** issues a single RediSearch query with all filters applied server-side. Pagination uses Redis's native offset/limit on the result set, so page sizes are exact and pagination with filters works correctly.

If you expect to query task lists with status or name filters — for example in an operations dashboard where engineers browse in-flight or failed tasks — the JSON adapter will be noticeably more responsive at scale and will not exhibit the pagination edge cases.

---

## DAG-heavy workloads

If your primary interaction with Jobbers is submitting and monitoring DAGs rather than browsing individual task lists, the filtering performance difference above is largely irrelevant to you.

The two DAG-specific endpoints are adapter-neutral in practice:

**Listing DAG runs** (`GET /dag-runs`) uses a plain sorted set (`ZRANGE` with offset/limit) implemented in the shared base class. Both adapters behave identically here.

**Fetching tasks in a run** (`GET /dag-runs/{id}`) retrieves all task IDs associated with a DAG run. The msgpack adapter reads from a per-run sorted set; the JSON adapter issues a RediSearch query. Both return the complete task list without filtering. The performance characteristics are equivalent for typical DAG sizes.

DAG-heavy deployments running on plain Redis should choose the msgpack adapter without hesitation — DAG monitoring is unaffected by the `get_all_tasks` limitations.

---

## Dead letter queue

If you use `dead_letter_policy=DeadLetterPolicy.SAVE` and need to browse or filter the DLQ:

**`RedisDeadQueue`** indexes by queue and task name using Redis sets. Filtering by either dimension alone returns results sorted by `failed_at`. Filtering by both queue and name simultaneously uses a set intersection, which loses ordering — results are returned in arbitrary order.

**`RedisJSONDeadQueue`** indexes via RediSearch and always returns results sorted by `failed_at` descending, regardless of which filters are applied.

If your operational workflow involves browsing the DLQ by recency while filtering by task name or queue, use the JSON adapter.

---

## Consistency mode

Both adapters implement `AtomicTaskStateProtocol`, so `StateManager` operates in **atomic pipeline mode** by default: related state mutations (save blob + enqueue, save blob + add to DLQ, etc.) are grouped into a single Redis MULTI/EXEC transaction. This is the only consistency mode that currently ships.

`StateManager` also supports a **saga mode** for cross-backend deployments where task state and the task queue live on different backends. In saga mode, writes are sequenced with the blob always written first, and the Cleaner reconciles any orphans left by mid-sequence crashes. This mode is activated automatically when any of the task adapter, scheduler, or dead letter queue does not implement its Atomic sub-protocol, or when `force_saga=True` is passed to `StateManager`.

See [datastore-architecture.md](datastore-architecture.md) for details on the consistency model, what each mode guarantees, and what would be required to implement task state on SQL or queuing on RabbitMQ.

---

## Summary: which adapter to choose

**Choose the msgpack adapter (`RedisTaskState` / `RedisTaskSubmit` + `RedisDeadQueue`) when:**

- You are running plain Redis (AWS ElastiCache, Azure Cache for Redis, or any managed Redis without module support).
- Your workload is primarily DAG-based and you access tasks through DAG run endpoints rather than the task list API.
- Task payloads are large and Redis memory is a concern.
- You do not need filtered pagination or DLQ ordering.

**Choose the JSON adapter (`RedisJSONTaskState` / `RedisJSONTaskSubmit` + `RedisJSONDeadQueue`) when:**

- Redis Stack is available (self-hosted, Redis Cloud, or Docker in development).
- You use the task list API or admin dashboard to browse tasks by status, name, or queue.
- You use the DLQ and need browsing results sorted by recency when filters are applied.
- You need reliable offset-based pagination across filtered result sets.
