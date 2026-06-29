# Task Backend Feature Matrix

Compares `redis`, `redis_json`, `sql`, and `static` across protocol support, feature availability, per-operation complexity, and storage footprint.

---

## Protocol support

| Protocol | `redis` | `redis_json` | `sql` | `static` |
| --- | --- | --- | --- | --- |
| `TaskStateProtocol` | ✅ | ✅ | ✅ | ❌ |
| `AtomicTaskStateProtocol` | ✅ | ✅ | ✅ | ❌ |
| `TaskSubmitProtocol` | ✅ | ✅ | ✅ | ❌ |
| `TaskSchedulerProtocol` | ✅ | ❌ (delegates to `redis`) | ✅ | ❌ |
| `AtomicTaskSchedulerProtocol` | ✅ | ❌ | ✅ | ❌ |
| `DeadQueueProtocol` | ✅ | ✅ | ✅ | ❌ |
| `AtomicDeadQueueProtocol` | ✅ | ✅ | ✅ | ❌ |
| `CronDAGSchedulerProtocol` | ✅ | ❌ (delegates to `redis`) | ✅ | ✅ read-only |
| `AtomicCronDAGSchedulerProtocol` | ✅ | ❌ | ✅ | ❌ |

`redis_json` has no native `TaskScheduler` or `CronDAGScheduler` — when those are set to `redis_json`, the system falls back to the plain `redis` implementations (sorted-set based, not JSON).

---

## Feature support

| Feature | `redis` | `redis_json` | `sql` | `static` |
| --- | --- | --- | --- | --- |
| Rate-limited submission | ✅ | ✅ | ✅ | ❌ |
| Server-side task filtering | ❌ Python-side after fetch | ✅ RediSearch `FT.SEARCH` | ✅ SQL `WHERE` clause | N/A |
| Multi-field sort on task list | ❌ `submitted_at` only (sorted-set score) | ✅ any RediSearch sortable field | ✅ any indexed column | N/A |
| Write operations | ✅ | ✅ | ✅ | ❌ raises `RoutingBackendReadOnlyError` |
| Durable storage | ✅ Redis AOF/RDB | ✅ Redis AOF/RDB | ✅ database | ❌ in-process only, resets on restart |
| Atomic pipeline mode | ✅ MULTI/EXEC | ✅ MULTI/EXEC | ✅ `SELECT FOR UPDATE` | ❌ |
| External dependency | Redis | Redis Stack (RedisJSON + RediSearch) | SQLAlchemy + database driver | None |

---

## Operation complexity

Counts the number of network round-trips and server-side operations per call. Lua scripts execute atomically on the Redis server and count as **1 round-trip** regardless of how many Redis commands they contain internally.

### Task submission

| Operation | `redis` | `redis_json` | `sql` |
| --- | --- | --- | --- |
| `submit_task` | 1 Lua script: `EXISTS` + `ZADD` + `SET` + `SADD`/`SREM` + 2×`ZADD` (DAG) | 1 Lua script: `EXISTS` + `ZADD` + `JSON.SET` + `SADD`/`SREM` + `ZADD` (DAG) | 2 transactions: `UPSERT tasks` + `UPDATE`/`INSERT task_queue` + optional `SELECT`/`INSERT dag_runs` |
| `submit_rate_limited_task` | 1 Lua script: `ZREMRANGEBYSCORE` + `ZCARD` + 2×`ZADD` + `SET` + `SADD`/`SREM` + 2×`ZADD` (DAG) | 1 Lua script: same, `JSON.SET` instead of `SET` | 1 transaction: `INSERT` anchor row (savepoint, ignore conflict) + `SELECT ... FOR UPDATE` anchor + `DELETE` expired `rate_limit_entries` + `SELECT COUNT` + `UPSERT rate_limit_entries` + `UPSERT tasks` + `UPDATE`/`INSERT task_queue` |
| `get_next_task` (pop) | 1 Lua script: `ZPOPMIN` + `GET` | 1 Lua script: `ZPOPMIN` + `JSON.GET` | 1 transaction: `SELECT FOR UPDATE` + `DELETE task_queue` + `SELECT tasks` |

### Task state reads and writes

| Operation | `redis` | `redis_json` | `sql` |
| --- | --- | --- | --- |
| `get_task` (single lookup) | 1× `GET` | 1× `JSON.GET` | 1× `SELECT` |
| `save_task` (update) | 1 pipeline: `SET` + optional `ZADD` heartbeat | 1 pipeline: `JSON.SET` + optional `ZADD` heartbeat | 1× `UPDATE` |
| `get_all_tasks` (list + filter) | 1× `ZRANGE`/`ZRANGEBYSCORE` (fetches `limit × 5` IDs) + **N× `GET`** (one per candidate), filtered in Python | 1× `FT.SEARCH` with server-side filter and sort | 1× `SELECT` with `WHERE` + `ORDER BY` + `LIMIT` |
| `get_dag_run` (DAG fan-in) | 1× `ZRANGE` on `dag-run:{id}:tasks` sorted set + N× `GET` | 1× `FT.SEARCH` filtering by `dag_run_id` tag | 1× `SELECT` joining `tasks` on `dag_run_id` |

The critical difference in `get_all_tasks`: `redis` does **N+1 round-trips** (one range scan followed by one `GET` per candidate), and may return fewer than `limit` results when filters are selective relative to the `limit × 5` prefetch window. `redis_json` and `sql` both complete in a single server-side query.

### Scheduling and dead letter queue

| Operation | `redis` | `redis_json` | `sql` |
| --- | --- | --- | --- |
| `add` to task scheduler | 1× `ZADD` | 1× `ZADD` (delegates to `redis`) | 1× `INSERT` |
| `get_due_tasks` (poll) | 1× `ZRANGEBYSCORE` | 1× `ZRANGEBYSCORE` (delegates to `redis`) | 1× `SELECT WHERE scheduled_at <= now` |
| `push` to dead queue | 1× `ZADD` + `SET` | 1× `ZADD` + `JSON.SET` | 1× `INSERT` |
| `get_dead_tasks` (list) | 1× `ZRANGE` + N× `GET`, filtered in Python | 1× `FT.SEARCH` | 1× `SELECT` with `WHERE` + `LIMIT` |

---

## Storage footprint

| Dimension | `redis` | `redis_json` | `sql` |
| --- | --- | --- | --- |
| Task blob encoding | msgpack (binary) | JSON (text) | JSON in `task_data` text column |
| Relative blob size | smallest | ~2–3× msgpack | ~2–3× msgpack |
| Index overhead | sorted sets per queue + per-name `SADD` sets | RediSearch inverted index (maintained on every write) | B-tree indexes on `tasks.queue`, `tasks.status`, `task_queue.submitted_at` |
| Schema overhead | none — sparse key-value | none — document store | fixed schema: `tasks`, `task_queue`, `dag_runs`, `task_fan_in` tables |
| Heartbeat storage | ZADD score in `heartbeats` sorted set | ZADD score in `heartbeats` sorted set | nullable `heartbeat_at` column in `tasks` table |
| Fan-in sets | Redis sets with TTL (cleaned by Cleaner) | Redis sets with TTL (cleaned by Cleaner) | `task_fan_in` table rows (no TTL, cleaned by Cleaner) |
| Rate limit window | sorted set per queue (`ZADD`/`ZREMRANGEBYSCORE`) | sorted set per queue (delegates to `redis` Lua script) | `rate_limit_entries` table row per (queue, task_id) + one `rate_limit_anchors` row per queue for `SELECT FOR UPDATE` serialization; pruned by age via `clean_rate_limiter` (Cleaner) |

---

## StateManager consistency modes

`StateManager` selects one of two consistency strategies at construction time based on which adapters are wired in.

### Atomic pipeline mode

**Condition:** all three adapters — `TaskStateProtocol`, `TaskSchedulerProtocol`, and `DeadQueueProtocol` — implement their `Atomic*` sub-protocols *and* `force_saga=False`.

In practice this means all adapters belong to the same backend:

| Backend combination | Atomic mode | Mechanism |
| --- | --- | --- |
| `redis` task state + `redis` scheduler + `redis` DLQ | ✅ | WATCH / MULTI / EXEC; retries on `WatchError` |
| `redis_json` task state + `redis` scheduler + `redis_json` DLQ | ✅ | same Redis client; WATCH / MULTI / EXEC |
| `sql` task state + `sql` scheduler + `sql` DLQ | ✅ | `SELECT FOR UPDATE` inside a single transaction; no retry needed |
| any mixed combination | ❌ saga | — |
| `force_saga=True` | ❌ saga | explicit override for testing |

Each multi-step operation builds a `pipeline(transaction=True)`, stages all writes with `stage_*` methods, then calls `pipe.execute()` once. The pipeline either commits entirely or not at all — no partial state is possible.

### Saga mode

**Condition:** any adapter does not implement its `Atomic*` sub-protocol, or `force_saga=True`.

Operations are split into sequential `await` calls ordered so the most durable write comes first. Partial failures leave the system in an intermediate state that the Cleaner process detects and compensates.

| Operation | Atomic pipeline | Saga (sequential fallback) | Cleaner compensation |
| --- | --- | --- | --- |
| `fail_task` | `stage_save` + `stage_add` DLQ in one pipeline | `save_task` → `add_to_dlq` | if save succeeds but DLQ add fails, Cleaner re-adds from task blob |
| `resubmit_dead_tasks` | `stage_requeue` + `stage_remove` DLQ per task | `save_task` (all) → `remove_from_dlq` (each) | if remove fails, Cleaner ignores DLQ entries whose task status is SUBMITTED/STARTED |
| `schedule_new_task` / `schedule_retry_task` | `stage_add` scheduler + `stage_save` in one pipeline | `save_task` → `scheduler.add` | if save succeeds but scheduler add fails, task stays SCHEDULED with no due entry; Cleaner's `recover_orphans` rescues it |
| `dispatch_scheduled_task` | `atomic_dispatch_scheduled` (WATCH/MULTI or SELECT FOR UPDATE) + `stage_remove` scheduler | `compare_and_set_status(SCHEDULED → SUBMITTED)` → `scheduler.remove` → enqueue | `compare_and_set_status` is the optimistic guard; a concurrent cancel sets a different status, causing the CAS to no-op |
| `request_task_cancellation` (SUBMITTED) | `stage_remove_from_queue` + `stage_save(CANCELLED)` | `save_task(CANCELLED)` only — queue entry left in place | Cleaner removes queue entries whose task blob is CANCELLED |
| `request_task_cancellation` (SCHEDULED) | `stage_remove` scheduler + `stage_save(CANCELLED)` | `scheduler.remove` → `save_task(CANCELLED)` | if remove fails, task is re-dispatched and immediately re-cancelled on next heartbeat check |
| `save_task` | `stage_save` in pipeline | `save_task` directly | N/A — single write |
| `submit_tasks_batch` | `stage_submit_task` per task in one pipeline | `save_task` (all, concurrent `asyncio.gather`) | N/A — enqueue is a no-op when task blob is authoritative |
| `requeue_task` | `stage_requeue` in pipeline | `save_task(SUBMITTED)` only | N/A — same as submit path |

### Cron atomic sub-mode

Cron DAG dispatch has an additional check independent of `_atomic_mode`. The cron scheduler is folded into the *same* pipeline as task-state ops only when both share a backend, detected via `backend_key` equality:

| Cron scheduler | Task-state adapter | Cron sub-mode |
| --- | --- | --- |
| `RedisCronDAGScheduler` | `RedisTaskState` or `RedisJSONTaskState` | ✅ same pipeline: reschedule + fan-in init + (optional) submit in one MULTI/EXEC |
| `SQLCronDAGScheduler` | `SQLTaskState` | ✅ same pipeline: reschedule + fan-in init + submit in one transaction |
| `RedisCronDAGScheduler` | `SQLTaskState` | ❌ cross-backend: cron pipeline fires first (`stage_reschedule` + `stage_set_active_run`), then task-state ops separately |
| `StaticCronDAGScheduler` | any | ❌ no pipeline: sequential `reschedule` → fan-in init → `submit_task` calls |

The write ordering in cron dispatch is intentional: reschedule + fan-in init are committed **before** the root task is submitted. A crash between those two writes means the cron entry fires again on the next poll (a tolerable duplicate), but the cron schedule is never silently lost.

---

## Backend-specific quirks

### `redis` (plain Redis)

#### `get_all_tasks` — filtering and pagination

Filters (`task_name`, `task_version`, `status`) are applied in Python after fetching `limit × 5` candidate IDs from the queue sorted set. Two problems follow from this:

**Under-sized pages.** When filters are selective, fewer than `limit` tasks survive the filter pass and the returned page is smaller than requested. There is no continuation cursor or "has more" signal — the caller cannot distinguish a short page caused by filtering from one caused by reaching the end of the queue.

**Offset applied before filtering.** Both `ZRANGEBYSCORE` and `ZRANGE` apply `offset` to raw sorted-set positions, not to filtered results. With a non-zero offset and active filters, pages can overlap (a task already seen on page N can re-appear on page N+1) or leave gaps (tasks that would survive the filter are skipped because their raw position falls below the offset window). Reliable paginated queries require either no filters or switching to `redis_json` / `sql`.

#### DLQ `get_by_filter` — filtered queries are unordered and unbounded

`get_by_filter` returns results ordered by `failed_at` only when no filter is provided — that path uses `ZREVRANGE` on the main DLQ sorted set. Any filter (queue, task name, or both) switches to set membership lookups (`SMEMBERS` or `SINTER`), which return members in arbitrary order. The version filter is always applied in Python after bulk-fetching all matching blobs. The `limit` parameter trims the final Python list but does not bound how many task blobs are fetched from Redis — every filtered query reads all matches regardless of the requested page size.

---

### `sql` (SQLAlchemy)

#### Concurrent workers on SQLite can claim the same task

`get_next_task` uses `SELECT FOR UPDATE SKIP LOCKED` on PostgreSQL to atomically claim a task row. On SQLite, `FOR UPDATE` is unsupported and is omitted (controlled by `"sqlite" not in dsn`). Two concurrent workers can `SELECT` the same task ID within overlapping transactions before either commits. Both then issue `DELETE` — one deletes the row, the other deletes 0 rows but the transaction still commits without error — and both proceed to fetch the task blob and execute it. **SQLite is not safe for multi-worker deployments; use PostgreSQL.**

#### Rate-limited submission serializes on a per-queue anchor row

`submit_rate_limited_task` (`jobbers/adapters/sql/task_submit.py`) reproduces the Redis sorted-set sliding window with two tables: `rate_limit_anchors` (one row per queue, that row's sole purpose is to be locked) and `rate_limit_entries` (one row per task_id currently inside the window). Each call inserts the anchor row if missing (via a savepoint that swallows the `IntegrityError` from a concurrent insert), then locks it with `SELECT ... FOR UPDATE` — on PostgreSQL this serializes all concurrent submitters to the same queue; on SQLite the lock is a no-op (same `"sqlite" not in dsn` guard as `get_next_task`), so concurrent SQLite submitters can race past the count check and both succeed when only one slot remains. Expired entries (`submitted_at < now - period`) are deleted before the `COUNT` check on every call rather than via a separate cleanup pass — `clean_rate_limiter` (called from the Cleaner) only prunes entries older than `rate_limit_age`, a coarser safety net independent of any specific queue's period. Re-submitting a task_id already inside the window is idempotent: it refreshes `submitted_at` without consuming a new slot, matching the Redis Lua script's `ZADD` semantics on an existing member.
