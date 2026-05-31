# Datastore Architecture and Consistency Model

This document describes how Jobbers separates task storage across three independent protocols, how `StateManager` chooses between atomic and saga consistency modes depending on whether those protocols share a backend, and what constraints an alternative datastore (SQL for task blobs, RabbitMQ for queuing) would need to satisfy.

---

## The task storage protocols

Jobbers splits task storage across four independent Protocol classes defined in `protocols.py`:

| Protocol | `@runtime_checkable` | Responsibility |
| --- | --- | --- |
| `TaskStateProtocol` | Yes | Task blob persistence: status, parameters, results, heartbeats, fan-in sets, DAG run index |
| `TaskSubmitProtocol` | No | Composite submit/pop operations that require co-located state and queue (Lua scripts in Redis) |
| `TaskQueueProtocol` | Yes | Active queue membership: enqueue, blocking pop, remove from queue, rate limiting |
| `TaskSchedulerProtocol` | Yes | Delayed/scheduled task queue: add at a future time, pop due tasks |

These are separate concerns. `TaskStateProtocol` is the durable source of truth for a task's lifecycle. `TaskQueueProtocol` is ephemeral membership data — a task's presence in a queue says "a worker should pick this up", not "this task exists". `TaskSubmitProtocol` groups the three operations (`submit_task`, `submit_rate_limited_task`, `get_next_task`) that atomically touch both state and queue via Lua scripts. `TaskSchedulerProtocol` is a timed trigger that eventually moves a task back into `TaskQueueProtocol`.

The current production adapters implement all three on a single Redis instance, which allows `StateManager` to use MULTI/EXEC transactions across all three in one round trip. The protocol split exists so that future adapters can use different backends for each layer without redesigning `StateManager`.

---

## TaskSubmitProtocol: the composite submit/pop boundary

Three operations require the task blob store and the queue to be co-located because they execute atomically via Lua scripts in Redis: `submit_task`, `submit_rate_limited_task`, and `get_next_task`. These are grouped into `TaskSubmitProtocol` in `protocols.py`.

`StateManager` takes `task_state: TaskStateProtocol` and `task_submit: TaskSubmitProtocol` as separate constructor parameters. In the common Redis deployment both are the same adapter object. The split means a future adapter can implement `TaskStateProtocol` on SQL (pure blob store) while using a Redis adapter for `TaskSubmitProtocol` to keep the Lua-script boundary intact.

`TaskSubmitProtocol` is **not** `@runtime_checkable` — mypy verifies structural satisfaction statically; no `isinstance` check is needed at runtime.

---

## Same-backend detection and consistency mode selection

`StateManager.__init__` checks whether the task adapter, task scheduler, and dead letter queue all implement the corresponding **Atomic sub-protocols** (`AtomicTaskStateProtocol`, `AtomicTaskSchedulerProtocol`, `AtomicDeadQueueProtocol`). If all three qualify, `StateManager` operates in **atomic pipeline mode**. If any one does not, or if `force_saga=True` is passed, it falls back to **saga mode**.

The Atomic sub-protocols extend their base protocols with pipeline staging methods:

| Base protocol | Atomic extension | Adds |
|---|---|---|
| `TaskStateProtocol` | `AtomicTaskStateProtocol` | `pipeline()`, `stage_save()`, `stage_requeue()`, `stage_submit_task()`, `stage_remove_from_queue()`, `stage_remove_heartbeat()`, `stage_init_fan_in()`, `read_for_watch()`, `optimistic_dispatch_scheduled()` |
| `TaskSchedulerProtocol` | `AtomicTaskSchedulerProtocol` | `pipeline()`, `stage_add()`, `stage_remove()` |
| `DeadQueueProtocol` | `AtomicDeadQueueProtocol` | `backend_key`, `pipeline()` |
| `CronDAGSchedulerProtocol` | `AtomicCronDAGSchedulerProtocol` | `backend_key`, `pipeline()`, `stage_reschedule()`, `stage_set_active_run()`, `stage_clear_active_run()` |

`pipeline()` returns a `TransactionHandle` — an opaque write batch with a single `async def execute() -> list[object]` method. For Redis, this is a `Pipeline`. For SQL, it could be a list of deferred statements that commit in one transaction.

---

## Atomic pipeline mode

When all adapters share the same backend (same Redis instance), `StateManager` batches related mutations into a single `MULTI/EXEC` transaction. This gives strong atomicity guarantees: either all writes in a batch succeed or none do.

Operations that use atomic pipelines:

| Operation | What is batched |
|---|---|
| Submit task | ZADD queue + save blob + register DAG run |
| Fail task with DLQ | Save blob + add to DLQ |
| Schedule task | Save blob + ZADD scheduler |
| Cancel submitted task | ZREM from queue + save blob |
| Resubmit from DLQ | Save blob + ZADD queue + ZREM from DLQ |
| Mark task stalled | Save blob + ZREM heartbeat |
| Dispatch scheduled task | ZADD queue + save blob + HDEL scheduler entry |
| Complete cron task | Save blob + clear active-run marker |

`optimistic_dispatch_scheduled` (new in `AtomicTaskStateProtocol`) encapsulates the WATCH/MULTI loop for dispatching a scheduled task: it reads the current task state under WATCH, verifies the task is not cancelled, sets status to SUBMITTED, stages a requeue, and calls a caller-supplied `stage_extra` function (used to stage the scheduler removal) before committing. Retries transparently on `WatchError`. For a SQL adapter this would instead use `SELECT FOR UPDATE`.

`stage_remove_heartbeat` (new in `AtomicTaskStateProtocol`) stages the heartbeat sorted-set removal onto an existing pipeline, so stall detection can batch the blob save and the heartbeat removal in one transaction without `StateManager` touching Redis key names directly.

---

## Saga mode

When adapters operate on different backends, `StateManager` performs each write sequentially, with the task blob (in `TaskStateProtocol`) always written first as the authoritative source of truth. Queue and scheduler mutations follow as best-effort.

If the process crashes between writes, the Cleaner reconciles:

- **SUBMITTED tasks not in any queue**: the Cleaner's stale-task scan detects SUBMITTED tasks with no heartbeat and requeues or marks them stalled.
- **DLQ entries for tasks that were resubmitted**: the Cleaner cross-references DLQ entries against task status; tasks in SUBMITTED or STARTED state are removed from the DLQ.
- **Scheduler entries for tasks that were cancelled**: the scheduler's `next_due_bulk` skips tasks whose status is CANCELLED when popped.

Saga mode makes the following consistency tradeoffs explicit:

| Operation | Risk window | Cleaner compensation |
|---|---|---|
| Fail task → add to DLQ | Blob saved FAILED, DLQ write not yet executed | Not cleaned — task stays FAILED but is invisible in DLQ until next retry |
| Resubmit from DLQ | Blob saved SUBMITTED, ZADD not yet executed | Cleaner detects SUBMITTED task with no queue entry and requeues |
| Cancel submitted task | Blob saved CANCELLED, ZREM not yet executed | Worker pops the task, checks status before executing, discards |
| Dispatch scheduled task | CAS applied, scheduler remove not yet executed | Scheduler pops the task again; `compare_and_set_status` from SCHEDULED → SUBMITTED returns False (already SUBMITTED); task is silently skipped |

---

## What a SQL task state adapter would require

`TaskStateProtocol` maps cleanly to SQL. All required methods have direct SQL equivalents:

| Protocol method | SQL implementation |
|---|---|
| `save_task` / `get_task` | `INSERT OR REPLACE` / `SELECT` on a tasks table |
| `compare_and_set_status` | `UPDATE tasks SET status = ? WHERE id = ? AND status = ? RETURNING status` |
| `update_task_heartbeat` | `UPDATE tasks SET heartbeat_at = ?` |
| `remove_task_heartbeat` | `UPDATE tasks SET heartbeat_at = NULL` |
| `init_fan_in` / `fan_in_complete` | Fan-in table with `DELETE WHERE task_id = ? RETURNING COUNT(*)` |
| `get_stale_tasks` | `SELECT ... WHERE heartbeat_at < ?` |
| `clean_terminal_tasks` | `DELETE WHERE status IN (...) AND completed_at < ?` |

For `AtomicTaskStateProtocol`, a SQL adapter would implement `pipeline()` to return a staged-transaction object that accumulates SQL statements and executes them in a single `BEGIN ... COMMIT`. The `stage_*` methods append to this list. `execute()` runs the transaction.

`optimistic_dispatch_scheduled` would use `SELECT FOR UPDATE` on the task row (within a transaction) rather than Redis WATCH/MULTI, giving the same optimistic-locking semantics under SQL isolation.

`read_for_watch` becomes a plain `SELECT` — there is no WATCH analogue in SQL, but since the optimistic locking is handled entirely inside `optimistic_dispatch_scheduled` (via `SELECT FOR UPDATE`), this method can simply return the task without additional locking.

---

## Why RabbitMQ for the task queue is harder

`TaskQueueProtocol` has three methods that are structurally incompatible with AMQP:

**`remove_from_queue(task_id, queue)`** — AMQP provides no way to remove a specific message from a queue by ID. Messages can only be consumed and acked/nacked. Cancelling a SUBMITTED task currently removes it from the queue before any worker can claim it. With RabbitMQ this is impossible: the worker would have to consume the message, check the task status from `TaskStateProtocol`, and discard it if CANCELLED. This makes cancel-SUBMITTED eventually consistent rather than immediate.

**`check_rate_limit_and_enqueue(task_id, queue, score, window_start, max_count)`** — atomically checks a sliding rate-limit window and enqueues only if under the limit. The sliding window state must live somewhere (Redis or SQL), and the check-then-publish step cannot be atomic across RabbitMQ and an external store. Rate limiting would need to be refactored into a separate interface.

**`enqueue(task_id, queue, score)`** — the `score` parameter encodes priority (submitted_at timestamp). RabbitMQ priority queues use coarse integer levels (0–255) and are FIFO within a level. Score-based ordering would need to be dropped or approximated.

A RabbitMQ-backed `TaskQueueProtocol` could be built if the above constraints are relaxed by protocol change:

1. Drop `remove_from_queue` from the protocol; require workers to check task status before executing and discard cancelled tasks.
2. Extract rate limiting into a separate `RateLimitProtocol` satisfied by an external store.
3. Replace `score` with a coarse priority level or drop priority ordering entirely.
4. Expand the Cleaner's reconciliation to handle tasks stuck in SUBMITTED state without a matching queue entry (since RabbitMQ does not expose queue membership for inspection the way Redis sorted sets do).

---

## backend_key and same-backend detection

`backend_key` is a stable string property on all Atomic adapters that identifies the backend instance. For Redis adapters, `SharedTaskAdapterMixin` returns `str(id(self.data_store))` — the Python object identity of the Redis client. Two adapters backed by the same Redis client object therefore share a `backend_key` and will be detected as same-backend.

`StateManager` uses `backend_key` at construction time for cron-specific same-backend detection: `_atomic_cron` is set only when the cron scheduler implements `AtomicCronDAGSchedulerProtocol` **and** its `backend_key` matches the task-state adapter's `backend_key`. When this condition is met, cron ops (`stage_reschedule`, `stage_set_active_run`, `stage_clear_active_run`) are folded into the same atomic pipeline as task-state ops during dispatch and completion. The three-way atomic mode check (task state + scheduler + DLQ) uses `isinstance` alone, without `backend_key` comparison, since those three adapters are always co-located on the same Redis instance in the current deployment model.
