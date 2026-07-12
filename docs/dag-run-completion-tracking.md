# DAG Run Completion Tracking: How Nested Fan-Out/Fan-In Is Tracked Efficiently

This document describes how Jobbers tracks DAG-run completion and fan-in state, and why it's shaped the way it is. It was motivated by nested dynamic fan-out (`DynamicFanOut.propagate_fan_in`, `--&gt;&gt;`/`--o` mermaid edges — see [dag-composition.md](dag-composition.md) and [mermaid-dag-spec.md](mermaid-dag-spec.md)), which can turn a single DAG run from a handful of tasks into thousands (arm count × nesting depth). Everything here is scoped per `dag_run_id`, so key count stays flat regardless of how wide or deeply nested a run is.

---

## Problem statement

### The O(R²) full-sibling-rescan pattern

Before this work, `TaskProcessor._maybe_cleanup` ran once **after every task completion**, and for any task with a `dag_run_id`, it:

1. Called `get_dag_run(dag_run_id)` to get the full current list of task IDs in the run.
2. Called `get_tasks_bulk(task_ids)` to fetch **every sibling task blob** in the run.
3. Checked whether any sibling was still active; if so, returned (run still in flight).
4. Only once all siblings were terminal did it compute and delete tasks whose `cleanup_on` config matched.

Each individual call was already round-trip-efficient (`get_tasks_bulk` does 2 pipelined round-trips for the Redis-family backends, a single `SELECT ... WHERE id IN (...)` for SQL), but the same "is everyone done yet?" computation was redone **from scratch on every one of the R completions in a run**, transferring and deserializing all R sibling blobs each time — O(R²) total data movement over the lifetime of one run.

For pre-nesting DAG runs (a handful of tasks), this was invisible. Nested fan-out (commit `2878523`, "Enable nested dynamic fanouts and update mermaid parser") is what makes R large — a dispatcher fanning out to hundreds of arms, each of which is itself a multi-step chain or another dispatcher, can put thousands of tasks in one `dag_run_id`. At that scale, O(R²) becomes the actual bottleneck.

### The ordering hazard (a real, previously-untested correctness gap)

`_maybe_cleanup` used to run *before* `post_process`/`_handle_dynamic_fanout`, which is what actually creates and registers a dispatcher's arm/collector tasks into the DAG run. If a dispatcher task was the last currently-known active sibling when it completed, `_maybe_cleanup` could conclude "all siblings terminal" and start deleting tasks — including possibly the dispatcher itself, if its `cleanup_on` matched — **before its arms existed**. Those arms were about to be created with `parent_id=dispatcher.id`; if the dispatcher record was gone first, a descendant relying on `inject_parent_results`/`parent_results()` would silently lose data.

This bug wasn't caused by nesting — it applied to single-level fan-out too — but nesting made the failure window far more common, since large nested runs spend more of their lifetime with exactly one active "frontier" task that looks like the last one standing.

---

## O(1) DAG-run-completion counter (implemented)

Reuses the fan-in idiom (predecessor-set + "last one out triggers the action") at the scope of a whole DAG run instead of a single collector.

### Redis structure

- **`DAG_RUN_PENDING = "dag-run:{dag_run_id}:pending".format`** — a Redis SET of task IDs currently "open" in the run (submitted, not yet resolved). (`jobbers/adapters/_shared.py:65`)
- **`DAG_RUN_CLOSED = "dag-run:{dag_run_id}:closed".format`** — a Redis SET of task IDs that have closed. Together with `DAG_RUN_PENDING`, this replaces what used to be a separate, always-growing `DAG_RUN_TASKS` sorted set (removed) — see "Fan-in and task-listing consolidation" below. (`jobbers/adapters/_shared.py:66`)
- **Population**: `SADD` into `DAG_RUN_PENDING` at every event that already writes `DAG_RUNS` — `SharedTaskAdapterMixin.stage_register_dag_run` (`jobbers/adapters/_shared.py:333-340`, shared by both Redis backends, no per-backend override needed), plus the Lua submit scripts in `jobbers/adapters/redis/task_submit.py` and `jobbers/adapters/redis_json/task_submit.py` via their `_extra_submit_keys`/`_extra_rate_limited_keys` mechanism (covers `submit_task`/`submit_rate_limited_task`: DAG roots, rate-limited arms).
- **Closing**: `_CLOSE_DAG_RUN_TASK_SCRIPT` (`jobbers/adapters/_shared.py:76-83`) — one Lua script that atomically `SREM`s the pending set, `SADD`s the closed set, and returns the remaining pending count:

  ```lua
  local removed = redis.call('SREM', KEYS[1], ARGV[1])
  if removed == 0 then
      return {0, -1}
  end
  redis.call('SADD', KEYS[2], ARGV[1])
  return {removed, redis.call('SCARD', KEYS[1])}
  ```

  The `{0, -1}` sentinel (task not a member — already closed, or the run's pending key expired/was swept) gives idempotent double-close protection for free.

### SQL structure

`dag_run_pending` table (`jobbers/migrations/schema.py:132-140`), mirroring the existing `task_fan_in` table's shape exactly — a `closed: Boolean` column updated in place (not a row deleted and recounted), no foreign key to `dag_runs` (same as `task_fan_in`, which also has none):

```python
dag_run_pending = Table(
    "dag_run_pending",
    metadata,
    Column("dag_run_id", String(26), nullable=False, primary_key=True),
    Column("task_id", String(26), nullable=False, primary_key=True),
    Column("closed", Boolean, nullable=False, server_default="0"),
)
Index("idx_dag_run_pending_run", dag_run_pending.c.dag_run_id)
```

Registered in `TABLE_GROUPS["task_state"]` (`jobbers/migrations/schema.py:243-251`).

- `SQLTaskState.stage_submit_task` (`jobbers/adapters/sql/task_state.py:191-215`) inserts a `dag_run_pending` row (savepoint/`IntegrityError`-swallow idiom, same as `task_fan_in`) inside its DAG-run-registration closure.
- `SQLTaskSubmit` has its own separate, non-staged `_register_dag_run` helper (`jobbers/adapters/sql/task_submit.py:32+`) used by `submit_task`/`submit_rate_limited_task` — this was a wrinkle the original design didn't anticipate: `stage_submit_task` (the atomic-pipeline path) and `_register_dag_run` (the single-call path) are two independent code paths that both needed the same `dag_run_pending` insert added.
- `close_dag_run_task` (`jobbers/adapters/sql/task_state.py:529-558`): `UPDATE dag_run_pending SET closed=True WHERE dag_run_id=? AND task_id=? AND closed=False` → if `rowcount == 0` return `-1`; else `SELECT COUNT(*) WHERE dag_run_id=? AND closed=False` and return it.
- `clean_dag_runs` (`jobbers/adapters/sql/task_state.py:514-527`) deletes `dag_run_pending` rows for any `dag_run_id` whose `dag_runs` entry just aged out, in the same transaction — no FK/CASCADE needed since it's done explicitly.

### Protocol addition

`TaskStateProtocol.close_dag_run_task` (`jobbers/protocols.py:261-269`):

```python
async def close_dag_run_task(self, dag_run_id: ULID, task_id: ULID) -> int:
    """
    Mark task_id resolved within dag_run_id's pending set; return the remaining count.

    Returns -1 if task_id was not pending (already closed, or the run's pending
    key/rows expired) — callers must treat -1 as "do not trigger the sweep",
    exactly like fan_in_complete's -1 sentinel.
    """
```

No `Atomic*` variant — it's a single logical operation (Lua-atomic on Redis, single-transaction on SQL) called standalone, not staged onto a caller-supplied pipeline, mirroring how `fan_in_complete` is called standalone from `Task.generate_callbacks`.

### `TaskProcessor` change

`_maybe_cleanup` was split into three methods, and its call site in `process()` moved from *before* `post_process`/`post_process_error` to *after* (`jobbers/task_processor.py:202-231`):

```python
await self.state_manager.remove_task_heartbeat(task)

if task.status == TaskStatus.COMPLETED:
    await self.post_process(task, dynamic_fanout)   # arms/collector registered -> pending SADD happens here
else:
    if task.status == TaskStatus.FAILED:
        await self.post_process_error(task)

# Runs after post_process/post_process_error so any fan-out arms this task
# spawned are already registered in the DAG run before this task is closed
# out of it — otherwise a dispatcher could look like the last active task
# and trigger cleanup before its own arms exist.
await self._maybe_cleanup(task)
```

```python
async def _maybe_cleanup(self, task: Task) -> None:
    if task.dag_run_id is None:
        await self._maybe_delete_self(task)
        return
    remaining = await self.state_manager.close_dag_run_task(task.dag_run_id, task.id)
    if remaining != 0:
        return  # still in flight (>0), or already closed (-1)
    await self._sweep_dag_run(task.dag_run_id)

async def _maybe_delete_self(self, task: Task) -> None:
    """Delete a standalone (non-DAG) task's record if its status matches cleanup_on."""
    ...

async def _sweep_dag_run(self, dag_run_id: ULID) -> None:
    """Delete every task in a now-fully-terminal DAG run whose config says to clean up."""
    ...
```

(`jobbers/task_processor.py:246-292`)

`close_dag_run_task` is now called for **every** DAG task on every completion (not gated on that task's own `cleanup_on`), since the pending-set is what tells us when to trigger the sweep — some *other* task type in the run may have `cleanup_on` set even if this one doesn't. The expensive full-sibling-fetch-and-delete (`_sweep_dag_run`) now runs exactly once per run, when the counter hits `0`, instead of once per completion.

This also fixes the ordering hazard directly: since `close_dag_run_task` now runs after `post_process`, a dispatcher's arms are always registered in `DAG_RUN_PENDING` (via `submit_tasks_batch` inside `_handle_dynamic_fanout`) *before* the dispatcher itself is removed from that same set — the run can never look "done" while arms it's about to spawn haven't been counted yet, because the children's registration happens-before the parent's close in program order within the same `process()` call.

### `StateManager` wrapper

```python
async def close_dag_run_task(self, dag_run_id: ULID, task_id: ULID) -> int:
    return await self.task_state.close_dag_run_task(dag_run_id, task_id)
```

(`jobbers/state_manager.py:865+`, follows the existing `init_fan_in` wrapper pattern at `jobbers/state_manager.py:809+`.)

### Consistency-model interaction

`close_dag_run_task` is single-backend-scoped (task-state adapter only — it never coordinates with the scheduler or DLQ adapters), so it's atomic within the task-state backend regardless of whether `StateManager` overall is in atomic-pipeline or saga mode. If the SADD-on-submit half is ever lost (e.g. a crash inside an all-or-nothing submit pipeline before commit), the task never appears in `pending`, and a run that never reaches `pending == 0` simply never triggers the sweep — the existing `clean_dag_runs` age-based sweep for abandoned runs is the safety net, unchanged. This degrades to "sweep never fires for that run" (the same failure mode a crash mid-run already produces) rather than a false-positive premature delete, since the sweep only fires when the atomically-computed remaining count hits exactly `0`.

---

## Fan-in and task-listing consolidation (implemented)

A follow-up question — "could we rely on the dag-wide set more, or restructure for better performance on fan-out/fan-in or other dag-scoped concerns?" — led to consolidating *all* fan-in tracking for a run, at *any* nesting depth, into two fixed-size Hashes per `dag_run_id`, and folding the per-run task listing into the pending/closed sets above instead of maintaining a third structure. This replaced an earlier, narrower proposal (a per-`dag_run_id` registry of individual fan-in Set keys, so cleanup could batch-delete them) with something that eliminates the key sprawl outright instead of just making it easier to clean up.

### Fan-in: from one Set-pair per collector to two Hashes per run

Previously, each nesting level's collector got its own pair of independently-TTL'd Redis Sets (`dag:fan-in:{id}` / `{fan_in_key}:members`), with no structural link back to the owning `dag_run_id`. Deep/wide nesting multiplied key count with no bound. Now, **every** collector in a run — at any nesting depth — shares exactly two Hash keys:

- **`DAG_RUN_FANIN = "dag-run:{dag_run_id}:fanin".format`** (`jobbers/adapters/_shared.py:69`) — one Hash per run holding, for every `fan_in_key`: a `remaining:{fan_in_key}` countdown field, and a `pending:{fan_in_key}:{predecessor_id}` marker field per not-yet-closed predecessor.
- **`DAG_RUN_FANIN_MEMBERS = "dag-run:{dag_run_id}:fanin-members".format`** (`jobbers/adapters/_shared.py:71`) — one Hash per run holding, for every `fan_in_key`, a single field whose value is a JSON-encoded list of that collector's permanent predecessor ULIDs (for `parent_results()` after the fan-in fires).

`stage_init_fan_in` (`jobbers/adapters/_shared.py:474-492`) writes both: one `remaining:` field plus one `pending:` field per predecessor into `DAG_RUN_FANIN`, and the JSON member list into `DAG_RUN_FANIN_MEMBERS`, refreshing the TTL on both keys each time (a later nested collector registering under the same run extends the window, rather than each collector tracking its own independent, potentially-shorter-lived TTL).

`fan_in_complete` closes via `_FAN_IN_HASH_SCRIPT` (`jobbers/adapters/_shared.py:89-97`):

```lua
local pending_field = 'pending:' .. ARGV[1] .. ':' .. ARGV[2]
if redis.call('HDEL', KEYS[1], pending_field) == 0 then
    return {0, -1}
end
local remaining_field = 'remaining:' .. ARGV[1]
local remaining = redis.call('HINCRBY', KEYS[1], remaining_field, -1)
return {1, remaining}
```

**Why `HDEL` on a per-predecessor field, not a separate "closed" marker check:** an earlier version of this script tracked closure with a `closed:{fan_in_key}:{predecessor_id}` marker checked via `HEXISTS`, decoupled from membership. That has a real bug — it can't distinguish "this ID was never a legitimate predecessor of this collector" from "this ID legitimately hasn't closed yet," so a completely unrelated task_id calling `fan_in_complete` with a guessed or reused `fan_in_key` could silently decrement someone else's countdown. This was caught by a contract test (`test_fan_in_complete_returns_minus_one_for_unknown_id`) failing against the buggy version. `HDEL` on a per-predecessor `pending:` field mirrors the old Set's `SREM`-on-a-member exactly: it returns 0 (triggering the `{0, -1}` sentinel) both when the ID was never registered *and* when it already closed, with no separate check needed — the same idempotency guarantee the Set-based design had for free.

`delegate_fan_in` (`jobbers/adapters/_shared.py:105-124, 518-526`) is one Lua script operating on both Hashes: renames the `pending:` field (old id → new id) in `DAG_RUN_FANIN` so the new id's eventual close is recognised, and rewrites the JSON member list in `DAG_RUN_FANIN_MEMBERS` via Lua's built-in `cjson`. It does not touch the `remaining:` count — a delegated ID hasn't closed yet, only its identity within pending/membership tracking changes.

**Fan-in now requires a `dag_run_id`.** All four fan-in protocol methods — `init_fan_in`, `fan_in_complete`, `get_fan_in_members`, `delegate_fan_in`, `stage_init_fan_in` (`jobbers/protocols.py:237-253`) — take a **required** `dag_run_id: ULID` (not optional). Fan-in is now strictly DAG-run-scoped by construction. Two corollaries:

- `Task.generate_callbacks` (`jobbers/models/task.py:242-254`) raises `ValueError` if a `FanInCallback` fires on a task with no `dag_run_id` — a clear failure instead of a confusing one.
- `TaskProcessor._handle_dynamic_fanout` (`jobbers/task_processor.py:410`) auto-generates a fresh `dag_run_id` (`parent.dag_run_id or ULID()`) when a fan-out is triggered by a task that wasn't already part of a DAG run — the fanned-out sub-graph becomes its own run.

**SQL is unchanged by this consolidation.** `task_fan_in` and `dag_run_pending` remain row-per-member tables — SQL never had Redis's per-key overhead problem, since `WHERE fan_in_key = ...` is already an indexed, O(1)-ish lookup regardless of how many collectors exist. The fan-in methods accept `dag_run_id` there too, for protocol parity, but don't use it for storage routing (documented in-code, matching the existing precedent that `ttl` is accepted-but-ignored for SQL).

### Task listing folded into pending/closed

The old `DAG_RUN_TASKS` sorted set (an always-growing, plain-Redis-only per-run index, with a separate RediSearch-based path for `RedisJSONTaskState`) is gone. `get_dag_run` is now **one shared implementation** for both Redis backends (`jobbers/adapters/_shared.py:356-369`) — no more per-backend override:

```python
async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
    score = await self.data_store.zscore(self.DAG_RUNS, bytes(dag_run_id))
    if score is None:
        return None
    submitted_at = dt.datetime.fromtimestamp(score, dt.UTC)
    raw_ids = await self.data_store.sunion(
        [self.DAG_RUN_PENDING(dag_run_id=dag_run_id), self.DAG_RUN_CLOSED(dag_run_id=dag_run_id)]
    )
    return submitted_at, [ULID.from_bytes(b) for b in raw_ids]
```

This works because `DAG_RUN_PENDING ∪ DAG_RUN_CLOSED` is always exactly "every task ever submitted to this run" — nothing is ever removed from either set except by `clean_dag_runs`. `RedisJSONTaskState` no longer needs its RediSearch-based `@dag_run_id` tag query for this purpose at all — both backends share the same Set-union logic. SQL's `get_dag_run` was already a single indexed `SELECT ... WHERE dag_run_id = ?` and is unaffected.

### Cleanup

`clean_dag_runs` (`jobbers/adapters/_shared.py:371-388`, shared by both Redis backends — no per-backend override) deletes all four per-run keys unconditionally for every stale `dag_run_id`: `DAG_RUN_PENDING`, `DAG_RUN_CLOSED`, `DAG_RUN_FANIN`, `DAG_RUN_FANIN_MEMBERS`. There's no registry to consult first — cleanup is a fixed four `DELETE`s per stale run, independent of how many collectors or how deeply nested the run was. Per-key TTLs on the fan-in Hashes remain a secondary safety net for abandoned runs that never reach `clean_dag_runs`.

### Debug/enumeration comes for free

A nice-to-have from the original proposal — being able to inspect a run's fan-in topology, since nesting makes it non-obvious from Redis alone — doesn't need a dedicated method. `HKEYS dag-run:{id}:fanin-members` already returns every `fan_in_key` used anywhere in a run, at any nesting depth, since each one is just a field name in that Hash. There is no `get_fan_in_keys_for_run` method, and none is needed — a registry never had to exist for this to work.

---

## What does NOT change

`delegate_fan_in`/`_DELEGATE_FAN_IN_HASH_SCRIPT` (`jobbers/adapters/_shared.py:105-124`) and `propagate_fan_in` semantics in `TaskProcessor._handle_dynamic_fanout` (`jobbers/task_processor.py:437-450`) work exactly as before the consolidation: delegation only swaps membership *within* one collector's own tracking (now the two Hashes, previously the two Sets) and never touches `DAG_RUN_PENDING`/`DAG_RUN_CLOSED` — the dispatcher and its collector are each independently submitted/registered through their own normal submit calls, which is what populates those. Nested fan-out of arbitrary depth continues to work unmodified.

---

## Files touched

| File | Change |
| --- | --- |
| `jobbers/protocols.py` | `close_dag_run_task` added to `TaskStateProtocol`. `init_fan_in`/`fan_in_complete`/`get_fan_in_members`/`delegate_fan_in`/`stage_init_fan_in` gained a required `dag_run_id` parameter. |
| `jobbers/adapters/_shared.py` | New `DAG_RUN_PENDING`/`DAG_RUN_CLOSED`/`DAG_RUN_FANIN`/`DAG_RUN_FANIN_MEMBERS` key constants and three new Lua scripts (`_CLOSE_DAG_RUN_TASK_SCRIPT`, `_FAN_IN_HASH_SCRIPT`, `_DELEGATE_FAN_IN_HASH_SCRIPT`), replacing the old `_FAN_IN_SCRIPT`/`_DELEGATE_FAN_IN_SCRIPT`. `get_dag_run` and `clean_dag_runs` moved here as single shared implementations (previously abstract/per-backend). `stage_register_dag_run` gained pending-SADD. |
| `jobbers/adapters/redis/task_state.py` | `stage_register_dag_run`, `get_dag_run`, and `clean_dag_runs` overrides all deleted — fully covered by the shared base now. |
| `jobbers/adapters/redis_json/task_state.py` | `get_dag_run` override (RediSearch-based) deleted. |
| `jobbers/adapters/redis/task_submit.py`, `jobbers/adapters/redis_json/task_submit.py` | Lua submit scripts gained one `SADD` into `DAG_RUN_PENDING`, via `_extra_submit_keys`/`_extra_rate_limited_keys`. |
| `jobbers/adapters/sql/task_state.py` | New `close_dag_run_task`. `stage_submit_task` gained a `dag_run_pending` insert. Fan-in methods gained the (unused) `dag_run_id` parameter. |
| `jobbers/adapters/sql/task_submit.py` | `_register_dag_run` helper also gained a `dag_run_pending` insert (separate code path from `stage_submit_task`). |
| `jobbers/migrations/schema.py` | New `dag_run_pending` table, added to `TABLE_GROUPS["task_state"]`. |
| `jobbers/task_processor.py` | `_maybe_cleanup` split into `_maybe_cleanup`/`_maybe_delete_self`/`_sweep_dag_run`; call moved to after `post_process`/`post_process_error`. `_handle_dynamic_fanout` generates/threads `dag_run_id` into `init_fan_in`/`delegate_fan_in` calls. |
| `jobbers/state_manager.py` | New `close_dag_run_task` wrapper. `submit_dag` reordered to generate `dag_run_id` before the `init_fan_in` gather. `dispatch_cron_dag`'s three branches thread `dag_run_id` into fan-in init calls. `init_fan_in` wrapper gained the `dag_run_id` parameter. |
| `jobbers/models/task.py` | `generate_callbacks` raises `ValueError` for a `FanInCallback` on a task with no `dag_run_id`; threads `self.dag_run_id` into `fan_in_complete`/`get_fan_in_members`. |
| `tests/conftest.py` | `DummyTaskState`/`AtomicDummyTaskState` stubs updated to the new fan-in signatures (required for `isinstance(_, AtomicTaskStateProtocol)` structural checks to keep passing in unrelated `StateManager` tests). |

---

## Testing coverage

- **Protocol contract tests** — `tests/adapters/test_task_state_common.py` (parametrized over `redis`/`redis_json`/`sql`): `close_dag_run_task` decrement/idempotency (`test_close_dag_run_task_returns_decreasing_remaining_count`, `test_close_dag_run_task_returns_minus_one_when_already_closed`, `test_close_dag_run_task_returns_minus_one_for_unregistered_task`), and the fan-in methods against the new dag_run_id-scoped signatures (`test_init_fan_in_creates_expected_members`, `test_fan_in_complete_returns_remaining_count`, `test_fan_in_complete_returns_minus_one_for_unknown_id`, `test_get_fan_in_members_returns_predecessor_ids`, `test_delegate_fan_in_swaps_id_in_tracking_and_members_sets`).
- **Redis-specific structure tests** — `tests/adapters/test_redis_task_state.py`: `test_stage_init_fan_in_creates_tracking_and_members_sets` / `test_init_fan_in_creates_tracking_and_members_sets` assert the actual Hash field shapes (`remaining:{fan_in_key}` count, JSON-encoded members blob) directly against `data_store.hget`.
- **Orchestration tests** — `tests/test_task_processor.py`: `test_maybe_cleanup_dag_task_waits_when_siblings_still_active` / `test_maybe_cleanup_dag_task_deletes_when_all_siblings_terminal` assert `get_tasks_bulk` is called at most once per run (proves the O(R²)→O(R) fix); `test_maybe_cleanup_runs_after_dynamic_fanout_registers_arms` is the ordering-hazard regression test, asserting `submit_tasks_batch` is called before `close_dag_run_task` when a dispatcher fans out.
- **Manual inspection**: `HGETALL dag-run:{id}:fanin` and `HKEYS dag-run:{id}:fanin-members` show live fan-in state for a run (replacing the old per-collector `SMEMBERS dag:fan-in:{id}` inspection); `SMEMBERS dag-run:{id}:pending` shrinking to empty confirms run completion; after completion, `clean_dag_runs`/the Cleaner (`jobbers/runners/cleaner_proc.py`) sweeping all four per-run keys can be confirmed by checking they're gone once the run ages past `max_age`.

Run `pytest tests/adapters/test_task_state_common.py tests/adapters/test_redis_task_state.py tests/adapters/test_sql_task_state.py tests/test_task_processor.py tests/test_state_manager.py tests/models/test_task.py -q`, then the full suite, plus `mypy jobbers` and `ruff check`/`ruff format --check`.
