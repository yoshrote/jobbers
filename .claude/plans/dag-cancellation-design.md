# DAG Cancellation — Design Proposal

Status: **draft, pending review** — no implementation yet.

## 1. Problem

Today, `POST /task/{task_id}/cancel` (and the existing bulk `POST /tasks/cancel`) can only
cancel tasks the caller already knows the IDs of. There is no way to cancel an entire DAG
run. Worse, cancelling a single task inside a DAG today leaves the rest of the run in a
broken state: a cancelled task never calls `generate_callbacks()` (this is intentional for
single-task cancel — see `jobbers/task_processor.py:290-293`), so any `FanInCallback` or
`DynamicFanOutCallback` it carried simply never fires. Downstream collectors wait forever,
and siblings that are still `SUBMITTED`/`SCHEDULED`/running are unaffected and keep going.
There is no way to say "stop this whole run."

## 2. Current cancellation mechanism (relevant parts)

- `RedisCancellationBus` (`jobbers/adapters/redis/cancellation_bus.py`) is a single global
  Redis pub/sub channel, `CHANNEL = "task_cancellations"`. The payload is just
  `str(task_id)` — a bare ULID, no envelope.
- `StateManager.request_task_cancellation(task_id)` (`state_manager.py:610-647`) branches on
  status: `SCHEDULED`/`SUBMITTED` are cancelled directly (removed from scheduler/queue, no
  pub/sub needed); `STARTED` publishes one message on the bus; anything else raises
  `TaskException`. Note: `UNSUBMITTED` is in that catch-all, but it is not actually reachable
  via this method today — see the retry-path note in §4.4.
- Every worker process runs exactly one `run_cancel_listener()` background task
  (`state_manager.py:658-661`, started in `worker_proc.py`) that subscribes to the single
  channel — **every worker sees every cancellation message**, not just the one running the
  target task.
- Disambiguation among a worker's own concurrently-running tasks
  (`WORKER_CONCURRENT_TASKS`) happens client-side via an in-memory
  `_cancel_events: dict[ULID, asyncio.Event]` (`state_manager.py:117`), registered per
  in-flight task by `TaskProcessor.run()` (`task_processor.py:156`) for the duration of
  execution. `signal_cancel(task_id)` sets the event if present, else no-ops.
- `get_dag_run(dag_run_id)` already returns a `DAGRunDetail` with the **full set of task IDs
  ever registered to the run** (`models/dag.py:687-694`; Redis via `SUNION` of pending+closed
  sets, SQL via an indexed `dag_run_id` column) — so enumerating "every task in this DAG" is
  already solved and needs no new index.
- `DagRunStatus` (`models/dag.py:659-665`) is a **derived** value (`running` /
  `partial_failure` / `failed` / `complete`), computed from per-run completed/failed counters
  maintained by Lua scripts (Redis) — there is no persisted "cancelling" concept today.
- The cancellation bus is Redis-only and wired unconditionally in `db.py:265`, independent of
  `TASK_BACKEND`/`ROUTING_BACKEND`. This is a pre-existing constraint, not something this
  proposal changes.

## 3. Your proposal (starting point for this design)

Reuse the same pub/sub channel for both individual and DAG-wide cancellation by prefixing the
message payload: `"task:<task_id>"` for a single task, `"dag:<dag_run_id>"` for every task
belonging to that run. A worker that's running task X registers a cancel-wait keyed by X's
`task_id` (as today) *and* now also knows X's `dag_run_id`; when a `"dag:<id>"` message
arrives, every worker checks its own in-flight tasks locally and fires the ones whose
`dag_run_id` matches. This means **one pub/sub message cancels an arbitrarily large fan-out**
(e.g. a `DynamicFanOutCallback` with hundreds of concurrently-running arms) instead of
publishing one message per running task.

This is the core mechanism below; the rest of the design is what has to change to make a
"cancel the whole run" operation correct, not just the wire format.

## 4. Proposed design

### 4.1 Wire format

Keep one channel, change the payload to a `"<kind>:<id>"` string:

```
task:01J...   → cancel this one task (today's behavior)
dag:01J...    → cancel every task belonging to this DAG run
```

`CancellationBusProtocol` (`protocols.py:122-127`) becomes:

```python
CancellationKind = Literal["task", "dag"]

class CancellationMessage(NamedTuple):
    kind: CancellationKind
    id: ULID

class CancellationBusProtocol(Protocol):
    async def publish_cancellation(self, task_id: ULID) -> None: ...
    async def publish_dag_cancellation(self, dag_run_id: ULID) -> None: ...
    def listen_cancellations(self) -> AsyncIterator[CancellationMessage]: ...
```

`RedisCancellationBus` publishes `f"task:{task_id}"` / `f"dag:{dag_run_id}"` and parses
incoming messages by splitting on the first `:`; a payload with an unrecognized/missing
prefix is logged and dropped, matching today's malformed-ID handling
(`cancellation_bus.py:47-48`).

### 4.2 Worker-side: tracking `dag_run_id` per in-flight task

`_cancel_events` needs to carry each task's `dag_run_id` alongside its event:

```python
@dataclass
class _CancelHandle:
    event: asyncio.Event
    dag_run_id: ULID | None

self._cancel_events: dict[ULID, _CancelHandle] = {}
```

`cancel_event()` (`state_manager.py:185-191`) changes from taking `task_id: ULID` to taking
the `Task` itself (its only caller, `task_processor.py:156`, already has the full `Task`
object). `signal_cancel(task_id)` is unchanged in behavior. New:

```python
def signal_cancel_dag(self, dag_run_id: ULID) -> int:
    """Fire the cancel event for every in-flight task on this worker belonging to dag_run_id."""
    n = 0
    for handle in self._cancel_events.values():
        if handle.dag_run_id == dag_run_id:
            handle.event.set()
            n += 1
    return n
```

`run_cancel_listener()` dispatches on `msg.kind`:

```python
async def run_cancel_listener(self) -> None:
    async for msg in self.cancellation_bus.listen_cancellations():
        if msg.kind == "task":
            self.signal_cancel(msg.id)
        else:
            self.signal_cancel_dag(msg.id)
```

No change to `monitor_task_cancellation` / `TaskProcessor.run`'s cancellation-race handling —
a task that gets its event set via the DAG path raises the same `UserCancellationError` and
is handled by the existing `handle_user_cancelled_task` path (`state_manager.py:617-620`).

### 4.3 `StateManager.request_dag_cancellation(dag_run_id)`

New method, modeled on the existing bulk precedents (`resubmit_dead_tasks`,
`sweep_dag_run` — see §6):

1. `run = await self.get_dag_run(dag_run_id)`; `None` → caller returns 404.
2. Mark the run **cancelling** (persisted, idempotent — see §4.4) *before* touching any
   task, so the window in which a concurrently-completing task could still see "not
   cancelling" is as small as possible.
3. Bulk-load all tasks (`get_tasks_bulk(run.task_ids)`), filter to non-terminal
   (`TaskStatus.terminal_statuses()` = `COMPLETED, FAILED, CANCELLED, STALLED, DROPPED`).
4. For each non-terminal task, branch on status — the same cases as
   `request_task_cancellation` (no `UNSUBMITTED` case needed; see §4.4 for why):
   - `SCHEDULED` → remove from scheduler, set `CANCELLED`.
   - `SUBMITTED` → remove from queue, set `CANCELLED`.
   - `STARTED` → **no per-task publish**. Counted, but left for the single trailing
     broadcast in step 6.
   Stage all of these onto one atomic pipeline when `_atomic_state`/`_atomic_scheduler` are
   available (same convention as `resubmit_dead_tasks`, `state_manager.py:319-342`), else
   fall back to sequential saga-mode writes.
5. Save the pipeline / await the sequential writes.
6. If any tasks were `STARTED`, call `await self.cancellation_bus.publish_dag_cancellation(dag_run_id)`
   **exactly once**, regardless of how many `STARTED` tasks there were. This is the
   noise-avoidance property from your proposal: a 500-arm `DynamicFanOutCallback` that's
   mid-flight is cancelled with one Redis PUBLISH, not 500.
7. Return a summary (counts per bucket) for the API response.

### 4.4 Persisted "cancelling" state on the DAG run + gating new descendants

`DagRunStatus` gains two values: `CANCELLING`, `CANCELLED`. Since the existing status is
purely *derived* from completed/failed counters, "cancelling" needs an actual persisted
marker — a run can be cancelling while zero tasks have failed yet. Add a nullable
`cancelled_at` field to the run's storage:

- Redis: new field on the `DAG_RUN_META` hash (`dag-run:{id}:meta`,
  `adapters/redis/task_state.py:66-73`), set via `HSETNX` (idempotent — a second cancel
  request doesn't reset the timestamp).
- SQL: new nullable `cancelled_at` column on `dag_runs`.

Status derivation: if `cancelled_at` is set, report `CANCELLING` while `pending > 0`,
`CANCELLED` once pending reaches 0; otherwise fall back to today's
running/partial_failure/failed/complete logic.

New lightweight protocol method (deliberately *not* `get_dag_run`, which does a full
`SUNION`/task-listing — too expensive to call from the hot path below):

```python
async def mark_dag_run_cancelling(self, dag_run_id: ULID) -> None: ...
async def is_dag_run_cancelling(self, dag_run_id: ULID) -> bool: ...
```

**Gate 1 — `TaskProcessor.post_process`** (`task_processor.py:352`), before any fan-out
handling or `generate_callbacks()` call:

```python
if task.dag_run_id and await self.state_manager.is_dag_run_cancelling(task.dag_run_id):
    return  # run is being cancelled; don't spawn new descendants
```

This closes the race where a task is `STARTED` when `request_dag_cancellation` fires and
completes normally microseconds later — without this it would still call
`generate_callbacks()`/spawn fan-out arms, resurrecting a run that was supposed to stop. The
task's own terminal bookkeeping (closing it out of `DAG_RUN_PENDING`, recording the outcome)
is unaffected — only new-descendant spawning is skipped.

**Gate 2 — `TaskProcessor._handle_retry`** (`task_processor.py:628`), same check, at the top,
before the `should_retry()`/schedule-vs-immediate branching:

```python
async def _handle_retry(self, task: Task, error_message: str) -> Task:
    task.errors.append(error_message)
    if task.dag_run_id and await self.state_manager.is_dag_run_cancelling(task.dag_run_id):
        await self.handle_user_cancelled_task(task)
        return task
    if not task.should_retry():
        ...
```

This covers the analogous race on the failure path: a task fails (was `STARTED`, so covered
by the pub/sub broadcast up to that point) right as its DAG is being cancelled, and would
otherwise retry — either via `schedule_retry_task` (→ `SCHEDULED`) or `queue_retry_task`
(→ immediately re-`SUBMITTED`) — resubmitting work into a run that's supposed to be stopping.
Cancellation wins over "retries remaining"; the task ends `CANCELLED`, reusing the same path
`handle_user_cancelled_task` already uses for a live pub/sub cancel signal (`state_manager.py:617-620`).

This is also why `request_dag_cancellation`'s bulk sweep (§4.3) doesn't need an `UNSUBMITTED`
branch: tracing `_handle_retry` → `queue_retry_task` (`state_manager.py:388-393`) shows
`task.set_status(TaskStatus.UNSUBMITTED)` is always immediately overwritten by
`task.set_status(TaskStatus.SUBMITTED)` before any save occurs, with no `await` in between —
so `UNSUBMITTED` is never actually persisted to storage under current code. No external caller
(including the bulk sweep) can ever observe a task in that status via `get_task`/`get_tasks_bulk`,
so there is nothing for the sweep to catch. Gate 2 above handles the real, reachable version of
this race by intercepting the transition itself rather than trying to catch a status that
storage never shows.

**Scope note — DAG-only, not all tasks.** The equivalent race exists in principle for a
standalone (non-DAG) task: a client's cancel request could arrive exactly as the task fails
and starts retrying, and miss it. Closing that would require a new persisted per-task
"cancellation requested" flag (`request_task_cancellation` currently has nothing to write for
a task that isn't `SCHEDULED`/`SUBMITTED`/`STARTED`) plus a check in `_handle_retry`
regardless of `dag_run_id` — a materially bigger change (new field, new write path, SQL
migration) for a narrower edge case than what this feature needs to fix. Proposed as
out-of-scope / a possible separate follow-up, not part of this design.

### 4.5 New endpoint

```
POST /dags/{dag_run_id}/cancel?verbose=false
```

404 if the run doesn't exist. Calls `request_dag_cancellation`, returns an aggregate summary
by default:

```json
{
  "dag_run_id": "01J...",
  "already_terminal": 5,
  "cancelled_immediately": 12,
  "signalled_running": 3
}
```

`?verbose=true` additionally includes the per-task breakdown, for debugging/UI use — mirroring
the per-item detail `POST /tasks/cancel` already returns, just opt-in here so the common case
(large fan-outs) stays a small, fixed-size response regardless of DAG size:

```json
{
  "dag_run_id": "01J...",
  "already_terminal": 5,
  "cancelled_immediately": 12,
  "signalled_running": 3,
  "tasks": [
    {"task_id": "01J...", "status": "already_terminal"},
    {"task_id": "01J...", "status": "cancelled"},
    {"task_id": "01J...", "status": "signalled"}
  ]
}
```

`request_dag_cancellation` always builds the per-task detail internally (it already has every
task loaded from `get_tasks_bulk` in step 3); the route just decides whether to include
`tasks` in the response, so `verbose` costs nothing extra on the backend.

### 4.6 Frontend surfacing

In scope for this change (per `frontend/CLAUDE.md`: keep `frontend/src/api/client.js` in sync
with `openapi.json`, which will gain the new endpoint and the two new `DagRunStatus` values):

- **`frontend/src/api/client.js`** — new `cancelDag(dagRunId, verbose)` calling
  `POST /dags/{dag_run_id}/cancel`, following the existing `cancelTask`/`cancelTasks` pattern
  (`client.js:78-85`).
- **`frontend/src/components/DagStatusBadge.jsx`** — no code change needed. It already derives
  its CSS class generically from the status string (`dag-status-${status?.toLowerCase()}`,
  line 8), so `cancelling`/`cancelled` render correctly as soon as the backend emits them.
  Only needs two new rules in `frontend/src/App.css` alongside the existing
  `.dag-status-*` block (`App.css:126-129`) — e.g. an amber/in-progress tone for
  `dag-status-cancelling` and the same neutral grey `TaskDetail`'s per-task
  `status_cancelled` already uses for `dag-status-cancelled`.
- **`frontend/src/pages/DagDetail.jsx`** — add a "Cancel DAG" button mirroring
  `TaskDetail.jsx`'s `handleCancel` (`TaskDetail.jsx:26-35`) and its `CANCELLABLE` status guard
  (`TaskDetail.jsx:6`): a `DAG_CANCELLABLE = new Set(['running', 'partial_failure', 'failed'])`
  check against `run.status` to hide/disable the button once a run is already
  `cancelling`/`cancelled`/`complete`. On confirm, call `cancelDag(dagRunId)` and reload
  (existing `load()` at `DagDetail.jsx:76-95` already re-fetches `run` and re-renders the task
  table, so cancelled tasks' `StatusBadge`s update the same way any other status change does —
  no new task-table logic needed).
- **`frontend/src/pages/DagList.jsx`** — no change proposed. `DagStatusBadge` there is already
  generic (same reasoning as above); adding a bulk-cancel-from-the-list action is a bigger UX
  decision than "surface the new states" calls for and isn't proposed here.

## 5. Race conditions considered

- **Task transitions `SUBMITTED` → `STARTED` during the sweep.** Because the bulk phase
  (steps 3-5) fully completes before the single `publish_dag_cancellation` (step 6), any task
  that slips from queued to running during the sweep is still covered by that trailing
  broadcast — by the time it publishes, the now-`STARTED` task has already registered its
  `cancel_event` (entered in `TaskProcessor.run()` before `process()` starts). No extra
  handling needed.
- **New descendants spawned by a task that completes concurrently with the cancel sweep.**
  Handled by the `is_dag_run_cancelling` gate in `post_process` (Gate 1, §4.4) — the actual
  spawn point, not the dispatch queue, so no `task_generator`-level check is required.
- **A task fails and retries concurrently with the cancel sweep.** Handled by the same
  `is_dag_run_cancelling` check in `_handle_retry` (Gate 2, §4.4) — cancellation pre-empts the
  retry instead of racing a bulk sweep against an unreachable `UNSUBMITTED` status.
- **Removing a `SUBMITTED` task from the queue while a worker is mid-pop.** This is the same
  race that already exists for single-task cancel (`request_task_cancellation`'s `SUBMITTED`
  branch) and isn't new here; no additional mitigation proposed.

## 6. Precedent this design follows

- `POST /tasks/cancel` (`task_routes.py:161-188`) — bulk-by-explicit-ID-list shape,
  per-item try/except. The new endpoint resolves the ID list itself (`get_dag_run`) instead
  of taking it from the caller.
- `StateManager.resubmit_dead_tasks` (`state_manager.py:319-342`) — atomic-pipeline-when-
  same-backend, else-sequential-saga convention for a bulk multi-task write.
- `StateManager.sweep_dag_run` (`state_manager.py:1074-1109`) — `get_dag_run` →
  `get_tasks_bulk` → filter → `gather`, the closest existing "do X to every task in a run"
  pattern.

## 7. Rollout / backward compatibility

**Decision: plain cutover, no dual-publish transition.** No production users of the project
yet, so the rolling-deploy hazard below is not a current concern — noted here only so a future
maintainer isn't surprised if this project ever needs a zero-downtime rollout of a
cancellation-bus wire format change again.

The wire-format change on the shared channel is, in general, the kind of thing that needs care
during a rolling deploy: an **old** worker still parsing bare-ULID payloads
(`ULID.from_str(raw)`, `cancellation_bus.py:46`) will fail to parse a `"task:<id>"` message
from a **new** publisher — it hits the existing `except Exception` swallow-and-log path
(`cancellation_bus.py:47-48`), meaning single-task cancellation would silently stop reaching
old workers for the duration of a mixed-version rollout window. If/when that ever matters, the
fix is a one-release dual-publish transition (`publish_cancellation` emits both the old
bare-ULID form and the new `"task:<id>"` form; each worker version ignores the format it
doesn't understand via the same parse-failure path); not needed today.

## 8. Testing plan (per CLAUDE.md's atomic-path testing rule)

- `RedisCancellationBus`: new tests for `publish_dag_cancellation` + parsing both prefixes,
  and that a malformed/unknown prefix logs and is skipped (mirrors existing malformed-ULID
  test).
- `state_manager`: orchestration test with `DummyTaskAdapter` (right methods called, right
  branching per status) **and** a `state_manager_real_ta`-backed test proving the "N
  concurrently-`STARTED` tasks in one DAG run → exactly one `publish_dag_cancellation` call,
  all N cancel events fire" property — this is exactly the kind of atomicity/race-prone
  interaction CLAUDE.md calls out as needing a real-backend test, not just `Dummy*`.
- `task_processor`: test that a task completing after its run was marked cancelling does
  *not* spawn a `DynamicFanOutCallback`/`FanInCallback` descendant (Gate 1).
- `task_processor`: test that a task which fails (retryable, retries remaining) while its DAG
  run is marked cancelling ends `CANCELLED` and is never rescheduled/resubmitted (Gate 2) —
  cover both the `should_schedule()` and immediate-retry branches of `_handle_retry`.

## 9. Decisions

1. **Rollout strategy** — plain cutover, no dual-publish transition (§7). No production users
   yet; revisit if that changes.
2. **Response shape** — aggregate counts by default, `?verbose=true` adds the per-task
   breakdown (§4.5).
3. **Frontend surfacing** — in scope for this change: `DagStatusBadge` needs no code change
   (already generic), `DagDetail` gets a cancel button, two new CSS rules for the
   `cancelling`/`cancelled` badge colors (§4.6).
4. **Standalone-task retry/cancel race** (§4.4 scope note) — out of scope here; to be written
   up as a separate follow-up proposal if/when it's prioritized.

This design is ready to move into implementation planning.
