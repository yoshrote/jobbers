# DAG Resume — Design Proposal

Status: **draft, pending review** — no implementation yet.

Scope note up front: this proposal is explicitly limited to DAG runs whose task
records, run index, and fan-in tracking have **not** been expunged by the Cleaner
(`--completed-task-age`, `--dlq-age`) or by fan-in TTL expiry. A run that's been swept
is not resumable under this design — see §2.4 and §4.1 for exactly what "not
expunged" means and how resume detects and reports the alternative.

## 1. Problem

Once a DAG run gets a task into `TaskStatus.stuck_statuses()` (`FAILED`, `STALLED`,
`CANCELLED`, `DROPPED`), the run is permanently stuck. This isn't accidental —
`TaskStatus.stuck_statuses()`'s own docstring (`jobbers/models/task_status.py:35-46`)
already says so:

> "A task in one of these statuses never calls `generate_callbacks()`, so any
> `FanInCallback`/`DynamicFanOutCallback` it carries never fires — the DAG's collector
> is permanently blocked **until the task is retried**... left untouched (fan-in
> tracking, sibling task records) **so it can be manually resumed**."

The persistence and cleanup-suppression side of this was already built
(`finalize_dag_run_task`, `jobbers/state_manager.py:1107-1146` — a stuck task's
sibling records and fan-in state are deliberately preserved instead of swept). What's
missing is the other half: an actual mechanism to take a stuck DAG run, retry its
stuck tasks from their stored parameters, and let the DAG continue from there. Today
the only recovery tool is `POST /dead-letter-queue/resubmit`, which is per-task and
DLQ-only (see §2.3 for why that's insufficient on its own).

## 2. Current mechanism (relevant parts)

### 2.1 What "stuck" leaves behind

A DAG run is a set of `Task` blobs sharing one `dag_run_id`
(`jobbers/models/task.py:82`), not a separate row with its own structure. Each task
carries its own `parameters`, `results`, `errors`, `parent_ids`, and
`dag_callbacks: list[DAGCallback]` (`jobbers/models/dag.py:209`) describing what to
submit next on success. `get_dag_run(dag_run_id)` (`protocols.py:288`) returns a
`DAGRunDetail` with the full `task_ids` list (Redis: `SUNION` of a pending set and a
closed set, per `jobbers/adapters/redis/task_state.py:197-244`; SQL: an indexed
`dag_run_id` column) — enumerating "every task in this run" needs no new index.

Critically, per `finalize_dag_run_task`'s docstring
(`jobbers/state_manager.py:1126-1131`):

> "A DAG task in a stuck status... never closes out of the pending counter... Leaving
> the counter open preserves the run's fan-in tracking and sibling task records
> (within their TTLs) instead of sweeping them away."

So a stuck task's ID **stays in `DAG_RUN_PENDING` forever** — it was never moved to
`DAG_RUN_CLOSED`. This matters for resume: there is no "reopen a closed task" problem
to solve. The task is already, and remains, pending from the run's point of view.

### 2.2 The counter problem this proposal has to solve

`record_dag_run_task_terminal(dag_run_id, outcome)` (`protocols.py:302`) is called
once per terminal status via a Lua script (`jobbers/adapters/redis/task_state.py:31-44`)
that does `HINCRBY meta failed 1` (or `completed`) and recomputes
`status ∈ {running, partial_failure, failed}` from those two counters. **Both counters
are monotonic — nothing in the codebase ever decrements `failed`.**
`mark_dag_run_complete` (`protocols.py:306`) only ever sets `status='complete'` when
`failed == 0`.

Consequence: if a stuck task is simply resubmitted and happens to succeed the second
time, `finalize_dag_run_task` runs again for the same task, calling
`record_dag_run_task_terminal(..., "completed")` — but the original `"failed"`
increment from the first attempt is still sitting in the hash. The run wobbles from
`failed`/`partial_failure` to... still `partial_failure`, and `mark_dag_run_complete`
can now *never* fire for this run, even after every task ultimately succeeds. **A
correct resume design cannot just resubmit the task; it must also reconcile the run's
aggregate counters**, or the run's displayed status becomes permanently wrong. See
§4.3.

### 2.3 Why this doesn't need — and shouldn't be gated on — the DLQ

This is the question the proposal was asked to answer directly: **no, resume should
not require `dead_letter_policy=SAVE` on every task, and should not read from the
DLQ at all.** Reasons, from the adapter code rather than from the DLQ's stated intent:

- **DLQ enrollment is per-task-type opt-in and covers a narrower set of failures than
  `stuck_statuses()`.** `TaskConfig.dead_letter_policy` (`models/task_config.py:32-37`)
  defaults to `NONE`. It's checked in exactly two places:
  `StateManager.fail_task` (only for a **permanently** failed task, i.e.
  `should_retry()` returned `False`) and the stale-heartbeat branch of
  `StateManager.clean` (for `STALLED`). **`CANCELLED` is never DLQ'd under any
  policy** — `request_dag_cancellation`/`handle_user_cancelled_task` have no DLQ call
  anywhere. A DLQ-gated resume would silently be unable to resume any cancelled DAG —
  exactly one of the two cases the user asked this feature to cover.
- **The DLQ doesn't hold extra data anyway.** `RedisDeadQueue`'s own docstring
  (`jobbers/adapters/redis/dead_queue.py:27`) says it "reuses `task:<task_id>` keys
  for task data rather than duplicating blobs" — every DLQ read
  (`get_by_ids`/`get_by_filter`/`get_history`) delegates straight to
  `task_state.get_tasks_bulk(...)`. The DLQ is a set of secondary indexes
  (`queue`/`name`/`version`/`failed_at`) over the *same* task blob resume would read
  directly. There is nothing in a DLQ entry that isn't already on the task itself.
- **The task blob already has everything a retry needs, independent of DLQ policy**:
  `parameters` (the original call args — unchanged by the failure), `dag_callbacks`
  (unconsumed, since a stuck task never calls `generate_callbacks()`), `parent_ids`
  (so `parent_results()`/`FromParent` resolution works identically on retry — the
  *parents'* blobs are untouched by their child's failure), and `dag_run_id`. Reading
  this straight off `get_tasks_bulk(dag_run.task_ids)` — the same call
  `request_dag_cancellation` already makes (`state_manager.py:686`) — needs no DLQ
  involvement.
- **Precedent already does exactly this, minus DLQ removal.**
  `StateManager.resubmit_dead_tasks` (`state_manager.py:336-359`) is "reset
  `retry_attempt`/`errors`, set `SUBMITTED`, `stage_requeue` the *existing* task
  object" — the DLQ-specific part is only the paired `stage_remove` from the DLQ
  index. Resume reuses the same reset-and-`stage_requeue` shape without that one step
  (§4.3), plus the DAG-specific counter reconciliation from §2.2 that a standalone DLQ
  resubmit never needed to do.

So: DLQ stays exactly what it is today — an optional, per-task-type, standalone-task
recovery/triage index. DAG resume reads and writes through `TaskStateProtocol`
directly (`get_dag_run`, `get_tasks_bulk`, `stage_requeue`) and is unaffected by
whichever tasks in the run do or don't have `dead_letter_policy=SAVE` set.

### 2.4 What "not expunged" has to mean, concretely

Three independent expiries bound resumability, none of which sets a "this is now
unresumable" sentinel — they simply delete state:

1. **Task blobs.** `clean_terminal_tasks` (Cleaner `--completed-task-age`) deletes a
   *terminal* task's blob once `completed_at` is older than the cutoff — reachable for
   a stuck task since `FAILED`/`STALLED`/`CANCELLED`/`DROPPED` are all terminal.
2. **The run index itself** (`DAG_RUN_PENDING`/`CLOSED`/`META`/`FANIN*`).
   `clean_dag_runs` (same `--completed-task-age` flag) deletes it once the run's
   *submission* time (not completion time) predates the cutoff — **regardless of
   whether the run is still stuck**. A long-stuck run can lose its index purely from
   elapsed wall-clock time.
3. **Fan-in tracking.** `DAG_RUN_FANIN`/`DAG_RUN_FANIN_MEMBERS` carry their own Redis
   `EXPIRE` (`init_fan_in`'s `ttl`, default 86400s —
   `jobbers/models/dag.py:203,602`), refreshed only upward (`EXPIRE ... GT`) each time
   a *new* collector under the same run registers. A run stuck for longer than the
   longest `fan_in_ttl` any of its collectors used can silently lose fan-in state
   while task blobs and the run index are both still intact.

None of these three failure modes are mutually exclusive or reported anywhere before
resume is attempted — §4.1 makes checking all three the first thing
`resume_dag_run` does, and distinguishes "run doesn't exist" (index gone) from "some
task in it is gone" (blob pruned) from "a fan-in this resume depends on has expired,"
because each is a different, actionable error for the caller.

## 3. Your proposal (starting point for this design)

Retry every task in the run whose status is in `TaskStatus.stuck_statuses()`, reusing
its already-stored `parameters`/`dag_callbacks`/`parent_ids` exactly as
`resubmit_dead_tasks` reuses a DLQ'd task's blob — but sourced from
`get_dag_run`/`get_tasks_bulk` instead of the DLQ, so it works regardless of
`dead_letter_policy` and covers `CANCELLED` (which the DLQ never does). Once each
retried task is back in `SUBMITTED`, the *existing* `TaskProcessor.post_process` →
`generate_callbacks()` machinery continues the DAG exactly as it would have the first
time — no new "replay the graph" logic is needed, because a task that never got to
call `generate_callbacks()` the first time will call it normally the second time it
completes.

## 4. Proposed design

### 4.1 `StateManager.can_resume_dag_run(dag_run_id) -> DAGResumePrecheck`

A read-only precheck, separate from the mutating step, because the three failure
modes in §2.4 need to be distinguishable to the caller before anything is retried:

```python
class DAGResumePrecheck(BaseModel):
    dag_run_id: ULID
    resumable: bool
    reason: str | None          # set iff not resumable
    stuck_task_ids: list[ULID]  # tasks that would be retried
```

1. `run = await self.get_dag_run(dag_run_id)`; `None` → `resumable=False`,
   `reason="dag_run_not_found_or_expired"` (§2.4 case 1 — the caller can't tell "never
   existed" from "index pruned" and doesn't need to; both mean the same thing here).
2. `tasks = await self.task_state.get_tasks_bulk(run.task_ids)`; any `None` entry →
   `resumable=False`, `reason="task_history_incomplete"` (§2.4 case 2 — at least one
   sibling's blob was pruned by `clean_terminal_tasks` while the run index survived
   `clean_dag_runs`; these ages are independent, so this split state is reachable).
3. `stuck = [t for t in tasks if t.status in TaskStatus.stuck_statuses()]`; empty →
   `resumable=False`, `reason="no_stuck_tasks"` (run is still active, or already fully
   succeeded — resume is a no-op either way, and silently doing nothing would be
   confusing in the API response).
4. Fan-in liveness (§2.4 case 3): for each stuck task, if any of its own
   `dag_callbacks` is a `FanInCallback`/or it is itself a registered fan-in member
   awaited by a sibling's `FanInCallback`, confirm `DAG_RUN_FANIN`
   still exists for this run (`EXISTS dag-run:{id}:fanin` — one shared key per run
   covers every collector registered under it, so this is a single cheap check, not
   one per fan-in edge). Missing → `resumable=False`,
   `reason="fan_in_tracking_expired"`. This is a best-effort check, not a guarantee
   (see §5) — but it catches the common case (a run stuck for days past the default
   24h `fan_in_ttl`) before any task is resubmitted, instead of after, where the
   failure mode is a silently-stuck collector with no error at all.
5. Otherwise `resumable=True`, `stuck_task_ids=[t.id for t in stuck]`.

Exposed as `GET /dags/{dag_run_id}/resume-check` so the frontend can show "Resume" as
disabled/enabled with a reason, without side effects.

### 4.2 New `TaskStateProtocol` methods

Two small additions, both cheap, both needed because nothing existing does this today:

```python
async def reconcile_dag_run_task_retry(self, dag_run_id: ULID, count: int = 1) -> None:
    """
    Undo `count` earlier 'failed' terminal-outcome records so a fresh success
    doesn't leave the run stuck at partial_failure forever (see §2.2).

    Decrements the run's 'failed' counter by `count` (floor 0) and recomputes
    status exactly as record_dag_run_task_terminal does, without touching
    DAG_RUN_PENDING/CLOSED (the tasks are already pending — see §2.1). No-op if
    the run's meta hash is already gone.
    """
    ...

async def refresh_dag_run_fan_in_ttl(self, dag_run_id: ULID, ttl: int = 86400) -> None:
    """
    Extend (never shrink -- EXPIRE ... GT) the shared DAG_RUN_FANIN /
    DAG_RUN_FANIN_MEMBERS keys for this run, buying time for a resumed run's
    in-flight collectors before they'd otherwise expire mid-resume.
    """
    ...
```

`reconcile_dag_run_task_retry` is the direct fix for §2.2: it's the mirror image of
`record_dag_run_task_terminal`'s `"failed"` branch, called once per `resume_dag_run`
call *before* the stuck tasks are resubmitted, with `count=len(stuck_tasks)`, so the
run's counters read "these tasks are back to pending," not "these tasks both failed
and succeeded." It takes a count instead of one call per task (and instead of a
`task_id` parameter, which it never needs — it only ever adjusts the run-level
aggregate, not anything per-task) so that resuming N stuck tasks costs one round trip
against the run's meta hash/row, not N. Implemented as one more small Lua script
(Redis/RedisJSON) / `UPDATE ... SET failed = failed - count WHERE ...` (SQL),
following the exact pattern of `_RECORD_DAG_RUN_TERMINAL_SCRIPT`
(`jobbers/adapters/redis/task_state.py:31-44`).

`refresh_dag_run_fan_in_ttl` matters because a resumed run's retried task may take
nontrivial wall-clock time to re-run before it calls `fan_in_complete`; without
refreshing, a run that only barely passed the §4.1 liveness check could still expire
mid-resume. Cheap to call unconditionally in `resume_dag_run` (step 3 below) since
it's a no-op if the keys don't exist and `GT` semantics mean it can never make things
worse.

### 4.3 `StateManager.resume_dag_run(dag_run_id) -> DAGResumeResult`

Modeled directly on `resubmit_dead_tasks` (`state_manager.py:336-359`) and
`request_dag_cancellation`'s "load once, bulk-mutate" shape:

1. Run `can_resume_dag_run` (§4.1) internally; if not resumable, raise/return the same
   `reason` so the mutating endpoint gives the identical error the precheck would have
   (no separate code path to keep in sync).
2. If the run is `cancelling`/`cancelled` (`is_dag_run_cancelling` — `protocols.py:317`),
   clear the marker. New protocol method `clear_dag_run_cancellation(dag_run_id)`
   (`HDEL meta cancelled_at` / `SET cancelled_at = NULL`) — required because
   `TaskProcessor.post_process`'s Gate 1 and `_handle_retry`'s Gate 2
   (`jobbers/task_processor.py`, added for cancellation — see
   `docs/dag-cancellation-design.md` §4.4) check this exact flag and would otherwise
   suppress the resumed task's own descendants/retries the moment it starts running
   again — resuming a cancelled run while leaving the cancelling marker set would look
   like it worked (task resubmits) and then silently fail to progress past it.
3. `await self.refresh_dag_run_fan_in_ttl(dag_run_id)`.
4. `await self.task_state.reconcile_dag_run_task_retry(dag_run_id, count=len(stuck_tasks))`
   — one call for the whole batch, not one per task. Then, for each stuck task: reset
   `task.retry_attempt = 0`, append a resume marker to `task.errors` (not replace —
   see §6.1), `task.set_status(TaskStatus.SUBMITTED)`.
5. Stage all writes onto one atomic pipeline when `_atomic_state` is available
   (`stage_requeue` per task, same as `resubmit_dead_tasks`), else sequential saga
   writes (`save_task` then `enqueue`, same ordering rationale as
   `resubmit_dead_tasks`'s saga branch — blob is the source of truth, write it before
   the queue pointer that references it).
6. Return `DAGResumeResult(dag_run_id, resumed_task_ids=[...])`.

No change needed to `TaskProcessor`/`generate_callbacks()`/fan-in dispatch — once a
resumed task is `SUBMITTED` and a worker picks it up, everything downstream (retry of
its own body, `parent_results()` resolution from its still-intact parents,
`post_process()` → `generate_callbacks()` on eventual success) is the exact same code
path a first-attempt task goes through. This is the core simplifying property of the
proposal: **"continue the DAG from where it stopped" requires no new graph-replay
logic**, because the graph was never actually mutated by the failure — only the one
task's status and the run's counters were, and step 4/2 undo exactly those two things.

### 4.4 New endpoints

```
GET  /dags/{dag_run_id}/resume-check   → DAGResumePrecheck  (§4.1, read-only)
POST /dags/{dag_run_id}/resume         → DAGResumeResult    (§4.3)
```

`POST` returns 404 if `get_dag_run` returns `None`, 409 with the `reason` string from
§4.1 for the other three non-resumable cases (distinct from 404 — the run *is* known,
just not currently resumable), 200 with `DAGResumeResult` on success:

```json
{
  "dag_run_id": "01J...",
  "resumed_task_ids": ["01J...", "01J..."]
}
```

### 4.5 Frontend surfacing

Mirrors `docs/dag-cancellation-design.md` §4.6's pattern:

- `frontend/src/api/client.js` — `getDagResumeCheck(dagRunId)` and
  `resumeDag(dagRunId)`.
- `frontend/src/pages/DagDetail.jsx` — a "Resume" button alongside the existing
  "Cancel DAG" button, enabled only when `run.status` is `partial_failure`, `failed`,
  or `cancelled` **and** the resume-check call reports `resumable: true`; disabled
  with the `reason` shown as a tooltip otherwise. On confirm, call `resumeDag` and
  reload, same as the existing cancel flow.

## 5. Race conditions / limitations considered

- **Fan-in liveness is checked at resume time, not guaranteed through to the resumed
  task's eventual completion.** §4.1 step 4 and §4.3 step 3 reduce the window (check,
  then immediately extend the TTL) but don't eliminate it — a resumed task that itself
  takes longer than the refreshed TTL to complete (or that spawns a
  `DynamicFanOutCallback` with a longer `fan_in_ttl` than what was refreshed) can still
  land on an expired fan-in and silently fail to notify its collector, exactly as an
  unresumed run's fan-in can today. This is a pre-existing limitation of the fan-in TTL
  design (not introduced by resume), just newly *relevant* because resume is the first
  feature that deliberately re-extends a stuck run's life. No new mitigation proposed
  beyond the TTL refresh in §4.2 — closing it fully would mean fan-in tracking
  surviving without any TTL for a run that's still open, a bigger change than this
  feature needs.
- **Concurrent resume requests for the same run.** Two overlapping `POST .../resume`
  calls would both pass the §4.1 precheck and both attempt to resubmit the same stuck
  tasks. Not different in kind from two overlapping `resubmit_dead_tasks` calls
  today — `stage_requeue`'s `ZADD` is idempotent (same member, last score wins) and
  `save_task` is a plain overwrite, so the outcome is a harmless double-enqueue (task
  gets pulled and processed once as normal; the second queue entry — if the first was
  already popped — points at a task blob that's no longer in a poppable state). Worth
  a note in the endpoint's docstring rather than new locking, consistent with how
  `resubmit_dead_tasks` handles the same shape of race today.
- **A task completes normally between the §4.1 precheck and the §4.3 mutation.**
  Vanishingly unlikely for a genuinely stuck task (nothing is currently processing
  it), but if it did, `reconcile_dag_run_task_retry` would decrement `failed` for a
  task that's actually fine, and `stage_requeue` would re-run an already-completed
  task. Given `resume_dag_run` re-derives `stuck` from a fresh `get_tasks_bulk` call
  rather than trusting the caller's precheck response, the only real exposure is the
  gap between `resume_dag_run`'s own read and write — the same class of race
  `request_dag_cancellation` already accepts for its `SCHEDULED`/`SUBMITTED` bulk
  branch (§5 of the cancellation design doc doesn't harden that either).

## 6. Decisions

### 6.1 `task.errors`: append, with a resume marker separating attempts

Append, not clear (`resubmit_dead_tasks` clears; DAG resume doesn't) — a DAG resume
is an operator action worth keeping a full audit trail for (`"failed 3 times,
manually resumed by X at <time>, retry succeeded"` vs. losing that history).

To keep pre-resume and post-resume errors distinguishable without a schema change
(`errors` stays `list[str]` — no new field, no migration), `resume_dag_run` appends a
sentinel line before resubmitting each stuck task:

```python
task.errors.append(f"--- resumed by operator, dag_run_id={task.dag_run_id}, at {now.isoformat()} ---")
```

Any error appended after this point (via `_handle_retry`'s
`task.errors.append(error_message)`, `task_processor.py:635`, or a fresh
`post_process failed:` entry) is unambiguously from the post-resume attempt — a
caller reading `task.errors` splits on the last line matching `^--- resumed`. This
was chosen over a structured `list[TaskAttempt]` type (grouping errors by attempt
with real fields) as the smaller change for what's currently just a display/audit
need; worth revisiting only if something needs to *query* by attempt boundary
programmatically rather than just render it.

### 6.2 `CANCELLED` tasks: treated the same as `FAILED`/`STALLED`/`DROPPED`, no opt-in

No special-casing, no `resume_cancelled` flag. Rationale: a DAG can easily be in a
mixed state — some branches genuinely failed, and *then* a user cancelled the whole
run (`POST /dags/{id}/cancel`) rather than leaving broken branches to spin. Resume
should recover that DAG as a whole, not force the caller to reason about which of its
stuck tasks were "real" failures versus "a user asked it to stop" — from the resume
operation's point of view both are just "this task didn't reach `COMPLETED`, retry it
from its stored parameters." This is already what `TaskStatus.stuck_statuses()`
naturally gives §4.1's precheck with no extra logic — the earlier draft of this design
proposed gating `CANCELLED` behind a flag; that's removed.

### 6.3 Retry budget: reset to zero on every resume

`task.retry_attempt = 0` on resume (matches `resubmit_dead_tasks`'s default). A
manual resume is a deliberate operator action, not an automatic retry — it should get
the task's full configured `max_retries` budget again, the same way a human re-running
a failed CI job gets a clean slate rather than inheriting a decremented counter from
the run that just failed. No separate "resume attempt" ceiling is introduced; if a
resumed task exhausts `max_retries` again, it becomes stuck again and is eligible to
be resumed again, same as the first time.

This design is ready to move into implementation planning.
