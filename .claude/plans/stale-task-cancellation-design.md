# Stale Task Cancellation — Design Proposal

Status: implemented.

## 1. Problem

When the Cleaner marks a `STARTED` task `STALLED` (heartbeat older than
`TaskConfig.max_heartbeat_interval`), it only ever touches storage —
`StateManager.clean`'s stale branch (`jobbers/state_manager.py:277-326`) sets the
status, removes the heartbeat entry, and optionally DLQs the task. It never signals
the worker that might still be executing it. Two consequences:

1. **The worker's concurrency slot stays occupied for nothing.** If the worker
   process is alive but just not calling `task.heartbeat()` (or is temporarily slow),
   its local `current_tasks_by_queue` entry (`state_manager.py:202-213`) — which is
   what `TaskGenerator`'s per-queue `max_concurrent` enforcement actually reads
   (`task_generator.py:86-99`) — isn't freed until the task's own coroutine happens to
   finish on its own. The Cleaner marking a task `STALLED` is supposed to mean "give up
   on this slot," but nothing acts on that.
2. **A live worker can silently overwrite the Cleaner's verdict.** `handle_success`,
   `fail_task`, and `handle_user_cancelled_task` (`task_processor.py:664-670`,
   `state_manager.py` `fail_task`, `task_processor.py:623-626`) all persist their
   outcome via an unconditional `save_task`/equivalent, with no check that the stored
   status is still what the worker last observed. A task the Cleaner just marked
   `STALLED` can have that overwritten by a `COMPLETED` or `FAILED` write moments
   later from the worker that was "stalled" all along. `Thoughts.md`'s note — "no test
   asserts that tasks in COMPLETED/FAILED/CANCELLED/STALLED/DROPPED cannot be
   transitioned further" — is exactly this gap.

The ask: when the Cleaner marks a task stale, attempt to cancel it, so a live worker
frees the slot promptly instead of only when its task coroutine happens to return.

## 2. Current mechanism (relevant parts)

- The cancellation bus (`RedisCancellationBus`, `jobbers/adapters/redis/cancellation_bus.py`)
  is a single Redis pub/sub channel, `CHANNEL = "task_cancellations"`, wired
  unconditionally in `db.py:272` regardless of `TASK_BACKEND`/`ROUTING_BACKEND` — so
  the Cleaner process always has a working `state_manager.cancellation_bus`, the same
  object a worker or the API uses.
- Per `docs/dag-cancellation-design.md` (now implemented), the wire payload is
  `"<kind>:<id>"` with `kind ∈ {"task", "dag"}`
  (`cancellation_bus.py:33-37`, parsed at `cancellation_bus.py:50-57`).
  `CancellationBusProtocol` (`protocols.py:122-137`):

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

- Every worker runs one `run_cancel_listener()` background task
  (`state_manager.py:878-884`, started in `worker_proc.py:37-49,96`) that dispatches:

  ```python
  async for msg in self.cancellation_bus.listen_cancellations():
      if msg.kind == "task":
          self.signal_cancel(msg.id)
      else:
          self.signal_cancel_dag(msg.id)
  ```

- Disambiguation among a worker's own in-flight tasks is entirely in-process:
  `_CancelHandle` (`state_manager.py:96-101`) = `(event: asyncio.Event, dag_run_id:
  ULID | None)`, keyed by `task_id` in `self._cancel_events`
  (`state_manager.py:147`), registered for the duration of `TaskProcessor.run()`
  (`state_manager.py:215-221`, `task_processor.py:156`).
  `signal_cancel(task_id)` (`state_manager.py:223-228`) sets the event if present,
  else silently no-ops — pub/sub is fire-and-forget, with no delivery confirmation
  back to the publisher.
- `monitor_task_cancellation(task_id)` (`state_manager.py:869-876`) awaits the event
  and raises `UserCancellationError`. `TaskProcessor.run()` races `process(task)`
  against `monitor_task_cancellation(task)` in an `asyncio.TaskGroup`
  (`task_processor.py:155-168`); whichever finishes first cancels the other via
  `add_done_callback`.
- `TaskProcessor.monitor_task_cancellation` (`task_processor.py:317-323`) catches
  `UserCancellationError`, calls `handle_user_cancelled_task(task)` — which sets
  `task.status = CANCELLED` and saves (`task_processor.py:623-626`) — then re-raises to
  unwind the `TaskGroup`. `process()`'s own `except asyncio.CancelledError` branch
  (`task_processor.py:244-249`) sees `task.status == CANCELLED` already set and just
  `pass`es through, preserving it.
- `task_in_registry`'s context manager (`state_manager.py:206-213`) wraps the body of
  `process()` (`task_processor.py:183`) and always frees the queue slot in its
  `finally`, regardless of which exception (or none) ended the block — so **slot
  release already works correctly today for any cancellation that actually reaches
  the worker**; the only missing piece is getting a signal there at all for a
  stale-marked task, without corrupting the status the Cleaner already wrote.
- `TaskStateProtocol.compare_and_set_status(task_id, expected, new)`
  (`protocols.py:252-262`) already exists on every backend (Redis/RedisJSON via
  WATCH/MULTI, `_shared.py:250-277`; SQL via `UPDATE ... WHERE status = ?`) but is
  currently used in exactly one place — `dispatch_scheduled_task`'s saga-mode
  SCHEDULED→SUBMITTED guard (`state_manager.py:459-465`). Its existing shape reads
  the *current stored* task, flips its status, and saves that — it does not accept a
  caller-supplied task blob, so it cannot be reused as-is to guard a write that also
  carries new `results`/`errors` (see §4.3).

## 3. Proposal (starting point for this design)

Reuse the cancellation bus with a **third, distinguishable message kind** — `"stale"`
— published by the Cleaner right after it persists `STALLED`. A worker that receives
it aborts the task locally (freeing the slot immediately, same mechanism as any other
cancellation) but must **not** write `CANCELLED` over the Cleaner's `STALLED`, and
must not re-remove a heartbeat entry the Cleaner already removed. Separately, harden
every completion writer with a compare-and-set guard, so a signal that never arrives
(worker crashed, or its event loop is genuinely wedged) doesn't leave a silent
overwrite as the only failure mode.

## 4. Proposed design

### 4.1 Wire format — a new `"stale"` kind, not a payload sub-field

Add `"stale"` as a sibling of `"task"`/`"dag"` rather than embedding a reason inside
the `"task:<id>"` payload. This keeps `payload.partition(":")` parsing
(`cancellation_bus.py:50`) completely unchanged — a third fixed prefix is a smaller
diff than teaching the parser a variable number of segments, and avoids the
`"task:<id>:stale"` payload silently failing `ULID.from_str` on the now-`"<id>:stale"`
tail if the reason were tacked onto the existing `"task"` kind instead.

```python
class CancellationKind(StrEnum):
    TASK = "task"
    DAG = "dag"
    STALE = "stale"

class CancellationBusProtocol(Protocol):
    async def publish_cancellation(self, task_id: ULID) -> None: ...
    async def publish_dag_cancellation(self, dag_run_id: ULID) -> None: ...
    async def publish_stale_cancellation(self, task_id: ULID) -> None: ...
    def listen_cancellations(self) -> AsyncIterator[CancellationMessage]: ...
```

`RedisCancellationBus.publish_stale_cancellation` publishes `f"stale:{task_id}"`;
`_listen_gen`'s `kind in ("task", "dag")` check (`cancellation_bus.py:51`) becomes
`kind in (CancellationKind.TASK, CancellationKind.DAG, CancellationKind.STALE)`. No
other parsing change.

### 4.2 Cleaner wiring

In the stale loop, right after the pipeline/saga write that persists `STALLED` +
heartbeat removal (`state_manager.py:301-314` for atomic backends, `:322-324` for
saga), publish once per task that was actually flipped to `STALLED`:

```python
task.set_status(TaskStatus.STALLED)
...  # existing stage_save / stage_remove_heartbeat / DLQ staging, unchanged
stale_cancel_publishes.append(self.cancellation_bus.publish_stale_cancellation(task.id))
```

collected alongside the existing `stale_pipes`/`dlq_saga_tasks` lists and awaited
together at the end of the `if stale_time:` block (`state_manager.py:320-326`), so the
publish happens after the write is durable rather than racing it. This runs
unconditionally — a no-op `signal_cancel` (worker crashed, or no worker has this task
registered) is exactly as cheap and side-effect-free as today's fire-and-forget
`publish_cancellation` calls elsewhere in the codebase.

### 4.3 In-process dispatch: a signal that never touches status

`_CancelHandle` gains a reason so `signal_cancel` can tell `monitor_task_cancellation`
which kind of cancellation fired. Same reasoning as `CancellationKind`'s conversion —
this is another fixed, closed set of string values, so it gets its own `StrEnum`
rather than a `Literal`:

```python
class CancelReason(StrEnum):
    USER = "user"
    STALE = "stale"

@dataclass
class _CancelHandle:
    event: asyncio.Event
    dag_run_id: ULID | None
    reason: CancelReason = CancelReason.USER

def signal_cancel(self, task_id: ULID, reason: CancelReason = CancelReason.USER) -> bool:
    handle = self._cancel_events.get(task_id)
    if handle is not None:
        handle.reason = reason
        handle.event.set()
        return True
    return False
```

`run_cancel_listener` (`state_manager.py:878-884`) gains the third branch:

```python
async for msg in self.cancellation_bus.listen_cancellations():
    if msg.kind == CancellationKind.TASK:
        self.signal_cancel(msg.id)
    elif msg.kind == CancellationKind.STALE:
        self.signal_cancel(msg.id, reason=CancelReason.STALE)
    else:
        self.signal_cancel_dag(msg.id)
```

New exception, sibling to `UserCancellationError` (`state_manager.py:110-113`):

```python
class StaleTaskCancelledError(Exception):
    """Raised when a worker is told to abort a task the Cleaner already marked STALLED."""
```

`monitor_task_cancellation` (`state_manager.py:869-876`) branches on the handle's
reason before raising:

```python
async def monitor_task_cancellation(self, task_id: ULID) -> None:
    handle = self._cancel_events.get(task_id)
    if handle is None:
        return
    await handle.event.wait()
    if handle.reason == CancelReason.STALE:
        logger.warning("Task %s aborted locally: already marked STALLED by cleaner.", task_id)
        raise StaleTaskCancelledError(f"Task {task_id} was marked stale by the cleaner.")
    logger.info("Received cancellation signal for task %s", task_id)
    raise UserCancellationError(f"Task {task_id} was cancelled by user request.")
```

`TaskProcessor.monitor_task_cancellation` (`task_processor.py:317-323`) gets a
matching branch that does **not** call `handle_user_cancelled_task` (which would set
`CANCELLED` and save):

```python
async def monitor_task_cancellation(self, task: Task) -> None:
    try:
        await self.state_manager.monitor_task_cancellation(task.id)
    except StaleTaskCancelledError:
        raise  # nothing to persist -- the cleaner's STALLED write is already authoritative
    except UserCancellationError:
        await self.handle_user_cancelled_task(task)
        raise
```

`run()`'s `ExceptionGroup` filter (`task_processor.py:163-168`) treats
`StaleTaskCancelledError` as the same kind of normal control-flow signal
`UserCancellationError` already is:

```python
except ExceptionGroup as eg:
    for exc in eg.exceptions:
        if not isinstance(exc, (UserCancellationError, StaleTaskCancelledError)):
            raise
```

`process()`'s own `except asyncio.CancelledError` branch (`task_processor.py:244-249`)
needs a third case so it doesn't fall into `handle_system_cancelled_task` (which would
save the task with its still-`STARTED` local status, re-persisting over the Cleaner's
`STALLED`):

```python
except asyncio.CancelledError as exc:
    if task.status == TaskStatus.CANCELLED:
        pass  # user cancellation already handled; keep CANCELLED status
    elif self.state_manager.cancel_reason(task.id) == CancelReason.STALE:
        pass  # cleaner already marked this task STALLED; nothing to persist
    else:
        ex = exc
        await self.handle_system_cancelled_task(task)
```

`cancel_reason(task_id) -> CancelReason | None` is a small new read-only accessor on
`StateManager` (`self._cancel_events.get(task_id)` and return `.reason` or `None`) so
`TaskProcessor` doesn't reach into the private `_cancel_events` dict directly — the
handle is still present at this point because `run()`'s `with
self.state_manager.cancel_event(...)` block (`task_processor.py:156`) doesn't pop it
until the whole `TaskGroup` (including `process()`) has finished.

After this branch, execution falls through to `remove_task_heartbeat`
(`task_processor.py:264`) exactly as today — this is an unconditional `ZREM`/`UPDATE
... SET heartbeat_at = NULL`, naturally idempotent against the Cleaner having already
removed the same entry, so no special-casing is needed there. Metrics recording
(`task_processor.py:266-282`) also runs unchanged, tagging with whatever
`task.status` currently holds locally (still `STARTED`, since nothing in the stale
path touches it) — acceptable since these are best-effort counters, not the
system of record; the persisted status remains `STALLED` regardless.

### 4.4 Defense in depth: guard completion writers against a stale-marked task

Delivery is still best-effort — a crashed worker never had a chance to react, and a
genuinely wedged event loop (the harder case) won't process the pub/sub message
either. `handle_success`, `fail_task`, and `handle_user_cancelled_task`
(`task_processor.py:664-670`, `state_manager.py`, `task_processor.py:623-626`) need a
guard so a write that does eventually happen can't silently clobber a `STALLED`
verdict the Cleaner already persisted.

`compare_and_set_status` (`protocols.py:252-262`) is close but not sufficient here: as
implemented (`_shared.py:250-277`), it re-reads the *stored* task under WATCH, flips
its status, and saves *that* re-read copy — it never sees the caller's in-memory
`task` object, so a caller with freshly-computed `results`/`errors` can't use it
without losing that data. This needs a new sibling method:

```python
async def save_task_if_status(self, task: Task, expected: TaskStatus) -> bool:
    """
    Persist `task` exactly as given (results, errors, status, everything) only if
    the *stored* task's status still equals `expected`. Returns False — the write
    is dropped — if the stored status has already moved on (e.g. the Cleaner marked
    it STALLED while this worker was still computing a result for it).
    """
```

- **Redis/RedisJSON** (`_shared.py`): WATCH the task key, read only the stored
  status (not the full blob — a cheap `HGET`/field read where possible, or reuse
  `read_for_watch` and inspect `.status`), compare to `expected`; if it matches,
  `MULTI` + `stage_save(pipe, task)` with the **caller's** `task` object + `EXEC`;
  else `UNWATCH` and return `False`. Same optimistic-retry-on-`WatchError` shape as
  `compare_and_set_status`.
- **SQL** (`sql/task_state.py`): `UPDATE tasks SET <all columns from task> WHERE id =
  ? AND status = ?`, check `rowcount`; SQLite/Postgres both support this without a
  separate `SELECT ... FOR UPDATE` round trip.

Call sites become:

```python
# handle_success
task.set_status(TaskStatus.COMPLETED)
if task.cron_id is not None:
    await self.state_manager.complete_cron_task(task)  # unchanged — cron completion path, see note below
else:
    applied = await self.state_manager.task_state.save_task_if_status(task, TaskStatus.STARTED)
    if not applied:
        logger.warning("Task %s completed after being marked stale; discarding this result.", task.id)
        tasks_completed_after_stale.add(1, {"queue": task.queue, "task": task.name})
```

and analogously in `fail_task` / `handle_user_cancelled_task`. `complete_cron_task`
is intentionally left out of scope for this guard — a stale cron task is expected to
be rare (cron dispatch already goes through `atomic_dispatch_scheduled`'s own
WATCH-based guard) and folding the CAS into its separate active-run-marker pipeline
is a bigger change than this feature needs; flagged as a follow-up if it turns out to
matter in practice.

**Decided:** a late-arriving success never overturns `STALLED` — `STALLED` stays
authoritative, the late write is dropped, and the event is surfaced (not silently
swallowed) via the `logger.warning` call and `tasks_completed_after_stale` counter
increment shown above, so an operator can tell whether `max_heartbeat_interval` is
mistuned for that task type. This matches `docs/dag-resume-design.md`'s model, where a
stuck task's *only* sanctioned path back to `SUBMITTED` is an explicit
operator-triggered resume (`resume_dag_run`), not a race between the Cleaner and
whichever writer happens to land last. If the default heartbeat window turns out to be
wrong in practice for some task type, the fix is tuning `max_heartbeat_interval` for
that task, not relaxing this guard. See §9, decision 3.

### 4.5 Metrics

- `stale_cancellations_published` (counter, Cleaner-side) — one per task actually
  flipped to `STALLED` in a sweep, tagged `queue`/`task`/`version`.
- `tasks_completed_after_stale` (counter, from §4.4) — surfaces exactly how often the
  CAS guard actually had to drop a write; a nonzero rate over time is itself a signal
  that `max_heartbeat_interval` is mistuned for some task type.
- No new metric needed for "signal delivered" vs. "no-op" — `signal_cancel`'s
  fire-and-forget nature already means the codebase doesn't track that distinction
  anywhere else (e.g. `request_task_cancellation`'s existing `publish_cancellation`
  call has the identical property).

## 5. Race conditions considered

- **Cleaner publishes `"stale:<id>"` for a task no worker currently has registered**
  (worker crashed, or the task actually finished microseconds before the sweep ran but
  the Cleaner's `get_stale_tasks` snapshot predates that). `signal_cancel` no-ops
  (`state_manager.py:223-228`, unchanged) — nothing to clean up, exactly as today's
  `publish_cancellation` behaves when no worker is running the target task.
- **The stale signal and a genuine completion race on the same worker.** If
  `process()`'s awaited task-function coroutine returns *before* `monitor_task`
  observes the stale event, `handle_success`/`fail_task` runs first and
  `save_task_if_status` (§4.4) fails its CAS (stored status is already `STALLED`) —
  the result is dropped with a warning, and `monitor_task_cancellation`'s subsequent
  `StaleTaskCancelledError` (if it still fires — the `TaskGroup`'s done-callback
  ordering means this is possible but harmless either way) hits the no-op branch in
  §4.3. No corruption in either ordering.
- **A `"stale"` signal for a task whose worker already exited normally and started a
  *new*, unrelated task reusing... no, task IDs are ULIDs and never reused; not a
  concern.**
- **Two Cleaner processes racing the same sweep** (e.g. mis-deployed with more than
  one Cleaner replica). Each independently re-reads `get_stale_tasks`, but the
  underlying write (`stage_save`+`stage_remove_heartbeat`) is not itself guarded by a
  CAS on the Cleaner side — this is a pre-existing property of `clean()`, not
  introduced by this proposal (the write is idempotent: setting `STALLED` on an
  already-`STALLED` task and re-publishing a `"stale:<id>"` message that no worker
  reacts to a second time is harmless). Not hardened further here.

## 6. Precedent this design follows

- `docs/dag-cancellation-design.md` §4.1 — extending the same channel with a new
  `"<kind>:<id>"` prefix instead of a new channel; this proposal adds a third kind the
  same way that design added `"dag"` alongside `"task"`.
- `handle_system_cancelled_task` vs. `handle_user_cancelled_task`
  (`task_processor.py:614-626`) — the existing precedent for "more than one flavor of
  cancellation needs its own handler with different persistence behavior"; this design
  adds a third flavor to the same family.
- `compare_and_set_status` / `dispatch_scheduled_task`'s saga-mode guard
  (`state_manager.py:459-465`) — the existing precedent for "read-current-status
  before writing, to avoid clobbering a concurrent transition"; `save_task_if_status`
  is the same idea extended to carry a caller-supplied blob instead of a bare status
  flip.

## 7. Rollout / backward compatibility

Same posture as `docs/dag-cancellation-design.md` §7: **plain cutover, no dual-publish
transition** — no production users yet. Noted for the same reason that doc notes it:
an old worker parsing only `{"task", "dag"}` kinds would hit the existing
malformed/unknown-prefix log-and-drop path (`cancellation_bus.py:56-57`) for a
`"stale:<id>"` message from a new Cleaner during a mixed-version rollout, meaning
stale-cancellation delivery (not correctness — the Cleaner's `STALLED` write already
landed regardless) would silently no-op against old workers for that window. Revisit
with a dual-publish transition if this project ever needs zero-downtime rollouts.

## 8. Testing plan (per CLAUDE.md's atomic-path testing rule)

- `RedisCancellationBus`: `publish_stale_cancellation` round-trip test, plus
  confirming `"task"`/`"dag"` parsing is unaffected by the new third kind.
- `state_manager_real_ta`-backed test: register a live `_cancel_events` handle for a
  task (simulating an in-flight worker), run the Cleaner's stale sweep against it,
  assert (a) the stale signal is published, (b) `monitor_task_cancellation` raises
  `StaleTaskCancelledError` (not `UserCancellationError`), (c) the final persisted
  status is `STALLED` (not overwritten), and (d) heartbeat is removed exactly once —
  this is exactly the kind of atomicity/ordering-sensitive interaction CLAUDE.md
  calls out as needing a real backend, not a `DummyTaskAdapter`.
- `save_task_if_status`: contract test in `tests/adapters/test_task_adapter_common.py`
  against all three backends — CAS succeeds when status matches, fails (no write) when
  it doesn't, and the failed case doesn't partially apply any field.
- `task_processor`: test that a task whose stale-cancel handle fires mid-execution
  ends with `STALLED` still persisted, `current_tasks_by_queue` slot freed, and no
  `CANCELLED` write — covering the `process()`/`monitor_task_cancellation` interaction
  from §4.3 with `DummyTaskAdapter` (pure control-flow assertion, no atomicity claim
  needed for this part).

## 9. Decisions

1. **New cancellation kind, not a payload sub-field** (§4.1) — keeps the existing
   parser unchanged; `"stale"` sits alongside `"task"`/`"dag"`.
2. **A stale-cancelled task's persisted status stays `STALLED`, never `CANCELLED`**
   (§4.3) — preserves the DAG-resume semantics `docs/dag-resume-design.md` already
   built around `stuck_statuses()`.
3. **Late completions after a stale mark are dropped, not applied** (§4.4) —
   `STALLED` is authoritative once the Cleaner has written it; recovery is
   operator-triggered resume, not a race between writers. Surfaced via a
   `logger.warning` log line and the `tasks_completed_after_stale` counter so an
   operator can tell how often this happens and whether `max_heartbeat_interval` needs
   retuning for that task type.
4. **`complete_cron_task`'s path is out of scope** (§4.4) — a fast-follow if stale cron
   tasks turn out to matter in practice.
5. **Rollout: plain cutover** (§7) — consistent with the precedent set in
   `docs/dag-cancellation-design.md` §7; no production users yet.

## 10. Implementation notes (decisions made during/after implementation)

1. **`save_task_if_status` split into a base method + an Atomic-only
   `atomic_save_if_status`.** `fail_task`'s atomic branch stages a task save *and* a
   DLQ add in one transaction; a bare 2-arg `save_task_if_status` had no way to fold
   the DLQ add into the same guarded write without reopening the exact race this
   feature closes (task DLQ'd even though its guarded save was dropped, or vice
   versa). Added `atomic_save_if_status(task, expected, stage_extra)` on
   `AtomicTaskStateProtocol`, mirroring the existing `compare_and_set_status` (base) /
   `atomic_dispatch_scheduled` (atomic, `stage_extra`) split — same precedent, same
   shape. `fail_task`'s atomic branch uses it; the saga branch uses the plain
   `save_task_if_status` and gates the separate `dead_queue.add_to_dlq` call on the
   returned `applied` bool.
2. **Gap 1 resolved: `expected=TaskStatus.STARTED` is hardcoded, not threaded as a
   parameter**, in `handle_success`, `fail_task`, and `handle_user_cancelled_task`.
   Traced all call sites — every one is entered with the task's *stored* status still
   `STARTED` (set by `mark_task_as_started` at the top of `process()` and never
   mutated before any of the three writers run) — so the doc's "analogously" for
   `handle_user_cancelled_task` needed no further design work.
3. **A second, previously-unguarded race was found and closed post-implementation:
   the Cleaner's own `STALLED` write was itself unconditional.** `clean()`'s stale
   loop staged `stage_save`/`save_task` using the in-memory snapshot from
   `get_stale_tasks()`, with no check that the *stored* status still matched what was
   scanned. A worker that legitimately completes/fails/cancels a task in the window
   between the Cleaner's scan and its write (via the CAS-guarded paths above,
   §4.4) could have that real outcome silently overwritten back to `STALLED` — and,
   if `dead_letter_policy=SAVE`, incorrectly DLQ'd — by the Cleaner moments later.
   This is the mirror image of the problem this whole feature set out to fix. Closed
   by adding `StateManager._mark_task_stale(task, needs_dlq, now)`, which CAS-guards
   the Cleaner's write against `TaskStatus.STARTED` the same way (atomic branch folds
   heartbeat removal + DLQ add into `atomic_save_if_status`'s `stage_extra`; saga
   branch uses `save_task_if_status` then a separate heartbeat removal). A task that
   resolved first is now left untouched — no STALLED overwrite, no DLQ, no stale
   cancellation published. Covered by
   `test_mark_task_stale_drops_write_when_task_resolved_during_race_window`
   (`tests/test_state_manager.py`) against a real `RedisTaskState`.
4. **Test-double bug found via (3): `DummyTaskState.get_stale_tasks` yielded the same
   object reference stored in `_store`**, not a fresh copy — real backends always
   deserialize a new object per read, so mutating a scanned snapshot (`task.set_status
   (STALLED)`) never affects "what's stored" until an explicit write. The Dummy's
   aliasing meant the new CAS guard's comparison was corrupted by the Cleaner's own
   pre-write mutation. Fixed by yielding `task.model_copy(deep=True)`
   (`tests/conftest.py`) to match real-backend semantics.
5. **Two doc corrections found during implementation, no design impact:**
   `RedisCancellationBus` is constructed at `db.py:278`, not `272`; the contract-test
   home named in §8 (`tests/adapters/test_task_adapter_common.py`) doesn't exist — the
   real file is `tests/adapters/test_task_state_common.py`, which already hosts
   `compare_and_set_status`'s contract tests in the same pattern.

This design is implemented; see `jobbers/state_manager.py`, `jobbers/task_processor.py`,
`jobbers/protocols.py`, `jobbers/adapters/_shared.py`,
`jobbers/adapters/sql/task_state.py`, and `jobbers/adapters/redis/cancellation_bus.py`.
