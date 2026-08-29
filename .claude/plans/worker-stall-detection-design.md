# Worker Stall Detection — Design Proposal

Status: **draft, pending review** — no implementation yet.

## 1. Problem

Jobbers has no concept of a *worker's* liveness, only a *task's*. A worker process
whose event loop is wedged — a blocking, non-yielding call in task code, a deadlock, a
runaway sync dependency — currently looks the same to the rest of the system as a
worker that's simply idle between tasks: nothing is watching it at all. The only
existing signal, per-task heartbeat (`docs/stale-task-cancellation-design.md` §2
covers this mechanism in depth), only exists while a task happens to be running and
only catches that *one task*, not the process as a whole. A worker with no task
assigned right now, or one whose currently-running task type has no
`max_heartbeat_interval` configured, can be completely wedged with nothing in the
system able to tell.

The ask: detect when an entire worker has stalled (not just one of its tasks), and
surface that to an operator so they can bounce the worker or the machine it's running
on. This is explicitly **detect-and-alert, not auto-remediate** — nothing here kills a
process or restarts a machine.

## 2. Current mechanism (relevant parts — there isn't one)

- No worker identity exists anywhere in the codebase. `WORKER_ROLE`, `WORKER_TTL`,
  `WORKER_CONCURRENT_TASKS` (`worker_proc.py:53-55`) are process-local environment
  variables read once at startup; `WORKER_ROLE` selects which queues to poll
  (`task_generator.py:79-84`, a routing concept) but is never persisted as "worker X is
  running role Y" — many workers can and do share the same role with no way to tell
  them apart.
- No hostname/pid/worker-id is generated, stored, or exchanged anywhere. Grepping
  `jobbers/` for `worker_id`, `hostname`, `os.getpid`, `socket.gethostname` returns
  nothing outside this proposal.
- Per-task heartbeats (`Task.heartbeat_at`, `docs/stale-task-cancellation-design.md`
  §2) are the closest existing thing, but are scoped to one task's execution and
  require the task author to call `task.heartbeat()` — a worker between tasks, or
  running a task type with no configured `max_heartbeat_interval`, produces no
  heartbeat activity at all, stale or otherwise.
- Concurrency accounting (`StateManager.current_tasks_by_queue`,
  `state_manager.py:146,202-213`) is in-memory, per-process, and invisible to any
  other process — the Cleaner (a separate process) has no way to ask "how many tasks
  is worker X currently running" today, let alone "is worker X still alive."
- The existing supervised-background-task pattern —
  `_run_cancel_listener_supervised` (`worker_proc.py:37-49`), started alongside
  `main()`'s task-fetch loop and cleanly cancelled/awaited in the `finally` block
  (`worker_proc.py:96,120-122`) — is the shape this design reuses for a new
  self-heartbeat loop (§4.3).
- `CLAUDE.md`'s OTel metrics table is the existing, sole alerting surface in this
  project (OTLP → OpenObserve); there is no other notification integration (email,
  Slack, PagerDuty) anywhere in the codebase.

## 3. Proposal (starting point for this design)

Give each worker process an identity (`worker_id` + hostname + pid + role), have it
heartbeat that identity on its own event loop on a fixed interval — deliberately
*not* per-task, so it also goes stale exactly when the event loop itself is wedged,
not just when a specific task stops progressing — and have the Cleaner sweep for
workers whose heartbeat has gone stale, marking them `UNRESPONSIVE` in a small new
worker registry and emitting a metric an operator's alerting can key on.

## 4. Proposed design

### 4.1 Worker identity

Generate a `worker_id: ULID` once per process at startup in `worker_proc.py:main()`,
alongside `hostname = socket.gethostname()` and `pid = os.getpid()`. Not persisted on
the `Task` model and not threaded through `TaskProcessor` — this proposal's scope is
"is this worker process alive," which needs only the identity itself plus its own
heartbeat loop, not per-task attribution (see §7 for that as an explicit follow-up).

### 4.2 New protocol: `WorkerRegistryProtocol`

Following the repo's `{Backend}{Protocol}` adapter convention
(`CLAUDE.md` "Adapter naming convention"):

```python
class WorkerInfo(BaseModel):
    worker_id: ULID
    hostname: str
    pid: int
    role: str
    registered_at: dt.datetime
    heartbeat_at: dt.datetime
    status: Literal["active", "unresponsive"]

class WorkerRegistryProtocol(Protocol):
    async def register_worker(self, worker_id: ULID, hostname: str, pid: int, role: str) -> None: ...
    async def update_worker_heartbeat(self, worker_id: ULID) -> None: ...
    async def deregister_worker(self, worker_id: ULID) -> None: ...
    async def get_active_workers(self) -> list[WorkerInfo]: ...
    async def get_stale_workers(self, stale_time: dt.timedelta) -> list[WorkerInfo]: ...
    async def mark_worker_unresponsive(self, worker_id: ULID) -> None: ...
```

This is a new, independent store — not folded into `TaskStateProtocol` — since it has
nothing to do with task blobs and every backend combination (any `TASK_BACKEND` +
any `ROUTING_BACKEND`) should be able to opt into it independently.

Implementations, mirroring the existing per-protocol backend matrix:

- **`RedisWorkerRegistry`** (`adapters/redis/worker_registry.py`) — one hash per
  worker (`WORKER_DETAILS = "worker:{worker_id}".format`, storing
  hostname/pid/role/registered_at/status) plus one global heartbeat sorted set
  (`WORKER_HEARTBEATS = "worker-heartbeats"`, score = last heartbeat timestamp,
  member = worker ID bytes) — the exact same shape as the existing per-task heartbeat
  sorted set (`HEARTBEAT_SCORES`, `_shared.py:66`), just not partitioned by queue
  since workers aren't queue-scoped. `get_stale_workers` is a `ZRANGE ... BYSCORE 0
  cutoff` identical in structure to `get_stale_tasks`
  (`_shared.py:434-450`).
- **`SQLWorkerRegistry`** (`adapters/sql/worker_registry.py`) — new `workers` table
  (`worker_id` PK, `hostname`, `pid`, `role`, `registered_at`, `heartbeat_at` nullable,
  `status`), migrated the same way `tasks`/`dag_runs` are (new migration in
  `jobbers/migrations/`). `get_stale_workers` is `SELECT ... WHERE heartbeat_at <
  cutoff AND status = 'active'`.

Neither implementation needs an Atomic sub-protocol — nothing here participates in
the cross-store atomic-pipeline decision `StateManager` makes for task
submit/retry/dispatch, since worker registration/heartbeat has no ordering
dependency on task-state writes.

### 4.3 Worker-side self-heartbeat loop

New `WORKER_HEARTBEAT_INTERVAL` env var (default: a fraction of the intended
`WORKER_STALE_TIME`, e.g. 15s against a 60s stale cutoff — same ratio convention as
`SCHEDULER_POLL_INTERVAL` vs. how the scheduler is used elsewhere). New supervised
background task, structured identically to `_run_cancel_listener_supervised`
(`worker_proc.py:37-49`):

```python
async def _run_worker_heartbeat_supervised(
    state_manager: StateManager, worker_id: ULID, hostname: str, pid: int, role: str
) -> None:
    """Heartbeat this worker's liveness, restarting (with backoff) if the loop dies unexpectedly."""
    while True:
        try:
            await state_manager.worker_registry.update_worker_heartbeat(worker_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Worker heartbeat failed; retrying in 1s.")
            await asyncio.sleep(1)
            continue
        await asyncio.sleep(WORKER_HEARTBEAT_INTERVAL)
```

Registered once via `register_worker` before the main loop starts in `main()`
(`worker_proc.py:56-59`), started as a third supervised task alongside
`cancel_listener` (`worker_proc.py:96`), and cancelled/awaited plus deregistered in
the existing `finally` block (`worker_proc.py:116-135`) — same lifecycle as the
cancellation listener, just one more entry in the same start/stop sequence.

**Deliberate property, stated explicitly:** because this loop runs a plain
`asyncio.sleep`-driven coroutine on the *same* event loop that executes tasks, a
genuinely wedged loop (a task blocking the loop with non-yielding sync work, a
deadlock) stops this heartbeat too — which is exactly the failure mode an operator
needs paged for. This is intentionally complementary to, not a replacement for, the
per-task heartbeat in `docs/stale-task-cancellation-design.md`: a worker that's alive
and responsive but running a task that itself never calls `task.heartbeat()` is a
per-task concern (that design's problem to catch), not a worker-liveness one.

### 4.4 Cleaner-side sweep

New `--worker-stale-time` CLI flag on `cleaner_proc.py`, mirroring the existing
`--stale-time` flag exactly (`cleaner_proc.py:36-41`):

```python
parser.add_argument(
    "--worker-stale-time",
    type=lambda x: dt.timedelta(seconds=int(x)),
    default=None,
    help="Mark workers as unresponsive if their heartbeat is older than this many seconds",
)
```

threaded into `StateManager.clean(...)` as a new `worker_stale_time` parameter, gated
the same way as the existing stale-task branch (`state_manager.py:277`):

```python
if worker_stale_time:
    stale_workers = await self.worker_registry.get_stale_workers(worker_stale_time)
    for worker in stale_workers:
        await self.worker_registry.mark_worker_unresponsive(worker.worker_id)
        logger.error(
            "Worker %s (%s, pid=%s, role=%s) has not heartbeat since %s; marking unresponsive.",
            worker.worker_id, worker.hostname, worker.pid, worker.role, worker.heartbeat_at,
        )
        worker_stall_detected.add(1, {"hostname": worker.hostname, "role": worker.role})
```

- **Marks, doesn't delete.** An operator needs to see the record (hostname, pid,
  last-heartbeat time, role) until they've acted on it and it's cleared — deleting on
  detection would erase exactly the information the alert needs to point at.
- **No retry, no escalation, no remediation.** This is the full extent of the
  system's involvement — it's an alert, not an action. Bouncing the worker or machine
  is explicitly left to the operator (the ask's own wording).
- **Idempotent re-marking.** A worker that's still stale on the next Cleaner run is
  simply marked unresponsive again (no-op if already `unresponsive`); no separate
  "already alerted" suppression is proposed — OTel/OpenObserve alerting rules are the
  right layer to dedupe repeated firings, not this code.

### 4.5 Metrics

`worker_stall_detected` (counter, tagged `hostname`/`role`) — the primary alerting
signal, following the exact pattern of every other metric in `CLAUDE.md`'s table
(OTLP → OpenObserve). This is the mechanism an operator's paging rule should actually
watch; the API/UI surface below is a secondary, manual-check surface.

### 4.6 Operator-facing API + UI (secondary surface)

```
GET /workers            → list[WorkerInfo]   (active + unresponsive)
GET /workers/{worker_id} → WorkerInfo
```

mirroring the existing queue/role CRUD route shape in `task_routes.py`. `openapi.json`
regenerated as part of implementation (source of truth for the frontend, per
`CLAUDE.md`). Frontend: new `frontend/src/pages/Workers.jsx` list page reusing the
existing `StatusBadge` component for `active`/`unresponsive`, added to the nav
alongside the existing Queues/Roles pages; `frontend/src/api/client.js` gains
`getWorkers()`/`getWorker(workerId)`. No write endpoints proposed — this surface is
read-only, matching the detect-and-alert (not remediate) scope.

## 5. Race conditions / limitations considered

- **A worker deregisters (graceful shutdown) concurrently with the Cleaner's sweep
  reading it as stale.** `deregister_worker` should remove the worker from the
  heartbeat sorted set/table row entirely (not just stop heartbeating), so a
  gracefully-shutting-down worker simply disappears from `get_stale_workers`'
  candidate set rather than getting flagged `unresponsive` on its way out. If the
  Cleaner's read happens to land in the narrow window between "last heartbeat" and
  "deregister," the worst case is one spurious `worker_stall_detected` firing for a
  worker that's actually shutting down cleanly — cosmetic, not incorrect, and no
  different in kind from any other heartbeat-based staleness check's inherent
  detection-window slop (this is the same class of imprecision
  `docs/airflow-comparison.md` §12 already calls out for task-level stale detection:
  "detection window is `max_heartbeat_interval` + Cleaner poll interval").
- **Two Cleaner replicas racing the same sweep.** Same non-issue as the equivalent
  case in `docs/stale-task-cancellation-design.md` §5 — `mark_worker_unresponsive` is
  idempotent, and firing `worker_stall_detected` twice for the same worker in the same
  poll window is a metrics-dedup concern for the alerting rule, not a correctness bug
  here.
- **A worker's event loop is only *briefly* stalled** (a long GC pause, a burst of
  CPU-bound work between `await` points that doesn't reach `WORKER_HEARTBEAT_INTERVAL`
  × the stale cutoff). Not a false positive by design — `WORKER_STALE_TIME` should be
  set comfortably above normal jitter, the same tuning tradeoff
  `max_heartbeat_interval` already requires per task type.

## 6. Precedent this design follows

- `HEARTBEAT_SCORES` / `get_stale_tasks` (`_shared.py:66,434-450`) — the exact sorted-
  set-of-timestamps shape this proposal reuses for worker heartbeats.
- `_run_cancel_listener_supervised` (`worker_proc.py:37-49`) — the supervised-
  background-task-with-backoff pattern the new heartbeat loop copies exactly.
- The Cleaner's existing `--stale-time`/stale-task sweep
  (`cleaner_proc.py:36-41`, `state_manager.py:277-326`) — the direct structural
  precedent for `--worker-stale-time`/the worker sweep: same CLI-flag-gates-a-`clean()`
  branch shape, same "only act past a configurable age" logic.
- Queue/role CRUD routes in `task_routes.py` — the precedent for `GET /workers`'s
  shape and the `openapi.json`-is-the-frontend-contract convention.

## 7. Explicitly out of scope (flagged as follow-ups)

1. **Stamping `worker_id` onto `Task` records**, so a stale *task*
   (`docs/stale-task-cancellation-design.md`) can be correlated with a stalled
   *worker* ("this task went stale because its worker died," vs. "its worker is fine,
   the task itself just hung"). Would let an operator jump straight from a stale task
   to the worker registry entry that (dis)confirms the worker is also down. Not
   included here because it touches the `Task` model and a schema migration on all
   three task-state backends (Redis/RedisJSON/SQL) — a materially bigger change than
   this proposal's core ask.
2. **Any remediation beyond alerting** (auto-restarting a worker, killing a wedged
   process, cordoning a host) — explicitly not requested and not proposed.
3. **Non-metrics notification** (Slack/email/PagerDuty integration) — the project has
   no existing integration of this kind; recommendation is to stay on the
   OTel→OpenObserve path already used for every other metric and let an operator's
   existing alerting stack (whatever's watching OpenObserve today) handle paging, per
   the reference metrics table in `CLAUDE.md`.

## 8. Testing plan

- New `worker_registry` fixture, parametrized `["redis", "sql"]`, mirroring
  `queue_config_adapter`'s shape (`CLAUDE.md` "Adapter fixtures").
- New `tests/adapters/test_worker_registry_common.py` protocol-contract suite:
  register → heartbeat → appears in `get_active_workers`; heartbeat ages past
  `stale_time` → appears in `get_stale_workers`; `mark_worker_unresponsive` →
  `status` flips and is idempotent on a second call; `deregister_worker` → vanishes
  from both active and stale queries.
- `StateManager.clean()` orchestration test: `--worker-stale-time` gates the new
  branch identically to how `--stale-time` gates the existing one (can use a
  `DummyWorkerRegistry` here — this sweep is a straightforward time-cutoff filter with
  no cross-store atomicity claim, unlike the stale-task-cancellation interaction in
  the companion design, so it doesn't need a real-backend test per CLAUDE.md's rule).
- `worker_proc.py`: test that the heartbeat loop registers on startup, heartbeats on
  the configured interval, and deregisters cleanly on shutdown alongside the existing
  cancel-listener shutdown test coverage.

## 9. Decisions

1. **Worker liveness is event-loop-cooperative by design** (§4.3) — the heartbeat
   loop shares the event loop with task execution so a wedged loop naturally goes
   stale too; this is a feature, not a limitation to work around.
2. **Detect-and-alert only, no remediation** (§4.4, §7.2) — matches the ask exactly;
   bouncing a worker/machine stays a human action.
3. **OTel metric is the primary alerting surface; the `GET /workers` API/UI is
   secondary** (§4.5, §4.6) — consistent with how every other signal in this project
   reaches an operator today.
4. **`worker_id` is not stamped onto `Task` records in this iteration** (§7.1) —
   correlating a stale task with its worker is a valuable fast-follow, deferred here
   because it requires a schema change across all three task-state backends.

This design is ready to move into implementation planning.
