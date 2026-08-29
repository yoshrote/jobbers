# Interacting with DAGs

This document covers the operational lifecycle of a DAG run once it exists: submitting it, scheduling it on a cron, and — because a DAG run is a group of related tasks rather than one task — cancelling or resuming the whole group with a single call. See [dag-composition.md](dag-composition.md) for how to build the graph itself (`DAGNode`, fan-out/fan-in, error callbacks) and [mermaid-dag-spec.md](mermaid-dag-spec.md) for the diagram grammar used over HTTP.

For single-task submission, scheduling, cancellation, and DLQ recovery, see [interacting-with-tasks.md](interacting-with-tasks.md) — DAG task management diverges from that in one important way, covered in [§4](#4-resuming-dag-runs): a failed DAG task is normally recovered by **resuming the run**, not by resubmitting from the DLQ.

## Quick reference

| Operation | Use when |
| --- | --- |
| **Submit a DAG** | Work is structured as dependent steps or parallel branches |
| **Cron** | A DAG repeats on a schedule (daily jobs, periodic pipelines) |
| **Cancel a DAG run** | The whole run needs to stop, not just one task |
| **Resume a DAG run** | One or more tasks in the run ended up FAILED/STALLED/CANCELLED/DROPPED and should retry from their stored parameters |
| **Configuring DAG task types** | Deciding `cleanup_on`, `dead_letter_policy`, `max_heartbeat_interval`, `fan_in_ttl`, `on_shutdown`, or retry policy for a task that runs inside a DAG |

---

## 1. Submit a DAG to a queue

A DAG submits a graph of tasks where each node runs after its predecessors complete. Fan-out, fan-in, and error-callback patterns are all supported. See [dag-composition.md](dag-composition.md) for the full reference.

### Programmatic

```python
from jobbers.models.dag import DAGNode
from jobbers.db import get_state_manager

root   = DAGNode("ingest_data")
middle = DAGNode("transform_data")
end    = DAGNode("publish_results")

root.then(middle)
middle.then(end)

dag_run_id, submitted_roots = await get_state_manager().submit_dag(root)
```

`submit_dag` accepts multiple root nodes for multi-root DAGs. It returns the shared `dag_run_id` (a ULID) and the list of root `Task` objects that were enqueued.

Using `@register_task` wrappers:

```python
root = ingest_data.node(queue="etl")
middle = transform_data.node(queue="etl")
end = publish_results.node(queue="etl")
root.then(middle)
middle.then(end)
dag_run_id, _ = await get_state_manager().submit_dag(root)
```

### HTTP

The API accepts a [Mermaid flowchart](mermaid-dag-spec.md) and assigns ULIDs automatically:

```http
POST /submit-dag
Content-Type: application/json

{
  "diagram": "graph LR\n  A[ingest_data] --> B[transform_data] --> C[publish_results]"
}
```

Returns `dag_run_id` and the IDs of the submitted root tasks. All task names in the diagram must already be registered on the workers that will receive them.

### Checking run status

```http
GET /dags/{dag_run_id}
```

Returns the run's `name`, aggregate `status` (`running`, `complete`, `partial_failure`, `failed`, `cancelling`, `cancelled`), `submitted_at`, and the full `task_ids` list for the run. `cancelling` means `POST /dags/{dag_run_id}/cancel` was called but at least one task hasn't reached a terminal status yet; `cancelled` means every task in the run has. For live per-task status with diagram colouring, poll `GET /task-status/{task_id}` on any task in the run and read its `dag_diagram` field (see [dag-composition.md](dag-composition.md)).

---

## 2. Cron DAGs

A `CronDAGEntry` wraps any DAG (including a single-task DAG) with a cron expression and fires it repeatedly. The Scheduler process computes the next `run_at` after each fire and re-adds the entry to the schedule. `ConcurrencyPolicy` controls overlap between runs.

### Programmatic

```python
import datetime as dt
from croniter import croniter
from jobbers.models.cron_dag import CronDAGEntry, ConcurrencyPolicy
from jobbers.models.dag import DAGNode
from jobbers.db import get_state_manager

# Single-task cron: wrap a one-node DAG
root = DAGNode("generate_report", parameters={"format": "pdf"})
entry = CronDAGEntry(
    name="daily_report",
    cron_expr="0 6 * * *",             # 06:00 UTC every day
    dag_spec=root.to_spec(),
    concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING,
)

sm = get_state_manager()
await sm.add_cron_dag(entry)
```

For a multi-task DAG, build the graph first then pass the root spec:

```python
ingest  = DAGNode("nightly_ingest")
process = DAGNode("nightly_process")
ingest.then(process)

entry = CronDAGEntry(
    name="nightly_pipeline",
    cron_expr="0 2 * * *",
    dag_spec=ingest.to_spec(),
    concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING,
)
await sm.add_cron_dag(entry)
```

`CronDAGEntry` is stored in the `CronDAGScheduler` backend. The entry persists across restarts — call `sm.remove_cron_dag(entry.id)` to cancel it permanently.

### HTTP

```http
POST /cron-dags
Content-Type: application/json

{
  "name": "nightly_pipeline",
  "cron_expr": "0 2 * * *",
  "diagram": "graph LR\n  A[nightly_ingest] --> B[nightly_process]",
  "enabled": true,
  "concurrency_policy": "skip_if_running"
}
```

Returns the entry including its assigned `id` and `next_run_at`.

Other lifecycle endpoints:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/cron-dags` | List all entries with next run times |
| `GET` | `/cron-dags/{id}` | Retrieve a single entry |
| `PUT` | `/cron-dags/{id}` | Replace diagram/settings; resets the schedule |
| `DELETE` | `/cron-dags/{id}` | Remove permanently |

### `ConcurrencyPolicy`

| Value | Behaviour |
| --- | --- |
| `always` (default) | Fire even if the previous run is still active |
| `skip_if_running` | Skip this fire if any task from the previous run is still active |

---

## 3. Cancelling DAG runs

A DAG run is a group of tasks, so cancelling it by calling `POST /tasks/cancel` once per task ID would mean N pub/sub messages and N round trips for a large fan-out. `POST /dags/{dag_run_id}/cancel` does it with one call and, for tasks that are already `STARTED`, one broadcast.

| Status at cancel time | What happens |
| --- | --- |
| `submitted` / `scheduled` | Cancelled directly in the same sweep — removed from the queue/scheduler, marked `cancelled` immediately. |
| `started` | Not cancelled individually. A single `publish_dag_cancellation` pub/sub broadcast is sent for the whole run; each worker running one of the run's tasks interrupts it at the next `await`. |
| already terminal | Left alone, reported as `already_terminal`. |

The run is marked `cancelling` before any task is touched, so in-flight `TaskProcessor` callbacks stop generating new descendants or retries for the run as early as possible — a task completing mid-cancellation won't spawn its normal successor.

### HTTP

```http
POST /dags/{dag_run_id}/cancel
```

```json
{
  "dag_run_id": "01JXXX...",
  "already_terminal": 2,
  "cancelled_immediately": 3,
  "signalled_running": 1
}
```

`already_terminal + cancelled_immediately + signalled_running` always equals the number of tasks in the run. Pass `?verbose=true` to get a per-task breakdown instead of just the counts:

```http
POST /dags/{dag_run_id}/cancel?verbose=true
```

```json
{
  "dag_run_id": "01JXXX...",
  "already_terminal": 1,
  "cancelled_immediately": 1,
  "signalled_running": 1,
  "tasks": [
    {"task_id": "01AAA...", "status": "already_terminal"},
    {"task_id": "01BBB...", "status": "cancelled"},
    {"task_id": "01CCC...", "status": "signalled"}
  ]
}
```

Returns `404` if the DAG run does not exist.

Cancellation is best-effort for `signalled` tasks in the same way single-task cancellation is: the worker interrupts at the next `await` checkpoint, so a task in a tight non-async loop won't stop until it hits one.

---

## 4. Resuming DAG runs

When one or more tasks in a run end up `FAILED`, `STALLED`, `CANCELLED`, or `DROPPED`, `POST /dags/{dag_run_id}/resume` resubmits each of those tasks from its stored parameters — same `dag_callbacks` and `parent_ids`, fresh attempt. Once a resumed task completes, the normal `post_process` → `generate_callbacks()` path continues the DAG exactly as it would have on a first attempt; no separate graph-replay logic is involved.

> **This is why DAG tasks lean less on the DLQ than standalone tasks do.** [DLQ resubmit](interacting-with-tasks.md#4-resubmitting-from-the-dead-letter-queue) puts a single task back on its queue — for a DAG task, that skips reintegration with the run's fan-in tracking and DAG-run counters. Resume operates at the run level and keeps that bookkeeping intact, so for DAG tasks it's the primary recovery path even if `dead_letter_policy=SAVE` also saved a copy to the DLQ.

### Why a run might not be resumable

Resume depends on state that the Cleaner will eventually prune, so it isn't available forever after a failure:

| Reason | Meaning |
| --- | --- |
| `dag_run_not_found_or_expired` | The run's index was never found, or was pruned by `clean_dag_runs`. |
| `task_history_incomplete` | The run's index survived, but at least one sibling task's stored blob was pruned by `clean_terminal_tasks` (see `--completed-task-age` in the [operations guide](operations.md#cleaner)). |
| `no_stuck_tasks` | Every task is still active or already succeeded — nothing to retry. |
| `fan_in_tracking_expired` | The run uses fan-in and its shared tracking hash has outlived its `fan_in_ttl` (see [dag-composition.md](dag-composition.md) on fan-in TTLs). |

Tasks configured with `cleanup_on` that matches a terminal status are deleted as soon as they reach it (see [The Cleaner in README.md](../README.md#the-cleaner)) — a run built entirely from such tasks may lose resumability faster than one relying solely on the Cleaner's age-based sweep.

### Checking resumability

Read-only, no side effects — safe to poll to drive a "Resume" button's enabled state:

```http
GET /dags/{dag_run_id}/resume-check
```

```json
{
  "dag_run_id": "01JXXX...",
  "resumable": true,
  "reason": null,
  "stuck_task_ids": ["01BBB...", "01CCC..."]
}
```

When `resumable` is `false`, `reason` is one of the values above and `stuck_task_ids` is empty.

### Resuming

```http
POST /dags/{dag_run_id}/resume
```

```json
{
  "dag_run_id": "01JXXX...",
  "resumed_task_ids": ["01BBB...", "01CCC..."]
}
```

`resume_dag_run` re-derives the same resumability checks itself rather than trusting an earlier `resume-check` call, since the two are separate round trips and state could change in between — so a `409 Conflict` with the current `reason` is still possible even right after a `resume-check` reported `resumable: true`. `404` means the run does not exist.

If the run is still flagged `cancelling` from an earlier `POST /dags/{dag_run_id}/cancel`, resume clears that flag before resubmitting anything. Otherwise the resumed tasks would complete into a run that `TaskProcessor` still treats as cancelling, and their descendants/retries would be silently suppressed by the same gate that stops a cancelled run from spawning more work.

---

## 5. Task configuration for DAG workflows

Task types that run primarily inside DAGs have different configuration priorities than standalone tasks, mostly because run-level cancel/resume (§3, §4) exists as the primary recovery mechanism instead of per-task DLQ handling.

### `cleanup_on` is safe to combine with resumability

It's tempting to assume `cleanup_on` and "I might want to resume this DAG later" are in tension — set `cleanup_on={TaskStatus.COMPLETED}` and a task's blob is gone, so what's left to resume from? In practice this isn't a real conflict: `sweep_dag_run` (the code path that actually deletes `cleanup_on`-matching task blobs) only runs once **every** task in the run has closed out of `DAG_RUN_PENDING` — and a task in a stuck status (`FAILED`/`STALLED`/`CANCELLED`/`DROPPED`, the same set §4 resumes) never closes. So a run with anything left to resume never triggers the sweep in the first place; `cleanup_on` can't delete a sibling out from under a run you still intend to resume.

The setting that actually bounds your resume window is the Cleaner's age-based `clean_terminal_tasks` (`--completed-task-age`), which deletes **any** terminal task blob — regardless of `dag_run_id`, `cleanup_on`, or stuck status — once it's older than that threshold. This is the mechanism behind `task_history_incomplete` (§4). If you want DAG runs to stay resumable for, say, a day after failing, run the Cleaner with `--completed-task-age` at least that long (see the [operations guide](operations.md#cleaner)). If you never intend to resume a given DAG's failures, `cleanup_on` is still a good way to keep memory bounded independent of the Cleaner's schedule.

### Think twice before `dead_letter_policy=SAVE` on DAG-only task types

`resume_dag_run` resubmits stuck tasks straight from their own stored blob — it never touches the DLQ. (`remove_from_dlq` is only called from the DLQ resubmit path, `resubmit_dead_tasks`.) So a task with `dead_letter_policy=SAVE` that later gets fixed via DAG resume leaves a stale entry behind in the DLQ: it still reads as a permanent failure even though the task has since been retried — and may have succeeded — through the run-level resume endpoint. For task types used only inside DAGs, the default `dead_letter_policy=NONE` plus resume as the recovery path avoids that inconsistency. Reserve `SAVE` for task types that are also submitted standalone outside of a DAG, where [DLQ resubmit](interacting-with-tasks.md#4-resubmitting-from-the-dead-letter-queue) is the intended recovery flow.

### Heartbeats matter more inside a DAG

A hung task blocks everything waiting on it — most acutely a fan-in collector, which won't fire until every predecessor reaches a terminal status. Without `max_heartbeat_interval` configured, a hung `STARTED` task has no path to a terminal status at all until something else intervenes (SIGTERM, a manual cancel) — Cleaner-driven stall detection is what turns it into `STALLED`, a resumable stuck status that shows up in `resume-check` and gives you a way out. Set `max_heartbeat_interval` on any DAG task type whose function might legitimately run long, especially fan-in predecessors — without a heartbeat, "hung" and "still working" are indistinguishable from outside the task.

### `fan_in_ttl` bounds the resume window for dynamic fan-out

Static fan-in (two or more `-->` edges into the same node) tracks predecessors permanently until they all resolve — no TTL to worry about. Dynamic fan-out (a `DynamicFanOut` return value, or a `-->>` mermaid edge) is different: its fan-in tracking hash expires after `fan_in_ttl` (default `86400` seconds / 24h) regardless of whether the arms have resolved. If an arm stalls and nobody resumes the run within that window, `resume-check` starts reporting `fan_in_tracking_expired` (§4) even though the task history itself may still be intact. If your operational process for noticing and resuming a stuck fan-out run might take longer than a day, raise `fan_in_ttl` on the `DynamicFanOut(...)` call or the `-->>` callback accordingly — see [dag-composition.md](dag-composition.md) for where it's set.

### `on_shutdown` and retry policy

`resume_dag_run` resets `retry_attempt` to `0` and treats `STALLED` the same as `FAILED`/`CANCELLED`/`DROPPED`, so the choice between `on_shutdown=STOP` (→ `STALLED` on SIGTERM, resumable but waits for an operator to call resume) and `on_shutdown=RESUBMIT` (→ automatically re-enqueued, no resume needed) matters more for DAG tasks than standalone ones. `RESUBMIT` keeps a routine worker deploy or restart from turning into an operator-facing "this run needs resuming" event; `STOP` is more appropriate when you specifically want a human to confirm the run should continue before it does.

Likewise, tune `max_retries` / `retry_delay` / `backoff_strategy` to absorb the failures you expect to be transient automatically — treat DAG resume as the residual, operator-driven case (retries exhausted, or a deliberate cancellation) rather than as a substitute for retry policy.
