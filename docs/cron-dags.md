# Cron DAGs

Cron DAGs let you fire a DAG (or a single task) on a recurring schedule defined by a standard cron expression. They are separate from the [retry-delay scheduler](operations.md#scheduler) — that handles tasks waiting out a backoff delay; this handles user-defined recurring schedules.

---

## Key Concepts

A **`CronDAGEntry`** describes a recurring DAG to fire. It stores:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `id` | `ULID` | auto | Unique identifier for this cron entry. |
| `name` | `str` | required | Human-readable label for the entry. |
| `cron_expr` | `str` | required | Standard 5-field cron expression (e.g. `"0 9 * * 1-5"`). |
| `dag_spec` | `DAGTaskSpec` | required | Root task (or DAG root) to submit on each run. |
| `enabled` | `bool` | `True` | When `False`, due entries are rescheduled but not dispatched. |
| `concurrency_policy` | `ConcurrencyPolicy` | `ALWAYS` | What to do when the previous run is still active. |

A **`DAGTaskSpec`** describes the root task node:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | `str` | required | Registered task name to submit. |
| `queue` | `str` | `"default"` | Queue to submit into. |
| `version` | `int` | `0` | Task version. |
| `parameters` | `dict` | `{}` | Fixed parameters passed to the root task on every run. |
| `dag_callbacks` | `list` | `[]` | Child nodes in the DAG graph (see [DAG Patterns](dags.md)). |

---

## Concurrency Policy

Controls what happens when the Scheduler fires a cron entry but a previous run's root task is still active.

| Policy | Behaviour |
| --- | --- |
| `ALWAYS` | Always submit a new run regardless of whether the previous run is still active. Default. |
| `SKIP_IF_RUNNING` | Skip the new run if the previous run's root task is still in a non-terminal state (`submitted`, `started`, or `heartbeat`). The entry is still rescheduled for its next occurrence. |

`SKIP_IF_RUNNING` is implemented with a Redis key `cron-active:{cron_id}` (TTL 24 h) that stores the active root task ID. The check and the SET NX that records the new run are staged atomically in the same pipeline as the fan-in init, so concurrent dispatches cannot both pass the guard.

---

## Registering a Cron Entry

There is no HTTP API for managing cron entries. Register them directly via `StateManager` during application startup or migration:

```python
import datetime as dt
from croniter import croniter

from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGTaskSpec

# Define the DAG spec (root task only — for multi-node DAGs see below)
spec = DAGTaskSpec(
    name="generate_daily_report",
    queue="reports",
    version=1,
    parameters={"report_type": "daily"},
)

entry = CronDAGEntry(
    name="daily_report",
    cron_expr="0 6 * * *",          # 06:00 UTC daily
    dag_spec=spec,
    concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING,
)

# Compute the first run time
first_run_at = croniter(entry.cron_expr, dt.datetime.now(dt.UTC)).get_next(dt.datetime)

# Register atomically
pipe = state_manager.job_store.pipeline(transaction=True)
state_manager.cron_dag_scheduler.stage_add(pipe, entry, first_run_at)
await pipe.execute()
```

---

## Using a Full DAG as the Recurring Payload

When the recurring job is a multi-step DAG rather than a single task, build the graph using `DAGNode` and convert the root to a `DAGTaskSpec`:

```python
from jobbers.models.dag import DAGNode, DAGTaskSpec

# Build the graph
extract   = DAGNode("extract_data",    version=1, parameters={"source": "warehouse"})
transform = DAGNode("transform_data",  version=1)
load      = DAGNode("load_results",    version=1)
extract.then(transform).then(load)

# Convert the root node to a DAGTaskSpec for the cron entry
spec = extract.to_spec()

entry = CronDAGEntry(
    name="nightly_etl",
    cron_expr="0 2 * * *",   # 02:00 UTC nightly
    dag_spec=spec,
    concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING,
)
```

Each cron fire generates a **fresh copy** of the DAG spec with brand-new ULIDs for every node, so repeated runs never share Redis fan-in keys.

---

## Disabling and Deleting Entries

### Disable (pause without deleting)

```python
# Fetch the entry, set enabled=False, re-register under the same ID
pipe = state_manager.job_store.pipeline(transaction=True)
state_manager.cron_dag_scheduler.stage_add(pipe, entry_with_enabled_false, next_run_at)
await pipe.execute()
```

Disabled entries are still picked up by the Scheduler on schedule — they are rescheduled for their next occurrence but not dispatched. This means the schedule is never permanently lost.

### Delete

```python
pipe = state_manager.job_store.pipeline(transaction=True)
state_manager.cron_dag_scheduler.stage_remove(pipe, entry.id)
await pipe.execute()
```

---

## How the Scheduler Fires Cron Entries

The Scheduler polls both retry-delayed tasks and cron entries on every iteration:

```
while True:
    task_entries, cron_entries = await asyncio.gather(
        task_scheduler.next_due_bulk(batch_size, queues=queues),
        cron_dag_scheduler.next_due_bulk(batch_size),
    )
    # dispatch task retries
    # dispatch cron runs
    await asyncio.sleep(poll_interval)   # only if nothing was due
```

The `next_due_bulk` call is atomic: it removes due entries from the `cron-schedule` sorted set in a Lua script, preventing two Scheduler instances from both claiming the same entry. Run exactly **one** Scheduler per Redis instance.

---

## Data Safety Guarantees

`dispatch_cron_dag` writes **atomically** before submitting any task:

1. **Reschedule** — the entry's next `run_at` is staged in a single pipeline with fan-in init sets and (for non-rate-limited queues) the task submission itself.
2. **Crash safety** — if the Scheduler crashes after the pipeline but before `submit_task`, the entry is still in `cron-schedule` and fires again on the next poll. This can produce a tolerable duplicate run but will never silently drop the schedule.
3. **Fan-in correctness** — fan-in Redis sets are populated before the root task is enqueued, so a fast-completing predecessor cannot decrement a set that does not yet exist.

---

## Quick Reference

```python
from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGTaskSpec

# Single-task cron
spec = DAGTaskSpec(name="my_task", queue="my_queue", version=1)
entry = CronDAGEntry(
    name="my_schedule",
    cron_expr="*/15 * * * *",       # every 15 minutes
    dag_spec=spec,
    concurrency_policy=ConcurrencyPolicy.ALWAYS,
)

from croniter import croniter
import datetime as dt
first_run = croniter(entry.cron_expr, dt.datetime.now(dt.UTC)).get_next(dt.datetime)

pipe = state_manager.job_store.pipeline(transaction=True)
state_manager.cron_dag_scheduler.stage_add(pipe, entry, first_run)
await pipe.execute()
```
