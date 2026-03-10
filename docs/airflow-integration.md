# Airflow + Jobbers Integration: Design Proposal

## Context

Apache Airflow uses an **executor** abstraction to decouple DAG/task orchestration from actual task execution. The CeleryExecutor sends tasks to Celery queues; workers execute them and report status back. The goal is to replace this Celery layer with Jobbers while keeping Airflow responsible for DAG definition, dependency resolution, scheduling, and the UI.

## Integration Model: Jobbers as an Airflow Executor

Airflow's division of responsibility is:

| Airflow responsibility (unchanged) | Jobbers responsibility (new) |
|------------------------------------|------------------------------|
| DAG definitions (Python files)     | Distributed task execution   |
| Task dependency resolution         | Stall detection via heartbeats |
| Scheduler (decides what runs next) | Retry logic with backoff     |
| Metadata DB, XCom, UI             | Cancellation, DLQ           |
| Connections/Variables/config       | Concurrency control          |

This is viable because Airflow's executor is a well-defined plugin interface, and CeleryExecutor is a clear reference implementation.

## What Needs to Be Built

### 1. A `JobbersExecutor` Airflow plugin (new library, e.g. `airflow-jobbers`)

Implements `BaseExecutor` with:
- `execute_async()`: Submits a task to the Jobbers Manager API (`POST /submit-task`)
- `sync()`: Polls Jobbers for status changes and reports back to Airflow's scheduler
- State mapping: Jobbers statuses → Airflow `TaskInstanceState`
- Configuration: Jobbers manager URL, queue name, auth

### 2. A built-in Airflow runner task in Jobbers

A registered `@register_task` function that:
- Receives an Airflow task instance descriptor as parameters (dag_id, task_id, run_id, etc.)
- Executes it via `airflow tasks run <dag_id> <task_id> <run_id>` (subprocess) or direct Python invocation
- Reports heartbeats during long-running operators
- Returns success/failure in a format the executor can map

This is the bridge between "Jobbers task" and "Airflow operator execution."

### 3. Possibly a bulk status endpoint on the Jobbers Manager

The executor may need to poll many task states simultaneously. A `POST /task-status/bulk` endpoint would be more efficient than N individual calls. The existing endpoint structure makes this straightforward to add.

## Key Gaps in Jobbers to Address

| Gap | Severity | Notes |
|-----|----------|-------|
| No bulk status query | Low | Easy to add; executor polling scales linearly without it |
| Synchronous operator support | Low | Airflow operators are sync; Jobbers workers can run them via `asyncio.to_thread()` |
| No webhook/callback | Low | Polling works; Celery also polls |
| Worker environment | Medium | Jobbers workers need Airflow installed + DAG file access (same requirement as CeleryExecutor workers) |
| State mapping | Low | Straightforward: COMPLETED→SUCCESS, FAILED/STALLED→FAILED, CANCELLED→REMOVED |

## Why This Is Viable

- Airflow's executor interface is stable and designed for this kind of replacement
- Jobbers already has submission, status tracking, cancellation, and retry infrastructure
- The CeleryExecutor is ~600 lines; a JobbersExecutor would be comparable in scope
- Jobbers adds value over Celery: better stall detection, structured retry policies, cancellation, and DLQ

## Rough Scope

Two deliverables:
1. **Jobbers changes**: built-in Airflow runner task + optional bulk status endpoint
2. **New `airflow-jobbers` package**: `JobbersExecutor` class + state mapping + config

No changes to Airflow's core are needed — this is entirely an executor plugin.
