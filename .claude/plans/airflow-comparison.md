# Jobbers vs Apache Airflow: Feature Comparison

This document compares Jobbers and [Apache Airflow](https://airflow.apache.org/) across the dimensions most relevant to choosing a task/workflow execution system. Airflow sits in an adjacent niche to Jobbers — it is a **batch workflow orchestrator** built around scheduled, data-interval-driven DAG runs, whereas Jobbers is a **task queue with DAG composition bolted on**. The comparison is as much about scope and intended workload shape as it is about features.

---

## 1. Philosophy and Design Goals

**Airflow** orchestrates DAGs of tasks ("operators") that are typically triggered on a recurring schedule tied to a *data interval* (e.g., "process yesterday's data, once a day"). Its core abstractions — the scheduler, DAG runs, task instances, catchup/backfill — all assume the common case is "run this pipeline for a bounded time window, on a timer, and know exactly which window each run covers." It has a enormous ecosystem of provider packages for talking to external systems (S3, Snowflake, Spark, dbt, BigQuery, etc.), making it the default choice for ETL/ELT orchestration.

**Jobbers** is a task queue: tasks are submitted (by API call, by another task, or by a cron-scheduled DAG) and executed as soon as a worker is free, with no inherent notion of a "data interval" or "logical date." DAGs exist to express dependencies between tasks (chain/fan-out/fan-in), not to represent a recurring batch window. It optimizes for low-latency, high-throughput, ad-hoc and event-driven task dispatch — the same niche as Celery/TaskIQ — with DAG composition and cron scheduling layered on top.

**Verdict:** These overlap only at the edges. If your workload *is* "run a batch pipeline once a day/hour against a well-defined data window, with backfill support," Airflow's data-interval model is purpose-built for it and Jobbers has no equivalent. If your workload is "execute many small tasks quickly, submitted continuously, with live traffic control and sub-second dispatch latency," Jobbers is a better fit and Airflow's scheduler is not built for that cadence. Some organizations run both: Airflow for scheduled data pipelines, Jobbers/Celery-style queues for request-triggered or high-frequency async work.

---

## 2. Getting Started

**Airflow:**

```bash
pip install apache-airflow
airflow standalone   # initializes the metadata DB, creates an admin user, starts webserver + scheduler
```

```python
from airflow.sdk import dag, task

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def my_pipeline():
    @task
    def add(x, y):
        return x + y

    add(2, 2)

my_pipeline()
```

`airflow standalone` is convenient for local trial but production deployments require a metadata database (Postgres/MySQL), a scheduler process, a webserver, and (depending on executor) a message broker and worker processes — commonly deployed via the official Helm chart on Kubernetes.

**Jobbers:**

```bash
pip install -e ".[test]"
jobbers_migrate
jobbers_manager my_tasks
jobbers_worker my_tasks
jobbers_cleaner
jobbers_scheduler
```

```python
from jobbers.registry import register_task

@register_task(name="add", version=1)
async def add(x, y):
    return {"result": x + y}
```

**Verdict:** Airflow's single-binary dev mode is comparably easy to try, but its production topology (metadata DB + scheduler + webserver + executor-specific workers, often on Kubernetes) is heavier than Jobbers' four lightweight processes over Redis. Jobbers has no scheduler-side DAG-parsing cost either — Airflow's scheduler continuously re-parses DAG files from disk, which is a well-known source of scheduler lag at scale.

---

## 3. DAG Authoring Model

**Airflow** offers two authoring styles that can be mixed: the classic **operator** style (`PythonOperator`, `BashOperator`, provider-specific operators like `S3ToSnowflakeOperator`) wired together with `>>`/`<<`, and the **TaskFlow API** (`@dag`/`@task` decorators, since 2.0) which infers dependencies from Python function calls and return values:

```python
from airflow.sdk import dag, task

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def etl():
    @task
    def extract():
        return {"rows": 100}

    @task
    def transform(data: dict):
        return {"rows": data["rows"] * 2}

    @task
    def load(data: dict):
        print(data)

    load(transform(extract()))

etl()
```

Fan-out/fan-in, branching (`BranchPythonOperator`), and trigger rules (`all_success`, `one_failed`, `none_failed`, `all_done`, etc. — a richer set than a single `on_error` hook) are all first-class. Dynamic task mapping (`task.expand()`, since 2.3) generates a variable number of task instances at runtime from a list — Airflow's equivalent of runtime fan-out.

**Jobbers** provides an explicit graph API (`DAGNode`, `.then()`, `.merge()`, `DynamicFanOut`) serialized as Mermaid flowcharts — see [docs/dag-composition.md](dag-composition.md). The graph is data, not code: it can be submitted directly as a Mermaid diagram via `POST /submit-dag`, rendered natively in GitHub/VS Code, and round-tripped through the API (`dag_diagram` in task status responses).

```python
from jobbers.models.dag import DAGNode

extract = DAGNode("extract", version=1)
transform = DAGNode("transform", version=1)
load = DAGNode("load", version=1)
extract.then(transform)
transform.then(load)
await state_manager.submit_dag(extract)
```

**Key differences:**

| Dimension | Airflow | Jobbers |
| --- | --- | --- |
| Graph shape | Python code (operators or TaskFlow decorators) | `DAGNode` graph or Mermaid text |
| Graph format | Python only (Python objects define the DAG structure) | Mermaid text — portable, renderable, API-submittable |
| Result passing | XCom (implicit push/pull, size-limited by metadata DB) | `parent_results()` or `Annotated[T, FromParent("key")]` injection |
| Runtime fan-out | `task.expand()` | `TaskResult(fanout=DynamicFanOut(...))` |
| Branching | `BranchPythonOperator`, trigger rules | `on_error=` callback only; no conditional branching primitive |
| Trigger rules | Rich set (`all_success`, `one_failed`, `none_failed`, `all_done`, `all_skipped`, ...) | `FAILED` triggers `on_error`; no equivalent for skip/partial-success routing |
| DAG introspection | Graph/Grid view in Web UI | `dag_diagram` field — render anywhere Mermaid is supported |

**Verdict:** Airflow wins on expressiveness — trigger rules, branching operators, and a huge library of pre-built provider operators mean many pipelines require no custom Python at all. Jobbers wins on portability and inspectability of the DAG definition itself: a Mermaid diagram is plain text that renders everywhere and can be authored without importing Python DAG-definition modules, but it has no built-in branching primitive beyond fan-out/fan-in/error-routing.

---

## 4. Scheduling: Data Intervals, Catchup, and Backfill

This is Airflow's signature capability and the sharpest difference between the two systems.

**Airflow** ties every DAG run to a **data interval** — a `[start, end)` time window the run represents (e.g., "the run that covers 2026-07-20 00:00–2026-07-21 00:00"), not just "the moment it happened to execute." This unlocks:

- **Catchup:** if a DAG's `start_date` is in the past and `catchup=True` (the historical default; explicit `catchup=False` is now recommended), Airflow automatically schedules one run per missed interval since `start_date`.
- **Backfill:** `airflow dags backfill -s 2026-01-01 -e 2026-02-01` re-runs a DAG for an arbitrary historical range, exactly as if it had been scheduled for those intervals — essential for "we fixed a bug, now replay three months of data."
- **Idempotent design pressure:** because the same DAG can run for the same interval more than once (retries, backfills, manual re-triggers), Airflow pushes users toward writing tasks whose behavior is a pure function of the data interval.

**Jobbers** has no concept of a data interval. Cron DAGs (`CronDAGEntry`, `cron_expr`) fire a fresh DAG submission when the cron expression is due, with a `ConcurrencyPolicy` (e.g., `SKIP_IF_RUNNING`) controlling overlap — but there is no notion of "the run for last Tuesday" and no backfill command. Replaying historical work means manually resubmitting DAGs with whatever parameters represent the desired window.

**Verdict:** Airflow wins decisively for any workload that needs backfill or catchup — this is a capability gap, not a tradeoff. If "reprocess the last N days" is a routine operational need, Jobbers has no built-in answer; you would have to build the interval bookkeeping and replay logic yourself.

---

## 5. Async / Execution Model

**Airflow:** Most operators are synchronous and occupy a worker slot for their full duration, including blocking waits (e.g., a `Sensor` polling for a file). **Deferrable operators** (since 2.2) solve this for wait-heavy tasks: the operator suspends and hands control to a separate **triggerer** process (an asyncio event loop), freeing the worker slot until the trigger condition fires — but this requires the operator to explicitly support deferral, and most third-party operators do not.

**Jobbers:** asyncio is the only execution model — every task function is `async def`, and worker concurrency is an `asyncio.Semaphore`, so I/O-bound tasks never occupy a full worker "slot" the way a synchronous Airflow operator does. There is no separate mechanism needed for "wait without blocking" — it's the default behavior of any `await`.

**Verdict:** Jobbers wins for I/O-heavy, wait-heavy workloads by default. Airflow can match this via deferrable operators, but only for operators built to support it — plain `PythonOperator` code blocks a worker slot for its full runtime regardless.

---

## 6. Retry Policies

**Airflow:**

```python
@task(retries=5, retry_delay=timedelta(seconds=10), retry_exponential_backoff=True, max_retry_delay=timedelta(hours=1))
def my_task():
    ...  # raise — retry is automatic once retries/retry_delay are set
```

Retry count, delay, and exponential backoff (with optional jitter) are declared per task. `retry_exponential_backoff=True` doubles the delay each attempt up to `max_retry_delay`. SLAs (`sla=timedelta(...)`, deprecated in favor of Deadline Alerts in newer versions) can trigger callbacks when a task run overruns.

**Jobbers:**

```python
@register_task(
    name="my_task",
    version=1,
    max_retries=5,
    retry_delay=10,
    backoff_strategy=BackoffStrategy.EXPONENTIAL,
    max_retry_delay=3600,
)
async def my_task(**kwargs):
    ...  # just raise — retry logic is automatic
```

| Strategy | Computed Delay |
| --- | --- |
| `CONSTANT` | `retry_delay` |
| `LINEAR` | `retry_delay × attempt` |
| `EXPONENTIAL` | `retry_delay × 2^attempt` |
| `EXPONENTIAL_JITTER` | `uniform(0, retry_delay × 2^attempt)` |

**Verdict:** Comparable feature sets — both are declarative, automatic, and support exponential backoff capped at a maximum delay. Airflow additionally has SLA/Deadline Alert callbacks for "this took too long" independent of failure; Jobbers relies on heartbeat timeouts (`max_heartbeat_interval`) plus Cleaner-driven stall detection for a similar signal, but it's framed around liveness rather than a duration SLA.

---

## 7. Failure Handling and Dead Letter Queues

**Airflow:** No built-in DLQ. Failure handling is callback-driven: `on_failure_callback` on a task or DAG, `sla_miss_callback`, or provider-specific alerting (Slack, PagerDuty, email via `EmailOperator`). A failed task instance can be manually cleared and retried from the UI/CLI, and its full run history (logs, attempt count, duration) remains queryable indefinitely in the metadata DB, but there is no separate "moved to dead letter storage" concept or bulk-resubmit API for permanently-failed tasks.

**Jobbers:** First-class DLQ via `DeadLetterPolicy.SAVE`. A task that exhausts its retries is automatically written to the DLQ with its full history, queryable and bulk-resubmittable via API:

| Endpoint | Purpose |
| --- | --- |
| `GET /dead-letter-queue` | Search DLQ entries (filter by `queue`, `task_name`, `task_version`, `limit`) |
| `GET /dead-letter-queue/{task_id}/history` | Full chronological failure history for a dead-lettered task |
| `POST /dead-letter-queue/resubmit` | Bulk resubmit by `task_ids` or by filter, with optional retry count reset |
| `DELETE /dead-letter-queue` | Bulk remove by `task_ids` |

Jobbers also has a DAG-run-level analog to "clear and re-run" that sits alongside the DLQ: `GET /dags/{dag_run_id}/resume-check` and `POST /dags/{dag_run_id}/resume` retry every `FAILED`/`STALLED`/`CANCELLED`/`DROPPED` task in a run from its stored parameters, reconciling the run's aggregate status so it can reach `complete` instead of staying `partial_failure` forever. Unlike the DLQ, resume works regardless of `dead_letter_policy` and covers `CANCELLED` tasks. It is closer in spirit to Airflow's "clear this task instance, let downstream re-trigger" than to a data-interval backfill — it retries in place rather than reconstructing a historical run — and it inherits the same expiry as everything else in Jobbers: once the Cleaner's `--completed-task-age` has pruned the run's task blobs/index, or a stuck fan-in has outlived its `fan_in_ttl`, the run is no longer resumable and the precheck endpoint reports why.

**Verdict:** Jobbers wins on structured failure management for individual tasks — a queryable, bulk-resubmittable DLQ API is more operationally convenient than Airflow's "clear the task instance in the UI" model — and DAG resume closes most of the gap at the run level too, letting a whole stuck run continue from its failed branches with one call instead of clearing instances one by one. Airflow's advantage is that *every* task instance (not just terminally-failed ones) retains full historical logs and run metadata indefinitely by default, which is a broader audit trail than Jobbers keeps, and its clearing/backfill tooling is unbounded by a retention window the way Jobbers' resume is.

---

## 8. Traffic Management

**Airflow:** Concurrency is controlled at several layers, most adjustable at runtime without restarting the scheduler:

| Control | Scope | Live update? |
| --- | --- | --- |
| **Pools** | Named resource pools with a slot count; tasks assigned `pool=` compete for slots | Yes — create/resize via UI, CLI, or API |
| `max_active_tasks` | Per-DAG concurrent task instance cap | Yes (DAG param, takes effect next parse) |
| `max_active_runs` | Per-DAG concurrent DAG-run cap | Yes |
| `priority_weight` | Task priority within a pool | Yes |
| Celery `queue=` routing | Which Celery queue a task is sent to | Task-level, requires matching worker `-Q` flags |

Which Celery queues a given **worker** listens to is set at worker startup (`celery worker -Q queue1,queue2`) — like plain Celery, reassigning a worker to different queues requires a restart. Pools, however, are Airflow's own concept and are fully dynamic.

**Jobbers:** Four composable controls, all adjustable at runtime without worker restarts (see [docs/resource-management.md](resource-management.md)):

| Control | Scope | Live update? |
| --- | --- | --- |
| `WORKER_CONCURRENT_TASKS` | Per worker process | No (env var) |
| Queue `max_concurrent` | Per queue, per worker | Yes — `PUT /queues/{name}` |
| Queue rate limit | Per queue, at submission | Yes — `PUT /queues/{name}` |
| Role → queue mapping | Which queues a worker polls | Yes — `PUT /roles/{name}`, propagated via `refresh_tag` + pub/sub, no restart |

**Verdict:** Close. Airflow's pools are a genuine live-adjustable concurrency primitive comparable to Jobbers' queue caps, and `priority_weight` has no direct Jobbers equivalent. But reassigning *which queues a worker consumes* is static at worker startup in Airflow's CeleryExecutor (same limitation as plain Celery), whereas Jobbers propagates role→queue changes to running workers without a restart via the `refresh_tag` mechanism.

---

## 9. Observability

**Airflow:** The Web UI is extensive — Grid view (run/task-instance history), Graph view (DAG structure), Gantt chart (timing), and per-task-instance logs, all backed by the metadata DB. StatsD metrics have long been supported for scheduler/executor/task metrics (heartbeats, queue sizes, duration histograms); native OpenTelemetry trace export was added in later 2.x releases but requires explicit configuration, not on by default. There is no equivalent to Jobbers' single OTLP pipeline covering traces, metrics, and logs together out of the box.

**Jobbers:** OpenTelemetry traces, metrics, and logs are emitted via OTLP with no task-side instrumentation required — `tasks_processed`, `task_execution_time`, `tasks_dead_lettered`, `time_in_queue`, `refresh_lag_ms`, and others (see the metrics table in [CLAUDE.md](../CLAUDE.md)). A React admin UI provides live task/DLQ/queue inspection.

**Verdict:** Different strengths. Airflow's Web UI is unmatched for visualizing a specific DAG's structure and historical run pattern over time (Grid/Gantt views have no Jobbers equivalent). Jobbers' OTEL-first design gets full traces+metrics+logs into an existing observability stack (Grafana, OpenObserve, Datadog, etc.) with zero extra wiring, where Airflow needs StatsD/OTEL configuration to reach the same place.

---

## 10. Data Passing Between Tasks

**Airflow:** **XCom** ("cross-communication") is the built-in mechanism — a task's return value is automatically pushed to XCom (TaskFlow API) or explicitly via `ti.xcom_push()`, and downstream tasks pull it by argument (TaskFlow) or `ti.xcom_pull()`. XComs are stored in the metadata database by default, which makes them unsuitable for large payloads (rows of data, files) — the standard guidance is to pass references (S3 keys, table names) rather than data itself, or configure a custom XCom backend (e.g., S3/GCS-backed) for larger objects.

**Jobbers:** Task results are part of the task's stored state blob. Downstream tasks retrieve parent results either explicitly (`await get_current_task().parent_results()`) or automatically via per-field injection (`Annotated[T, FromParent("key")]`). Storage is whatever the configured `TASK_BACKEND` uses (Redis, Redis Stack, or SQL) — same size/backend considerations as Airflow's default XCom apply (don't store large payloads directly in task state).

**Verdict:** Conceptually equivalent (both push results to the store and pull them downstream), with Jobbers' `Annotated[T, FromParent("key")]` field-level injection being slightly more ergonomic than XCom's pull-by-key pattern. Airflow's pluggable custom XCom backends give it a cleaner story for large-payload passing than Jobbers currently has.

---

## 11. Task/DAG Introspection and Cancellation

**Airflow:** The Web UI and REST API expose full DAG-run and task-instance history: status, logs, duration, try count, per-instance XCom values. Tasks can be manually marked `success`/`failed`, cleared for re-run, or a whole DAG run can be manually triggered outside its schedule. Cancellation ("marking failed" while running) sends a `SIGTERM` to the task's process — coarse compared to cooperative in-code cancellation, though `on_kill()` hooks let operators clean up.

**Jobbers:**

| Endpoint | Purpose |
| --- | --- |
| `GET /task-status/{id}` | Full task detail: status, queue, results, retry count, heartbeat; `dag_diagram` for DAG roots |
| `GET /task-list?queue=...&status=...` | Paginated task search |
| `GET /active-tasks` | Tasks with a live heartbeat record |
| `GET /scheduled-tasks` | Tasks waiting in the scheduler |
| `POST /task/{id}/cancel` / `POST /tasks/cancel` | Single or bulk cooperative cancellation |
| `GET /dags` / `GET /dags/{id}` | List/inspect DAG runs, including aggregate status |
| `POST /dags/{id}/cancel` | Cancel every non-terminal task in a run with one call (single pub/sub broadcast for in-flight tasks, not one message per task) |
| `GET /dags/{id}/resume-check` / `POST /dags/{id}/resume` | Check, then retry, a stuck run's failed/stalled/cancelled/dropped tasks in place |

Cancellation is cooperative: a running task observes the cancellation signal at each `await` point, no process signal required.

**Verdict:** Comparable breadth of introspection. Cancellation semantics differ — Jobbers' cooperative model (checked at `await` points, no OS signal) is finer-grained than Airflow's SIGTERM-based task kill, which depends on the operator's `on_kill()` handling cleanup correctly. At the DAG-run level, Jobbers' single-call cancel/resume endpoints are more convenient than Airflow's per-task-instance UI actions for stopping or restarting an entire run at once, though "manually triggering a DAG run outside its schedule" and clearing individual task instances remain Airflow-only for reconstructing a specific historical data-interval run.

---

## 12. Risk of Data Loss / Idempotency Model

**Airflow:** Task instance state is persisted to the metadata DB at each transition. A worker crash mid-task typically leaves the task instance `running` until the scheduler's heartbeat-timeout detection marks it failed (subject to `scheduler_zombie_task_threshold`) — conceptually similar to Jobbers' heartbeat/Cleaner stall detection, with a comparable detection-window tradeoff. Because retries and backfills can re-run the same logical interval, Airflow's whole design assumes tasks are (or should be) idempotent with respect to their data interval.

**Jobbers:** Task state transitions to `STARTED` immediately on execution start. A crashed worker (no SIGTERM) is detected by the Cleaner via missing heartbeat and marked `STALLED`; detection window is `max_heartbeat_interval` + Cleaner poll interval. For adapters implementing the Atomic sub-protocols, cross-store writes (task state + scheduler + DLQ) happen in a single MULTI/EXEC or SQL transaction rather than sequential saga-style writes.

**Verdict:** Comparable "zombie/stall detection" mechanisms and detection-window tradeoffs. Airflow's idempotency expectation is more deeply baked into the platform (retries and backfills routinely re-execute the same interval); Jobbers doesn't structurally encourage or require idempotency the way Airflow's data-interval model does, though it's still good practice for any task with `max_retries > 0`.

---

## 13. Broker and Backend Flexibility

**Airflow:** The **executor** determines how task instances actually run:

| Executor | Model | Notes |
| --- | --- | --- |
| `SequentialExecutor` | One task at a time, same process | Dev/testing only |
| `LocalExecutor` | Subprocesses on the scheduler host | Single-machine parallelism |
| `CeleryExecutor` | Celery workers via a broker (Redis or RabbitMQ) | Horizontal scaling, same broker options as Celery itself |
| `KubernetesExecutor` | One Kubernetes pod per task instance | Per-task isolation and resource limits, no persistent worker pool |
| `CeleryKubernetesExecutor` | Hybrid: routes tasks to Celery or K8s per-task | Mix steady-state and bursty workloads |

Metadata database: Postgres or MySQL (SQLite for local dev only). Custom XCom backends can offload large payloads to S3/GCS/etc.

**Jobbers:**

- **Broker:** Redis (queues are Redis sorted sets/Lua scripts)
- **Task state** (`TASK_BACKEND`): `redis`, `redis_json` (default), or `sql`
- **DLQ** (`DLQ_BACKEND`): `redis` (default), `redis_json`, or `sql`
- **Scheduler** (`TASK_SCHEDULER_BACKEND`): `redis` (default) or `sql`
- **Cron DAG scheduler** (`CRON_DAG_SCHEDULER_BACKEND`): `redis` (default), `sql`, or read-only `static`
- **Routing** (`ROUTING_BACKEND`): `sql` (default), `redis`, `redis_json`, or `static`

Every concern can independently run on Redis, Redis Stack, or SQL, including all-SQL/zero-Redis or all-Redis/zero-SQL deployments.

**Verdict:** Airflow wins for deployment-model flexibility — `KubernetesExecutor` gives genuine per-task pod isolation (different resource limits, even different container images per task) that Jobbers has no equivalent for; Jobbers' workers are a fixed pool of long-lived asyncio processes. Jobbers wins for storage-layer flexibility on the state/DLQ/scheduling side (independently swappable per concern) versus Airflow's single metadata-DB-for-everything model.

---

## 14. Security

**Airflow:** Built-in RBAC via Flask-AppBuilder, with pluggable auth backends (LDAP, OAuth, database, Kerberos) and per-DAG/per-resource permissions out of the box. This is a meaningfully more mature built-in security story than most task queues ship with.

**Jobbers:** No built-in API authentication; the FastAPI server accepts unauthenticated requests by default. All security must be enforced at the deployment layer — reverse proxy with auth, network policies, Redis ACLs.

**Verdict:** Airflow wins clearly. Built-in RBAC and multiple auth backend options are a real capability Jobbers does not attempt to provide.

---

## 15. Ecosystem and Providers

**Airflow:** Close to 100 official [provider packages](https://airflow.apache.org/docs/#providers-packages-docs-apache-airflow-providers-index-html) covering AWS, GCP, Azure, Snowflake, Databricks, dbt, Spark, and dozens more — pre-built operators, sensors, and hooks mean many integrations require zero custom code. This is arguably Airflow's single biggest practical advantage for data-engineering teams.

**Jobbers:** No provider ecosystem — every integration is plain Python inside a task function using whatever SDK/client the target system requires. There's no equivalent of "install a package, get a ready-made operator."

**Verdict:** Airflow wins decisively. This is less a "feature" comparison than a reflection of Airflow's much larger install base and maturity as the default orchestration tool for data engineering.

---

## Summary

| Feature | Airflow | Jobbers | Notes |
| --- | --- | --- | --- |
| **Getting started** | Single dev binary; heavier in prod | Moderate | Airflow prod needs DB + scheduler + webserver + executor infra; Jobbers needs Redis + 4 processes |
| **DAG authoring** | Python (operators / TaskFlow) | `DAGNode` graph or Mermaid text | Airflow more expressive (branching, trigger rules); Jobbers more portable/inspectable |
| **Scheduling model** | Data-interval-driven, with catchup + backfill | Cron DAGs, no data-interval concept | Airflow wins decisively for historical replay workloads |
| **Execution model** | Sync by default; deferrable operators for async waits | Native asyncio throughout | Jobbers wins by default for I/O-heavy work |
| **Retry policies** | Declarative, exponential backoff, SLA callbacks | Declarative, exponential backoff + jitter | Comparable |
| **Failure handling** | Callbacks + manual clear/retry, full history in metadata DB | First-class queryable DLQ + bulk resubmit, plus whole-run resume | Jobbers wins on structured DLQ tooling and single-call run recovery; Airflow's history/backfill is unbounded by retention |
| **Traffic management** | Pools (live), DAG concurrency caps (live), Celery queue routing (static per worker) | Live roles/queues, no restart, refresh-tag propagation | Close; Jobbers' queue reassignment is more dynamic |
| **Observability** | Rich Web UI (Grid/Graph/Gantt); StatsD/OTEL needs config | OTEL out of the box; simpler admin UI | Different strengths |
| **Data passing** | XCom (metadata DB, pluggable backend for large payloads) | Task state blob + `FromParent` injection | Conceptually equivalent |
| **Introspection/cancellation** | Full history in UI/API; SIGTERM-based kill | Full lifecycle API; cooperative cancellation; single-call DAG-run cancel + resume | Jobbers' cancellation is finer-grained and run-level is one call; Airflow's per-instance history is deeper and unbounded |
| **Crash recovery** | Zombie/heartbeat detection, idempotency assumed | Heartbeat + Cleaner stall detection | Comparable mechanism and tradeoffs |
| **Executor/backend flexibility** | Local, Celery, Kubernetes (per-task pods), hybrid | Redis broker; state/DLQ/scheduler/routing independently Redis/SQL | Airflow wins on per-task isolation; Jobbers wins on storage-layer flexibility |
| **Security** | Built-in RBAC + pluggable auth backends | Proxy-layer only | Airflow wins clearly |
| **Ecosystem** | ~100 provider packages (AWS, GCP, dbt, Spark, ...) | None — plain Python per integration | Airflow wins decisively |

### When to choose Airflow

- Your workload is fundamentally batch ETL/ELT: recurring pipelines tied to a data interval, where **backfill and catchup** for historical windows are routine operational needs.
- You want pre-built, maintained integrations (provider packages) for cloud services, data warehouses, and tools like dbt/Spark rather than hand-rolling API clients.
- You need rich branching/trigger-rule logic (`all_success`, `one_failed`, `none_failed`, conditional branches) expressed declaratively.
- You want built-in RBAC and pluggable enterprise auth without adding a reverse proxy.
- Per-task resource isolation matters (`KubernetesExecutor` — different CPU/memory/image per task).

### When to choose Jobbers

- Your workload is task-queue-shaped: continuously submitted, event- or request-triggered work rather than scheduled batch windows.
- Low-latency dispatch matters — Jobbers has no DAG-file-parsing scheduler loop to add lag, and asyncio tasks never block a worker slot on I/O by default.
- You want live traffic control (reroute queues, throttle rate, reassign which queues a worker polls) without restarting workers.
- You want a structured, queryable, bulk-resubmittable dead letter queue rather than manually clearing failed task instances — and when a whole run gets stuck, you want to cancel or resume it with one call instead of clearing instances one at a time.
- You want OpenTelemetry traces/metrics/logs out of the box, and you don't want to run a metadata-DB-backed scheduler + webserver + executor stack just to dispatch tasks.
- You want DAGs defined in a portable, renderable format (Mermaid) rather than Python modules the scheduler must import and re-parse.
