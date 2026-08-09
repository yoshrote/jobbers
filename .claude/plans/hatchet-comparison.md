# Jobbers vs Hatchet: Feature Comparison

This document compares Jobbers and [Hatchet](https://hatchet.run/) across the dimensions most relevant to choosing a task/workflow execution system. Hatchet sits between Jobbers and Temporal on the durability spectrum: it markets itself as a task queue, a DAG orchestrator, and a durable execution engine all at once, built on Postgres rather than a bespoke event-history store. It is the newest entrant of the systems in this comparison set — it's worth treating this doc as more likely to drift out of date than the others, since Hatchet's SDK and feature surface have moved quickly (the Python SDK went through a significant v1 redesign, for instance).

---

## 1. Philosophy and Design Goals

**Hatchet** aims to be "one platform" for background tasks, AI-agent orchestration, and durable workflows, so a team doesn't need Celery for jobs, Airflow for pipelines, and Temporal for durable orchestration as three separate systems. It offers three usage modes on the same engine: a plain task queue, DAG-based workflows (parent/child steps with typed inputs/outputs), and durable tasks (Temporal-style replay-based durability for a specific task, via `@hatchet.durable_task`). PostgreSQL is the source of truth for workflow definitions and execution state; RabbitMQ is optional and only recommended once you outgrow Postgres-only throughput.

**Jobbers** is also Redis/SQL-based and also supports DAG composition, but it doesn't attempt Hatchet's third mode — there is no replay-based durable-task primitive in Jobbers, only explicit stored task state (the same distinction drawn in [the Temporal comparison](temporal-comparison.md)). Jobbers' DAGs are Mermaid-serialized graphs rather than typed parent/child Python classes, and its scope stays fixed at "task queue with DAG composition," not an umbrella platform for agent orchestration.

**Verdict:** Hatchet is broader in ambition — it wants to replace three different tools with one. That breadth is valuable if your organization genuinely needs all three modes (simple jobs, DAGs, and durable-replay workflows) and would rather standardize on one engine than run separate systems. If you only need a task queue with DAG composition and don't need Temporal-style indefinite-suspension durability, Jobbers covers that narrower scope with less to operate (no Postgres-as-execution-log requirement, no RabbitMQ scaling story to plan for).

---

## 2. Getting Started

**Hatchet:**

```bash
# self-hosted: docker compose up (Postgres + engine + optional RabbitMQ),
# or point HATCHET_CLIENT_TOKEN at Hatchet Cloud
pip install hatchet-sdk
```

```python
from hatchet_sdk import Hatchet, Context, EmptyModel

hatchet = Hatchet()

@hatchet.task()
def add(input: EmptyModel, ctx: Context) -> dict:
    return {"result": 2 + 2}

worker = hatchet.worker("my-worker", workflows=[add])
worker.start()
```

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

**Verdict:** Comparable ease for a first task. Hatchet's self-hosted path needs Postgres (plus optionally RabbitMQ at higher throughput) and a control-plane engine process talking gRPC to workers; Jobbers needs Redis and four lightweight Python processes. Hatchet Cloud removes the self-hosting question entirely — Jobbers has no managed offering, so you always run your own infrastructure either way.

---

## 3. Execution and Durability Model

**Hatchet** offers two execution styles that matter here. Plain `@hatchet.task` functions behave like an ordinary job: run once, retry per policy, no replay semantics. `@hatchet.durable_task` functions get Temporal-style durability — the engine persists execution state so the task can suspend (via `ctx.aio_sleep_for()` or waiting on a durable event) for arbitrarily long periods and resume exactly where it left off, backed by Postgres rather than a bespoke history store. This is a genuinely different capability from a plain retried task: it's mid-function resumption, not just re-running from the start.

**Jobbers** has no replay-based durability at all — every task either runs to completion or is retried from its beginning. Where Jobbers narrows this gap is DAG-run resume: `POST /dags/{dag_run_id}/resume` retries a stuck DAG run's `FAILED`/`STALLED`/`CANCELLED`/`DROPPED` tasks from their stored parameters, letting the run continue past the point it got stuck. This is task-level retry-in-place, not function-level replay — a resumed task starts over, not from wherever inside its body it crashed — and it's bounded by the Cleaner's retention window and fan-in TTLs rather than being available indefinitely.

**Verdict:** Hatchet's `durable_task` gives it a real answer to "suspend for hours and resume with local state intact" that Jobbers structurally cannot match with explicit task state — the same gap described against Temporal, since Hatchet's durable tasks use the same replay idea. For plain (non-durable) tasks and DAG workflows, though, the two systems are closer: both retry from stored state rather than replaying execution, and both now let you recover a stuck multi-step run (Hatchet via its retention-backed execution history and manual replay from the dashboard/API, Jobbers via `POST /dags/{id}/resume`) rather than starting over.

---

## 4. Retry Policies

**Hatchet:**

```python
@hatchet.task(retries=5, backoff_factor=2.0, backoff_max_seconds=300)
def my_task(input: EmptyModel, ctx: Context) -> dict:
    ...  # raise — retry is automatic once retries is set
```

Retries are declarative per task, with exponential backoff parameters (`backoff_factor`, `backoff_max_seconds`). `on_failure` tasks can be attached to a workflow to run when a step's retries are exhausted, similar in spirit to Jobbers' `on_error` DAG callbacks and Airflow's failure callbacks.

**Jobbers:**

```python
@register_task(
    name="my_task", version=1, max_retries=5, retry_delay=10,
    backoff_strategy=BackoffStrategy.EXPONENTIAL, max_retry_delay=3600,
)
async def my_task(**kwargs):
    ...
```

Four backoff strategies including `EXPONENTIAL_JITTER`.

**Verdict:** Comparable — both are declarative with exponential backoff and a failure-callback mechanism. Hatchet's `on_failure` is task/workflow-scoped in its own DSL; Jobbers' `on_error` is attached per DAG edge (`then()`/`merge()`), which is more granular if different branches of the same DAG need different failure handling.

---

## 5. Workflow/DAG Composition

**Hatchet:** DAGs are Python classes/functions with declared parent/child relationships and typed inputs/outputs flowing between steps — automatic parallelism between independent steps, branching, and "or groups" for expressing complex wait conditions, all rendered as a graph in the Hatchet dashboard. Child workflows can be spawned dynamically at runtime (Hatchet's equivalent of Jobbers' `DynamicFanOut`).

**Jobbers:** `DAGNode` graphs (chain/fan-out/fan-in), `DynamicFanOut` for runtime-determined fan-out, `on_error` callbacks — defined either as Python objects or directly as Mermaid text submitted via `POST /submit-dag`. The Mermaid format is the standout difference: a Jobbers DAG is plain text that renders natively in GitHub, VS Code, and any Mermaid-aware tool, and round-trips through the API (`dag_diagram` field) — see [docs/dag-composition.md](dag-composition.md).

**Verdict:** Close. Hatchet's typed input/output flow between steps (backed by Pydantic models) is more strongly typed than Jobbers' `parent_results()`/`FromParent` injection, and its dashboard graph view is a richer visual tool than a static Mermaid render. Jobbers' Mermaid-as-the-wire-format approach is more portable — a DAG definition that's also valid documentation, shareable outside any Hatchet-specific UI.

---

## 6. Concurrency and Rate Limiting

**Hatchet:** Multiple concurrency strategies settable per workflow — `GROUP_ROUND_ROBIN` (distribute across a key), `CANCEL_IN_PROGRESS` (cancel the currently-running instance for a key when a new one arrives), among others — plus static rate limits declared via the admin client and consumed per step. These are configured in code/API and take effect without redeploying workers, since the engine (not the worker process) enforces them.

**Jobbers:** Queue `max_concurrent` and queue rate limits, both live-adjustable via `PUT /queues/{name}` with no worker restart, propagated through the `refresh_tag` + pub/sub mechanism (see [docs/resource-management.md](resource-management.md)).

**Verdict:** Comparable live-adjustability; different granularity. Hatchet's per-key concurrency strategies (round-robin across a dynamic key, cancel-in-progress) are more expressive than Jobbers' flat per-queue cap — useful if you need "one in-flight job per customer ID," which Jobbers has no direct primitive for beyond routing to per-tenant queues yourself.

---

## 7. Scheduling and Periodic Tasks

**Hatchet:** Native cron scheduling and one-off scheduled runs, managed via SDK/API/dashboard, with the same durable-execution guarantees as any other workflow trigger.

**Jobbers:** Cron-scheduled DAGs (`CronDAGEntry`, standard 5-field cron expressions) with `ConcurrencyPolicy` and full REST CRUD — see the cron section of [the Celery comparison](celery-comparison.md#11-scheduling-and-periodic-tasks) for the mechanism in depth.

**Verdict:** Roughly comparable — both support cron-driven workflow/DAG runs with runtime CRUD, no code deploy required for either.

---

## 8. Observability

**Hatchet:** Execution history is retained (up to a configured retention period) and browsable in the Hatchet dashboard — every task, retry, and state transition. OpenTelemetry tracing and Prometheus metrics are both supported natively, not bolted on. This is a materially more built-in observability story than Celery, Dramatiq, or arq have, and closer to what Jobbers offers.

**Jobbers:** OpenTelemetry traces, metrics, and logs via OTLP with zero task-side instrumentation, plus a React admin UI for task/DLQ/queue inspection (see the metrics table in [CLAUDE.md](../CLAUDE.md)).

**Verdict:** Close to a draw. Both ship OTEL and a dashboard out of the box — this is one of the few dimensions where Hatchet doesn't have a clear edge over Jobbers the way it does over Celery/Airflow/Dramatiq/arq. Hatchet's dashboard is purpose-built and actively developed by a company selling it as a product; Jobbers' React UI is functional but narrower in scope (task/queue/DLQ management, not a full execution-history browser).

---

## 9. Task/Workflow Introspection and Cancellation

**Hatchet:** Every task's full history is queryable via API/dashboard (inputs, outputs, retries, timing). Cancellation is a first-class, cooperative operation (`ctx.exit_flag`/`ctx.cancelled` checked in task code) and can also be triggered automatically by the `CANCEL_IN_PROGRESS` concurrency strategy — the platform can cancel a running task on your behalf when a newer one for the same key arrives, not just on explicit request.

**Jobbers:** `GET /task-status/{id}`, `GET /task-list?status=...`, `GET /active-tasks`, cooperative `POST /task/{id}/cancel` / `POST /tasks/cancel`, and at the DAG level `POST /dags/{id}/cancel` (single call, single broadcast for in-flight tasks) plus `GET /dags/{id}/resume-check` / `POST /dags/{id}/resume`.

**Verdict:** Comparable cancellation semantics (both cooperative). Hatchet's automatic cancellation via `CANCEL_IN_PROGRESS` has no direct Jobbers equivalent — Jobbers would require you to detect and cancel the superseded task yourself. Jobbers' DAG-run resume has no clean Hatchet equivalent either, beyond Hatchet's general "replay from history" tooling for durable tasks specifically.

---

## 10. Operational Complexity

**Hatchet:** Self-hosted requires Postgres (the durable store for everything) plus the Hatchet engine (gRPC API + admin); RabbitMQ is optional and only needed once you outgrow Postgres-only throughput (roughly "hundreds of tasks/sec" per engine instance before you'd consider adding it). `hatchet-lite` bundles engine + RabbitMQ + migrations into one Docker image for low-volume/local use. Hatchet Cloud removes all of this at the cost of a hosted dependency.

**Jobbers:** Four lightweight processes (manager, worker, cleaner, scheduler) plus Redis (or SQL, depending on backend selection). No managed-cloud option.

**Verdict:** Comparable for small deployments (Postgres-only Hatchet vs. Redis-only Jobbers are both single-datastore setups); Hatchet's path to scale (adding RabbitMQ, tuning the engine) is a more defined, documented story than Jobbers currently has for outgrowing a single Redis instance, but Hatchet Cloud is the more likely choice for teams that don't want to own either scaling path themselves — Jobbers has no equivalent.

---

## 11. Security

**Hatchet:** Token-based auth (`HATCHET_CLIENT_TOKEN`) between workers and the engine is standard even in self-hosted setups, and Hatchet Cloud adds team/tenant-level access control. This is a real built-in auth story, not something you bolt on at the proxy layer.

**Jobbers:** No built-in API authentication; the FastAPI server accepts unauthenticated requests by default. All security must be enforced at the deployment layer (reverse proxy auth, network policies, Redis ACLs).

**Verdict:** Hatchet wins. Worker-to-engine auth being a default rather than an add-on is a meaningful operational advantage Jobbers doesn't have.

---

## Summary

| Feature | Hatchet | Jobbers | Notes |
| --- | --- | --- | --- |
| **Getting started** | Postgres + engine (or Cloud) | Redis + 4 processes | Comparable; Hatchet Cloud removes self-hosting entirely |
| **Execution model** | Plain tasks + replay-based durable tasks | Explicit stored task state only | Hatchet's `durable_task` can suspend/resume mid-function; Jobbers cannot |
| **Stuck-run recovery** | Durable-task replay (indefinite); manual replay from history for plain tasks | `POST /dags/{id}/resume`, retention-bounded | Hatchet's durable mode is stronger; DAG-level recovery is roughly comparable |
| **Retry policies** | Declarative, exponential backoff, `on_failure` hooks | Declarative, 4 backoff strategies, `on_error` per edge | Comparable |
| **Workflow/DAG composition** | Typed Python DAGs, dashboard graph view | `DAGNode`/Mermaid, portable text format | Hatchet: stronger typing. Jobbers: more portable format |
| **Concurrency control** | Per-key strategies (round-robin, cancel-in-progress), rate limits | Per-queue caps + rate limits, live | Hatchet more granular (per-key); both live-adjustable |
| **Cron scheduling** | Native, dashboard/API-managed | Cron DAGs + REST CRUD | Comparable |
| **Observability** | OTEL + Prometheus + dashboard, built in | OTEL (traces/metrics/logs) + React UI, built in | Closest dimension between the two systems |
| **Introspection/cancellation** | Full history; cooperative + automatic (`CANCEL_IN_PROGRESS`) cancel | Full lifecycle API; cooperative cancel, single/bulk/DAG-level | Comparable; different automation vs. explicit-call tradeoffs |
| **Operational complexity** | Postgres (+ optional RabbitMQ at scale), or Cloud | Redis (or SQL) + 4 processes, no managed option | Comparable footprint; Hatchet has a cloud escape hatch |
| **Security** | Token auth built in, Cloud adds RBAC | Proxy-layer only | Hatchet wins |

### When to choose Hatchet

- You want one platform covering plain background jobs, DAG workflows, *and* Temporal-style durable/suspendable tasks, rather than reaching for a separate durable-execution system when one workflow needs it.
- You want built-in worker-to-engine authentication without adding a reverse proxy.
- Per-key concurrency control (round-robin across a tenant key, auto-cancel superseded runs) matters for your workload.
- You'd rather run on Postgres (likely already in your stack) than stand up Redis, or you want the option of Hatchet Cloud to avoid self-hosting entirely.

### When to choose Jobbers

- You want DAG workflows defined in a portable, renderable text format (Mermaid) rather than typed Python workflow classes tied to a specific SDK.
- You don't need replay-based durable suspension — task-level retries and DAG-level resume cover your recovery needs, and you'd rather not take on the conceptual overhead (or, on Hatchet, the `durable_task`/plain-`task` distinction) that comes with a replay model.
- You're already invested in Redis and want task state, DLQ, scheduling, and routing to be independently configurable across Redis, Redis Stack, and SQL rather than centralized on Postgres.
- You want a narrower, more predictable scope — a task queue with DAG composition — over an actively-evolving multi-mode platform where the SDK surface (as with Hatchet's v0→v1 Python migration) can shift under you.
