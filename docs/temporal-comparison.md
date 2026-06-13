# Jobbers vs Temporal: Feature Comparison

This document compares Jobbers and [Temporal](https://temporal.io/) across the dimensions most relevant to choosing a task/workflow execution system. Temporal sits one layer up the abstraction stack from Jobbers — it is a durable execution engine, not a task queue — so the comparison is as much about scope as it is about features.

---

## 1. Philosophy and Design Goals

**Temporal** provides *durable execution*: you write workflow code as ordinary (deterministic) functions in a general-purpose language, and the platform persists the full event history of that execution so it can be replayed and resumed from any point — across worker crashes, deploys, and even months-long pauses. Activities (the actual side-effecting work — HTTP calls, DB writes, etc.) are retried independently of the workflow that orchestrates them. Temporal's scope covers single tasks, multi-step workflows, human-in-the-loop signals, and long-running sagas.

**Jobbers** is a task queue with first-class tracking: it stores task state explicitly (status, retries, heartbeats) rather than reconstructing it from a replayed event log, and it composes tasks into DAGs via an explicit graph (chain/fan-out/fan-in nodes) rather than arbitrary orchestration code. It assumes Redis (or optionally SQL) and asyncio, and trades Temporal's replay-based durability model for a much simpler operational footprint.

**Verdict:** These solve overlapping but distinct problems. Temporal is built for workflows that must survive arbitrarily long suspensions and complex human/system interaction (signals, queries, updates). Jobbers is built for high-throughput task dispatch with strong observability and DLQ handling. Many systems use both: Jobbers/Celery-style queues for bulk async work, Temporal for the small number of workflows that need durable, long-lived orchestration.

---

## 2. Getting Started

**Temporal:**

```bash
temporal server start-dev   # local dev server: frontend, history, matching, worker services + SQLite
```

```python
from temporalio import workflow, activity

@activity.defn
async def add(x: int, y: int) -> int:
    return x + y

@workflow.defn
class AddWorkflow:
    @workflow.run
    async def run(self, x: int, y: int) -> int:
        return await workflow.execute_activity(
            add, args=[x, y], start_to_close_timeout=timedelta(seconds=10)
        )
```

A `Worker` process polls a task queue for both workflow and activity tasks. Production deployments require a persistence store (Cassandra, MySQL, or Postgres) and, for visibility queries beyond basic filters, Elasticsearch — or a subscription to Temporal Cloud, which manages all of this for you.

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

**Verdict:** Temporal's dev server is a single binary and quick to try locally, but production self-hosting (persistence + visibility store + multiple server services) is significantly heavier than Jobbers' four-process, Redis-only footprint. Temporal Cloud removes that burden at the cost of being a hosted dependency. Jobbers has no managed-cloud option; you always run and own the infrastructure.

---

## 3. Execution and Durability Model

**Temporal:** Workflow code must be *deterministic* — no direct `random`, wall-clock time, network I/O, or unguarded mutable global state inside workflow functions; the SDK provides deterministic substitutes (`workflow.now()`, `workflow.random()`, etc.). This constraint exists because the workflow's history of events (activity completions, signals, timers) is replayed from the start on every worker pickup to reconstruct in-memory state. The payoff is durability: a workflow can run for months, survive any number of worker restarts, and resume exactly where it left off — including local variables and control flow position — with no explicit state-saving code.

**Jobbers:** Each task's state (status, retry count, results, heartbeat) is stored explicitly as a blob via `TaskStateProtocol`, not reconstructed via replay. There is no determinism constraint on task code — a task function can call any I/O, use real `random`/`time`, anything. The tradeoff is that only the *task's own state* survives a crash; a task does not get to "resume mid-function" the way a Temporal workflow does. DAG-level continuation (which downstream nodes have run) is tracked via the fan-in/fan-out bookkeeping in `TaskStateProtocol`, not via replaying the orchestrating code.

**Verdict:** Temporal wins decisively for workflows that need to suspend indefinitely (e.g., "wait up to 30 days for a signal") or resume mid-function after a crash with full local state intact. Jobbers wins for simplicity of task code — no determinism discipline, no replay model to reason about, and ordinary debugging tools work unmodified.

---

## 4. Retry Policies

**Temporal:** Both workflows and activities have independently configurable `RetryPolicy` (initial interval, backoff coefficient, max interval, max attempts, non-retryable error types). Activities also support heartbeating for long-running work and cancellation propagation from the workflow.

```python
await workflow.execute_activity(
    my_activity,
    retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0, maximum_attempts=5),
    heartbeat_timeout=timedelta(seconds=30),
)
```

**Jobbers:** Retry policy is declared once per task type via `@register_task` (`max_retries`, `retry_delay`, `backoff_strategy`), and is automatic — no `self.retry()` call needed. Heartbeating works the same way conceptually (`task.heartbeat()` inside long-running tasks, `max_heartbeat_interval` to detect stalls).

**Verdict:** Functionally comparable. Temporal's retry policy is set per-activity-call-site (more flexible within one workflow); Jobbers' is set per-task-type at registration (simpler, but less granular if the same task needs different policies in different call contexts).

---

## 5. Workflow/DAG Composition

**Temporal:** Orchestration is just code — `await` activities in sequence, branch with `if`, fan out with `asyncio.gather`-equivalents, fan in by awaiting multiple futures, loop indefinitely. Child workflows and `continue-as-new` handle long-running or unbounded-iteration cases (history size is otherwise bounded). There is no separate "DAG specification" — the workflow function *is* the DAG, which makes arbitrarily complex control flow (conditionals, dynamic activity counts, loops) natural to express.

**Jobbers:** DAGs are explicit graphs (`DAGNode`, fan-out/fan-in, `DynamicFanOut` for runtime-determined fan-out, `on_error` callbacks), submitted as a structure — defined and rendered as a Mermaid flowchart — rather than written as imperative control flow.

**Verdict:** Temporal wins for workflows with complex, code-like control flow (loops, conditionals interleaved with external waits). Jobbers' explicit DAG spec is easier to visualize, diff, and validate statically (e.g., the Mermaid diagram doubles as documentation and as the API payload) but is less expressive for deeply dynamic orchestration logic.

---

## 6. Human-in-the-Loop and Long-Running Interaction

**Temporal:** First-class **signals** (async messages delivered to a running workflow), **queries** (synchronous read-only inspection of workflow state), and **updates** (synchronous read-write interaction with validation). These make Temporal a natural fit for workflows like "wait for approval," "long-running saga with manual intervention points," or "stateful entity" patterns.

**Jobbers:** No equivalent. A task is in one state at a time (per the task lifecycle state machine) and external interaction is limited to cancellation (`POST /cancel-task`) and scheduling. There is no way to "send a message" to a running task or synchronously query mid-execution state beyond what's in the task's stored blob/heartbeat.

**Verdict:** Temporal wins clearly. This is a capability gap, not a tradeoff — if your use case needs mid-execution human interaction or external signaling, Jobbers has no answer.

---

## 7. Scheduling / Cron

**Temporal:** The Schedules API (`temporal schedule create`, or `client.create_schedule()`) supports cron expressions and calendar-based specs, with overlap policies (`SKIP`, `BUFFER_ONE`, `ALLOW_ALL`, etc.) analogous to Jobbers' `ConcurrencyPolicy`. Schedules are managed via CLI, SDK, or Web UI.

**Jobbers:** Cron-scheduled DAGs (`CronDAGEntry`) with `cron_expr`, `ConcurrencyPolicy.SKIP_IF_RUNNING`, and a full REST CRUD API (`POST /cron-dags`, etc.), where the DAG is specified as the same Mermaid diagram used elsewhere.

**Verdict:** Roughly comparable in capability (cron expressions, overlap/concurrency control, runtime CRUD). Temporal's schedules trigger a full workflow execution (with all of Temporal's durability); Jobbers' cron DAGs trigger the lighter DAG-node graph. Neither supports true sub-minute intervals natively via cron syntax.

---

## 8. Observability

**Temporal:** Workflow execution history is itself a complete audit trail (every activity start/complete/fail, every signal/timer) visible in the Web UI or via `tctl`/CLI without any extra instrumentation. OpenTelemetry tracing is supported via SDK interceptors but requires explicit wiring, not enabled by default. Temporal Cloud adds metrics dashboards out of the box; self-hosted requires scraping the server's own Prometheus metrics endpoint separately from workflow-level tracing.

**Jobbers:** OpenTelemetry traces, metrics, and logs are emitted via OTLP out of the box with no task-side instrumentation required (`hit_counter`, `time_in_queue`, `tasks_selected`, `queue_config_refreshes`, `refresh_lag_ms`, etc. — see the metrics table in the developer guide). A React admin UI gives live task/DLQ/queue inspection.

**Verdict:** Different strengths. Temporal's per-workflow event history is unmatched for "what exactly happened to this one execution, in order" debugging. Jobbers' OTEL-first design is unmatched for "what's the aggregate health of my system right now" without any extra setup.

---

## 9. Task/Workflow Introspection and Cancellation

**Temporal:** `WorkflowHandle.cancel()` requests cooperative cancellation (the workflow observes a `CancelledError` at the next yield point, same cooperative model as Jobbers); `terminate()` forcibly kills it without running cleanup. Full execution history is queryable via the Web UI or API indefinitely (subject to retention policy).

**Jobbers:** `GET /task-status/{id}`, `GET /tasks?status=...`, `GET /active-tasks`, `POST /cancel-task` — cooperative cancellation via the same yield-point model.

**Verdict:** Comparable cancellation semantics. Temporal's retained history is richer (full causal chain of events) but Jobbers' task-status API is simpler to query for the common case ("is this task done, and what was the result").

---

## 10. Operational Complexity

**Temporal:** Self-hosted requires the multi-service Temporal server (frontend, history, matching, worker services — or the all-in-one binary for dev/small deployments), a persistence database (Cassandra, MySQL, or Postgres), and optionally Elasticsearch for advanced visibility filtering. Temporal Cloud eliminates all of this but is a paid hosted dependency with its own namespace/auth model.

**Jobbers:** Four lightweight processes (manager, worker, cleaner, scheduler), Redis (or SQL for some adapters), no separate visibility store. No managed-cloud option exists.

**Verdict:** Jobbers is materially simpler to self-host. Temporal's complexity buys durability guarantees Jobbers does not attempt to provide; whether that trade is worth it depends entirely on whether your workloads need multi-day/multi-month durable suspension.

---

## 11. Security

**Temporal:** Supports mTLS between workers and the server, namespace-level isolation for multi-tenancy, and Temporal Cloud adds API-key/SAML-based auth and per-namespace RBAC. Self-hosted auth (authorization plugins) requires more setup.

**Jobbers:** No built-in API authentication; the FastAPI server accepts unauthenticated requests by default. All security must be enforced at the deployment layer (reverse proxy auth, network policies, Redis ACLs).

**Verdict:** Temporal wins, particularly via Temporal Cloud's managed auth. Both require deliberate effort to harden a self-hosted deployment.

---

## Summary

| Feature | Temporal | Jobbers | Notes |
| --- | --- | --- | --- |
| **Getting started** | Single dev binary; heavier in prod | Moderate | Temporal prod needs DB + optional ES; Jobbers needs Redis only |
| **Execution model** | Durable replay (deterministic workflow code) | Explicit stored task state | Temporal resumes mid-function across crashes; Jobbers resumes per-task only |
| **Orchestration style** | Imperative code (loops, conditionals, `await`) | Explicit DAG graph (Mermaid) | Temporal more expressive; Jobbers more inspectable/diffable |
| **Retry policies** | Per-activity-call-site `RetryPolicy` | Per-task-type at registration | Comparable; different granularity |
| **Human-in-the-loop** | Signals, queries, updates | None (cancel only) | Temporal-only capability |
| **Cron scheduling** | Schedules API + overlap policies | Cron DAGs + REST CRUD | Comparable; neither does sub-minute natively |
| **Observability** | Full event history; OTEL via interceptors | OTEL + metrics out of the box; React UI | Different strengths (causal history vs. aggregate health) |
| **Cancellation** | Cooperative (`cancel`) or forced (`terminate`) | Cooperative only | Similar cooperative model |
| **Self-hosting complexity** | High (multi-service + DB + optional ES) | Low (4 processes + Redis) | Temporal Cloud removes this burden at a cost |
| **Security** | mTLS, namespaces, Cloud auth/RBAC | Proxy-layer only | Temporal has a real built-in story; Jobbers does not |
| **Language support** | Go, Java, Python, TypeScript, .NET, PHP, Ruby | Python (asyncio) only | Temporal is polyglot |

### When to choose Temporal

- You need workflows that suspend for hours/days/months and resume with full local state intact.
- You need signals, queries, or updates to interact with a running execution from the outside.
- Your orchestration logic is genuinely dynamic — loops, conditionals, and external waits interleaved — rather than a static graph.
- You want a managed option (Temporal Cloud) and are willing to depend on a hosted service or invest in self-hosting a more complex stack.
- You need polyglot worker support across multiple languages in the same system.

### When to choose Jobbers

- Your orchestration is naturally expressed as a static DAG (chain/fan-out/fan-in) rather than arbitrary code, and you want that graph to double as documentation (Mermaid).
- You want a much smaller operational footprint — Redis plus four lightweight processes, no dedicated visibility store or multi-service server.
- You want OpenTelemetry metrics and traces out of the box without wiring SDK interceptors.
- You don't need mid-execution human interaction (signals/queries) — cancellation and a queryable status API are sufficient.
- You're already asyncio-native Python and don't need polyglot workers.
