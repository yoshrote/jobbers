# Jobbers vs Dramatiq and arq: Feature Comparison

This document compares Jobbers against two lighter-weight Celery alternatives: [Dramatiq](https://dramatiq.io/) (Bogdanp) and [arq](https://arq-docs.helpmanual.io/) (python-arq). Both occupy the same "simpler than Celery" niche but from different angles — Dramatiq is sync-by-default with optional async support, arq is asyncio-only, like Jobbers. They're covered together because most teams evaluating them are choosing between "which small Redis-backed queue" rather than "which orchestration paradigm," which is a different question than the Celery/Airflow/Temporal comparisons answer.

---

## 1. Philosophy and Design Goals

**Dramatiq** is a reaction against Celery's configuration surface: sane defaults, no result backend required unless you opt in, a small and readable core, and a middleware system for extending behavior (retries, rate limiting, age limits, shutdown). It supports RabbitMQ and Redis as brokers, with RabbitMQ treated as the "full-featured" option (dead-letter exchanges, priority queues) and Redis as the simpler, lower-guarantee choice.

**arq** is asyncio-first and Redis-only. Where Celery and Dramatiq are primarily sync frameworks that bolt on async support, arq's entire execution model — job functions, worker pool, context — assumes `async def` throughout, the same premise Jobbers is built on. Its scope is deliberately narrow: job queuing, retries, cron, and not much else.

**Jobbers** shares arq's asyncio-only, Redis-first premise but adds the things arq and Dramatiq both leave out: explicit task-state tracking with a queryable API, a first-class DLQ, DAG composition with fan-in/fan-out, live traffic control without worker restarts, and OpenTelemetry by default. Both Dramatiq and arq assume you'll reach for a separate result backend, logging setup, or monitoring tool if you need visibility beyond "did the job run" — Jobbers builds that in.

**Verdict:** Dramatiq and arq are both meaningfully smaller in scope than Jobbers — closer to "a good `send()`/`enqueue_job()`" than a system with task lifecycle tracking. If your workload is simple background jobs with straightforward retry needs, that smallness is the point, not a gap. If you need DAGs, a DLQ, live traffic control, or aggregate observability without bolting on more libraries, Jobbers is doing more out of the box.

---

## 2. Getting Started

**Dramatiq:**

```python
import dramatiq
from dramatiq.brokers.redis import RedisBroker

dramatiq.set_broker(RedisBroker(host="localhost"))

@dramatiq.actor(max_retries=5, min_backoff=1000, max_backoff=900_000)
def add(x, y):
    return x + y

add.send(2, 2)
```

```bash
dramatiq my_module   # start workers (threads by default; use --processes/--threads to tune)
```

**arq:**

```python
from arq import create_pool
from arq.connections import RedisSettings

async def add(ctx, x, y):
    return x + y

class WorkerSettings:
    functions = [add]
    redis_settings = RedisSettings()
```

```bash
arq my_module.WorkerSettings
```

```python
redis = await create_pool(RedisSettings())
await redis.enqueue_job("add", 2, 2)
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

**Verdict:** Dramatiq and arq both win decisively on getting-started simplicity — one process, one dependency (Redis), a handful of lines. Jobbers' four-process topology (manager, worker, cleaner, scheduler) is a real cost for prototypes and small deployments; it pays for itself once you need the DLQ, DAG, or traffic-control capabilities neither Dramatiq nor arq have.

---

## 3. Async Python Support

**Dramatiq:** Actors are synchronous by default and run on a thread pool. `async def` actors require explicitly adding `AsyncIOMiddleware` to the broker, which spins up one event-loop thread per worker process; sync and async actors can coexist, with sync ones scheduled on worker threads and async ones on the event loop thread. This is opt-in, not the default posture of the framework.

**arq:** asyncio is the only execution model — job functions are `async def`, and the worker runs a pool of `asyncio.Task`s, so I/O-bound jobs never occupy a full worker slot the way a blocking thread would.

**Jobbers:** Same asyncio-only model as arq — every task function is `async def`, concurrency is an `asyncio.Semaphore`, no threads involved.

**Verdict:** arq and Jobbers are equivalent here — both asyncio-native by design, not by addition. Dramatiq's async support is real but is an opt-in middleware layered onto a fundamentally sync/threaded core, which matters if you're running a fully async I/O-bound workload and don't want the thread-pool machinery in the picture at all.

---

## 4. Retry Policies

**Dramatiq:**

```python
@dramatiq.actor(max_retries=5, min_backoff=1000, max_backoff=900_000, on_retry_exhausted="log_failure")
def my_task():
    ...  # raise — retries are automatic once max_retries is set
```

Retries are declarative and exponential by default (`min_backoff`/`max_backoff` bound the delay); `on_retry_exhausted` names another actor to invoke when retries run out, which is Dramatiq's closest thing to a failure hook.

**arq:**

```python
from arq import Retry

async def my_task(ctx):
    if ctx["job_try"] > 3:
        raise Exception("give up")
    raise Retry(defer=ctx["job_try"] * 5)  # explicit retry, explicit delay
```

`max_tries` (default 5) is set per job or worker-wide; unlike Dramatiq and Jobbers, arq's retry is not automatic on any raised exception — an unhandled exception fails the job outright, and you must explicitly raise `arq.worker.Retry` (optionally with `defer=`) to get another attempt. This is closer to Celery's `self.retry()` model than to Jobbers'/Dramatiq's declare-and-forget approach.

**Jobbers:**

```python
@register_task(
    name="my_task", version=1, max_retries=5, retry_delay=10,
    backoff_strategy=BackoffStrategy.EXPONENTIAL, max_retry_delay=3600,
)
async def my_task(**kwargs):
    ...  # just raise — retry logic is automatic
```

Four backoff strategies (`CONSTANT`, `LINEAR`, `EXPONENTIAL`, `EXPONENTIAL_JITTER`), all declared once at registration, all capped at `max_retry_delay`.

**Verdict:** Dramatiq and Jobbers are close — both retry automatically on any exception, both configure backoff declaratively. arq requires an explicit `raise Retry(...)` in the task body for every retryable path, which is more control (you decide per-exception whether it's retryable) at the cost of more boilerplate than either Dramatiq or Jobbers need.

---

## 5. Task Composition (Workflows)

**Dramatiq:** Core Dramatiq has no workflow primitives. `dramatiq.group` runs a set of actors in parallel and can block for completion; there is no built-in chain/pipeline in the core library. Chains, more complex fan-out/fan-in, and DAG-shaped workflows require a third-party extension (e.g., `dramatiq-workflow`) layered on top.

**arq:** No workflow primitives at all. Composing multi-step jobs means one job enqueuing the next job itself (`await ctx["redis"].enqueue_job(...)` from within a running job) — hand-rolled chaining, no fan-in/fan-out support, no dependency graph the framework understands.

**Jobbers:** `DAGNode` graphs (chain/fan-out/fan-in), `DynamicFanOut` for runtime-determined fan-out, `on_error` callbacks, all serialized as Mermaid flowcharts submittable via `POST /submit-dag`. Full DAG-run tracking (`GET /dags`, aggregate status, cancellation, resume) — see [docs/dag-composition.md](dag-composition.md).

**Verdict:** Jobbers wins decisively. Neither Dramatiq nor arq ship workflow composition in the core library — Dramatiq needs a third-party package for even basic chains/groups, and arq has no concept of a multi-step job at all beyond a task enqueuing another task by hand. If your workload has any real dependency structure between tasks, Jobbers is doing that natively; the other two are not.

---

## 6. Dead Letter Queues and Failure Handling

**Dramatiq:** DLQ support is broker-dependent. With `RabbitmqBroker`, exhausted messages land in a RabbitMQ dead-letter exchange (retained ~7 days by default, replayable from RabbitMQ tooling) — a real, broker-native DLQ. With `RedisBroker`, there is no equivalent; Redis lacks the dead-letter-exchange primitive RabbitMQ provides, so the only failure hook available to every broker is `on_retry_exhausted`, which you must wire up yourself to persist anything.

**arq:** No DLQ concept. A job that exceeds `max_tries` (or explicitly aborts) simply fails; there's no queryable store of failed jobs, no resubmit tooling. Failure handling is whatever your own logging/alerting does with the exception.

**Jobbers:** First-class DLQ via `DeadLetterPolicy.SAVE`, independent of which broker/backend you're running — queryable (`GET /dead-letter-queue`), with full failure history (`GET /dead-letter-queue/{id}/history`), and bulk resubmit (`POST /dead-letter-queue/resubmit`), backed by Redis, Redis Stack, or SQL depending on `DLQ_BACKEND`. For DAG workloads specifically, `POST /dags/{id}/resume` complements the DLQ by resuming an entire stuck run in place rather than resubmitting individual dead-lettered tasks.

**Verdict:** Jobbers wins clearly. Dramatiq's DLQ only exists if you're on RabbitMQ (not the Redis broker most small deployments reach for first); arq has nothing. Neither has anything resembling a management API for failed work.

---

## 7. Traffic Management (Concurrency, Rate Limiting, Routing)

**Dramatiq:** Worker concurrency (`--processes`/`--threads`) is set at process startup. Per-actor rate limiting is available via middleware (e.g., `ConcurrentRateLimiter`, `WindowRateLimiter`) backed by Redis/Memcached. Queue assignment is per-actor (`@dramatiq.actor(queue_name=...)`) and which queues a worker consumes is set via `--queues` at startup. Changing concurrency, rate limits, or queue assignment all require a worker restart.

**arq:** `max_jobs` (concurrency) and `job_timeout` are set on `WorkerSettings` at startup. No built-in per-queue rate limiting — arq has a single queue per Redis connection by default, with a `queue_name` you can vary, but no rate-limit primitive comparable to Dramatiq's middleware or Jobbers' queue config. Everything here requires a restart to change.

**Jobbers:** Four composable controls, all live-adjustable without worker restarts — queue `max_concurrent`, queue rate limits, and role→queue assignment all propagate to running workers via a `refresh_tag` + pub/sub mechanism (see [docs/resource-management.md](resource-management.md)).

**Verdict:** Jobbers wins clearly. Both Dramatiq and arq require a deploy/restart to change concurrency, rate limits, or routing; Jobbers changes all three live via the API.

---

## 8. Observability

**Dramatiq:** A built-in `Prometheus` middleware exposes metrics (message counts, latencies, in-progress counts) on port 9191 by default, scrapeable directly — no extra library needed for basic Prometheus metrics. No built-in OpenTelemetry tracing; getting traces requires manual instrumentation or a third-party integration.

**arq:** No built-in metrics or tracing of any kind. Observability is whatever you build yourself around job start/finish logging.

**Jobbers:** OpenTelemetry traces, metrics, and logs emitted via OTLP with zero task-side instrumentation — `tasks_processed`, `task_execution_time`, `time_in_queue`, `refresh_lag_ms`, and others (see the metrics table in [CLAUDE.md](../CLAUDE.md)). A React admin UI provides live task/DLQ/queue inspection.

**Verdict:** Jobbers wins clearly. Dramatiq's Prometheus middleware is a real, useful built-in (better than arq's nothing), but it's metrics-only, whereas Jobbers gets traces + metrics + logs into an existing OTEL pipeline with no extra wiring, plus a UI neither alternative has.

---

## 9. Scheduling and Periodic Tasks

**Dramatiq:** No built-in cron/periodic scheduler in the core library — `dramatiq-crontab` and similar are third-party add-ons. Delayed execution of a single message (`actor.send_with_options(delay=...)`) is supported natively for both brokers.

**arq:** Native `cron_jobs` on `WorkerSettings`, using `cron(func, hour=..., minute=..., ...)` — closer to crontab semantics than a full 5-field expression string, run in-process by the worker rather than a separate scheduler.

**Jobbers:** Cron-scheduled DAGs (`CronDAGEntry`, standard 5-field `cron_expr`) with a `ConcurrencyPolicy` and full REST CRUD (`POST/GET/PUT/DELETE /cron-dags`), run by a dedicated Scheduler process — see the cron scheduling section of [the Celery comparison](celery-comparison.md#11-scheduling-and-periodic-tasks) for the same mechanism in more depth.

**Verdict:** Jobbers wins on both cron expressiveness (standard cron syntax vs arq's field-based `cron()` calls, vs nothing built into Dramatiq) and on live manageability (REST CRUD vs code-defined schedules requiring redeploys in both alternatives) — and it's the only one of the three where a cron entry can dispatch a whole DAG rather than a single job.

---

## 10. Task Introspection and Cancellation

**Dramatiq:** No result backend by default; enabling one (e.g., the Redis or RabbitMQ result backends) lets you check `message.get_result()`, but there's no task-status query API, no "list all in-flight actors," and no built-in cancellation — a running actor cannot be told to stop short of killing the worker process.

**arq:** `Job` objects support `await job.status()` and `await job.result()` for a specific job you already have a reference to; there's no query-by-status/queue listing API. Cancellation (`job.abort()`) exists but only takes effect if the worker was started with `allow_abort_jobs=True`, and it's a hard abort rather than cooperative — the job is killed, not signalled to wind down.

**Jobbers:** Full lifecycle API — `GET /task-status/{id}`, `GET /task-list?queue=...&status=...`, `GET /active-tasks`, `GET /scheduled-tasks`, single and bulk cooperative cancellation (`POST /task/{id}/cancel`, `POST /tasks/cancel`), plus DAG-run-level listing, single-call cancel, and resume (`GET /dags`, `POST /dags/{id}/cancel`, `POST /dags/{id}/resume`).

**Verdict:** Jobbers wins decisively. Neither alternative has a queryable task-status/search API; Dramatiq has no cancellation at all, and arq's is a hard, opt-in abort rather than the cooperative, always-available cancellation Jobbers provides.

---

## 11. Broker and Backend Flexibility

**Dramatiq:** RabbitMQ (full-featured: priorities, dead-letter exchanges, durable queues) or Redis (simpler, weaker guarantees — messages can be lost on worker crash without extra tuning). Optional result backends (Redis, RabbitMQ RPC, custom) if you need return values.

**arq:** Redis only, no alternative broker.

**Jobbers:** Redis-only queue transport, but task state, DLQ, scheduler, cron scheduler, and routing config are each independently selectable between Redis, Redis Stack, and SQL — including all-SQL/zero-Redis or all-Redis/zero-SQL deployments (see the broker/backend section of [the Celery comparison](celery-comparison.md#10-broker-and-backend-flexibility)).

**Verdict:** Dramatiq wins on transport choice (RabbitMQ is a genuinely more durable option than anything Redis-based offers). arq and Jobbers share the same Redis-only queue transport; Jobbers is well ahead of both on storage-layer flexibility for everything besides the queue itself.

---

## Summary

| Feature | Dramatiq | arq | Jobbers | Notes |
| --- | --- | --- | --- | --- |
| **Getting started** | Simple | Simple | Moderate | Both alternatives: 1 process, few lines. Jobbers: 4 processes |
| **Async model** | Opt-in middleware | Native | Native | arq and Jobbers share the same asyncio-first premise |
| **Retries** | Automatic, declarative | Explicit `raise Retry(...)` required | Automatic, declarative | Dramatiq and Jobbers closest; arq is more Celery-like here |
| **Workflows/DAGs** | None in core (3rd-party ext.) | None | `DAGNode` + Mermaid + fan-in/out | Jobbers wins decisively |
| **Dead letter queue** | RabbitMQ only | None | First-class, backend-agnostic | Jobbers wins clearly |
| **Traffic management** | Restart required | Restart required | Live, no restart | Jobbers wins clearly |
| **Observability** | Prometheus middleware (metrics only) | None built in | OTEL (traces+metrics+logs) + UI | Jobbers wins clearly |
| **Cron scheduling** | 3rd-party only | Native, field-based | Native, cron-string + REST CRUD + DAGs | Jobbers most complete |
| **Task introspection/cancel** | None / no cancellation | Query by reference only; hard abort, opt-in | Full query API + cooperative cancel | Jobbers wins decisively |
| **Broker flexibility** | RabbitMQ or Redis | Redis only | Redis only (queue); Redis/Redis Stack/SQL (state, DLQ, etc.) | Dramatiq wins on transport; Jobbers wins on storage |

### When to choose Dramatiq or arq

- Your workload is genuinely simple: enqueue a function call, retry it a few times, done — no DAGs, no DLQ management, no live traffic control needed.
- You want the smallest possible operational footprint: one worker process, Redis (or RabbitMQ for Dramatiq), nothing else.
- (Dramatiq specifically) You need RabbitMQ's durability/routing guarantees, or you have a mix of sync and async task code and want both to coexist naturally.
- (arq specifically) You're already asyncio-first FastAPI/asyncio code and want the lightest possible Redis job queue that shares that model, with no interest in DAGs, DLQ, or built-in observability.

### When to choose Jobbers

- You need multi-step workflows (chains, fan-out, fan-in, runtime-determined fan-out) — neither alternative has this in its core library.
- You want a structured, queryable dead letter queue regardless of which broker you're on, plus DAG-run-level resume for stuck workflows.
- You need live traffic control (reroute queues, throttle rate, cap concurrency) without restarting workers.
- You want OpenTelemetry traces/metrics/logs and a task-status/search API out of the box, not something you wire up yourself.
- You want cancellation to actually be a supported, cooperative operation rather than a hard-kill opt-in (arq) or entirely absent (Dramatiq).
