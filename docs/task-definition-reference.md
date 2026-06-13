# Task Definition Reference

`register_task` is a decorator that registers an async function as a named, versioned task and wraps it in a `TaskWrapper` that exposes `.submit()`, `.schedule()`, and `.node()` helpers.

```python
from jobbers.registry import register_task
from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy

@register_task(
    name="process_order",
    version=1,
    max_retries=5,
    retry_delay=30,
    max_retry_delay=3600,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    dead_letter_policy=DeadLetterPolicy.SAVE,
    max_heartbeat_interval=dt.timedelta(minutes=2),
)
async def process_order(order_id: int, **kwargs):
    ...
```

---

## Identity

### `name: str`

The unique task name. Workers use `(name, version)` together to look up the registered function when dequeuing a task. If a task arrives with an unknown `(name, version)` pair, it is set to `DROPPED`.

### `version: int`

An integer schema version. Increment this when the task function's parameter signature changes in a backwards-incompatible way so that in-flight tasks created under the old signature can still be dispatched to workers that run both versions concurrently.

Registering the same `(name, version)` to a different function raises `ValueError`. Re-registering the same function is allowed (logs a warning).

---

## Return values

Task functions may return a plain dict, `None`, or a `TaskResult`:

| Return type | Behaviour |
| --- | --- |
| `dict` | Stored as `task.results`; available to downstream tasks via `task.parent_results`. |
| `None` | Treated as an empty result (`{}`). Use for fire-and-forget tasks that produce no output. |
| `TaskResult` | Only needed to trigger a `DynamicFanOut`. See [dag-composition.md](dag-composition.md). |

For most tasks, return a plain dict:

```python
@register_task(name="compute_stats", version=1)
async def compute_stats(dataset_id: int, **kwargs) -> dict:
    rows = await load(dataset_id)
    return {"count": len(rows)}
```

Use `task.make_result()` only when triggering a dynamic fan-out:

```python
from jobbers.context import get_current_task
from jobbers.models.dag import DynamicFanOut, DAGNode

@register_task(name="dispatch_work", version=1)
async def dispatch_work(**kwargs):
    task = get_current_task()
    items = await fetch_items()
    children = [DAGNode("process_item", parameters={"id": i}) for i in items]
    collector = DAGNode("aggregate_results")
    return task.make_result(
        results={"count": len(items)},
        fanout=DynamicFanOut(children=children, collector=collector),
    )
```

---

## Concurrency

### `max_concurrent: int | None` (default: `1`)

The maximum number of this task type that may run concurrently **within a single worker process**. `None` means unlimited.

> **Note:** this field is stored on the task config but is not currently enforced by the task generator or processor. Queue-level concurrency is controlled separately via `QueueConfig.max_concurrent` on the queue itself (configured via `POST /queues/{queue_name}`).

---

## Execution

### `timeout: int | None` (default: `None`)

Maximum wall-clock seconds the task function is allowed to run before it is interrupted. When the timeout fires, the task is treated as a retryable failure: `_handle_retry` is called, incrementing `retry_attempt` and either rescheduling the task (if `retry_delay` is set) or re-queuing it immediately.

`None` means no timeout — the task runs until it completes, raises, or is cancelled.

---

## Retry behaviour

### `max_retries: int` (default: `3`)

Maximum number of retry attempts. The check is `retry_attempt < max_retries`, so a value of `3` allows up to three retries (four total executions: the original plus three retries). When all retries are exhausted the task transitions to `FAILED`.

Only retries triggered by `expected_exceptions` or a timeout count against this limit. Unexpected exceptions immediately set the task to `FAILED` without retrying.

### `retry_delay: int | None` (default: `None`)

Base delay in seconds before scheduling a retry. When `None`, failing tasks are re-queued immediately without passing through the scheduler (status goes back to `UNSUBMITTED`). When set, the task is moved to the scheduler queue with a computed `run_at` (status becomes `SCHEDULED`).

The computed delay is capped at `max_retry_delay`.

### `max_retry_delay: int` (default: `3600`)

Upper bound in seconds on the delay computed by any backoff strategy. Regardless of how many retries have accumulated, the scheduler will never delay longer than this value.

### `backoff_strategy: BackoffStrategy` (default: `BackoffStrategy.EXPONENTIAL`)

Controls how `retry_delay` scales across successive retry attempts. Only meaningful when `retry_delay` is set. The formula uses `attempt` (the current `retry_attempt` value after incrementing) and `base = float(retry_delay)`.

| Value | Formula | Behaviour |
| --- | --- | --- |
| `CONSTANT` | `base` | Same fixed delay on every retry. Use when the failure is likely transient and there is no benefit to waiting longer. |
| `LINEAR` | `base × attempt` | Delay grows proportionally with the attempt count. Use when you want moderate back-pressure without the steep growth of exponential. |
| `EXPONENTIAL` | `base × 2^attempt` | Delay doubles each retry. The default; suitable for most cases where the downstream system needs time to recover. |
| `EXPONENTIAL_JITTER` | `uniform(0, base × 2^attempt)` | Randomly samples between 0 and the exponential ceiling. Use when many tasks retry at the same time and you want to spread the retry load (thundering-herd prevention). |

All strategies clamp the result to `max_retry_delay` before scheduling.

### `expected_exceptions: tuple[type[Exception], ...] | None` (default: `None`)

A tuple of exception types that are considered retriable. When the task function raises an exception that is an instance of any listed type, the retry path is taken (incrementing `retry_attempt` and scheduling or re-queuing according to `retry_delay` and `backoff_strategy`). Any other exception causes an immediate, permanent `FAILED` with no retry.

`None` means no exceptions are retriable — every unhandled exception fails the task permanently without incrementing `retry_attempt`.

`isinstance` matching is used, so a subclass of a listed type also triggers the retry path.

`TimeoutError` (raised when `timeout` fires) is handled separately before the `expected_exceptions` check, but it also takes the retry path — you do not need to include it here.

```python
import httpx
from jobbers.registry import register_task

@register_task(
    name="call_external_api",
    max_retries=5,
    retry_delay=10,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    expected_exceptions=(httpx.TimeoutException, httpx.HTTPStatusError),
)
async def call_external_api(url: str, **kwargs) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()  # stored as task.results
```

Only exceptions listed here (or a `TimeoutError` from `timeout`) count against `max_retries`. Programming errors — `AttributeError`, `KeyError`, unhandled `ValueError`, etc. — immediately set the task to `FAILED` so bugs surface immediately rather than burning through retries.

---

## Heartbeat monitoring

### `max_heartbeat_interval: dt.timedelta | None` (default: `None`)

The maximum time the Cleaner process will wait between heartbeat updates before declaring the task stalled. Long-running tasks should call `task.heartbeat()` periodically — more frequently than this interval — to prove they are still alive.

When the Cleaner finds a task whose last heartbeat is older than `max_heartbeat_interval`, it marks the task as `STALLED`.

`None` disables heartbeat monitoring for this task type. Do not set a value for tasks that complete quickly; only use this for tasks that may run for minutes or longer.

```python
import asyncio
import datetime as dt
from jobbers.registry import register_task
from jobbers.context import get_current_task

@register_task(
    name="long_running_task",
    max_heartbeat_interval=dt.timedelta(minutes=5),
)
async def long_running_task(**kwargs):
    task = get_current_task()
    for chunk in get_chunks():
        process(chunk)
        await task.heartbeat()  # call more frequently than max_heartbeat_interval
```

---

## Shutdown behaviour

### `on_shutdown: TaskShutdownPolicy` (default: `TaskShutdownPolicy.STOP`)

Controls what happens to an in-flight task when the worker receives `SIGTERM`.

| Value | Status after shutdown | Behaviour |
| --- | --- | --- |
| `STOP` | `STALLED` | The task coroutine is cancelled and the task is marked `STALLED`. The Cleaner or an operator must decide whether to resubmit it. |
| `RESUBMIT` | `UNSUBMITTED` (re-queued) | The task is cancelled and immediately re-enqueued on its original queue without incrementing `retry_attempt`. Use for idempotent tasks that should transparently survive worker restarts. |
| `CONTINUE` | stays `STARTED` | The task coroutine is wrapped in `asyncio.shield()` before it runs, so it continues executing even after the worker's event loop starts shutting down. The worker process will not exit until the task completes. Use only for tasks that are cheap to shield and where partial execution is harmful. |

---

## Dead letter queue

### `dead_letter_policy: DeadLetterPolicy` (default: `DeadLetterPolicy.NONE`)

Determines whether permanently failed tasks (status `FAILED`) are saved to the dead letter queue for later inspection.

| Value | Behaviour |
| --- | --- |
| `NONE` | Failed tasks are not written to the DLQ. Their state remains in the task store until cleaned up by the Cleaner, but they are not separately indexed for review. |
| `SAVE` | Failed tasks are written to the dead letter queue. They can be inspected via `GET /dead-letter-queue`, bulk resubmitted via `POST /dead-letter-queue/resubmit`, or bulk removed via `DELETE /dead-letter-queue`. |

Use `SAVE` for tasks where the payload and error context need to be preserved for manual intervention or audit.

---

## Dependency Injection

Task functions can declare shared resources as parameters annotated with `Depends()`. The worker resolves each dependency before calling the task and tears it down afterwards, regardless of whether the task succeeded, raised, timed out, or was cancelled.

```python
from typing import Annotated, AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from jobbers.di import Depends
from jobbers.registry import register_task

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with session_factory() as session:
        try:
            yield session
        finally:
            pass  # teardown always runs

@register_task(name="process_record", version=1)
async def process_record(
    record_id: int,                                # from task parameters — submitted by the caller
    db: Annotated[AsyncSession, Depends(get_db)],  # injected by the worker; never in the payload
) -> dict:
    row = await db.get(MyModel, record_id)
    ...
    return {"status": "ok"}
```

`record_id` is part of the submitted task payload. `db` is never submitted — the worker opens a session, injects it, and closes it after the task exits.

### Provider types

| Provider shape | When to use |
| --- | --- |
| `async def` generator (`yield`) | Resources that need teardown — DB sessions, HTTP clients, locks |
| `def` generator (`yield`) | Same, but synchronous (e.g. temp files, context managers) |
| `async def` (no `yield`) | Async factories with no teardown — config loaded from Redis, etc. |
| `def` (no `yield`) | Plain synchronous factories — in-process config, constants |

### Cleanup guarantee

Teardown code must go in a `try/finally` block inside the generator. The worker calls `aclose()` / `close()` on every open generator after the task exits, even if the task raised or was cancelled.

```python
async def get_http_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            yield client
        finally:
            pass  # AsyncClient.__aexit__ handles the actual close
```

### Nested dependencies

Providers can themselves declare `Depends()` parameters. The framework builds the full dependency graph at decoration time and resolves it in topological order (leaves first) at execution time.

```python
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with session_factory() as session:
        try:
            yield session
        finally:
            pass

async def get_user_repo(
    db: Annotated[AsyncSession, Depends(get_db)],
) -> UserRepository:
    return UserRepository(db)

@register_task(name="update_user", version=1)
async def update_user(
    user_id: int,
    repo: Annotated[UserRepository, Depends(get_user_repo)],
) -> dict:
    ...
```

### Shared dependencies

If two parameters depend on the same provider function, the provider is called once and the result is shared across all dependants within that task execution.

```python
async def get_user_repo(db: Annotated[AsyncSession, Depends(get_db)]) -> UserRepository: ...
async def get_audit_log(db: Annotated[AsyncSession, Depends(get_db)]) -> AuditLog: ...

@register_task(name="audit_update", version=1)
async def audit_update(
    user_id: int,
    repo: Annotated[UserRepository, Depends(get_user_repo)],
    audit: Annotated[AuditLog, Depends(get_audit_log)],
    # get_db is called once; both repo and audit share the same session
) -> dict: ...
```

### Cycle detection

The dependency graph is validated at `@register_task` decoration time (i.e. at import). A circular dependency raises `ValueError` immediately, before any task is ever submitted or executed.

### Testing with dependency overrides

Use `dependency_overrides` to replace a provider for the duration of a test. The original is restored automatically when the context exits, even if the test raises.

```python
from jobbers.di import dependency_overrides

async def fake_db() -> AsyncGenerator[AsyncSession, None]:
    yield FakeSession()

async def test_process_record():
    with dependency_overrides({get_db: fake_db}):
        await process_record.submit(queue="default", record_id=1)
```
