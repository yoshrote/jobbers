"""
Implementation-specific tests for RedisTaskSubmit and RedisJSONTaskSubmit.

Parametrized over both Redis backends via the ``redis_task_adapter`` fixture
(yields a ``(state, submit)`` pair for ``["redis", "redis_json"]``).
Tests verify Redis-internal state (sorted sets, keys, rate-limiter) after submit
operations, going beyond what the protocol contract tests cover.
"""

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")
ULID5 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG05")


def _make_rate_task(task_id: ULID, submitted_at: dt.datetime) -> Task:
    return Task(
        id=task_id, name="test", queue="default", status=TaskStatus.SUBMITTED, submitted_at=submitted_at
    )


def _default_queue_config(rate_numerator: int = 2) -> QueueConfig:
    return QueueConfig(
        name="default", rate_numerator=rate_numerator, rate_denominator=1, rate_period=RatePeriod.MINUTE
    )


# ── submit_task ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_task_adds_to_sorted_set(redis_task_adapter):
    """submit_task places the task in the queue sorted set."""
    state, submit = redis_task_adapter
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    task.set_status(TaskStatus.SUBMITTED)
    await submit.submit_task(task)
    task_list = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in task_list


@pytest.mark.asyncio
async def test_submit_task_twice_does_not_duplicate_queue_entry(redis_task_adapter):
    """Submitting the same task ID twice results in exactly one queue entry."""
    state, submit = redis_task_adapter
    task = Task(id=ULID1, name="Initial Task", status="unsubmitted")
    task.set_status(TaskStatus.SUBMITTED)
    await submit.submit_task(task)
    updated = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await submit.submit_task(updated)
    task_list = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue=task.queue), 0, -1)
    assert task_list == [bytes(ULID1)]


# ── get_next_task ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_task_removes_from_sorted_set(redis_task_adapter):
    """get_next_task pops the task from the queue sorted set."""
    state, submit = redis_task_adapter
    task = Task(id=ULID1, name="t", queue="default", status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME)
    await submit.submit_task(task)
    result = await submit.get_next_task(queues={"default"}, pop_timeout=1)
    assert result is not None
    assert result.id == ULID1
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) not in members


@pytest.mark.asyncio
async def test_get_next_task_skips_missing_data_and_returns_none(redis_task_adapter):
    """When a task is popped from the queue but its blob is absent, None is returned."""
    state, submit = redis_task_adapter
    queue_key = state.TASKS_BY_QUEUE(queue="default")
    if isinstance(queue_key, str):
        queue_key = queue_key.encode()
    pop_results = iter(
        [
            (queue_key, bytes(ULID1), FROZEN_TIME.timestamp()),
            None,
        ]
    )
    with patch.object(submit._data_store, "bzpopmin", new_callable=AsyncMock, side_effect=pop_results):
        result = await submit.get_next_task(queues={"default"}, pop_timeout=1)
    assert result is None
    assert await state.data_store.zscore(state.DLQ_MISSING_DATA, bytes(ULID1)) is not None


# ── submit_rate_limited_task ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_rate_limited_enqueues_when_empty(redis_task_adapter, redis_task_adapter_dt_module):
    """Atomically enqueues the task and records it in the rate-limiter when the set is empty."""
    state, submit = redis_task_adapter
    task = _make_rate_task(ULID1, FROZEN_TIME)
    with patch(redis_task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await submit.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert bytes(ULID1) in await state.data_store.zrange(state.QUEUE_RATE_LIMITER(queue="default"), 0, -1)
    assert bytes(ULID1) in await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert await state.data_store.exists(state.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_submit_rate_limited_enqueues_with_room(redis_task_adapter, redis_task_adapter_dt_module):
    """Enqueues when one slot is already used out of two."""
    state, submit = redis_task_adapter
    await state.data_store.zadd(
        state.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 1}
    )
    task = _make_rate_task(ULID2, FROZEN_TIME)
    with patch(redis_task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await submit.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert bytes(ULID2) in await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)


@pytest.mark.asyncio
async def test_submit_rate_limited_prunes_expired_entries(redis_task_adapter, redis_task_adapter_dt_module):
    """Expired rate-limiter entries are pruned; the new task is accepted."""
    state, submit = redis_task_adapter
    await state.data_store.zadd(
        state.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 60}
    )
    await state.data_store.zadd(
        state.QUEUE_RATE_LIMITER(queue="default"), {ULID2.bytes: FROZEN_TIME.timestamp() - 61}
    )
    task = _make_rate_task(ULID5, FROZEN_TIME)
    with patch(redis_task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await submit.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert ULID5.bytes in await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)


@pytest.mark.asyncio
async def test_submit_rate_limited_rejects_when_full(redis_task_adapter, redis_task_adapter_dt_module):
    """Task is not enqueued when rate limit is reached."""
    state, submit = redis_task_adapter
    await state.data_store.zadd(
        state.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 1}
    )
    await state.data_store.zadd(
        state.QUEUE_RATE_LIMITER(queue="default"), {ULID2.bytes: FROZEN_TIME.timestamp() - 2}
    )
    task = _make_rate_task(ULID5, FROZEN_TIME)
    with patch(redis_task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await submit.submit_rate_limited_task(task, _default_queue_config())
    assert result is False
    assert ULID5.bytes not in await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert not await state.data_store.exists(state.TASK_DETAILS(task_id=ULID5))


@pytest.mark.asyncio
async def test_submit_rate_limited_concurrent_respects_limit(redis_task_adapter):
    """Concurrent submissions must not collectively exceed the rate limit."""
    state, submit = redis_task_adapter
    now = dt.datetime.now(dt.UTC)
    limit = 5
    queue_config = _default_queue_config(rate_numerator=limit)
    tasks = [_make_rate_task(ULID(), now) for _ in range(10)]
    results = await asyncio.gather(*[submit.submit_rate_limited_task(t, queue_config) for t in tasks])
    accepted = sum(1 for r in results if r)
    assert accepted == limit
    assert await state.data_store.zcard(state.QUEUE_RATE_LIMITER(queue="default")) == limit
    assert await state.data_store.zcard(state.TASKS_BY_QUEUE(queue="default")) == limit
