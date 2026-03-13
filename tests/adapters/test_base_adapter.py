"""Tests for _BaseTaskAdapter shared methods."""

import datetime as dt
from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")


def make_task(
    task_id: ULID = ULID1,
    queue: str = "default",
    status: TaskStatus = TaskStatus.SUBMITTED,
) -> Task:
    task = Task(id=task_id, name="my_task", version=1, queue=queue, status=status)
    task.submitted_at = FROZEN_TIME
    return task


# ── stage_requeue ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_requeue_adds_task_to_queue(msgpack_adapter, redis):
    """stage_requeue enqueues the task back into its sorted-set queue."""
    task = make_task()
    pipe = redis.pipeline(transaction=True)
    msgpack_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    members = await redis.zrange(msgpack_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_stage_requeue_saves_task_data(msgpack_adapter, redis):
    """stage_requeue also persists the task blob so it can be retrieved."""
    task = make_task()
    pipe = redis.pipeline(transaction=True)
    msgpack_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    saved = await msgpack_adapter.get_task(ULID1)
    assert saved is not None
    assert saved.id == ULID1


@pytest.mark.asyncio
async def test_stage_requeue_uses_submitted_at_as_score(msgpack_adapter, redis):
    """The queue sorted-set score matches the task's submitted_at timestamp."""
    task = make_task()
    pipe = redis.pipeline(transaction=True)
    msgpack_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    score = await redis.zscore(msgpack_adapter.TASKS_BY_QUEUE(queue="default"), bytes(ULID1))
    assert score == pytest.approx(FROZEN_TIME.timestamp())


@pytest.mark.asyncio
async def test_stage_requeue_does_not_duplicate_queue_entry(msgpack_adapter, redis):
    """Re-queuing the same task ID does not create a duplicate entry."""
    task = make_task()
    for _ in range(3):
        pipe = redis.pipeline(transaction=True)
        msgpack_adapter.stage_requeue(pipe, task)
        await pipe.execute()

    members = await redis.zrange(msgpack_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert members.count(bytes(ULID1)) == 1


# ── remove_task_heartbeat ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_task_heartbeat_removes_entry(msgpack_adapter, redis):
    """remove_task_heartbeat removes the task from the heartbeat sorted set."""
    task = make_task()
    task.heartbeat_at = FROZEN_TIME
    await msgpack_adapter.update_task_heartbeat(task)
    assert await redis.zscore(msgpack_adapter.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)) is not None

    await msgpack_adapter.remove_task_heartbeat(task)
    assert await redis.zscore(msgpack_adapter.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)) is None


@pytest.mark.asyncio
async def test_remove_task_heartbeat_noop_when_absent(msgpack_adapter):
    """remove_task_heartbeat is a no-op when the task has no heartbeat entry."""
    task = make_task()
    await msgpack_adapter.remove_task_heartbeat(task)  # should not raise


# ── get_active_tasks ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_tasks_returns_empty_for_no_heartbeats(msgpack_adapter):
    """get_active_tasks returns [] when no tasks have heartbeat entries."""
    result = await msgpack_adapter.get_active_tasks({"default"})
    assert result == []


@pytest.mark.asyncio
async def test_get_active_tasks_returns_tasks_with_heartbeats(msgpack_adapter, redis):
    """get_active_tasks returns tasks that have a heartbeat entry."""
    task = make_task()
    await msgpack_adapter.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await msgpack_adapter.update_task_heartbeat(task)

    result = await msgpack_adapter.get_active_tasks({"default"})
    assert len(result) == 1
    assert result[0].id == ULID1


# ── clean (no-op branch) ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_noop_when_no_age_params(msgpack_adapter, redis):
    """clean() with neither min_queue_age nor max_queue_age does nothing."""
    task = make_task()
    await msgpack_adapter.submit_task(task)

    now = dt.datetime.now(dt.UTC)
    await msgpack_adapter.clean(queues={b"default"}, now=now)

    # Task should still be in the queue
    members = await redis.zrange(msgpack_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members


# ── get_next_task: missing data path ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_task_skips_missing_data_and_returns_none(msgpack_adapter, redis):
    """When a task is popped from the queue but its blob is absent, it is logged and None is returned."""
    fake_id = ULID1
    queue_key = (
        msgpack_adapter.TASKS_BY_QUEUE(queue="default").encode()
        if isinstance(msgpack_adapter.TASKS_BY_QUEUE(queue="default"), str)
        else msgpack_adapter.TASKS_BY_QUEUE(queue="default")
    )
    # bzpopmin returns (key, member, score); first call returns the fake ID, second returns None (timeout)
    pop_results = iter(
        [
            (queue_key, bytes(fake_id), FROZEN_TIME.timestamp()),
            None,
        ]
    )
    with patch.object(
        msgpack_adapter.data_store, "bzpopmin", new_callable=AsyncMock, side_effect=pop_results
    ):
        result = await msgpack_adapter.get_next_task(queues={"default"}, pop_timeout=1)
    assert result is None
    # The missing ID should be tracked in the DLQ missing-data key
    assert await redis.zscore(msgpack_adapter.DLQ_MISSING_DATA, bytes(fake_id)) is not None
