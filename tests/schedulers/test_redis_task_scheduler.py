"""Redis-specific scheduler tests."""

import datetime as dt

import pytest

from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
FROZEN_TIME = dt.datetime.fromisoformat("2021-01-01T00:00:00+00:00")


def make_task(task_id: str = "01JQC31AJP7TSA9X8AEP64XG01", queue: str = "default") -> Task:
    return Task(id=task_id, name="test_task", version=1, queue=queue, status=TaskStatus.SCHEDULED)


async def schedule(s: object, task: Task, run_at: dt.datetime) -> None:
    pipe = s.pipeline(transaction=True)  # type: ignore[attr-defined]
    s.stage_add(pipe, task, run_at)  # type: ignore[attr-defined]
    await pipe.execute()


# ── missing-blob scenario ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_by_filter_skips_none_tasks(redis_scheduler, dummy_task_adapter):
    """
    get_by_filter silently skips schedule entries whose task data is missing.

    Redis stores task data separately from the schedule. When a task ID is in
    the sorted set but the corresponding task blob is absent, get_tasks_bulk
    returns None for that entry and get_by_filter must skip it.
    """
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    await dummy_task_adapter.save_task(t1)
    await schedule(redis_scheduler, t1, PAST)
    await schedule(redis_scheduler, t2, PAST)  # t2 blob absent → get_tasks_bulk returns None

    results = await redis_scheduler.get_by_filter(queue="default")
    assert len(results) == 1
    assert results[0][0].id == t1.id


# ── recover_orphans ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_recover_orphans_readds_orphaned_task(redis, redis_scheduler, dummy_task_adapter):
    """A task in the hash with SCHEDULED status but absent from the sorted set is re-added."""
    task = make_task()
    await dummy_task_adapter.save_task(task)
    await redis.hset(redis_scheduler.SCHEDULE_TASK_QUEUE, str(task.id), task.queue)
    # Deliberately do NOT seed the sorted set — this is the orphan condition.

    await redis_scheduler.recover_orphans(FROZEN_TIME)

    score = await redis.zscore(redis_scheduler.SCHEDULE_QUEUE(queue=task.queue), bytes(task.id))
    assert score == FROZEN_TIME.timestamp()


@pytest.mark.asyncio
async def test_recover_orphans_skips_present_entry(redis, redis_scheduler, dummy_task_adapter):
    """A task already in the sorted set keeps its original score and is not overwritten."""
    task = make_task()
    original_score = FROZEN_TIME.timestamp() + 3600
    await dummy_task_adapter.save_task(task)
    await redis.hset(redis_scheduler.SCHEDULE_TASK_QUEUE, str(task.id), task.queue)
    await redis.zadd(redis_scheduler.SCHEDULE_QUEUE(queue=task.queue), {bytes(task.id): original_score})

    await redis_scheduler.recover_orphans(FROZEN_TIME)

    score = await redis.zscore(redis_scheduler.SCHEDULE_QUEUE(queue=task.queue), bytes(task.id))
    assert score == original_score


@pytest.mark.asyncio
async def test_recover_orphans_removes_stale_hash_entry(redis, redis_scheduler, dummy_task_adapter):
    """A hash entry whose task is no longer SCHEDULED (e.g. SUBMITTED) is removed from the hash."""
    task = make_task()
    task.status = TaskStatus.SUBMITTED
    await dummy_task_adapter.save_task(task)
    await redis.hset(redis_scheduler.SCHEDULE_TASK_QUEUE, str(task.id), task.queue)

    await redis_scheduler.recover_orphans(FROZEN_TIME)

    remaining = await redis.hget(redis_scheduler.SCHEDULE_TASK_QUEUE, str(task.id))
    assert remaining is None
    score = await redis.zscore(redis_scheduler.SCHEDULE_QUEUE(queue=task.queue), bytes(task.id))
    assert score is None
