"""SQL-specific scheduler tests."""

import datetime as dt

import pytest

from jobbers.adapters.sql import SQLQueueConfigAdapter, SQLTaskScheduler
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
TASK_ID = "01JQC31AJP7TSA9X8AEP64XG01"


def make_task(task_id: str = TASK_ID, queue: str = "default") -> Task:
    return Task(id=task_id, name="test_task", version=1, queue=queue, status=TaskStatus.SCHEDULED)


async def schedule(s: SQLTaskScheduler, task: Task, run_at: dt.datetime) -> None:
    pipe = s.pipeline(transaction=True)
    s.stage_add(pipe, task, run_at)
    await pipe.execute()


# ── embedded task data ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_add_embeds_task_data(session_factory):
    """
    SQLTaskScheduler stores full task data in the schedule row.

    Unlike the Redis scheduler, no separate task adapter is required: the
    task returned by next_due() is reconstructed entirely from the DB row.
    """
    qca = SQLQueueConfigAdapter(session_factory)
    scheduler = SQLTaskScheduler(session_factory, qca.get_all_queues)
    task = make_task()
    await schedule(scheduler, task, PAST)

    result = await scheduler.next_due(["default"])
    assert result is not None
    assert result.id == task.id
    assert result.name == "test_task"
    assert result.version == 1


@pytest.mark.asyncio
async def test_stage_add_updates_existing_entry(session_factory):
    """stage_add acts as an upsert: re-adding the same task_id replaces its run_at."""
    qca = SQLQueueConfigAdapter(session_factory)
    scheduler = SQLTaskScheduler(session_factory, qca.get_all_queues)
    task = make_task()
    future = dt.datetime(2099, 1, 1, tzinfo=dt.UTC)
    await schedule(scheduler, task, future)

    # Re-schedule to PAST — next_due should now return it
    await schedule(scheduler, task, PAST)

    result = await scheduler.next_due(["default"])
    assert result is not None
    assert result.id == task.id


@pytest.mark.asyncio
async def test_stage_remove_deletes_entry(session_factory):
    """stage_remove removes the row; get_run_at returns None afterwards."""
    qca = SQLQueueConfigAdapter(session_factory)
    scheduler = SQLTaskScheduler(session_factory, qca.get_all_queues)
    task = make_task()
    await schedule(scheduler, task, PAST)

    pipe = scheduler.pipeline(transaction=True)
    scheduler.stage_remove(pipe, task.id, task.queue)
    await pipe.execute()

    assert await scheduler.get_run_at(task.id) is None
    assert await scheduler.next_due(["default"]) is None
