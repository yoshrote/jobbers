"""
Contract tests for TaskSubmitProtocol implementations.

Runs against all three backends via the ``task_adapter`` fixture (yields a
``(state, submit)`` pair).  Assertions use ``state`` only to verify that the
submit operation left the expected data; the operations under test are always
called on ``submit``.
"""

import datetime as dt

import pytest
from ulid import ULID

from jobbers.adapters.sql import SQLTaskSubmit
from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")


def make_task(
    task_id: ULID = ULID1,
    name: str = "my_task",
    version: int = 1,
    queue: str = "default",
    status: TaskStatus = TaskStatus.SUBMITTED,
    submitted_at: dt.datetime = FROZEN_TIME,
) -> Task:
    task = Task(id=task_id, name=name, version=version, queue=queue, status=status)
    task.submitted_at = submitted_at
    return task


# ── submit_task ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_task(task_adapter):
    """submit_task enqueues the task and persists its data."""
    state, submit = task_adapter
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    task.set_status(TaskStatus.SUBMITTED)
    await submit.submit_task(task)

    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.name == "Test Task"
    assert saved.status == TaskStatus.SUBMITTED
    assert saved.submitted_at == task.submitted_at
    assert await state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_submit_task_twice_updates_only(task_adapter):
    """Submitting the same task ID twice updates the data without duplicating the queue entry."""
    state, submit = task_adapter
    task = Task(id=ULID1, name="Initial Task", status="unsubmitted")
    task.set_status(TaskStatus.SUBMITTED)
    await submit.submit_task(task)

    updated = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await submit.submit_task(updated)

    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.name == "Updated Task"
    assert saved.status == TaskStatus.COMPLETED
    assert saved.submitted_at == task.submitted_at
    assert len(await state.get_all_tasks(TaskPagination(queue=task.queue))) == 1


# ── get_next_task ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_task_returns_submitted_task(task_adapter):
    """get_next_task pops and returns the next available task, leaving the queue empty."""
    state, submit = task_adapter
    await submit.submit_task(make_task())

    result = await submit.get_next_task(queues={"default"}, pop_timeout=1)
    assert result is not None
    assert result.id == ULID1

    result2 = await submit.get_next_task(queues={"default"}, pop_timeout=1)
    assert result2 is None


# ── submit_rate_limited_task ──────────────────────────────────────────────────


def _rate_limited_queue(name: str = "default") -> QueueConfig:
    return QueueConfig(name=name, rate_numerator=5, rate_denominator=1, rate_period=RatePeriod.MINUTE)


@pytest.mark.asyncio
async def test_submit_rate_limited_task_enqueues_and_stores(task_adapter):
    """submit_rate_limited_task enqueues the task and stores it when the rate limit is not exceeded."""
    state, submit = task_adapter
    if isinstance(submit, SQLTaskSubmit):
        pytest.xfail("SQLTaskSubmit does not support rate-limited submission")
    task = make_task()
    task.set_status(TaskStatus.SUBMITTED)
    queue_config = _rate_limited_queue()

    result = await submit.submit_rate_limited_task(task, queue_config)

    assert result is True
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_submit_rate_limited_task_rejects_when_limit_reached(task_adapter):
    """submit_rate_limited_task returns False when the per-period rate limit is exhausted."""
    state, submit = task_adapter
    if isinstance(submit, SQLTaskSubmit):
        pytest.xfail("SQLTaskSubmit does not support rate-limited submission")
    queue_config = QueueConfig(
        name="default", rate_numerator=1, rate_denominator=1, rate_period=RatePeriod.MINUTE
    )
    first = make_task(task_id=ULID1)
    first.set_status(TaskStatus.SUBMITTED)
    await submit.submit_rate_limited_task(first, queue_config)

    second = make_task(task_id=ULID2, submitted_at=FROZEN_TIME)
    second.set_status(TaskStatus.SUBMITTED)
    result = await submit.submit_rate_limited_task(second, queue_config)

    assert result is False


# ── clean_rate_limiter ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_rate_limiter_removes_expired_entries(task_adapter):
    """clean_rate_limiter removes rate-limit entries older than the configured age."""
    state, submit = task_adapter
    if isinstance(submit, SQLTaskSubmit):
        pytest.xfail("SQLTaskSubmit.clean_rate_limiter is a no-op")
    queue_config = _rate_limited_queue()
    task = make_task()
    task.set_status(TaskStatus.SUBMITTED)
    await submit.submit_rate_limited_task(task, queue_config)

    far_future = FROZEN_TIME + dt.timedelta(hours=2)
    await submit.clean_rate_limiter({b"default"}, now=far_future, rate_limit_age=dt.timedelta(hours=1))

    tasks_in_queue = await state.get_all_tasks(TaskPagination(queue="default"))
    assert len(tasks_in_queue) == 1
