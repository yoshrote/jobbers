"""
SQL-specific tests for SQLTaskState and SQLTaskSubmit.

Tests the atomic pipeline path (stage_* methods via SQLTransactionBatch),
verified through the public saga API rather than raw DB queries.
"""

import datetime as dt

import pytest
from ulid import ULID

from jobbers.adapters.sql import SQLTaskState, SQLTaskSubmit
from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")
ULID3 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG03")


def make_task(
    task_id: ULID = ULID1,
    name: str = "my_task",
    queue: str = "default",
    status: TaskStatus = TaskStatus.SUBMITTED,
    submitted_at: dt.datetime = FROZEN_TIME,
) -> Task:
    task = Task(id=task_id, name=name, queue=queue, status=status)
    task.submitted_at = submitted_at
    return task


# ── stage_submit_task ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_submit_task_persists_and_retrieves(session_factory):
    """stage_submit_task inside a SQLTransactionBatch persists the task."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    task = make_task()
    pipe = adapter.pipeline(transaction=True)
    adapter.stage_submit_task(pipe, task)
    await pipe.execute()

    saved = await adapter.get_task(ULID1)
    assert saved is not None
    assert saved.id == ULID1
    assert saved.name == "my_task"


@pytest.mark.asyncio
async def test_stage_submit_task_appears_in_queue(session_factory):
    """A task staged via stage_submit_task is returned by get_all_tasks."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    task = make_task()
    pipe = adapter.pipeline(transaction=True)
    adapter.stage_submit_task(pipe, task)
    await pipe.execute()

    results = await adapter.get_all_tasks(TaskPagination(queue="default"))
    assert any(t.id == ULID1 for t in results)


# ── stage_requeue ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_requeue_persists_and_retrieves(session_factory):
    """stage_requeue inside a SQLTransactionBatch persists the task."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    task = make_task()
    pipe = adapter.pipeline(transaction=True)
    adapter.stage_requeue(pipe, task)
    await pipe.execute()

    saved = await adapter.get_task(ULID1)
    assert saved is not None
    assert saved.id == ULID1


# ── stage_remove_from_queue ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_remove_from_queue_removes_from_queue(session_factory):
    """stage_remove_from_queue removes the task from task_queue; tasks record is preserved."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    task = make_task()
    await adapter_submit.submit_task(task)

    pipe = adapter.pipeline(transaction=True)
    adapter.stage_remove_from_queue(pipe, task)
    await pipe.execute()

    assert await adapter.task_exists(ULID1)  # historic record survives
    assert await adapter_submit.get_next_task(queues={"default"}, pop_timeout=1) is None  # no longer queued


# ── stage_save ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_save_upserts(session_factory):
    """stage_save called twice with an updated name reflects the latest version."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    task = make_task()
    pipe = adapter.pipeline(transaction=True)
    adapter.stage_save(pipe, task)
    await pipe.execute()

    task.name = "updated_name"
    pipe = adapter.pipeline(transaction=True)
    adapter.stage_save(pipe, task)
    await pipe.execute()

    saved = await adapter.get_task(ULID1)
    assert saved is not None
    assert saved.name == "updated_name"


# ── stage_init_fan_in ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_init_fan_in_creates_members(session_factory):
    """stage_init_fan_in via SQLTransactionBatch → get_fan_in_members returns the set."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    fan_in_key = "fan-in:sql-test"
    predecessor_ids = {ULID1, ULID2, ULID3}

    pipe = adapter.pipeline(transaction=True)
    adapter.stage_init_fan_in(pipe, fan_in_key, predecessor_ids)
    await pipe.execute()

    result = await adapter.get_fan_in_members(fan_in_key)
    assert set(result) == predecessor_ids


# ── transaction atomicity ─────────────────────────────────────────────────────


# ── clean (SQL deletes rows, so get_all_tasks reflects it) ───────────────────


@pytest.mark.asyncio
async def test_clean_max_queue_age_removes_old_tasks(session_factory):
    """clean(max_queue_age) removes tasks submitted at or before the cutoff."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    old = make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=2))
    recent = make_task(ULID2, submitted_at=FROZEN_TIME)
    await adapter_submit.submit_task(old)
    await adapter_submit.submit_task(recent)

    cutoff = FROZEN_TIME - dt.timedelta(days=1)
    await adapter.clean(queues={b"default"}, now=FROZEN_TIME, max_queue_age=cutoff)

    ids = {t.id for t in await adapter.get_all_tasks(TaskPagination(queue="default"))}
    assert ULID1 not in ids
    assert ULID2 in ids


@pytest.mark.asyncio
async def test_clean_min_queue_age_removes_recent_tasks(session_factory):
    """clean(min_queue_age) removes tasks submitted at or after the floor."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    old = make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=10))
    recent = make_task(ULID2, submitted_at=FROZEN_TIME)
    await adapter_submit.submit_task(old)
    await adapter_submit.submit_task(recent)

    floor = FROZEN_TIME - dt.timedelta(days=1)
    await adapter.clean(queues={b"default"}, now=FROZEN_TIME, min_queue_age=floor)

    ids = {t.id for t in await adapter.get_all_tasks(TaskPagination(queue="default"))}
    assert ULID1 in ids
    assert ULID2 not in ids


@pytest.mark.asyncio
async def test_clean_noop_when_no_age_params(session_factory):
    """clean() with no age params is a no-op; all tasks remain."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    task = make_task()
    await adapter_submit.submit_task(task)

    await adapter.clean(queues={b"default"}, now=dt.datetime.now(dt.UTC))

    assert await adapter.task_exists(ULID1)


# ── transaction atomicity ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_multiple_stages_commit_atomically(session_factory):
    """All ops staged in one pipeline are committed together."""
    adapter = SQLTaskState(session_factory)
    adapter_submit = SQLTaskSubmit(session_factory)
    t1 = make_task(ULID1, name="task_a")
    t2 = make_task(ULID2, name="task_b")

    pipe = adapter.pipeline(transaction=True)
    adapter.stage_submit_task(pipe, t1)
    adapter.stage_submit_task(pipe, t2)
    await pipe.execute()

    results = await adapter.get_all_tasks(TaskPagination(queue="default"))
    ids = {t.id for t in results}
    assert ULID1 in ids
    assert ULID2 in ids
