"""
JsonTaskAdapter- and JsonDeadQueue-specific edge cases not covered by protocol contract tests.

Contract tests live in test_task_adapter_common.py and test_dead_queue_common.py
and run against all adapter implementations via parametrized fixtures.
"""

import datetime as dt
from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
FAILED_AT = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
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


def make_failed_task(
    task_id: ULID = ULID1,
    name: str = "my_task",
    version: int = 1,
    queue: str = "default",
) -> Task:
    return Task(
        id=task_id,
        name=name,
        version=version,
        queue=queue,
        status=TaskStatus.FAILED,
        errors=["error"],
    )


async def add_to_dlq(dq, task: Task, failed_at: dt.datetime) -> None:
    pipe = dq.data_store.pipeline(transaction=True)
    dq.stage_add(pipe, task, failed_at)
    await pipe.execute()


# ── clean_terminal_tasks: null blob ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_none_json_blob(json_adapter):
    """
    A task:* key whose JSON document is null is silently skipped.

    Covers the ``if task_data is None: continue`` guard in JsonTaskAdapter.clean_terminal_tasks.
    In production this guard protects against race conditions (key deleted between scan and
    JSON.GET), but can be triggered deterministically by storing a null root document.
    """
    key = f"task:{ULID1}"
    await json_adapter.data_store.json().set(key, "$", None)
    # Should not raise even though the JSON value is null
    await json_adapter.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))


# ── get_all_tasks: missing blob (race-condition guard) ────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_missing_blob_is_skipped(json_adapter):
    """
    _add_task_to_results silently drops a task_id when get_task returns None.

    In production this covers a race window where RediSearch still holds the
    document reference but the underlying key has been deleted.  We simulate it
    by patching get_task to return None after the search index has been populated.
    """
    task = make_task()
    await json_adapter.submit_task(task)

    with patch.object(json_adapter, "get_task", new_callable=AsyncMock, return_value=None):
        results = await json_adapter.get_all_tasks(TaskPagination(queue="default"))

    assert results == []


# ── JsonDeadQueue.get_by_filter: limit break and null task ────────────────────


@pytest.mark.asyncio
async def test_get_by_filter_skips_missing_task_blob(json_dead_queue):
    """
    get_by_filter silently skips a DLQ entry whose task blob is absent.

    Covers the ``if task is None: continue`` branch in JsonDeadQueue.get_by_filter.
    """
    task = make_failed_task()
    # Add to DLQ without saving the task blob — get_tasks_bulk will return [None]
    await add_to_dlq(json_dead_queue, task, FAILED_AT)

    results = await json_dead_queue.get_by_filter()
    assert results == []


# ── JsonDeadQueue: ordering ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_by_filter_sorted_by_failed_at_desc(json_dead_queue, json_adapter):
    """JsonDeadQueue returns get_by_filter results sorted by failed_at descending (most recent first)."""
    earlier = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
    later = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
    t1 = make_failed_task(task_id=ULID1)
    t2 = make_failed_task(task_id=ULID2)
    await json_adapter.save_task(t1)
    await json_adapter.save_task(t2)
    await add_to_dlq(json_dead_queue, t1, earlier)
    await add_to_dlq(json_dead_queue, t2, later)

    results = await json_dead_queue.get_by_filter()
    assert len(results) == 2
    assert results[0].id == t2.id  # LATER first
