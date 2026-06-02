"""
Implementation-specific tests for RedisJSONTaskState and RedisJSONDeadQueue.

Uses the ``json_adapter`` and ``json_dead_queue`` fixtures backed by a real
Redis Stack instance; tests skip automatically when Redis Stack is unavailable.

Contract tests live in test_task_state_common.py, test_task_submit_common.py,
and test_dead_queue_common.py and run against all adapter implementations.
"""

import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import ResponseError
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
        id=task_id, name=name, version=version, queue=queue, status=TaskStatus.FAILED, errors=["error"]
    )


async def _add_to_dlq(dq, task: Task, failed_at: dt.datetime) -> None:
    pipe = dq.data_store.pipeline(transaction=True)
    dq.stage_add(pipe, task, failed_at)
    await pipe.execute()


# ── RedisJSONTaskState ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_none_json_blob(json_adapter):
    """
    A task:* key whose JSON document is null is silently skipped.

    Covers the ``if task_data is None: continue`` guard in
    RedisJSONTaskState.clean_terminal_tasks; triggered deterministically by
    storing a null root document.
    """
    state, submit = json_adapter
    await state.data_store.json().set(f"task:{ULID1}", "$", None)
    await state.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))


@pytest.mark.asyncio
async def test_get_all_tasks_missing_blob_is_skipped(json_adapter):
    """
    _add_task_to_results silently drops a task_id when get_task returns None.

    Simulates a race window where RediSearch holds the document reference but the
    underlying key has already been deleted.
    """
    state, submit = json_adapter
    await submit.submit_task(make_task())
    with patch.object(state, "get_task", new_callable=AsyncMock, return_value=None):
        results = await state.get_all_tasks(TaskPagination(queue="default"))
    assert results == []


@pytest.mark.asyncio
async def test_ensure_index_reraises_non_duplicate_error(json_adapter):
    """ensure_index re-raises ResponseError when the message is not 'duplicate'."""
    state, submit = json_adapter
    mock_search = MagicMock()
    mock_search.info = AsyncMock(return_value={"attributes": []})
    mock_search.alter_schema_add = AsyncMock(side_effect=ResponseError("ERR: unknown field type"))
    with patch.object(state.data_store, "ft", return_value=mock_search):
        with pytest.raises(ResponseError, match="unknown field type"):
            await state.ensure_index()


# ── RedisJSONDeadQueue ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_by_filter_skips_missing_task_blob(json_dead_queue):
    """
    get_by_filter silently skips a DLQ entry whose task blob is absent.

    Covers the ``if task is None: continue`` branch in
    RedisJSONDeadQueue.get_by_filter.
    """
    task = make_failed_task()
    await _add_to_dlq(json_dead_queue, task, FAILED_AT)
    assert await json_dead_queue.get_by_filter() == []


@pytest.mark.asyncio
async def test_get_by_filter_sorted_by_failed_at_desc(json_dead_queue, json_adapter):
    """get_by_filter returns results sorted by failed_at descending (most recent first)."""
    state, submit = json_adapter
    earlier = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
    later = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
    t1 = make_failed_task(task_id=ULID1)
    t2 = make_failed_task(task_id=ULID2)
    await state.save_task(t1)
    await state.save_task(t2)
    await _add_to_dlq(json_dead_queue, t1, earlier)
    await _add_to_dlq(json_dead_queue, t2, later)
    results = await json_dead_queue.get_by_filter()
    assert len(results) == 2
    assert results[0].id == t2.id  # most recent first
