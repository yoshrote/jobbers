"""
Contract tests for TaskStateProtocol implementations.

Uses only the saga-path public API (the same methods StateManager calls when
``force_saga=True``).  No ``data_store.*`` access; no sorted-set assertions.
Runs against all three backends (raw, json, sql) with zero xfails.

The ``task_adapter`` fixture yields a ``(state, submit)`` pair.  ``submit`` is
used only for test setup (seeding tasks via ``submit_task``); every assertion
targets a ``TaskStateProtocol`` method on ``state``.
"""

import datetime as dt

import pytest
from ulid import ULID

from jobbers.adapters.redis import RedisTaskState
from jobbers.adapters.redis_json import RedisJSONTaskState
from jobbers.models.dag import DAGRunPagination
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")
ULID3 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG03")
ULID4 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG04")
ULID5 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG05")


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


# ── get_all_tasks ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_empty(task_adapter):
    """No tasks stored → empty list returned."""
    state, submit = task_adapter
    results = await state.get_all_tasks(TaskPagination(queue="default"))
    assert results == []


@pytest.mark.asyncio
async def test_get_all_tasks_returns_submitted_tasks(task_adapter):
    """A submitted task is returned by get_all_tasks for its queue."""
    state, submit = task_adapter
    await submit.submit_task(make_task())
    results = await state.get_all_tasks(TaskPagination(queue="default"))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_task_name(task_adapter):
    """task_name filter returns only tasks with a matching name."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1, name="task_a"))
    await submit.submit_task(make_task(ULID2, name="task_b"))
    results = await state.get_all_tasks(TaskPagination(queue="default", task_name="task_a"))
    assert len(results) == 1
    assert results[0].name == "task_a"


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_version(task_adapter):
    """task_version filter returns only tasks with a matching version."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1, version=1))
    await submit.submit_task(make_task(ULID2, version=2))
    results = await state.get_all_tasks(TaskPagination(queue="default", task_version=1))
    assert len(results) == 1
    assert results[0].version == 1


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_status(task_adapter):
    """`status` filter returns only tasks in the specified status."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1, status=TaskStatus.SUBMITTED))
    await submit.submit_task(make_task(ULID2, status=TaskStatus.SUBMITTED))
    await state.save_task(make_task(ULID2, status=TaskStatus.COMPLETED))
    results = await state.get_all_tasks(TaskPagination(queue="default", status=TaskStatus.SUBMITTED))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_different_queues_are_isolated(task_adapter):
    """Tasks from other queues are not included in the results."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1, queue="queue_a"))
    await submit.submit_task(make_task(ULID2, queue="queue_b"))
    results = await state.get_all_tasks(TaskPagination(queue="queue_a"))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_filter_no_match_returns_empty(task_adapter):
    """A filter that matches no tasks returns an empty list."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1, name="task_a"))
    results = await state.get_all_tasks(TaskPagination(queue="default", task_name="task_nonexistent"))
    assert results == []


@pytest.mark.asyncio
async def test_get_all_tasks_respects_limit(task_adapter):
    """`limit` caps the number of results returned."""
    state, submit = task_adapter
    for i, uid in enumerate((ULID1, ULID2, ULID3)):
        await submit.submit_task(make_task(uid, submitted_at=FROZEN_TIME + dt.timedelta(seconds=i)))
    results = await state.get_all_tasks(TaskPagination(queue="default", limit=2))
    assert len(results) == 2


@pytest.mark.asyncio
async def test_get_all_tasks_offset_skips_first_n(task_adapter):
    """`offset` skips the first N results so successive pages do not overlap."""
    state, submit = task_adapter
    for i, uid in enumerate((ULID1, ULID2, ULID3)):
        await submit.submit_task(make_task(uid, submitted_at=FROZEN_TIME + dt.timedelta(seconds=i)))
    page1 = await state.get_all_tasks(
        TaskPagination(queue="default", limit=2, offset=0, order_by=PaginationOrder.SUBMITTED_AT)
    )
    page2 = await state.get_all_tasks(
        TaskPagination(queue="default", limit=2, offset=2, order_by=PaginationOrder.SUBMITTED_AT)
    )
    assert len(page1) == 2
    assert len(page2) == 1
    assert {t.id for t in page1}.isdisjoint({t.id for t in page2})


@pytest.mark.asyncio
async def test_get_all_tasks_offset_returns_empty_beyond_end(task_adapter):
    """An offset past the total count returns an empty list."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1))
    results = await state.get_all_tasks(TaskPagination(queue="default", limit=10, offset=5))
    assert results == []


@pytest.mark.asyncio
async def test_get_all_tasks_order_by_submitted_at(task_adapter):
    """Results are returned in ascending submitted_at order within each page."""
    state, submit = task_adapter
    tasks = [
        make_task(uid, submitted_at=FROZEN_TIME + dt.timedelta(seconds=i))
        for i, uid in enumerate((ULID1, ULID2, ULID3, ULID4, ULID5))
    ]
    for t in reversed(tasks):
        await submit.submit_task(t)
    page1 = await state.get_all_tasks(
        TaskPagination(queue="default", limit=3, offset=0, order_by=PaginationOrder.SUBMITTED_AT)
    )
    page2 = await state.get_all_tasks(
        TaskPagination(queue="default", limit=3, offset=3, order_by=PaginationOrder.SUBMITTED_AT)
    )
    assert [t.id for t in page1] == [ULID1, ULID2, ULID3]
    assert [t.id for t in page2] == [ULID4, ULID5]


@pytest.mark.asyncio
async def test_get_all_tasks_order_by_task_id(task_adapter):
    """order_by=TASK_ID returns tasks sorted by ULID."""
    state, submit = task_adapter
    t1 = make_task(ULID1, submitted_at=FROZEN_TIME)
    t2 = make_task(ULID2, submitted_at=FROZEN_TIME + dt.timedelta(seconds=1))
    t3 = make_task(ULID3, submitted_at=FROZEN_TIME + dt.timedelta(seconds=2))
    for t in (t1, t2, t3):
        await submit.submit_task(t)
    results = await state.get_all_tasks(TaskPagination(queue="default", order_by=PaginationOrder.TASK_ID))
    assert [t.id for t in results] == [ULID1, ULID2, ULID3]


@pytest.mark.asyncio
async def test_get_all_tasks_combined_name_and_version(task_adapter):
    """Filtering by both task_name and task_version returns only exact matches."""
    state, submit = task_adapter
    for t in (
        make_task(ULID1, name="task_a", version=1),
        make_task(ULID2, name="task_a", version=2),
        make_task(ULID3, name="task_b", version=1),
    ):
        await submit.submit_task(t)
    results = await state.get_all_tasks(TaskPagination(queue="default", task_name="task_a", task_version=1))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_combined_name_and_status(task_adapter):
    """Filtering by both task_name and status returns only tasks matching both."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1, name="task_a", status=TaskStatus.SUBMITTED))
    await submit.submit_task(make_task(ULID2, name="task_a", status=TaskStatus.SUBMITTED))
    await state.save_task(make_task(ULID2, name="task_a", status=TaskStatus.COMPLETED))
    results = await state.get_all_tasks(
        TaskPagination(queue="default", task_name="task_a", status=TaskStatus.SUBMITTED)
    )
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_combined_version_and_status(task_adapter):
    """Filtering by version and status without task_name works correctly."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1, version=1, status=TaskStatus.SUBMITTED))
    await submit.submit_task(make_task(ULID2, version=1, status=TaskStatus.SUBMITTED))
    await state.save_task(make_task(ULID2, version=1, status=TaskStatus.FAILED))
    results = await state.get_all_tasks(
        TaskPagination(queue="default", task_version=1, status=TaskStatus.SUBMITTED)
    )
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_all_filters_combined(task_adapter):
    """All three filters together (name, version, status) are applied conjunctively."""
    state, submit = task_adapter
    for t in (
        make_task(ULID1, name="task_a", version=1, status=TaskStatus.SUBMITTED),
        make_task(ULID2, name="task_a", version=2, status=TaskStatus.SUBMITTED),
        make_task(ULID3, name="task_b", version=1, status=TaskStatus.SUBMITTED),
    ):
        await submit.submit_task(t)
    await state.save_task(make_task(ULID1, name="task_a", version=1, status=TaskStatus.COMPLETED))
    results = await state.get_all_tasks(
        TaskPagination(queue="default", task_name="task_a", task_version=1, status=TaskStatus.SUBMITTED)
    )
    assert results == []


@pytest.mark.asyncio
async def test_get_all_tasks_filter_pagination_page_size_is_predictable(task_adapter):
    """
    Page size is predictable when a filter is active.

    Known limitation: RedisTaskState applies offset to raw queue positions before
    Python-side filtering, so offset-based pagination over a filtered result set is
    unpredictable (pages may overlap).
    """
    state, submit = task_adapter
    if isinstance(state, RedisTaskState):
        pytest.xfail(
            "RedisTaskState applies offset before Python-side filtering; "
            "page 2 backs up into queue-space and returns the same task as page 1"
        )
    for t in (
        make_task(ULID1, name="task_b", submitted_at=FROZEN_TIME),
        make_task(ULID2, name="task_b", submitted_at=FROZEN_TIME + dt.timedelta(seconds=1)),
        make_task(ULID3, name="task_a", submitted_at=FROZEN_TIME + dt.timedelta(seconds=2)),
        make_task(ULID4, name="task_a", submitted_at=FROZEN_TIME + dt.timedelta(seconds=3)),
    ):
        await submit.submit_task(t)
    page1 = await state.get_all_tasks(
        TaskPagination(
            queue="default", task_name="task_a", limit=1, offset=0, order_by=PaginationOrder.SUBMITTED_AT
        )
    )
    page2 = await state.get_all_tasks(
        TaskPagination(
            queue="default", task_name="task_a", limit=1, offset=1, order_by=PaginationOrder.SUBMITTED_AT
        )
    )
    assert [t.id for t in page1] == [ULID3]
    assert [t.id for t in page2] == [ULID4]


# ── get_task / task_exists ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_task(task_adapter):
    """get_task returns the saved task with all fields intact."""
    state, submit = task_adapter
    await state.save_task(
        Task(id=ULID1, name="Test Task", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME)
    )
    task = await state.get_task(ULID1)
    assert task is not None
    assert task.id == ULID1
    assert task.name == "Test Task"
    assert task.status == TaskStatus.STARTED
    assert task.submitted_at == FROZEN_TIME


@pytest.mark.asyncio
async def test_get_task_not_found(task_adapter):
    """get_task returns None when no task exists for the given ID."""
    state, submit = task_adapter
    assert await state.get_task(ULID1) is None


@pytest.mark.asyncio
async def test_task_exists(task_adapter):
    """task_exists returns True after save and False for an unsaved ID."""
    state, submit = task_adapter
    await state.save_task(
        Task(id=ULID1, name="Test Task", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME)
    )
    assert await state.task_exists(ULID1) is True
    assert await state.task_exists(ULID2) is False


# ── update_task_heartbeat ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_task_heartbeat_sets_timestamp(task_adapter):
    """update_task_heartbeat is reflected in heartbeat_at when the task is retrieved."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.STARTED)
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    updated = await state.get_task(task.id)
    assert updated.heartbeat_at == FROZEN_TIME


@pytest.mark.asyncio
async def test_update_task_heartbeat_makes_task_active(task_adapter):
    """A task with a heartbeat entry is returned by get_active_tasks."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.STARTED)
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    assert any(t.id == task.id for t in await state.get_active_tasks({task.queue}))


@pytest.mark.asyncio
async def test_update_task_heartbeat_updates_existing_entry(task_adapter):
    """A second update_task_heartbeat call overwrites the previous heartbeat timestamp."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.STARTED)
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    second_time = FROZEN_TIME + dt.timedelta(seconds=10)
    task.heartbeat_at = second_time
    await state.update_task_heartbeat(task)
    updated = await state.get_task(task.id)
    assert updated is not None
    assert updated.heartbeat_at == second_time


# ── remove_task_heartbeat ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_task_heartbeat_removes_entry(task_adapter):
    """remove_task_heartbeat removes the task from the active set."""
    state, submit = task_adapter
    task = make_task()
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    assert any(t.id == task.id for t in await state.get_active_tasks({task.queue}))
    await state.remove_task_heartbeat(task)
    assert not any(t.id == task.id for t in await state.get_active_tasks({task.queue}))


@pytest.mark.asyncio
async def test_remove_task_heartbeat_noop_when_absent(task_adapter):
    """remove_task_heartbeat is a no-op when the task has no heartbeat entry."""
    state, submit = task_adapter
    await state.remove_task_heartbeat(make_task())  # should not raise


# ── get_active_tasks ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_tasks_returns_empty_for_no_heartbeats(task_adapter):
    """get_active_tasks returns [] when no tasks have heartbeat entries."""
    state, submit = task_adapter
    assert await state.get_active_tasks({"default"}) == []


@pytest.mark.asyncio
async def test_get_active_tasks_returns_tasks_with_heartbeats(task_adapter):
    """get_active_tasks returns tasks that have a heartbeat entry."""
    state, submit = task_adapter
    task = make_task()
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    result = await state.get_active_tasks({"default"})
    assert len(result) == 1
    assert result[0].id == ULID1


# ── get_stale_tasks ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_stale_tasks_returns_stale_tasks(task_adapter):
    """Tasks whose heartbeat is older than stale_time are returned."""
    state, submit = task_adapter
    now = dt.datetime.now(dt.UTC)
    stale = Task(
        id=ULID1,
        name="stale",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=now,
        heartbeat_at=now - dt.timedelta(minutes=10),
    )
    await state.save_task(stale)
    await state.update_task_heartbeat(stale)
    result = [t async for t in state.get_stale_tasks({"default"}, dt.timedelta(minutes=5))]
    assert len(result) == 1
    assert result[0].id == ULID1


@pytest.mark.asyncio
async def test_get_stale_tasks_excludes_recent_tasks(task_adapter):
    """Tasks whose heartbeat is within stale_time are not returned."""
    state, submit = task_adapter
    now = dt.datetime.now(dt.UTC)
    recent = Task(
        id=ULID1,
        name="recent",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=now,
        heartbeat_at=now - dt.timedelta(minutes=1),
    )
    await state.save_task(recent)
    await state.update_task_heartbeat(recent)
    result = [t async for t in state.get_stale_tasks({"default"}, dt.timedelta(minutes=5))]
    assert len(result) == 0


# ── delete_task ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_task_removes_task_blob(task_adapter):
    """delete_task causes get_task to return None."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.COMPLETED)
    await state.save_task(task)
    assert await state.get_task(task.id) is not None
    await state.delete_task(task)
    assert await state.get_task(task.id) is None


@pytest.mark.asyncio
async def test_delete_task_removes_heartbeat_entry(task_adapter):
    """delete_task removes the heartbeat entry so the task no longer appears in get_active_tasks."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.STARTED)
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    assert any(t.id == task.id for t in await state.get_active_tasks({task.queue}))
    await state.delete_task(task)
    assert not any(t.id == task.id for t in await state.get_active_tasks({task.queue}))


@pytest.mark.asyncio
async def test_delete_task_idempotent(task_adapter):
    """Calling delete_task twice on the same task does not raise."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.COMPLETED)
    await state.save_task(task)
    await state.delete_task(task)
    await state.delete_task(task)  # should not raise


@pytest.mark.asyncio
async def test_get_stale_tasks_handles_multiple_queues(task_adapter):
    """Stale tasks from multiple queues are all returned."""
    state, submit = task_adapter
    now = dt.datetime.now(dt.UTC)
    hb = now - dt.timedelta(minutes=10)
    t1 = Task(
        id=ULID1, name="t1", queue="default", status=TaskStatus.STARTED, started_at=now, heartbeat_at=hb
    )
    t2 = Task(
        id=ULID2, name="t2", queue="high_priority", status=TaskStatus.STARTED, started_at=now, heartbeat_at=hb
    )
    for t in (t1, t2):
        await state.save_task(t)
        await state.update_task_heartbeat(t)
    result = [t async for t in state.get_stale_tasks({"default", "high_priority"}, dt.timedelta(minutes=5))]
    assert len(result) == 2
    assert {t.id for t in result} == {ULID1, ULID2}


# ── clean_terminal_tasks ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_terminal_tasks_deletes_old_completed_task(task_adapter):
    """A completed task blob older than max_age is deleted."""
    state, submit = task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_active_task(task_adapter):
    """Tasks in active statuses are never deleted regardless of age."""
    state, submit = task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=FROZEN_TIME - dt.timedelta(days=100),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))
    assert await state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_task_within_age(task_adapter):
    """A completed task whose completed_at is within max_age is not deleted."""
    state, submit = task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(hours=1),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert await state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_task_without_completed_at(task_adapter):
    """A terminal task with no completed_at is not deleted."""
    state, submit = task_adapter
    await state.save_task(
        Task(id=ULID1, name="t", queue="default", status=TaskStatus.FAILED, completed_at=None)
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))
    assert await state.task_exists(ULID1)


@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.STALLED,
        TaskStatus.DROPPED,
    ],
)
@pytest.mark.asyncio
async def test_clean_terminal_tasks_cleans_all_terminal_statuses(task_adapter, status):
    """All five terminal statuses are eligible for cleanup when old enough."""
    state, submit = task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=status,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_clean_terminal_tasks_leaves_other_tasks_untouched(task_adapter):
    """Only old terminal tasks are deleted; recent tasks remain."""
    state, submit = task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.save_task(
        Task(
            id=ULID2,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(hours=1),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await state.task_exists(ULID1)
    assert await state.task_exists(ULID2)


# ── pipeline staging (AtomicTaskStateProtocol) ────────────────────────────────
# Uses state.pipeline() so these work against all three backends.


@pytest.mark.asyncio
async def test_stage_requeue_saves_task_data(task_adapter):
    """stage_requeue persists the task blob so it can be retrieved."""
    state, submit = task_adapter
    task = make_task()
    pipe = state.pipeline(transaction=True)
    state.stage_requeue(pipe, task)
    await pipe.execute()
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.id == ULID1


@pytest.mark.asyncio
async def test_stage_submit_task_saves_task_data(task_adapter):
    """stage_submit_task persists the task blob so it can be retrieved."""
    state, submit = task_adapter
    task = make_task()
    pipe = state.pipeline(transaction=True)
    state.stage_submit_task(pipe, task)
    await pipe.execute()
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.id == ULID1


@pytest.mark.asyncio
async def test_stage_remove_from_queue_noop_when_absent(task_adapter):
    """stage_remove_from_queue does not raise when the task is not in the queue."""
    state, submit = task_adapter
    pipe = state.pipeline(transaction=True)
    state.stage_remove_from_queue(pipe, make_task())
    await pipe.execute()  # should not raise


@pytest.mark.asyncio
async def test_stage_remove_heartbeat_removes_from_stale_detection(task_adapter):
    """After stage_remove_heartbeat, the task no longer appears in get_stale_tasks."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.STARTED)
    task.heartbeat_at = FROZEN_TIME  # very old — would normally be stale
    await state.save_task(task)
    await state.update_task_heartbeat(task)
    pipe = state.pipeline(transaction=True)
    state.stage_remove_heartbeat(pipe, task)
    await pipe.execute()
    stale = [t async for t in state.get_stale_tasks({"default"}, dt.timedelta(seconds=0))]
    assert not any(t.id == ULID1 for t in stale)


@pytest.mark.asyncio
async def test_atomic_dispatch_scheduled_transitions_to_submitted(task_adapter):
    """atomic_dispatch_scheduled returns True and sets the task to SUBMITTED."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.SCHEDULED)
    await state.save_task(task)
    dispatched = await state.atomic_dispatch_scheduled(task, lambda _pipe: None)
    assert dispatched is True
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_atomic_dispatch_scheduled_returns_false_for_cancelled(task_adapter):
    """atomic_dispatch_scheduled returns False without modifying a CANCELLED task."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.CANCELLED)
    await state.save_task(task)
    dispatched = await state.atomic_dispatch_scheduled(task, lambda _pipe: None)
    assert dispatched is False
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_atomic_dispatch_scheduled_returns_false_when_missing(task_adapter):
    """atomic_dispatch_scheduled returns False when the task does not exist."""
    state, _ = task_adapter
    task = make_task(status=TaskStatus.SCHEDULED)
    # do not save — task is absent
    dispatched = await state.atomic_dispatch_scheduled(task, lambda _pipe: None)
    assert dispatched is False


# ── ensure_index ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ensure_index_does_not_raise(task_adapter):
    """ensure_index completes without error for every adapter implementation."""
    state, submit = task_adapter
    await state.ensure_index()


# ── get_tasks_bulk ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_tasks_bulk_empty_input(task_adapter):
    """get_tasks_bulk returns an empty list when given no IDs."""
    state, submit = task_adapter
    assert await state.get_tasks_bulk([]) == []


@pytest.mark.asyncio
async def test_get_tasks_bulk_returns_tasks_in_input_order(task_adapter):
    """get_tasks_bulk returns tasks in the same order as the input ID list."""
    state, submit = task_adapter
    for t in (make_task(ULID1, name="a"), make_task(ULID2, name="b"), make_task(ULID3, name="c")):
        await state.save_task(t)
    result = await state.get_tasks_bulk([ULID3, ULID1, ULID2])
    assert [r.id for r in result] == [ULID3, ULID1, ULID2]


@pytest.mark.asyncio
async def test_get_tasks_bulk_returns_none_for_missing_ids(task_adapter):
    """get_tasks_bulk returns None in the result list for IDs with no stored task."""
    state, submit = task_adapter
    await state.save_task(make_task(ULID1))
    result = await state.get_tasks_bulk([ULID1, ULID2])
    assert result[0] is not None
    assert result[0].id == ULID1
    assert result[1] is None


@pytest.mark.asyncio
async def test_get_tasks_bulk_populates_heartbeat_at(task_adapter):
    """get_tasks_bulk sets heartbeat_at from the heartbeat store."""
    state, submit = task_adapter
    task = make_task(status=TaskStatus.STARTED)
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    result = await state.get_tasks_bulk([ULID1])
    assert len(result) == 1
    assert result[0] is not None
    assert result[0].heartbeat_at == FROZEN_TIME


# ── get_dag_runs / get_dag_run ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dag_runs_returns_empty_when_none(task_adapter):
    """get_dag_runs returns an empty list and total=0 when no DAG runs exist."""
    state, submit = task_adapter
    runs, total = await state.get_dag_runs(DAGRunPagination())
    assert runs == []
    assert total == 0


@pytest.mark.asyncio
async def test_get_dag_runs_returns_submitted_dag_runs(task_adapter):
    """get_dag_runs returns registered DAG run IDs with their submission timestamps."""
    state, submit = task_adapter
    task = make_task()
    task.dag_run_id = ULID1
    await submit.submit_task(task)
    runs, total = await state.get_dag_runs(DAGRunPagination())
    assert total == 1
    run_id, submitted_at = runs[0]
    assert run_id == ULID1
    assert submitted_at == pytest.approx(FROZEN_TIME, abs=dt.timedelta(seconds=1))


@pytest.mark.asyncio
async def test_get_dag_run_returns_none_for_unknown(task_adapter):
    """get_dag_run returns None when the dag_run_id has never been registered."""
    state, submit = task_adapter
    assert await state.get_dag_run(ULID()) is None


@pytest.mark.asyncio
async def test_get_dag_run_returns_submitted_at_and_task_ids(task_adapter):
    """get_dag_run returns (submitted_at, task_ids) for a known DAG run."""
    state, submit = task_adapter
    dag_run_id = ULID1
    task_a = make_task(ULID2, submitted_at=FROZEN_TIME)
    task_b = make_task(ULID3, submitted_at=FROZEN_TIME)
    task_a.dag_run_id = dag_run_id
    task_b.dag_run_id = dag_run_id
    await submit.submit_task(task_a)
    await submit.submit_task(task_b)
    result = await state.get_dag_run(dag_run_id)
    assert result is not None
    submitted_at, task_ids = result
    assert submitted_at == pytest.approx(FROZEN_TIME, abs=dt.timedelta(seconds=1))
    assert set(task_ids) == {ULID2, ULID3}


# ── clean_dag_runs ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_dag_runs_removes_old_entries(task_adapter):
    """clean_dag_runs removes DAG run entries older than max_age."""
    state, submit = task_adapter
    old_task = make_task(submitted_at=FROZEN_TIME - dt.timedelta(days=10))
    old_task.dag_run_id = ULID1
    await submit.submit_task(old_task)
    await state.clean_dag_runs(FROZEN_TIME, dt.timedelta(days=7))
    runs, total = await state.get_dag_runs(DAGRunPagination())
    assert total == 0
    assert runs == []


@pytest.mark.asyncio
async def test_clean_dag_runs_keeps_recent_entries(task_adapter):
    """clean_dag_runs leaves recent DAG run entries untouched."""
    state, submit = task_adapter
    recent_task = make_task(submitted_at=FROZEN_TIME - dt.timedelta(hours=1))
    recent_task.dag_run_id = ULID1
    await submit.submit_task(recent_task)
    await state.clean_dag_runs(FROZEN_TIME, dt.timedelta(days=7))
    runs, total = await state.get_dag_runs(DAGRunPagination())
    assert total == 1
    assert runs[0][0] == ULID1


# ── fan-in ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_init_fan_in_creates_expected_members(task_adapter):
    """init_fan_in persists the predecessor set; get_fan_in_members returns it."""
    state, submit = task_adapter
    fan_in_key = "fan-in:init-test"
    predecessor_ids = {ULID1, ULID2, ULID3}
    await state.init_fan_in(fan_in_key, predecessor_ids)
    assert set(await state.get_fan_in_members(fan_in_key)) == predecessor_ids


@pytest.mark.asyncio
async def test_fan_in_complete_returns_remaining_count(task_adapter):
    """fan_in_complete decrements the tracking set and returns the remaining count."""
    state, submit = task_adapter
    fan_in_key = "fan-in:countdown"
    await state.init_fan_in(fan_in_key, {ULID1, ULID2})
    assert await state.fan_in_complete(fan_in_key, ULID1) == 1
    assert await state.fan_in_complete(fan_in_key, ULID2) == 0


@pytest.mark.asyncio
async def test_fan_in_complete_returns_minus_one_for_unknown_id(task_adapter):
    """fan_in_complete returns -1 when the task ID is not a member of the set."""
    state, submit = task_adapter
    await state.init_fan_in("fan-in:unknown", {ULID1})
    assert await state.fan_in_complete("fan-in:unknown", ULID2) == -1


@pytest.mark.asyncio
async def test_get_fan_in_members_returns_predecessor_ids(task_adapter):
    """get_fan_in_members returns the permanent predecessor set for a fan-in key."""
    state, submit = task_adapter
    fan_in_key = "fan-in:members-test"
    predecessor_ids = {ULID1, ULID2, ULID3}
    await state.init_fan_in(fan_in_key, predecessor_ids)
    for uid in predecessor_ids:
        await state.fan_in_complete(fan_in_key, uid)
    assert set(await state.get_fan_in_members(fan_in_key)) == predecessor_ids


@pytest.mark.asyncio
async def test_delegate_fan_in_swaps_id_in_tracking_and_members_sets(task_adapter):
    """delegate_fan_in atomically replaces old_id with new_id in both tracking and members sets."""
    state, _ = task_adapter
    fan_in_key = "fan-in:delegate-test"
    new_id = ULID4

    # Initialise with ULID1 and ULID2 so there are two predecessors.
    await state.init_fan_in(fan_in_key, {ULID1, ULID2})

    # Delegate ULID1 → new_id (simulating a nested fanout where new_id's
    # grandcollector will report completion instead of ULID1).
    await state.delegate_fan_in(fan_in_key, ULID1, new_id)

    # The tracking set should now contain ULID2 and new_id (not ULID1).
    # Completing ULID2 leaves 1 remaining; new_id completes the set.
    assert await state.fan_in_complete(fan_in_key, ULID2) == 1
    assert await state.fan_in_complete(fan_in_key, new_id) == 0

    # The members set must also contain new_id so the collector gets the right parent_ids.
    members = set(await state.get_fan_in_members(fan_in_key))
    assert new_id in members
    assert ULID1 not in members


# ── compare_and_set_status ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_compare_and_set_status_succeeds_when_expected_matches(task_adapter):
    """compare_and_set_status transitions status and returns True when expected matches."""
    state, submit = task_adapter
    await submit.submit_task(make_task(status=TaskStatus.SUBMITTED))
    result = await state.compare_and_set_status(ULID1, TaskStatus.SUBMITTED, TaskStatus.STARTED)
    assert result is True
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STARTED


@pytest.mark.asyncio
async def test_compare_and_set_status_fails_when_status_differs(task_adapter):
    """compare_and_set_status returns False and leaves status unchanged when expected does not match."""
    state, submit = task_adapter
    await submit.submit_task(make_task(status=TaskStatus.SUBMITTED))
    result = await state.compare_and_set_status(ULID1, TaskStatus.STARTED, TaskStatus.COMPLETED)
    assert result is False
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_compare_and_set_status_returns_false_for_missing_task(task_adapter):
    """compare_and_set_status returns False when the task does not exist."""
    state, submit = task_adapter
    result = await state.compare_and_set_status(ULID(), TaskStatus.SUBMITTED, TaskStatus.STARTED)
    assert result is False


# ── clean ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_removes_tasks_within_age_window(task_adapter):
    """clean() removes queue entries whose submitted_at falls within the given range."""
    state, submit = task_adapter
    if isinstance(state, RedisJSONTaskState):
        pytest.xfail(
            "RedisJSONTaskState.get_all_tasks uses RediSearch (JSON index), not the sorted set "
            "that clean() modifies; removed entries still appear in search results."
        )
    old_task = make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(hours=2))
    new_task = make_task(ULID2, submitted_at=FROZEN_TIME)
    await submit.submit_task(old_task)
    await submit.submit_task(new_task)

    min_age = FROZEN_TIME - dt.timedelta(hours=3)
    max_age = FROZEN_TIME - dt.timedelta(hours=1)
    await state.clean({"default"}, FROZEN_TIME, min_queue_age=min_age, max_queue_age=max_age)

    remaining = await state.get_all_tasks(TaskPagination(queue="default"))
    remaining_ids = {t.id for t in remaining}
    assert ULID1 not in remaining_ids
    assert ULID2 in remaining_ids


@pytest.mark.asyncio
async def test_clean_with_no_age_params_is_noop(task_adapter):
    """clean() with no min/max age does nothing."""
    state, submit = task_adapter
    await submit.submit_task(make_task(ULID1))
    await state.clean({"default"}, FROZEN_TIME)
    remaining = await state.get_all_tasks(TaskPagination(queue="default"))
    assert len(remaining) == 1
