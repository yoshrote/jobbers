"""
Contract tests for TaskAdapterProtocol implementations.

Each test runs against all registered implementations via the ``task_adapter``
fixture defined in ``tests/adapters/conftest.py``.
"""

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers.adapters.raw_redis import MsgpackTaskAdapter
from jobbers.models.queue_config import QueueConfig, RatePeriod
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


# ── basic ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_empty(task_adapter):
    """No tasks stored → empty list returned."""
    results = await task_adapter.get_all_tasks(TaskPagination(queue="default"))
    assert results == []


@pytest.mark.asyncio
async def test_get_all_tasks_returns_submitted_tasks(task_adapter):
    """A submitted task is returned by get_all_tasks for its queue."""
    await task_adapter.submit_task(make_task())

    results = await task_adapter.get_all_tasks(TaskPagination(queue="default"))
    assert len(results) == 1
    assert results[0].id == ULID1


# ── filters ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_task_name(task_adapter):
    """task_name filter returns only tasks with a matching name."""
    await task_adapter.submit_task(make_task(ULID1, name="task_a"))
    await task_adapter.submit_task(make_task(ULID2, name="task_b"))

    results = await task_adapter.get_all_tasks(TaskPagination(queue="default", task_name="task_a"))
    assert len(results) == 1
    assert results[0].name == "task_a"


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_version(task_adapter):
    """task_version filter returns only tasks with a matching version."""
    await task_adapter.submit_task(make_task(ULID1, version=1))
    await task_adapter.submit_task(make_task(ULID2, version=2))

    results = await task_adapter.get_all_tasks(TaskPagination(queue="default", task_version=1))
    assert len(results) == 1
    assert results[0].version == 1


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_status(task_adapter):
    """`status` filter returns only tasks in the specified status."""
    await task_adapter.submit_task(make_task(ULID1, status=TaskStatus.SUBMITTED))
    await task_adapter.submit_task(make_task(ULID2, status=TaskStatus.SUBMITTED))
    await task_adapter.save_task(make_task(ULID2, status=TaskStatus.COMPLETED))

    results = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", status=TaskStatus.SUBMITTED)
    )
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_different_queues_are_isolated(task_adapter):
    """Tasks from other queues are not included in the results."""
    await task_adapter.submit_task(make_task(ULID1, queue="queue_a"))
    await task_adapter.submit_task(make_task(ULID2, queue="queue_b"))

    results = await task_adapter.get_all_tasks(TaskPagination(queue="queue_a"))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_filter_no_match_returns_empty(task_adapter):
    """A filter that matches no tasks returns an empty list."""
    await task_adapter.submit_task(make_task(ULID1, name="task_a"))

    results = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", task_name="task_nonexistent")
    )
    assert results == []


# ── pagination ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_respects_limit(task_adapter):
    """`limit` caps the number of results returned."""
    for i, uid in enumerate((ULID1, ULID2, ULID3)):
        await task_adapter.submit_task(
            make_task(uid, submitted_at=FROZEN_TIME + dt.timedelta(seconds=i))
        )

    results = await task_adapter.get_all_tasks(TaskPagination(queue="default", limit=2))
    assert len(results) == 2


@pytest.mark.asyncio
async def test_get_all_tasks_offset_skips_first_n(task_adapter):
    """`offset` skips the first N results so successive pages do not overlap."""
    for i, uid in enumerate((ULID1, ULID2, ULID3)):
        await task_adapter.submit_task(
            make_task(uid, submitted_at=FROZEN_TIME + dt.timedelta(seconds=i))
        )

    page1 = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", limit=2, offset=0, order_by=PaginationOrder.SUBMITTED_AT)
    )
    page2 = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", limit=2, offset=2, order_by=PaginationOrder.SUBMITTED_AT)
    )

    assert len(page1) == 2
    assert len(page2) == 1
    assert {t.id for t in page1}.isdisjoint({t.id for t in page2})


@pytest.mark.asyncio
async def test_get_all_tasks_offset_returns_empty_beyond_end(task_adapter):
    """An offset past the total count returns an empty list."""
    await task_adapter.submit_task(make_task(ULID1))

    results = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", limit=10, offset=5)
    )
    assert results == []


@pytest.mark.asyncio
async def test_get_all_tasks_order_by_submitted_at(task_adapter):
    """
    Results are returned in ascending submitted_at order within each page.

    Tasks are submitted newest-first to prove the sort is applied rather than
    relying on insertion order.  With 5 tasks and limit=3, page 1 returns the
    3 oldest in order and page 2 returns the 2 newest in order.
    """
    tasks = [
        make_task(uid, submitted_at=FROZEN_TIME + dt.timedelta(seconds=i))
        for i, uid in enumerate((ULID1, ULID2, ULID3, ULID4, ULID5))
    ]
    for t in reversed(tasks):  # submit newest-first to prove sort is applied
        await task_adapter.submit_task(t)

    page1 = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", limit=3, offset=0, order_by=PaginationOrder.SUBMITTED_AT)
    )
    page2 = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", limit=3, offset=3, order_by=PaginationOrder.SUBMITTED_AT)
    )

    assert [t.id for t in page1] == [ULID1, ULID2, ULID3]
    assert [t.id for t in page2] == [ULID4, ULID5]


# ── combined filters ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_combined_name_and_version(task_adapter):
    """Filtering by both task_name and task_version returns only exact matches."""
    t1 = make_task(ULID1, name="task_a", version=1)
    t2 = make_task(ULID2, name="task_a", version=2)
    t3 = make_task(ULID3, name="task_b", version=1)
    for t in (t1, t2, t3):
        await task_adapter.submit_task(t)

    results = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", task_name="task_a", task_version=1)
    )
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_combined_name_and_status(task_adapter):
    """Filtering by both task_name and status returns only tasks matching both."""
    t1 = make_task(ULID1, name="task_a", status=TaskStatus.SUBMITTED)
    t2 = make_task(ULID2, name="task_a", status=TaskStatus.SUBMITTED)
    await task_adapter.submit_task(t1)
    await task_adapter.submit_task(t2)
    await task_adapter.save_task(make_task(ULID2, name="task_a", status=TaskStatus.COMPLETED))

    results = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", task_name="task_a", status=TaskStatus.SUBMITTED)
    )
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_combined_version_and_status(task_adapter):
    """Filtering by version and status without task_name works correctly."""
    t1 = make_task(ULID1, version=1, status=TaskStatus.SUBMITTED)
    t2 = make_task(ULID2, version=1, status=TaskStatus.SUBMITTED)
    await task_adapter.submit_task(t1)
    await task_adapter.submit_task(t2)
    await task_adapter.save_task(make_task(ULID2, version=1, status=TaskStatus.FAILED))

    results = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", task_version=1, status=TaskStatus.SUBMITTED)
    )
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_all_filters_combined(task_adapter):
    """All three filters together (name, version, status) are applied conjunctively."""
    t1 = make_task(ULID1, name="task_a", version=1, status=TaskStatus.SUBMITTED)
    t2 = make_task(ULID2, name="task_a", version=2, status=TaskStatus.SUBMITTED)
    t3 = make_task(ULID3, name="task_b", version=1, status=TaskStatus.SUBMITTED)
    for t in (t1, t2, t3):
        await task_adapter.submit_task(t)
    await task_adapter.save_task(make_task(ULID1, name="task_a", version=1, status=TaskStatus.COMPLETED))

    results = await task_adapter.get_all_tasks(
        TaskPagination(queue="default", task_name="task_a", task_version=1, status=TaskStatus.SUBMITTED)
    )
    assert results == []


# ── filter + pagination ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_filter_pagination_page_size_is_predictable(task_adapter):
    """
    Page size is predictable when a filter is active (desired contract).

    When offset counts matched documents (not raw queue positions), each page returns
    exactly ``limit`` results (or fewer only when fewer matching tasks remain), and
    consecutive pages together cover all matching tasks without gaps or duplicates.

    Setup: 4 tasks submitted earliest-first — B1, B2, A1, A2.  Only A* match the filter.
    With limit=1 and filter task_name=task_a:
      page 1 (offset=0) → exactly 1 result (A1)
      page 2 (offset=1) → exactly 1 result (A2, not A1 again)

    Known limitation: MsgpackTaskAdapter applies offset to raw queue positions before
    Python-side filtering, so offset-based pagination over a filtered result set is
    unpredictable (pages may overlap).
    """
    if isinstance(task_adapter, MsgpackTaskAdapter):
        pytest.xfail(
            "MsgpackTaskAdapter applies offset before Python-side filtering; "
            "page 2 backs up into queue-space and returns the same task as page 1"
        )

    b1 = make_task(ULID1, name="task_b", submitted_at=FROZEN_TIME)
    b2 = make_task(ULID2, name="task_b", submitted_at=FROZEN_TIME + dt.timedelta(seconds=1))
    a1 = make_task(ULID3, name="task_a", submitted_at=FROZEN_TIME + dt.timedelta(seconds=2))
    a2 = make_task(ULID4, name="task_a", submitted_at=FROZEN_TIME + dt.timedelta(seconds=3))
    for t in (b1, b2, a1, a2):
        await task_adapter.submit_task(t)

    page1 = await task_adapter.get_all_tasks(
        TaskPagination(
            queue="default",
            task_name="task_a",
            limit=1,
            offset=0,
            order_by=PaginationOrder.SUBMITTED_AT,
        )
    )
    page2 = await task_adapter.get_all_tasks(
        TaskPagination(
            queue="default",
            task_name="task_a",
            limit=1,
            offset=1,
            order_by=PaginationOrder.SUBMITTED_AT,
        )
    )

    assert [t.id for t in page1] == [ULID3]
    assert [t.id for t in page2] == [ULID4]


# ── stage_requeue ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_requeue_adds_task_to_queue(task_adapter):
    """stage_requeue enqueues the task back into its sorted-set queue."""
    task = make_task()
    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    members = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_stage_requeue_saves_task_data(task_adapter):
    """stage_requeue also persists the task blob so it can be retrieved."""
    task = make_task()
    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    saved = await task_adapter.get_task(ULID1)
    assert saved is not None
    assert saved.id == ULID1


@pytest.mark.asyncio
async def test_stage_requeue_uses_submitted_at_as_score(task_adapter):
    """The queue sorted-set score matches the task's submitted_at timestamp."""
    task = make_task()
    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    score = await task_adapter.data_store.zscore(task_adapter.TASKS_BY_QUEUE(queue="default"), bytes(ULID1))
    assert score == pytest.approx(FROZEN_TIME.timestamp())


@pytest.mark.asyncio
async def test_stage_requeue_does_not_duplicate_queue_entry(task_adapter):
    """Re-queuing the same task ID does not create a duplicate entry."""
    task = make_task()
    for _ in range(3):
        pipe = task_adapter.data_store.pipeline(transaction=True)
        task_adapter.stage_requeue(pipe, task)
        await pipe.execute()

    members = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert members.count(bytes(ULID1)) == 1


# ── remove_task_heartbeat ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_task_heartbeat_removes_entry(task_adapter):
    """remove_task_heartbeat removes the task from the heartbeat sorted set."""
    task = make_task()
    task.heartbeat_at = FROZEN_TIME
    await task_adapter.update_task_heartbeat(task)
    assert await task_adapter.data_store.zscore(task_adapter.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)) is not None

    await task_adapter.remove_task_heartbeat(task)
    assert await task_adapter.data_store.zscore(task_adapter.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)) is None


@pytest.mark.asyncio
async def test_remove_task_heartbeat_noop_when_absent(task_adapter):
    """remove_task_heartbeat is a no-op when the task has no heartbeat entry."""
    task = make_task()
    await task_adapter.remove_task_heartbeat(task)  # should not raise


# ── get_active_tasks ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_tasks_returns_empty_for_no_heartbeats(task_adapter):
    """get_active_tasks returns [] when no tasks have heartbeat entries."""
    result = await task_adapter.get_active_tasks({"default"})
    assert result == []


@pytest.mark.asyncio
async def test_get_active_tasks_returns_tasks_with_heartbeats(task_adapter):
    """get_active_tasks returns tasks that have a heartbeat entry."""
    task = make_task()
    await task_adapter.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await task_adapter.update_task_heartbeat(task)

    result = await task_adapter.get_active_tasks({"default"})
    assert len(result) == 1
    assert result[0].id == ULID1


# ── clean (no-op branch) ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_noop_when_no_age_params(task_adapter):
    """clean() with neither min_queue_age nor max_queue_age does nothing."""
    task = make_task()
    await task_adapter.submit_task(task)

    now = dt.datetime.now(dt.UTC)
    await task_adapter.clean(queues={b"default"}, now=now)

    members = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members


# ── get_next_task: missing data path ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_task_skips_missing_data_and_returns_none(task_adapter):
    """When a task is popped from the queue but its blob is absent, it is logged and None is returned."""
    fake_id = ULID1
    queue_key = task_adapter.TASKS_BY_QUEUE(queue="default")
    if isinstance(queue_key, str):
        queue_key = queue_key.encode()
    pop_results = iter(
        [
            (queue_key, bytes(fake_id), FROZEN_TIME.timestamp()),
            None,
        ]
    )
    with patch.object(
        task_adapter.data_store, "bzpopmin", new_callable=AsyncMock, side_effect=pop_results
    ):
        result = await task_adapter.get_next_task(queues={"default"}, pop_timeout=1)
    assert result is None
    assert await task_adapter.data_store.zscore(task_adapter.DLQ_MISSING_DATA, bytes(fake_id)) is not None


# ── submit_task / get_task / task_exists ──────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_task(task_adapter):
    """submit_task enqueues the task and persists its data."""
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    task.set_status(TaskStatus.SUBMITTED)
    await task_adapter.submit_task(task)

    task_list = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in task_list
    saved_task = await task_adapter.get_task(ULID1)
    assert saved_task.name == "Test Task"
    assert saved_task.status == TaskStatus.SUBMITTED
    assert saved_task.submitted_at == task.submitted_at


@pytest.mark.asyncio
async def test_submit_task_twice_updates_only(task_adapter):
    """Submitting the same task ID twice updates the data without duplicating the queue entry."""
    task = Task(id=ULID1, name="Initial Task", status="unsubmitted")
    task.set_status(TaskStatus.SUBMITTED)
    await task_adapter.submit_task(task)

    updated_task = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await task_adapter.submit_task(updated_task)

    task_list = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue=task.queue), 0, -1)
    assert task_list == [bytes(ULID1)]

    saved_task = await task_adapter.get_task(ULID1)
    assert saved_task.name == "Updated Task"
    assert saved_task.status == TaskStatus.COMPLETED
    assert saved_task.submitted_at == task.submitted_at


@pytest.mark.asyncio
async def test_get_task(task_adapter):
    """get_task returns the saved task with all fields intact."""
    task_to_save = Task(id=ULID1, name="Test Task", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME)
    await task_adapter.save_task(task_to_save)
    task = await task_adapter.get_task(ULID1)
    assert task is not None
    assert task.id == ULID1
    assert task.name == "Test Task"
    assert task.status == TaskStatus.STARTED
    assert task.submitted_at == FROZEN_TIME


@pytest.mark.asyncio
async def test_get_task_not_found(task_adapter):
    """get_task returns None when no task exists for the given ID."""
    assert await task_adapter.get_task(ULID1) is None


@pytest.mark.asyncio
async def test_task_exists(task_adapter):
    """task_exists returns True after save and False for an unsaved ID."""
    await task_adapter.save_task(
        Task(id=ULID1, name="Test Task", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME)
    )
    assert await task_adapter.task_exists(ULID1) is True
    assert await task_adapter.task_exists(ULID2) is False


# ── update_task_heartbeat ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_task_heartbeat_sets_timestamp(task_adapter):
    """update_task_heartbeat is reflected in the heartbeat_at field when the task is retrieved."""
    task = make_task(status=TaskStatus.STARTED)
    await task_adapter.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await task_adapter.update_task_heartbeat(task)

    updated_task = await task_adapter.get_task(task.id)
    assert updated_task.heartbeat_at == task.heartbeat_at


@pytest.mark.asyncio
async def test_update_task_heartbeat_adds_to_scores(task_adapter):
    """update_task_heartbeat adds the task to the heartbeat sorted set."""
    task = make_task(status=TaskStatus.STARTED)
    await task_adapter.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await task_adapter.update_task_heartbeat(task)

    scores = await task_adapter.data_store.zrange(
        task_adapter.HEARTBEAT_SCORES(queue=task.queue), 0, -1, withscores=True
    )
    assert any(bytes(task.id) == task_id for task_id, _ in scores)


@pytest.mark.asyncio
async def test_update_task_heartbeat_updates_existing_score(task_adapter):
    """A second update_task_heartbeat call overwrites the previous heartbeat score."""
    task = make_task(status=TaskStatus.STARTED)
    await task_adapter.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await task_adapter.update_task_heartbeat(task)

    second_time = FROZEN_TIME + dt.timedelta(seconds=10)
    task.heartbeat_at = second_time
    await task_adapter.update_task_heartbeat(task)

    scores = await task_adapter.data_store.zrange(
        task_adapter.HEARTBEAT_SCORES(queue=task.queue), 0, -1, withscores=True
    )
    score = next(score for task_id, score in scores if bytes(task.id) == task_id)
    assert score == second_time.timestamp()


# ── get_stale_tasks ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_stale_tasks_returns_stale_tasks(task_adapter):
    """Tasks whose heartbeat is older than stale_time are returned."""
    now = dt.datetime.now(dt.UTC)
    stale_task = Task(
        id=ULID1,
        name="stale_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=now,
        heartbeat_at=now - dt.timedelta(minutes=10),
    )
    await task_adapter.save_task(stale_task)
    await task_adapter.update_task_heartbeat(stale_task)

    stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, dt.timedelta(minutes=5))]
    assert len(stale_tasks) == 1
    assert stale_tasks[0].id == stale_task.id


@pytest.mark.asyncio
async def test_get_stale_tasks_excludes_recent_tasks(task_adapter):
    """Tasks whose heartbeat is within stale_time are not returned."""
    now = dt.datetime.now(dt.UTC)
    recent_task = Task(
        id=ULID1,
        name="recent_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=now,
        heartbeat_at=now - dt.timedelta(minutes=1),
    )
    await task_adapter.save_task(recent_task)
    await task_adapter.update_task_heartbeat(recent_task)

    stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, dt.timedelta(minutes=5))]
    assert len(stale_tasks) == 0


@pytest.mark.asyncio
async def test_get_stale_tasks_handles_multiple_queues(task_adapter):
    """Stale tasks from multiple queues are all returned."""
    now = dt.datetime.now(dt.UTC)
    t1 = Task(
        id=ULID1,
        name="task_1",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=now,
        heartbeat_at=now - dt.timedelta(minutes=10),
    )
    t2 = Task(
        id=ULID2,
        name="task_2",
        queue="high_priority",
        status=TaskStatus.STARTED,
        started_at=now,
        heartbeat_at=now - dt.timedelta(minutes=10),
    )
    for t in (t1, t2):
        await task_adapter.save_task(t)
        await task_adapter.update_task_heartbeat(t)

    stale_tasks = [
        task async for task in task_adapter.get_stale_tasks({"default", "high_priority"}, dt.timedelta(minutes=5))
    ]
    assert len(stale_tasks) == 2
    assert {task.id for task in stale_tasks} == {t1.id, t2.id}


@pytest.mark.asyncio
async def test_get_stale_tasks_filters_out_missing_blobs(task_adapter):
    """Tasks whose blob has been deleted are silently excluded from stale results."""
    now = dt.datetime.now(dt.UTC)
    stale_task = Task(
        id=ULID1,
        name="stale_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=now,
        heartbeat_at=now - dt.timedelta(minutes=10),
    )
    await task_adapter.save_task(stale_task)
    await task_adapter.update_task_heartbeat(stale_task)
    await task_adapter.data_store.delete(task_adapter.TASK_DETAILS(task_id=stale_task.id))

    stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, dt.timedelta(minutes=5))]
    assert len(stale_tasks) == 0


# ── clean_terminal_tasks ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_terminal_tasks_deletes_old_completed_blob(task_adapter):
    """A completed task blob older than max_age is deleted."""
    task = Task(
        id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await task_adapter.save_task(task)
    await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_removes_heartbeat_entry(task_adapter):
    """The heartbeat sorted-set entry is removed along with the task blob."""
    task = Task(
        id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await task_adapter.save_task(task)
    await task_adapter.data_store.zadd(
        task_adapter.HEARTBEAT_SCORES(queue="default"),
        {ULID1.bytes: (FROZEN_TIME - dt.timedelta(days=8)).timestamp()},
    )
    await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert await task_adapter.data_store.zscore(task_adapter.HEARTBEAT_SCORES(queue="default"), ULID1.bytes) is None


@pytest.mark.asyncio
async def test_clean_terminal_tasks_removes_type_index_entry(task_adapter):
    """Orphaned task-type-idx entries are removed alongside the task blob."""
    task = Task(
        id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await task_adapter.save_task(task)
    await task_adapter.data_store.sadd(task_adapter.TASK_BY_TYPE_IDX(name="test_task"), ULID1.bytes)
    await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    members = await task_adapter.data_store.smembers(task_adapter.TASK_BY_TYPE_IDX(name="test_task"))
    assert ULID1.bytes not in members


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_active_task(task_adapter):
    """Tasks in active statuses are never deleted regardless of age."""
    task = Task(
        id=ULID1, name="test_task", queue="default", status=TaskStatus.STARTED,
        started_at=FROZEN_TIME - dt.timedelta(days=100),
    )
    await task_adapter.save_task(task)
    await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))
    assert await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_task_within_age(task_adapter):
    """A completed task whose completed_at is within max_age is not deleted."""
    task = Task(
        id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(hours=1),
    )
    await task_adapter.save_task(task)
    await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_task_without_completed_at(task_adapter):
    """A terminal task with no completed_at is not deleted."""
    task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.FAILED, completed_at=None)
    await task_adapter.save_task(task)
    await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))
    assert await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID1))


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
    task = Task(
        id=ULID1, name="test_task", queue="default", status=status,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await task_adapter.save_task(task)
    await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_leaves_other_tasks_untouched(task_adapter):
    """Only old terminal tasks are deleted; recent tasks remain."""
    old_task = Task(
        id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    recent_task = Task(
        id=ULID2, name="test_task", queue="default", status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(hours=1),
    )
    await task_adapter.save_task(old_task)
    await task_adapter.save_task(recent_task)
    await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID1))
    assert await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID2))


# ── submit_rate_limited_task ──────────────────────────────────────────────────


def _make_rate_task(task_id: ULID, submitted_at: dt.datetime) -> Task:
    return Task(id=task_id, name="test", queue="default", status=TaskStatus.SUBMITTED, submitted_at=submitted_at)


def _default_queue_config(rate_numerator: int = 2) -> QueueConfig:
    return QueueConfig(name="default", rate_numerator=rate_numerator, rate_denominator=1, rate_period=RatePeriod.MINUTE)


@pytest.mark.asyncio
async def test_submit_rate_limited_enqueues_when_empty(task_adapter, task_adapter_dt_module):
    """Atomically enqueues the task and records it in the rate-limiter when the set is empty."""
    task = _make_rate_task(ULID1, FROZEN_TIME)
    with patch(task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert bytes(ULID1) in await task_adapter.data_store.zrange(task_adapter.QUEUE_RATE_LIMITER(queue="default"), 0, -1)
    assert bytes(ULID1) in await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_submit_rate_limited_enqueues_with_room(task_adapter, task_adapter_dt_module):
    """Enqueues when one slot is already used out of two."""
    await task_adapter.data_store.zadd(
        task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 1}
    )
    task = _make_rate_task(ULID2, FROZEN_TIME)
    with patch(task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert bytes(ULID2) in await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)


@pytest.mark.asyncio
async def test_submit_rate_limited_prunes_expired_entries(task_adapter, task_adapter_dt_module):
    """Expired rate-limiter entries are pruned; the new task is accepted."""
    await task_adapter.data_store.zadd(
        task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 60}
    )
    await task_adapter.data_store.zadd(
        task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID2.bytes: FROZEN_TIME.timestamp() - 61}
    )
    new_id = ULID5
    task = _make_rate_task(new_id, FROZEN_TIME)
    with patch(task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert new_id.bytes in await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)


@pytest.mark.asyncio
async def test_submit_rate_limited_rejects_when_full(task_adapter, task_adapter_dt_module):
    """Task is not enqueued when rate limit is reached."""
    await task_adapter.data_store.zadd(
        task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 1}
    )
    await task_adapter.data_store.zadd(
        task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID2.bytes: FROZEN_TIME.timestamp() - 2}
    )
    task = _make_rate_task(ULID5, FROZEN_TIME)
    with patch(task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is False
    assert ULID5.bytes not in await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert not await task_adapter.data_store.exists(task_adapter.TASK_DETAILS(task_id=ULID5))


@pytest.mark.asyncio
async def test_submit_rate_limited_concurrent_respects_limit(task_adapter):
    """Concurrent submissions must not collectively exceed the rate limit."""
    now = dt.datetime.now(dt.UTC)
    limit = 5
    queue_config = _default_queue_config(rate_numerator=limit)
    tasks = [_make_rate_task(ULID(), now) for _ in range(10)]

    results = await asyncio.gather(*[task_adapter.submit_rate_limited_task(t, queue_config) for t in tasks])

    accepted = sum(1 for r in results if r)
    assert accepted == limit
    assert await task_adapter.data_store.zcard(task_adapter.QUEUE_RATE_LIMITER(queue="default")) == limit
    assert await task_adapter.data_store.zcard(task_adapter.TASKS_BY_QUEUE(queue="default")) == limit


# ── get_tasks_bulk ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_tasks_bulk_empty_input(task_adapter):
    """get_tasks_bulk returns an empty list when given no IDs."""
    result = await task_adapter.get_tasks_bulk([])
    assert result == []


@pytest.mark.asyncio
async def test_get_tasks_bulk_returns_tasks_in_input_order(task_adapter):
    """get_tasks_bulk returns tasks in the same order as the input ID list."""
    t1 = make_task(ULID1, name="task_a")
    t2 = make_task(ULID2, name="task_b")
    t3 = make_task(ULID3, name="task_c")
    for t in (t1, t2, t3):
        await task_adapter.save_task(t)

    result = await task_adapter.get_tasks_bulk([ULID3, ULID1, ULID2])
    assert [r.id for r in result] == [ULID3, ULID1, ULID2]


@pytest.mark.asyncio
async def test_get_tasks_bulk_returns_none_for_missing_ids(task_adapter):
    """get_tasks_bulk returns None in the result list for IDs with no stored task."""
    await task_adapter.save_task(make_task(ULID1))

    result = await task_adapter.get_tasks_bulk([ULID1, ULID2])
    assert result[0] is not None
    assert result[0].id == ULID1
    assert result[1] is None


@pytest.mark.asyncio
async def test_get_tasks_bulk_populates_heartbeat_at(task_adapter):
    """get_tasks_bulk sets heartbeat_at from the heartbeat sorted set."""
    task = make_task(status=TaskStatus.STARTED)
    await task_adapter.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await task_adapter.update_task_heartbeat(task)

    result = await task_adapter.get_tasks_bulk([ULID1])
    assert len(result) == 1
    assert result[0] is not None
    assert result[0].heartbeat_at == FROZEN_TIME


# ── get_next_task (happy path) ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_task_returns_submitted_task(task_adapter):
    """get_next_task pops and returns the next available task."""
    task = make_task()
    await task_adapter.submit_task(task)

    result = await task_adapter.get_next_task(queues={"default"}, pop_timeout=1)

    assert result is not None
    assert result.id == ULID1
    members = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) not in members


# ── clean (with age params) ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_max_queue_age_removes_old_entries(task_adapter):
    """max_queue_age removes queue entries at or before that timestamp."""
    old = make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=2))
    recent = make_task(ULID2, submitted_at=FROZEN_TIME)
    await task_adapter.submit_task(old)
    await task_adapter.submit_task(recent)

    cutoff = FROZEN_TIME - dt.timedelta(days=1)
    await task_adapter.clean(queues={b"default"}, now=FROZEN_TIME, max_queue_age=cutoff)

    members = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) not in members
    assert bytes(ULID2) in members


@pytest.mark.asyncio
async def test_clean_min_queue_age_removes_entries_below_floor(task_adapter):
    """min_queue_age removes queue entries at or after that timestamp, leaving older ones."""
    old = make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=10))
    recent = make_task(ULID2, submitted_at=FROZEN_TIME)
    await task_adapter.submit_task(old)
    await task_adapter.submit_task(recent)

    floor = FROZEN_TIME - dt.timedelta(days=1)
    await task_adapter.clean(queues={b"default"}, now=FROZEN_TIME, min_queue_age=floor)

    members = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members
    assert bytes(ULID2) not in members


# ── stage_remove_from_queue ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_remove_from_queue_removes_from_sorted_set(task_adapter):
    """stage_remove_from_queue removes the task ID from the queue sorted set."""
    task = make_task()
    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_remove_from_queue(pipe, task)
    await pipe.execute()

    score = await task_adapter.data_store.zscore(task_adapter.TASKS_BY_QUEUE(queue="default"), bytes(ULID1))
    assert score is None


@pytest.mark.asyncio
async def test_stage_remove_from_queue_removes_from_type_index(task_adapter):
    """stage_remove_from_queue removes the task ID from the type index set."""
    task = make_task()
    await task_adapter.data_store.sadd(task_adapter.TASK_BY_TYPE_IDX(name="my_task"), bytes(ULID1))

    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_remove_from_queue(pipe, task)
    await pipe.execute()

    members = await task_adapter.data_store.smembers(task_adapter.TASK_BY_TYPE_IDX(name="my_task"))
    assert bytes(ULID1) not in members


@pytest.mark.asyncio
async def test_stage_remove_from_queue_leaves_other_tasks_untouched(task_adapter):
    """stage_remove_from_queue only removes the target task, not its queue-mates."""
    t1 = make_task(ULID1)
    t2 = make_task(ULID2, submitted_at=FROZEN_TIME + dt.timedelta(seconds=1))
    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_requeue(pipe, t1)
    task_adapter.stage_requeue(pipe, t2)
    await pipe.execute()

    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_remove_from_queue(pipe, t1)
    await pipe.execute()

    members = await task_adapter.data_store.zrange(task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) not in members
    assert bytes(ULID2) in members


@pytest.mark.asyncio
async def test_stage_remove_from_queue_noop_when_absent(task_adapter):
    """stage_remove_from_queue does not raise when the task is not in the queue."""
    task = make_task()
    pipe = task_adapter.data_store.pipeline(transaction=True)
    task_adapter.stage_remove_from_queue(pipe, task)
    await pipe.execute()  # should not raise
