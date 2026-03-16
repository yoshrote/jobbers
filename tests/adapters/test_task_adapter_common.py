"""
Contract tests for TaskAdapterProtocol implementations.

Each test runs against all registered implementations via the ``task_adapter``
fixture defined in ``tests/adapters/conftest.py``.
"""

import datetime as dt

import pytest
from ulid import ULID

from jobbers.adapters.raw_redis import MsgpackTaskAdapter
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
