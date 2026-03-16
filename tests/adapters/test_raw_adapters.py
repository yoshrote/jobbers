"""
Raw-adapter-specific tripwires and edge cases not covered by protocol contract tests.

Covers MsgpackTaskAdapter (task storage) and DeadQueue (dead letter queue) — the two
plain-Redis implementations that share a FakeAsyncRedis fixture.

Contract tests (including ensure_index, read_for_watch, get_all_tasks missing-blob) live in
test_task_adapter_common.py and test_dead_queue_common.py and run against all implementations.
"""

import datetime as dt

import pytest
from ulid import ULID

from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")
ULID3 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG03")
ULID4 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG04")


def make_task(
    task_id: ULID = ULID1,
    name: str = "my_task",
    version: int = 1,
    queue: str = "default",
    status: TaskStatus = TaskStatus.SUBMITTED,
    submitted_at: dt.datetime = FROZEN_TIME,
):
    from jobbers.models.task import Task

    task = Task(id=task_id, name=name, version=version, queue=queue, status=status)
    task.submitted_at = submitted_at
    return task


# ── get_all_tasks: known limitations ─────────────────────────────────────────


@pytest.mark.xfail(
    reason=(
        "MsgpackTaskAdapter applies offset to queue positions before Python-side filtering, "
        "so offset=1 skips one *queue entry* (task_b), not one *filtered result* (task_a). "
        "Page 2 therefore returns the same task as page 1 — page size appears correct (1) "
        "but the pages overlap, making pagination over a filtered result set unpredictable."
    ),
    strict=True,
)
@pytest.mark.asyncio
async def test_get_all_tasks_filter_pagination_page_size_is_unpredictable(
    msgpack_adapter,
):
    """
    Tripwire: documents the known offset-before-filter limitation in MsgpackTaskAdapter.

    Because offset counts raw queue positions (before Python-side filtering), page 2 does
    not advance past the last result of page 1 — it backs up into queue-space and returns
    the same matching task again.

    Setup: 4 tasks submitted earliest-first — B1, B2, A1, A2.  Only A* match the filter.
    With limit=1 and filter task_name=task_a:
      page 1 (offset=0) → A1  ✓
      page 2 (offset=1) → expected A2, actual A1 again  ✗

    If this test unexpectedly passes, the limitation has been fixed — update
    ``test_task_adapter_common.py`` to remove the xfail for MsgpackTaskAdapter.
    """
    b1 = make_task(ULID1, name="task_b", submitted_at=FROZEN_TIME)
    b2 = make_task(ULID2, name="task_b", submitted_at=FROZEN_TIME + dt.timedelta(seconds=1))
    a1 = make_task(ULID3, name="task_a", submitted_at=FROZEN_TIME + dt.timedelta(seconds=2))
    a2 = make_task(ULID4, name="task_a", submitted_at=FROZEN_TIME + dt.timedelta(seconds=3))
    for t in (b1, b2, a1, a2):
        await msgpack_adapter.submit_task(t)

    page1 = await msgpack_adapter.get_all_tasks(
        TaskPagination(
            queue="default",
            task_name="task_a",
            limit=1,
            offset=0,
            order_by=PaginationOrder.SUBMITTED_AT,
        )
    )
    page2 = await msgpack_adapter.get_all_tasks(
        TaskPagination(
            queue="default",
            task_name="task_a",
            limit=1,
            offset=1,
            order_by=PaginationOrder.SUBMITTED_AT,
        )
    )

    assert [t.id for t in page1] == [ULID3]  # A1
    assert [t.id for t in page2] == [ULID4]  # A2 — expected but not what happens


@pytest.mark.xfail(
    reason=(
        "MsgpackTaskAdapter does not sort by task ID. "
        "order_by=TASK_ID uses zrange, which sorts by the queue sorted-set score "
        "(submitted_at timestamp), not by ULID. Results are returned in submitted_at "
        "order regardless of which PaginationOrder is requested."
    ),
    strict=True,
)
@pytest.mark.asyncio
async def test_get_all_tasks_task_id_order(msgpack_adapter):
    """
    Tripwire: MsgpackTaskAdapter ignores order_by=TASK_ID and sorts by submitted_at instead.

    Tasks are given submitted_at values that are the reverse of their ULID order so
    that submitted_at order and task-ID order point in opposite directions.
    The correct result is [ULID1, ULID2, ULID3]; the actual result is
    [ULID3, ULID2, ULID1] because the implementation sorts by submitted_at score.
    """
    # ULID order: ULID1 < ULID2 < ULID3, submitted_at order: ULID3 < ULID2 < ULID1
    t1 = make_task(ULID1, submitted_at=FROZEN_TIME + dt.timedelta(seconds=2))
    t2 = make_task(ULID2, submitted_at=FROZEN_TIME + dt.timedelta(seconds=1))
    t3 = make_task(ULID3, submitted_at=FROZEN_TIME)
    for t in (t1, t2, t3):
        await msgpack_adapter.submit_task(t)

    results = await msgpack_adapter.get_all_tasks(
        TaskPagination(queue="default", order_by=PaginationOrder.TASK_ID)
    )
    assert [r.id for r in results] == [ULID1, ULID2, ULID3]

    results = await msgpack_adapter.get_all_tasks(
        TaskPagination(queue="default", order_by=PaginationOrder.SUBMITTED_AT)
    )
    assert [r.id for r in results] == [ULID3, ULID2, ULID1]


# ── clean_terminal_tasks: edge cases ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_none_blob(msgpack_adapter, redis):
    """A task:* key that returns no data is silently skipped."""
    await redis.set("task:ghost", b"")
    await redis.delete("task:ghost")
    await redis.set("task:placeholder", b"data")
    await redis.delete("task:placeholder")
    await msgpack_adapter.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_non_ulid_keys(msgpack_adapter, redis):
    """A task:* key whose suffix is not a valid ULID is skipped without error."""
    await redis.set("task:not_a_valid_ulid_at_all", b"some data")
    await msgpack_adapter.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))
    assert await redis.exists("task:not_a_valid_ulid_at_all")


# ── DeadQueue: edge cases ─────────────────────────────────────────────────────

EARLIER = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
LATER = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)


async def add_to_dlq(dq, task: Task, failed_at: dt.datetime) -> None:
    pipe = dq.data_store.pipeline(transaction=True)
    dq.stage_add(pipe, task, failed_at)
    await pipe.execute()


@pytest.mark.asyncio
async def test_clean_handles_missing_meta(raw_dead_queue):
    """clean() removes the sorted-set entry even when the meta hash entry is absent.

    DeadQueue stores DLQ membership in a sorted set (``dlq``) and queue/name metadata
    in a separate hash (``dlq-meta``).  If the meta entry is missing (e.g. written by
    an older code version), clean() must still remove the sorted-set entry without error.
    """
    task = make_task(task_id=ULID1, status=TaskStatus.FAILED)
    old_time = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)

    await raw_dead_queue.data_store.zadd(raw_dead_queue.DLQ, {bytes(task.id): old_time.timestamp()})

    cutoff = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)
    await raw_dead_queue.clean(cutoff)

    score = await raw_dead_queue.data_store.zscore(raw_dead_queue.DLQ, bytes(task.id))
    assert score is None


@pytest.mark.xfail(
    strict=True,
    reason=(
        "DeadQueue uses zrange (ascending score) with no server-side sort option; "
        "results are returned earliest-first, not most-recent-first. "
        "Remove this xfail if DeadQueue gains sorted get_by_filter support."
    ),
)
@pytest.mark.asyncio
async def test_get_by_filter_sorted_by_failed_at_desc(raw_dead_queue, dummy_task_adapter):
    """Tripwire: DeadQueue does not sort get_by_filter results by failed_at descending."""
    t1 = make_task(task_id=ULID1, status=TaskStatus.FAILED)
    t2 = make_task(task_id=ULID2, status=TaskStatus.FAILED)
    await dummy_task_adapter.save_task(t1)
    await dummy_task_adapter.save_task(t2)
    await add_to_dlq(raw_dead_queue, t1, EARLIER)
    await add_to_dlq(raw_dead_queue, t2, LATER)

    results = await raw_dead_queue.get_by_filter()
    assert len(results) == 2
    assert results[0].id == t2.id  # LATER first — correct protocol behaviour, not what DeadQueue returns
