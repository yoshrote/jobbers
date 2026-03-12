"""Tests specific to MsgpackTaskAdapter methods not covered by protocol contract tests."""

import datetime as dt

import fakeredis
import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.adapters.raw_redis import DeadQueue, MsgpackTaskAdapter
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")
ULID3 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG03")


@pytest_asyncio.fixture
async def redis():
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.aclose()


@pytest.fixture
def adapter(redis) -> MsgpackTaskAdapter:
    return MsgpackTaskAdapter(redis)


def make_task(
    task_id: ULID = ULID1,
    name: str = "my_task",
    version: int = 1,
    queue: str = "default",
    status: TaskStatus = TaskStatus.SUBMITTED,
) -> Task:
    task = Task(id=task_id, name=name, version=version, queue=queue, status=status)
    task.submitted_at = FROZEN_TIME
    return task


# ── ensure_index ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ensure_index_is_noop(adapter):
    """ensure_index completes without error (no-op for msgpack backend)."""
    await adapter.ensure_index()


# ── get_all_tasks ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_returns_submitted_tasks(adapter):
    task = make_task()
    await adapter.submit_task(task)
    results = await adapter.get_all_tasks(TaskPagination(queue="default"))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_empty(adapter):
    results = await adapter.get_all_tasks(TaskPagination(queue="default"))
    assert results == []


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_task_name(adapter):
    t1 = make_task(ULID1, name="task_a")
    t2 = make_task(ULID2, name="task_b")
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    results = await adapter.get_all_tasks(TaskPagination(queue="default", task_name="task_a"))
    assert len(results) == 1
    assert results[0].name == "task_a"


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_version(adapter):
    t1 = make_task(ULID1, version=1)
    t2 = make_task(ULID2, version=2)
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    results = await adapter.get_all_tasks(TaskPagination(queue="default", task_version=1))
    assert len(results) == 1
    assert results[0].version == 1


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_status(adapter):
    t1 = make_task(ULID1, status=TaskStatus.SUBMITTED)
    t2 = make_task(ULID2, status=TaskStatus.SUBMITTED)
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)
    # Overwrite t2 with a terminal status
    t2_completed = make_task(ULID2, status=TaskStatus.COMPLETED)
    await adapter.save_task(t2_completed)

    results = await adapter.get_all_tasks(TaskPagination(queue="default", status=TaskStatus.SUBMITTED))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_respects_limit(adapter):
    for uid in (ULID1, ULID2, ULID3):
        await adapter.submit_task(make_task(uid))

    results = await adapter.get_all_tasks(TaskPagination(queue="default", limit=2))
    assert len(results) == 2


@pytest.mark.asyncio
async def test_get_all_tasks_order_by_submitted_at(adapter):
    t1 = make_task(ULID1)
    t2 = make_task(ULID2)
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    results = await adapter.get_all_tasks(
        TaskPagination(queue="default", order_by=PaginationOrder.SUBMITTED_AT)
    )
    assert len(results) == 2


@pytest.mark.asyncio
async def test_get_all_tasks_different_queues_are_isolated(adapter):
    t1 = make_task(ULID1, queue="queue_a")
    t2 = make_task(ULID2, queue="queue_b")
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    results = await adapter.get_all_tasks(TaskPagination(queue="queue_a"))
    assert len(results) == 1
    assert results[0].id == ULID1


# ── read_for_watch ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_read_for_watch_returns_task(adapter, redis):
    task = make_task()
    await adapter.save_task(task)

    task_key = adapter.TASK_DETAILS(task_id=ULID1)
    pipe = redis.pipeline()
    await pipe.watch(task_key)
    result = await adapter.read_for_watch(pipe, ULID1)
    await pipe.unwatch()

    assert result is not None
    assert result.id == ULID1
    assert result.name == "my_task"


@pytest.mark.asyncio
async def test_read_for_watch_returns_none_when_missing(adapter, redis):
    task_key = adapter.TASK_DETAILS(task_id=ULID1)
    pipe = redis.pipeline()
    await pipe.watch(task_key)
    result = await adapter.read_for_watch(pipe, ULID1)
    await pipe.unwatch()

    assert result is None


@pytest.mark.asyncio
async def test_read_for_watch_preserves_task_fields(adapter, redis):
    task = make_task(status=TaskStatus.STARTED)
    task.errors = ["oops"]
    task.retry_attempt = 2
    await adapter.save_task(task)

    task_key = adapter.TASK_DETAILS(task_id=ULID1)
    pipe = redis.pipeline()
    await pipe.watch(task_key)
    result = await adapter.read_for_watch(pipe, ULID1)
    await pipe.unwatch()

    assert result is not None
    assert result.status == TaskStatus.STARTED
    assert result.errors == ["oops"]
    assert result.retry_attempt == 2


# ── get_all_tasks: TASK_ID order branch (else) ────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_task_id_order(adapter):
    """The else branch of get_all_tasks (non-SUBMITTED_AT order) uses zrange."""
    t1 = make_task(ULID1)
    t2 = make_task(ULID2)
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    results = await adapter.get_all_tasks(
        TaskPagination(queue="default", order_by=PaginationOrder.TASK_ID)
    )
    assert len(results) == 2


# ── get_all_tasks: raw_data=None (deleted between scan and GET) ───────────────


@pytest.mark.asyncio
async def test_get_all_tasks_skips_missing_data(adapter, redis):
    """Task ID in the queue sorted set but blob deleted → silently skipped."""
    task = make_task()
    await adapter.submit_task(task)
    # Remove the task blob but leave the queue entry
    await redis.delete(adapter.TASK_DETAILS(task_id=ULID1))

    results = await adapter.get_all_tasks(TaskPagination(queue="default"))
    assert results == []


# ── clean_terminal_tasks: edge cases ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_none_blob(adapter, redis):
    """A task:* key that returns no data is silently skipped."""
    # Write a key that looks like a task key but has no data after we delete it
    await redis.set("task:ghost", b"")
    await redis.delete("task:ghost")
    # Seed a real key with no data at scan time by setting then immediately deleting
    await redis.set("task:placeholder", b"data")
    await redis.delete("task:placeholder")
    # clean_terminal_tasks should not raise
    await adapter.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_non_ulid_keys(adapter, redis):
    """A task:* key whose suffix is not a valid ULID is skipped without error."""
    await redis.set("task:not_a_valid_ulid_at_all", b"some data")
    await adapter.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))
    # Key should still exist (we didn't delete it)
    assert await redis.exists("task:not_a_valid_ulid_at_all")


# ── DeadQueue.get_by_filter: task data missing ────────────────────────────────


@pytest.mark.asyncio
async def test_dead_queue_get_by_filter_skips_missing_task_data(adapter, redis):
    """If a task is in the DLQ sorted set but blob is gone, it is skipped."""
    dq = DeadQueue(redis, adapter)
    task = make_task()
    failed_at = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)

    # Add to DLQ index but do NOT save the task blob
    pipe = redis.pipeline(transaction=True)
    dq.stage_add(pipe, task, failed_at)
    await pipe.execute()

    results = await dq.get_by_filter()
    assert results == []


# ── DeadQueue.clean: meta_bytes=None ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_dead_queue_clean_handles_missing_meta(adapter, redis):
    """clean() zrem's the DLQ entry even when meta hash entry is absent."""
    dq = DeadQueue(redis, adapter)
    task = make_task()
    old_time = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)

    # Add task to the main DLQ sorted set only — skip the meta hash
    await redis.zadd(dq.DLQ, {bytes(task.id): old_time.timestamp()})

    cutoff = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)
    await dq.clean(cutoff)

    # The entry should be removed from the sorted set
    score = await redis.zscore(dq.DLQ, bytes(task.id))
    assert score is None
