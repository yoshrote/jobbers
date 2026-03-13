"""Tests for SqlTaskAdapter and SqlDeadQueue."""

import datetime as dt

import aiosqlite
import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.adapters.sql import SqlDeadQueue, SqlTaskAdapter
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")
ULID3 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG03")


def make_task(
    task_id: ULID = ULID1,
    queue: str = "default",
    status: TaskStatus = TaskStatus.SUBMITTED,
    name: str = "my_task",
    version: int = 1,
) -> Task:
    task = Task(id=task_id, name=name, version=version, queue=queue, status=status)
    task.submitted_at = FROZEN_TIME
    return task


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def conn():
    """In-memory SQLite connection for the SQL adapter (separate from queue-config schema)."""
    async with aiosqlite.connect(":memory:") as c:
        c.row_factory = aiosqlite.Row
        yield c


@pytest_asyncio.fixture
async def adapter(conn):
    """SqlTaskAdapter backed by in-memory SQLite with schema applied."""
    a = SqlTaskAdapter(conn)
    await a.ensure_index()
    return a


@pytest_asyncio.fixture
async def dead_queue(conn, adapter):
    """SqlDeadQueue backed by the same in-memory SQLite connection."""
    dq = SqlDeadQueue(conn, adapter)
    await dq.ensure_index()
    return dq, adapter


# ---------------------------------------------------------------------------
# ensure_index
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_index_creates_tables(conn):
    """ensure_index creates all expected tables."""
    adapter = SqlTaskAdapter(conn)
    await adapter.ensure_index()

    rows = await (
        await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'jobber_%'"
        )
    ).fetchall()
    table_names = {r[0] for r in rows}
    assert "jobber_tasks" in table_names
    assert "jobber_task_queue" in table_names
    assert "jobber_task_heartbeats" in table_names
    assert "jobber_rate_limiter" in table_names
    assert "jobber_dlq" in table_names


@pytest.mark.asyncio
async def test_ensure_index_is_idempotent(conn):
    """Calling ensure_index twice does not raise."""
    adapter = SqlTaskAdapter(conn)
    await adapter.ensure_index()
    await adapter.ensure_index()  # should not raise


# ---------------------------------------------------------------------------
# submit_task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_task_returns_true_for_new_task(adapter):
    task = make_task()
    assert await adapter.submit_task(task) is True


@pytest.mark.asyncio
async def test_submit_task_returns_false_for_duplicate(adapter):
    task = make_task()
    await adapter.submit_task(task)
    assert await adapter.submit_task(task) is False


@pytest.mark.asyncio
async def test_submit_task_adds_to_queue(adapter, conn):
    task = make_task()
    await adapter.submit_task(task)

    row = await (
        await conn.execute("SELECT task_id FROM jobber_task_queue WHERE task_id = ?", [str(ULID1)])
    ).fetchone()
    assert row is not None


@pytest.mark.asyncio
async def test_submit_task_stores_task_data(adapter):
    task = make_task()
    await adapter.submit_task(task)

    retrieved = await adapter.get_task(ULID1)
    assert retrieved is not None
    assert retrieved.id == ULID1
    assert retrieved.name == "my_task"
    assert retrieved.queue == "default"


# ---------------------------------------------------------------------------
# get_task / get_tasks_bulk
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_task_returns_none_for_missing(adapter):
    result = await adapter.get_task(ULID1)
    assert result is None


@pytest.mark.asyncio
async def test_get_task_hydrates_heartbeat_at(adapter):
    task = make_task()
    await adapter.submit_task(task)
    task.heartbeat_at = FROZEN_TIME
    await adapter.update_task_heartbeat(task)

    retrieved = await adapter.get_task(ULID1)
    assert retrieved is not None
    assert retrieved.heartbeat_at is not None
    assert abs(retrieved.heartbeat_at.timestamp() - FROZEN_TIME.timestamp()) < 0.001


@pytest.mark.asyncio
async def test_get_tasks_bulk_returns_in_order(adapter):
    t1 = make_task(ULID1)
    t2 = make_task(ULID2)
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    results = await adapter.get_tasks_bulk([ULID1, ULID2])
    assert len(results) == 2
    assert results[0] is not None
    assert results[0].id == ULID1
    assert results[1] is not None
    assert results[1].id == ULID2


@pytest.mark.asyncio
async def test_get_tasks_bulk_returns_none_for_missing(adapter):
    t1 = make_task(ULID1)
    await adapter.submit_task(t1)

    results = await adapter.get_tasks_bulk([ULID1, ULID2])
    assert results[0] is not None
    assert results[1] is None


# ---------------------------------------------------------------------------
# task_exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_exists_false_for_missing(adapter):
    assert await adapter.task_exists(ULID1) is False


@pytest.mark.asyncio
async def test_task_exists_true_after_submit(adapter):
    await adapter.submit_task(make_task())
    assert await adapter.task_exists(ULID1) is True


# ---------------------------------------------------------------------------
# stage_save / pipeline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stage_save_upserts_task(adapter):
    task = make_task()
    await adapter.submit_task(task)

    task.status = TaskStatus.STARTED
    task.started_at = FROZEN_TIME
    pipe = adapter.pipeline()
    adapter.stage_save(pipe, task)
    await pipe.execute()

    retrieved = await adapter.get_task(ULID1)
    assert retrieved is not None
    assert retrieved.status == TaskStatus.STARTED


@pytest.mark.asyncio
async def test_stage_save_empty_pipeline_is_noop(adapter):
    """Executing an empty pipeline does not raise."""
    pipe = adapter.pipeline()
    await pipe.execute()


# ---------------------------------------------------------------------------
# stage_requeue / stage_remove_from_queue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stage_requeue_adds_to_queue(adapter, conn):
    task = make_task()
    await adapter.submit_task(task)
    # pop it first so queue is empty
    await adapter.get_next_task({"default"})

    task.set_status(TaskStatus.SUBMITTED)
    pipe = adapter.pipeline()
    adapter.stage_requeue(pipe, task)
    await pipe.execute()

    row = await (
        await conn.execute(
            "SELECT task_id FROM jobber_task_queue WHERE task_id = ?", [str(ULID1)]
        )
    ).fetchone()
    assert row is not None


@pytest.mark.asyncio
async def test_stage_remove_from_queue_deletes_entry(adapter, conn):
    task = make_task()
    await adapter.submit_task(task)

    pipe = adapter.pipeline()
    adapter.stage_remove_from_queue(pipe, task)
    await pipe.execute()

    row = await (
        await conn.execute(
            "SELECT task_id FROM jobber_task_queue WHERE task_id = ?", [str(ULID1)]
        )
    ).fetchone()
    assert row is None


# ---------------------------------------------------------------------------
# get_next_task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_next_task_returns_none_when_empty(adapter):
    result = await adapter.get_next_task({"default"}, pop_timeout=0)
    assert result is None


@pytest.mark.asyncio
async def test_get_next_task_returns_task(adapter):
    task = make_task()
    await adapter.submit_task(task)

    result = await adapter.get_next_task({"default"}, pop_timeout=0)
    assert result is not None
    assert result.id == ULID1


@pytest.mark.asyncio
async def test_get_next_task_removes_from_queue(adapter, conn):
    await adapter.submit_task(make_task())
    await adapter.get_next_task({"default"})

    row = await (
        await conn.execute(
            "SELECT task_id FROM jobber_task_queue WHERE task_id = ?", [str(ULID1)]
        )
    ).fetchone()
    assert row is None


@pytest.mark.asyncio
async def test_get_next_task_fifo_order(adapter):
    """Tasks are returned in ascending score (submitted_at) order."""
    t1 = make_task(ULID1)
    t1.submitted_at = FROZEN_TIME
    t2 = make_task(ULID2)
    t2.submitted_at = FROZEN_TIME + dt.timedelta(seconds=1)

    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    first = await adapter.get_next_task({"default"})
    assert first is not None
    assert first.id == ULID1

    second = await adapter.get_next_task({"default"})
    assert second is not None
    assert second.id == ULID2


@pytest.mark.asyncio
async def test_get_next_task_polls_until_task_arrives(adapter):
    """With pop_timeout > 0, get_next_task polls and returns once a task is enqueued."""
    import asyncio

    async def enqueue_later() -> None:
        await asyncio.sleep(0.6)
        task = make_task()
        await adapter.submit_task(task)

    enqueue_task = asyncio.create_task(enqueue_later())
    result = await adapter.get_next_task({"default"}, pop_timeout=2)
    await enqueue_task

    assert result is not None
    assert result.id == ULID1


@pytest.mark.asyncio
async def test_get_next_task_respects_timeout(adapter):
    """get_next_task returns None when no tasks appear within pop_timeout."""
    result = await adapter.get_next_task({"default"}, pop_timeout=1)
    assert result is None


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_task_heartbeat(adapter, conn):
    task = make_task()
    await adapter.submit_task(task)
    task.heartbeat_at = FROZEN_TIME
    await adapter.update_task_heartbeat(task)

    row = await (
        await conn.execute(
            "SELECT score FROM jobber_task_heartbeats WHERE task_id = ?", [str(ULID1)]
        )
    ).fetchone()
    assert row is not None
    assert abs(row[0] - FROZEN_TIME.timestamp()) < 0.001


@pytest.mark.asyncio
async def test_remove_task_heartbeat(adapter, conn):
    task = make_task()
    await adapter.submit_task(task)
    task.heartbeat_at = FROZEN_TIME
    await adapter.update_task_heartbeat(task)

    await adapter.remove_task_heartbeat(task)

    row = await (
        await conn.execute(
            "SELECT task_id FROM jobber_task_heartbeats WHERE task_id = ?", [str(ULID1)]
        )
    ).fetchone()
    assert row is None


@pytest.mark.asyncio
async def test_remove_task_heartbeat_noop_when_absent(adapter):
    """remove_task_heartbeat does not raise if no heartbeat exists."""
    task = make_task()
    await adapter.remove_task_heartbeat(task)  # should not raise


# ---------------------------------------------------------------------------
# get_active_tasks / get_stale_tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_active_tasks_empty_when_no_heartbeats(adapter):
    result = await adapter.get_active_tasks({"default"})
    assert result == []


@pytest.mark.asyncio
async def test_get_active_tasks_returns_tasks_with_heartbeats(adapter):
    task = make_task()
    await adapter.submit_task(task)
    task.heartbeat_at = FROZEN_TIME
    await adapter.update_task_heartbeat(task)

    result = await adapter.get_active_tasks({"default"})
    assert len(result) == 1
    assert result[0].id == ULID1


@pytest.mark.asyncio
async def test_get_stale_tasks_returns_tasks_below_cutoff(adapter):
    task = make_task()
    await adapter.submit_task(task)
    task.heartbeat_at = FROZEN_TIME  # old heartbeat
    await adapter.update_task_heartbeat(task)

    stale_time = dt.timedelta(minutes=5)

    # Manually set "now" by using a cutoff > heartbeat_at
    stale = []
    async for t in adapter.get_stale_tasks({"default"}, stale_time):
        stale.append(t)

    assert len(stale) == 1
    assert stale[0].id == ULID1


@pytest.mark.asyncio
async def test_get_stale_tasks_skips_fresh_tasks(adapter):
    task = make_task()
    await adapter.submit_task(task)
    task.heartbeat_at = dt.datetime.now(dt.UTC)  # very recent heartbeat
    await adapter.update_task_heartbeat(task)

    stale = []
    async for t in adapter.get_stale_tasks({"default"}, dt.timedelta(hours=1)):
        stale.append(t)

    assert stale == []


# ---------------------------------------------------------------------------
# get_all_tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_queue(adapter):
    from jobbers.models.task import TaskPagination

    await adapter.submit_task(make_task(ULID1, queue="q1"))
    await adapter.submit_task(make_task(ULID2, queue="q2"))

    results = await adapter.get_all_tasks(TaskPagination(queue="q1"))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_name(adapter):
    from jobbers.models.task import TaskPagination

    await adapter.submit_task(make_task(ULID1, name="task_a"))
    await adapter.submit_task(make_task(ULID2, name="task_b"))

    results = await adapter.get_all_tasks(TaskPagination(queue="default", task_name="task_a"))
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_version(adapter):
    from jobbers.models.task import TaskPagination

    await adapter.submit_task(make_task(ULID1, version=1))
    await adapter.submit_task(make_task(ULID2, version=2))

    results = await adapter.get_all_tasks(TaskPagination(queue="default", task_version=2))
    assert len(results) == 1
    assert results[0].id == ULID2


@pytest.mark.asyncio
async def test_get_all_tasks_filters_by_status(adapter):
    from jobbers.models.task import TaskPagination

    t1 = make_task(ULID1, status=TaskStatus.SUBMITTED)
    t2 = make_task(ULID2, status=TaskStatus.SUBMITTED)
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    # Mark t2 as started via save
    t2.status = TaskStatus.STARTED
    await adapter.save_task(t2)

    results = await adapter.get_all_tasks(
        TaskPagination(queue="default", status=TaskStatus.STARTED)
    )
    assert len(results) == 1
    assert results[0].id == ULID2


@pytest.mark.asyncio
async def test_get_all_tasks_limit_and_offset(adapter):
    from jobbers.models.task import TaskPagination

    for i, ulid in enumerate([ULID1, ULID2, ULID3]):
        t = make_task(ulid)
        t.submitted_at = FROZEN_TIME + dt.timedelta(seconds=i)
        await adapter.submit_task(t)

    page1 = await adapter.get_all_tasks(TaskPagination(queue="default", limit=2, offset=0))
    page2 = await adapter.get_all_tasks(TaskPagination(queue="default", limit=2, offset=2))

    assert len(page1) == 2
    assert len(page2) == 1


# ---------------------------------------------------------------------------
# clean_terminal_tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clean_terminal_tasks_removes_old_completed(adapter, conn):
    task = make_task(status=TaskStatus.COMPLETED)
    task.completed_at = FROZEN_TIME
    await adapter.submit_task(task)
    await adapter.save_task(task)

    now = FROZEN_TIME + dt.timedelta(days=2)
    await adapter.clean_terminal_tasks(now, max_age=dt.timedelta(days=1))

    assert await adapter.get_task(ULID1) is None


@pytest.mark.asyncio
async def test_clean_terminal_tasks_keeps_recent_completed(adapter):
    task = make_task(status=TaskStatus.COMPLETED)
    task.completed_at = dt.datetime.now(dt.UTC)
    await adapter.submit_task(task)
    await adapter.save_task(task)

    now = dt.datetime.now(dt.UTC)
    await adapter.clean_terminal_tasks(now, max_age=dt.timedelta(days=7))

    assert await adapter.get_task(ULID1) is not None


@pytest.mark.asyncio
async def test_clean_terminal_tasks_also_removes_heartbeat(adapter, conn):
    task = make_task(status=TaskStatus.COMPLETED)
    task.completed_at = FROZEN_TIME
    task.heartbeat_at = FROZEN_TIME
    await adapter.submit_task(task)
    await adapter.save_task(task)
    await adapter.update_task_heartbeat(task)

    now = FROZEN_TIME + dt.timedelta(days=2)
    await adapter.clean_terminal_tasks(now, max_age=dt.timedelta(days=1))

    hb = await (
        await conn.execute(
            "SELECT task_id FROM jobber_task_heartbeats WHERE task_id = ?", [str(ULID1)]
        )
    ).fetchone()
    assert hb is None


# ---------------------------------------------------------------------------
# clean (queue entries by score range)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clean_noop_when_no_age_params(adapter):
    task = make_task()
    await adapter.submit_task(task)

    now = dt.datetime.now(dt.UTC)
    await adapter.clean(queues={b"default"}, now=now)

    assert await adapter.get_next_task({"default"}) is not None


@pytest.mark.asyncio
async def test_clean_removes_queue_entries_in_range(adapter):
    task = make_task()
    await adapter.submit_task(task)

    now = FROZEN_TIME + dt.timedelta(hours=1)
    await adapter.clean(
        queues={b"default"},
        now=now,
        min_queue_age=FROZEN_TIME - dt.timedelta(seconds=1),
        max_queue_age=FROZEN_TIME + dt.timedelta(seconds=1),
    )

    assert await adapter.get_next_task({"default"}) is None


# ---------------------------------------------------------------------------
# submit_rate_limited_task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_rate_limited_task_accepts_when_under_limit(adapter):
    cfg = QueueConfig(name="default", rate_numerator=3, rate_denominator=1, rate_period="minute")
    task = make_task()
    result = await adapter.submit_rate_limited_task(task, cfg)
    assert result is True


@pytest.mark.asyncio
async def test_submit_rate_limited_task_rejects_when_over_limit(adapter):
    cfg = QueueConfig(name="default", rate_numerator=1, rate_denominator=1, rate_period="minute")

    t1 = make_task(ULID1)
    await adapter.submit_rate_limited_task(t1, cfg)

    t2 = make_task(ULID2)
    result = await adapter.submit_rate_limited_task(t2, cfg)
    assert result is False


@pytest.mark.asyncio
async def test_submit_rate_limited_task_allows_resubmission(adapter):
    """A task that already exists bypasses the rate limit check."""
    cfg = QueueConfig(name="default", rate_numerator=1, rate_denominator=1, rate_period="minute")

    task = make_task(ULID1)
    await adapter.submit_rate_limited_task(task, cfg)

    # Fill the rate limit with a second task
    t2 = make_task(ULID2)
    await adapter.submit_rate_limited_task(t2, cfg)

    # Re-submitting ULID1 should succeed even though limit is full
    result = await adapter.submit_rate_limited_task(task, cfg)
    assert result is True


# ---------------------------------------------------------------------------
# SqlDeadQueue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dlq_stage_add_and_get_by_ids(dead_queue):
    dq, adapter = dead_queue
    task = make_task()
    await adapter.submit_task(task)

    pipe = adapter.pipeline()
    dq.stage_add(pipe, task, FROZEN_TIME)
    await pipe.execute()

    results = await dq.get_by_ids([str(ULID1)])
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_dlq_stage_remove(dead_queue):
    dq, adapter = dead_queue
    task = make_task()
    await adapter.submit_task(task)

    pipe = adapter.pipeline()
    dq.stage_add(pipe, task, FROZEN_TIME)
    await pipe.execute()

    pipe2 = adapter.pipeline()
    dq.stage_remove(pipe2, task.id, task.queue, task.name)
    await pipe2.execute()

    results = await dq.get_by_ids([str(ULID1)])
    assert results == []


@pytest.mark.asyncio
async def test_dlq_get_by_filter_queue(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(ULID1, queue="q1")
    t2 = make_task(ULID2, queue="q2")
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    for task in [t1, t2]:
        pipe = adapter.pipeline()
        dq.stage_add(pipe, task, FROZEN_TIME)
        await pipe.execute()

    results = await dq.get_by_filter(queue="q1")
    assert len(results) == 1
    assert results[0].id == ULID1


@pytest.mark.asyncio
async def test_dlq_get_history_returns_errors(dead_queue):
    dq, adapter = dead_queue
    task = make_task()
    task.errors = ["error one", "error two"]
    await adapter.submit_task(task)

    pipe = adapter.pipeline()
    dq.stage_add(pipe, task, FROZEN_TIME)
    await pipe.execute()

    history = await dq.get_history(str(ULID1))
    assert len(history) == 2
    assert history[0] == {"attempt": 0, "error": "error one"}
    assert history[1] == {"attempt": 1, "error": "error two"}


@pytest.mark.asyncio
async def test_dlq_remove_many(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(ULID1)
    t2 = make_task(ULID2)
    await adapter.submit_task(t1)
    await adapter.submit_task(t2)

    for task in [t1, t2]:
        pipe = adapter.pipeline()
        dq.stage_add(pipe, task, FROZEN_TIME)
        await pipe.execute()

    await dq.remove_many([str(ULID1), str(ULID2)])
    assert await dq.get_by_ids([str(ULID1), str(ULID2)]) == []


@pytest.mark.asyncio
async def test_dlq_clean_removes_old_entries(dead_queue, conn):
    dq, adapter = dead_queue
    task = make_task()
    await adapter.submit_task(task)

    pipe = adapter.pipeline()
    dq.stage_add(pipe, task, FROZEN_TIME)
    await pipe.execute()

    await dq.clean(FROZEN_TIME + dt.timedelta(seconds=1))

    row = await (
        await conn.execute("SELECT task_id FROM jobber_dlq WHERE task_id = ?", [str(ULID1)])
    ).fetchone()
    assert row is None


# ---------------------------------------------------------------------------
# read_for_watch (SQL ignores pipe, just reads)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_for_watch_returns_task(adapter):
    task = make_task()
    await adapter.submit_task(task)

    pipe = adapter.pipeline()  # ignored by SQL adapter
    result = await adapter.read_for_watch(pipe, ULID1)
    assert result is not None
    assert result.id == ULID1


@pytest.mark.asyncio
async def test_read_for_watch_returns_none_for_missing(adapter):
    pipe = adapter.pipeline()
    result = await adapter.read_for_watch(pipe, ULID1)
    assert result is None
