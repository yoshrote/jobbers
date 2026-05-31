"""
Redis-specific tests for RedisTaskAdapter and RedisJSONTaskAdapter.

Uses the ``redis_task_adapter`` fixture (parametrized over ["redis", "redis_json"]).
Tests the atomic pipeline path (data_store.pipeline(), stage_* methods) and
verify Redis-internal state (sorted sets, hashes, sets, keys).
"""

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task, TaskPagination
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


# ── submit_task (sorted-set assertions) ──────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_task_adds_to_sorted_set(redis_task_adapter):
    """submit_task places the task in the queue sorted set."""
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    task.set_status(TaskStatus.SUBMITTED)
    await redis_task_adapter.submit_task(task)

    task_list = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) in task_list


@pytest.mark.asyncio
async def test_submit_task_twice_does_not_duplicate_queue_entry(redis_task_adapter):
    """Submitting the same task ID twice results in exactly one queue entry."""
    task = Task(id=ULID1, name="Initial Task", status="unsubmitted")
    task.set_status(TaskStatus.SUBMITTED)
    await redis_task_adapter.submit_task(task)

    updated_task = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await redis_task_adapter.submit_task(updated_task)

    task_list = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue=task.queue), 0, -1
    )
    assert task_list == [bytes(ULID1)]


# ── stage_requeue ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_requeue_adds_task_to_queue(redis_task_adapter):
    """stage_requeue enqueues the task back into its sorted-set queue."""
    task = make_task()
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_stage_requeue_uses_submitted_at_as_score(redis_task_adapter):
    """The queue sorted-set score matches the task's submitted_at timestamp."""
    task = make_task()
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    score = await redis_task_adapter.data_store.zscore(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), bytes(ULID1)
    )
    assert score == pytest.approx(FROZEN_TIME.timestamp())


@pytest.mark.asyncio
async def test_stage_requeue_does_not_duplicate_queue_entry(redis_task_adapter):
    """Re-queuing the same task ID does not create a duplicate entry."""
    task = make_task()
    for _ in range(3):
        pipe = redis_task_adapter.data_store.pipeline(transaction=True)
        redis_task_adapter.stage_requeue(pipe, task)
        await pipe.execute()

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert members.count(bytes(ULID1)) == 1


# ── stage_submit_task ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_submit_task_adds_task_to_queue(redis_task_adapter):
    """stage_submit_task enqueues the task into its sorted-set queue."""
    task = make_task()
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_submit_task(pipe, task)
    await pipe.execute()

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_stage_submit_task_uses_submitted_at_as_score(redis_task_adapter):
    """The queue sorted-set score matches the task's submitted_at timestamp."""
    task = make_task()
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_submit_task(pipe, task)
    await pipe.execute()

    score = await redis_task_adapter.data_store.zscore(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), bytes(ULID1)
    )
    assert score == pytest.approx(FROZEN_TIME.timestamp())


@pytest.mark.asyncio
async def test_stage_submit_task_does_not_duplicate_queue_entry(redis_task_adapter):
    """Staging the same task ID twice does not create a duplicate queue entry."""
    task = make_task()
    for _ in range(3):
        pipe = redis_task_adapter.data_store.pipeline(transaction=True)
        redis_task_adapter.stage_submit_task(pipe, task)
        await pipe.execute()

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert members.count(bytes(ULID1)) == 1


# ── stage_remove_from_queue ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_remove_from_queue_removes_from_sorted_set(redis_task_adapter):
    """stage_remove_from_queue removes the task ID from the queue sorted set."""
    task = make_task()
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_requeue(pipe, task)
    await pipe.execute()

    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_remove_from_queue(pipe, task)
    await pipe.execute()

    score = await redis_task_adapter.data_store.zscore(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), bytes(ULID1)
    )
    assert score is None


@pytest.mark.asyncio
async def test_stage_remove_from_queue_removes_from_type_index(redis_task_adapter):
    """stage_remove_from_queue removes the task ID from the type index set."""
    task = make_task()
    await redis_task_adapter.data_store.sadd(
        redis_task_adapter.TASK_BY_TYPE_IDX(name="my_task"), bytes(ULID1)
    )

    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_remove_from_queue(pipe, task)
    await pipe.execute()

    members = await redis_task_adapter.data_store.smembers(
        redis_task_adapter.TASK_BY_TYPE_IDX(name="my_task")
    )
    assert bytes(ULID1) not in members


@pytest.mark.asyncio
async def test_stage_remove_from_queue_leaves_other_tasks_untouched(redis_task_adapter):
    """stage_remove_from_queue only removes the target task, not its queue-mates."""
    t1 = make_task(ULID1)
    t2 = make_task(ULID2, submitted_at=FROZEN_TIME + dt.timedelta(seconds=1))
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_requeue(pipe, t1)
    redis_task_adapter.stage_requeue(pipe, t2)
    await pipe.execute()

    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_remove_from_queue(pipe, t1)
    await pipe.execute()

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) not in members
    assert bytes(ULID2) in members


# ── remove_task_heartbeat ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_task_heartbeat_removes_from_sorted_set(redis_task_adapter):
    """remove_task_heartbeat removes the task from the heartbeat sorted set."""
    task = make_task()
    task.heartbeat_at = FROZEN_TIME
    await redis_task_adapter.update_task_heartbeat(task)
    assert (
        await redis_task_adapter.data_store.zscore(
            redis_task_adapter.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)
        )
        is not None
    )

    await redis_task_adapter.remove_task_heartbeat(task)
    assert (
        await redis_task_adapter.data_store.zscore(
            redis_task_adapter.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)
        )
        is None
    )


# ── update_task_heartbeat (sorted-set assertions) ─────────────────────────────


@pytest.mark.asyncio
async def test_update_task_heartbeat_adds_to_sorted_set(redis_task_adapter):
    """update_task_heartbeat adds the task to the heartbeat sorted set."""
    task = make_task(status=TaskStatus.STARTED)
    await redis_task_adapter.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await redis_task_adapter.update_task_heartbeat(task)

    scores = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.HEARTBEAT_SCORES(queue=task.queue), 0, -1, withscores=True
    )
    assert any(bytes(task.id) == task_id for task_id, _ in scores)


@pytest.mark.asyncio
async def test_update_task_heartbeat_updates_existing_score(redis_task_adapter):
    """A second update_task_heartbeat call overwrites the previous heartbeat score."""
    task = make_task(status=TaskStatus.STARTED)
    await redis_task_adapter.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await redis_task_adapter.update_task_heartbeat(task)

    second_time = FROZEN_TIME + dt.timedelta(seconds=10)
    task.heartbeat_at = second_time
    await redis_task_adapter.update_task_heartbeat(task)

    scores = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.HEARTBEAT_SCORES(queue=task.queue), 0, -1, withscores=True
    )
    score = next(score for task_id, score in scores if bytes(task.id) == task_id)
    assert score == second_time.timestamp()


# ── get_next_task (sorted-set / blob-absence) ─────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_task_removes_from_sorted_set(redis_task_adapter):
    """get_next_task pops the task from the queue sorted set."""
    task = make_task()
    await redis_task_adapter.submit_task(task)

    result = await redis_task_adapter.get_next_task(queues={"default"}, pop_timeout=1)
    assert result is not None
    assert result.id == ULID1

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) not in members


@pytest.mark.asyncio
async def test_get_next_task_skips_missing_data_and_returns_none(redis_task_adapter):
    """When a task is popped from the queue but its blob is absent, it is logged and None is returned."""
    fake_id = ULID1
    queue_key = redis_task_adapter.TASKS_BY_QUEUE(queue="default")
    if isinstance(queue_key, str):
        queue_key = queue_key.encode()
    pop_results = iter(
        [
            (queue_key, bytes(fake_id), FROZEN_TIME.timestamp()),
            None,
        ]
    )
    with patch.object(
        redis_task_adapter.data_store, "bzpopmin", new_callable=AsyncMock, side_effect=pop_results
    ):
        result = await redis_task_adapter.get_next_task(queues={"default"}, pop_timeout=1)
    assert result is None
    assert (
        await redis_task_adapter.data_store.zscore(redis_task_adapter.DLQ_MISSING_DATA, bytes(fake_id))
        is not None
    )


# ── get_all_tasks: missing blob ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_tasks_skips_missing_data(redis_task_adapter):
    """Task registered in the queue but blob deleted → not returned by get_all_tasks."""
    task = make_task()
    await redis_task_adapter.submit_task(task)
    await redis_task_adapter.data_store.delete(redis_task_adapter.TASK_DETAILS(task_id=ULID1))

    results = await redis_task_adapter.get_all_tasks(TaskPagination(queue="default"))
    assert results == []


# ── get_stale_tasks: missing blob ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_stale_tasks_filters_out_missing_blobs(redis_task_adapter):
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
    await redis_task_adapter.save_task(stale_task)
    await redis_task_adapter.update_task_heartbeat(stale_task)
    await redis_task_adapter.data_store.delete(redis_task_adapter.TASK_DETAILS(task_id=stale_task.id))

    stale_tasks = [
        task async for task in redis_task_adapter.get_stale_tasks({"default"}, dt.timedelta(minutes=5))
    ]
    assert len(stale_tasks) == 0


# ── clean (sorted-set assertions) ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_noop_when_no_age_params_leaves_sorted_set_intact(redis_task_adapter):
    """clean() with neither min_queue_age nor max_queue_age does nothing to the sorted set."""
    task = make_task()
    await redis_task_adapter.submit_task(task)

    now = dt.datetime.now(dt.UTC)
    await redis_task_adapter.clean(queues={b"default"}, now=now)

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_clean_inverted_time_range(redis_task_adapter):
    """`clean()` removes everything outside the (max_queue_age, min_queue_age) window."""
    task = make_task(ULID1, submitted_at=FROZEN_TIME)
    await redis_task_adapter.submit_task(task)

    # min > max → inverted: removes [0, min] ∪ [max, now], keeping nothing
    await redis_task_adapter.clean(
        queues={b"default"},
        now=FROZEN_TIME + dt.timedelta(hours=1),
        min_queue_age=FROZEN_TIME + dt.timedelta(seconds=30),
        max_queue_age=FROZEN_TIME - dt.timedelta(seconds=30),
    )

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) not in members


@pytest.mark.asyncio
async def test_clean_max_queue_age_removes_old_entries_from_sorted_set(redis_task_adapter):
    """max_queue_age removes entries at or before that timestamp from the sorted set."""
    old = make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=2))
    recent = make_task(ULID2, submitted_at=FROZEN_TIME)
    await redis_task_adapter.submit_task(old)
    await redis_task_adapter.submit_task(recent)

    cutoff = FROZEN_TIME - dt.timedelta(days=1)
    await redis_task_adapter.clean(queues={b"default"}, now=FROZEN_TIME, max_queue_age=cutoff)

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) not in members
    assert bytes(ULID2) in members


@pytest.mark.asyncio
async def test_clean_min_queue_age_removes_entries_from_sorted_set(redis_task_adapter):
    """min_queue_age removes entries at or after that timestamp from the sorted set."""
    old = make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=10))
    recent = make_task(ULID2, submitted_at=FROZEN_TIME)
    await redis_task_adapter.submit_task(old)
    await redis_task_adapter.submit_task(recent)

    floor = FROZEN_TIME - dt.timedelta(days=1)
    await redis_task_adapter.clean(queues={b"default"}, now=FROZEN_TIME, min_queue_age=floor)

    members = await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert bytes(ULID1) in members
    assert bytes(ULID2) not in members


# ── clean_terminal_tasks (blob/sorted-set assertions) ────────────────────────


@pytest.mark.asyncio
async def test_clean_terminal_tasks_deletes_blob(redis_task_adapter):
    """A completed task blob older than max_age is deleted from the key-value store."""
    task = Task(
        id=ULID1,
        name="test_task",
        queue="default",
        status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await redis_task_adapter.save_task(task)
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_removes_heartbeat_entry(redis_task_adapter):
    """The heartbeat sorted-set entry is removed along with the task blob."""
    task = Task(
        id=ULID1,
        name="test_task",
        queue="default",
        status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await redis_task_adapter.save_task(task)
    await redis_task_adapter.data_store.zadd(
        redis_task_adapter.HEARTBEAT_SCORES(queue="default"),
        {ULID1.bytes: (FROZEN_TIME - dt.timedelta(days=8)).timestamp()},
    )
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert (
        await redis_task_adapter.data_store.zscore(
            redis_task_adapter.HEARTBEAT_SCORES(queue="default"), ULID1.bytes
        )
        is None
    )


@pytest.mark.asyncio
async def test_clean_terminal_tasks_removes_type_index_entry(redis_task_adapter):
    """Orphaned task-type-idx entries are removed alongside the task blob."""
    task = Task(
        id=ULID1,
        name="test_task",
        queue="default",
        status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await redis_task_adapter.save_task(task)
    await redis_task_adapter.data_store.sadd(
        redis_task_adapter.TASK_BY_TYPE_IDX(name="test_task"), ULID1.bytes
    )
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    members = await redis_task_adapter.data_store.smembers(
        redis_task_adapter.TASK_BY_TYPE_IDX(name="test_task")
    )
    assert ULID1.bytes not in members


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_active_task_blob(redis_task_adapter):
    """Tasks in active statuses are never deleted regardless of age."""
    task = Task(
        id=ULID1,
        name="test_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=FROZEN_TIME - dt.timedelta(days=100),
    )
    await redis_task_adapter.save_task(task)
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))
    assert await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_task_within_age_blob(redis_task_adapter):
    """A completed task whose completed_at is within max_age is not deleted."""
    task = Task(
        id=ULID1,
        name="test_task",
        queue="default",
        status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(hours=1),
    )
    await redis_task_adapter.save_task(task)
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_task_without_completed_at_blob(redis_task_adapter):
    """A terminal task with no completed_at is not deleted."""
    task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.FAILED, completed_at=None)
    await redis_task_adapter.save_task(task)
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))
    assert await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID1))


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
async def test_clean_terminal_tasks_cleans_all_terminal_statuses_blob(redis_task_adapter, status):
    """All five terminal statuses are eligible for cleanup when old enough."""
    task = Task(
        id=ULID1,
        name="test_task",
        queue="default",
        status=status,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await redis_task_adapter.save_task(task)
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID1))


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
async def test_clean_terminal_tasks_removes_heartbeat_for_all_terminal_statuses(redis_task_adapter, status):
    """Heartbeat entries are removed for every terminal status, not just COMPLETED."""
    task = Task(
        id=ULID1,
        name="test_task",
        queue="default",
        status=status,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    await redis_task_adapter.save_task(task)
    await redis_task_adapter.data_store.zadd(
        redis_task_adapter.HEARTBEAT_SCORES(queue="default"),
        {ULID1.bytes: (FROZEN_TIME - dt.timedelta(days=8)).timestamp()},
    )
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert (
        await redis_task_adapter.data_store.zscore(
            redis_task_adapter.HEARTBEAT_SCORES(queue="default"), ULID1.bytes
        )
        is None
    )


@pytest.mark.asyncio
async def test_clean_terminal_tasks_leaves_other_tasks_blob_untouched(redis_task_adapter):
    """Only old terminal tasks are deleted; recent task blobs remain."""
    old_task = Task(
        id=ULID1,
        name="test_task",
        queue="default",
        status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(days=8),
    )
    recent_task = Task(
        id=ULID2,
        name="test_task",
        queue="default",
        status=TaskStatus.COMPLETED,
        completed_at=FROZEN_TIME - dt.timedelta(hours=1),
    )
    await redis_task_adapter.save_task(old_task)
    await redis_task_adapter.save_task(recent_task)
    await redis_task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID1))
    assert await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID2))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_non_ulid_keys(redis_task_adapter):
    """
    A task:* key whose suffix is not a valid ULID is silently skipped.

    Both adapters parse the ULID before accessing the task blob, so a plain
    SET key with an invalid suffix triggers ValueError → continue without
    crashing or modifying the key.
    """
    await redis_task_adapter.data_store.set("task:not_a_valid_ulid", b"some data")
    await redis_task_adapter.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))
    assert await redis_task_adapter.data_store.exists("task:not_a_valid_ulid")


# ── clean_dag_runs (sorted-set assertions) ────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_dag_runs_removes_stale_entries_from_sorted_set(redis_task_adapter):
    """clean_dag_runs removes DAG run entries older than max_age from the sorted set."""
    dag_run_id = ULID1
    old_score = (FROZEN_TIME - dt.timedelta(days=10)).timestamp()
    await redis_task_adapter.data_store.zadd(redis_task_adapter.DAG_RUNS, {bytes(dag_run_id): old_score})

    await redis_task_adapter.clean_dag_runs(FROZEN_TIME, dt.timedelta(days=7))

    score = await redis_task_adapter.data_store.zscore(redis_task_adapter.DAG_RUNS, bytes(dag_run_id))
    assert score is None


@pytest.mark.asyncio
async def test_clean_dag_runs_noop_when_nothing_stale(redis_task_adapter):
    """clean_dag_runs leaves recent DAG run entries untouched."""
    dag_run_id = ULID1
    recent_score = (FROZEN_TIME - dt.timedelta(hours=1)).timestamp()
    await redis_task_adapter.data_store.zadd(redis_task_adapter.DAG_RUNS, {bytes(dag_run_id): recent_score})

    await redis_task_adapter.clean_dag_runs(FROZEN_TIME, dt.timedelta(days=7))

    score = await redis_task_adapter.data_store.zscore(redis_task_adapter.DAG_RUNS, bytes(dag_run_id))
    assert score is not None


# ── stage_register_dag_run ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_register_dag_run_adds_to_dag_runs_set(redis_task_adapter):
    """stage_submit_task with a dag_run_id registers the run in the DAG_RUNS sorted set."""
    dag_run_id = ULID()
    task = make_task()
    task.dag_run_id = dag_run_id
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_submit_task(pipe, task)
    await pipe.execute()

    score = await redis_task_adapter.data_store.zscore(redis_task_adapter.DAG_RUNS, bytes(dag_run_id))
    assert score is not None
    assert score == pytest.approx(FROZEN_TIME.timestamp())


@pytest.mark.asyncio
async def test_stage_register_dag_run_noop_when_no_dag_run_id(redis_task_adapter):
    """stage_submit_task without a dag_run_id does not add anything to DAG_RUNS."""
    task = make_task()
    assert task.dag_run_id is None
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_submit_task(pipe, task)
    await pipe.execute()

    count = await redis_task_adapter.data_store.zcard(redis_task_adapter.DAG_RUNS)
    assert count == 0


# ── stage_init_fan_in / init_fan_in (set assertions) ─────────────────────────


@pytest.mark.asyncio
async def test_stage_init_fan_in_creates_tracking_and_members_sets(redis_task_adapter):
    """stage_init_fan_in writes a tracking set and a permanent members set."""
    fan_in_key = "fan-in:test"
    predecessor_ids = {ULID1, ULID2}
    pipe = redis_task_adapter.data_store.pipeline(transaction=True)
    redis_task_adapter.stage_init_fan_in(pipe, fan_in_key, predecessor_ids)
    await pipe.execute()

    tracking = await redis_task_adapter.data_store.smembers(fan_in_key)
    members = await redis_task_adapter.data_store.smembers(f"{fan_in_key}:members")
    expected = {str(ULID1).encode(), str(ULID2).encode()}
    assert tracking == expected
    assert members == expected


@pytest.mark.asyncio
async def test_init_fan_in_creates_tracking_and_members_sets(redis_task_adapter):
    """init_fan_in writes the same sets as stage_init_fan_in but executes immediately."""
    fan_in_key = "fan-in:direct"
    predecessor_ids = {ULID1, ULID2, ULID3}
    await redis_task_adapter.init_fan_in(fan_in_key, predecessor_ids)

    tracking = await redis_task_adapter.data_store.smembers(fan_in_key)
    members = await redis_task_adapter.data_store.smembers(f"{fan_in_key}:members")
    expected = {str(uid).encode() for uid in predecessor_ids}
    assert tracking == expected
    assert members == expected


# ── read_for_watch ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_read_for_watch_returns_task(redis_task_adapter):
    """read_for_watch returns the task when it exists."""
    task = make_task()
    await redis_task_adapter.save_task(task)

    task_key = redis_task_adapter.TASK_DETAILS(task_id=ULID1)
    pipe = redis_task_adapter.data_store.pipeline()
    await pipe.watch(task_key)
    result = await redis_task_adapter.read_for_watch(pipe, ULID1)
    await pipe.unwatch()

    assert result is not None
    assert result.id == ULID1
    assert result.name == "my_task"


@pytest.mark.asyncio
async def test_read_for_watch_returns_none_when_missing(redis_task_adapter):
    """read_for_watch returns None when the task does not exist."""
    task_key = redis_task_adapter.TASK_DETAILS(task_id=ULID1)
    pipe = redis_task_adapter.data_store.pipeline()
    await pipe.watch(task_key)
    result = await redis_task_adapter.read_for_watch(pipe, ULID1)
    await pipe.unwatch()

    assert result is None


@pytest.mark.asyncio
async def test_read_for_watch_preserves_task_fields(redis_task_adapter):
    """read_for_watch returns a task with all fields intact."""
    task = make_task(status=TaskStatus.STARTED)
    task.errors = ["oops"]
    task.retry_attempt = 2
    await redis_task_adapter.save_task(task)

    task_key = redis_task_adapter.TASK_DETAILS(task_id=ULID1)
    pipe = redis_task_adapter.data_store.pipeline()
    await pipe.watch(task_key)
    result = await redis_task_adapter.read_for_watch(pipe, ULID1)
    await pipe.unwatch()

    assert result is not None
    assert result.status == TaskStatus.STARTED
    assert result.errors == ["oops"]
    assert result.retry_attempt == 2


# ── submit_rate_limited_task ──────────────────────────────────────────────────


def _make_rate_task(task_id: ULID, submitted_at: dt.datetime) -> Task:
    return Task(
        id=task_id, name="test", queue="default", status=TaskStatus.SUBMITTED, submitted_at=submitted_at
    )


def _default_queue_config(rate_numerator: int = 2) -> QueueConfig:
    return QueueConfig(
        name="default", rate_numerator=rate_numerator, rate_denominator=1, rate_period=RatePeriod.MINUTE
    )


@pytest.mark.asyncio
async def test_submit_rate_limited_enqueues_when_empty(redis_task_adapter, redis_task_adapter_dt_module):
    """Atomically enqueues the task and records it in the rate-limiter when the set is empty."""
    task = _make_rate_task(ULID1, FROZEN_TIME)
    with patch(redis_task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await redis_task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert bytes(ULID1) in await redis_task_adapter.data_store.zrange(
        redis_task_adapter.QUEUE_RATE_LIMITER(queue="default"), 0, -1
    )
    assert bytes(ULID1) in await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_submit_rate_limited_enqueues_with_room(redis_task_adapter, redis_task_adapter_dt_module):
    """Enqueues when one slot is already used out of two."""
    await redis_task_adapter.data_store.zadd(
        redis_task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 1}
    )
    task = _make_rate_task(ULID2, FROZEN_TIME)
    with patch(redis_task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await redis_task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert bytes(ULID2) in await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )


@pytest.mark.asyncio
async def test_submit_rate_limited_prunes_expired_entries(redis_task_adapter, redis_task_adapter_dt_module):
    """Expired rate-limiter entries are pruned; the new task is accepted."""
    await redis_task_adapter.data_store.zadd(
        redis_task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 60}
    )
    await redis_task_adapter.data_store.zadd(
        redis_task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID2.bytes: FROZEN_TIME.timestamp() - 61}
    )
    new_id = ULID5
    task = _make_rate_task(new_id, FROZEN_TIME)
    with patch(redis_task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await redis_task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert new_id.bytes in await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )


@pytest.mark.asyncio
async def test_submit_rate_limited_rejects_when_full(redis_task_adapter, redis_task_adapter_dt_module):
    """Task is not enqueued when rate limit is reached."""
    await redis_task_adapter.data_store.zadd(
        redis_task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID1.bytes: FROZEN_TIME.timestamp() - 1}
    )
    await redis_task_adapter.data_store.zadd(
        redis_task_adapter.QUEUE_RATE_LIMITER(queue="default"), {ULID2.bytes: FROZEN_TIME.timestamp() - 2}
    )
    task = _make_rate_task(ULID5, FROZEN_TIME)
    with patch(redis_task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await redis_task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is False
    assert ULID5.bytes not in await redis_task_adapter.data_store.zrange(
        redis_task_adapter.TASKS_BY_QUEUE(queue="default"), 0, -1
    )
    assert not await redis_task_adapter.data_store.exists(redis_task_adapter.TASK_DETAILS(task_id=ULID5))


@pytest.mark.asyncio
async def test_submit_rate_limited_concurrent_respects_limit(redis_task_adapter):
    """Concurrent submissions must not collectively exceed the rate limit."""
    now = dt.datetime.now(dt.UTC)
    limit = 5
    queue_config = _default_queue_config(rate_numerator=limit)
    tasks = [_make_rate_task(ULID(), now) for _ in range(10)]

    results = await asyncio.gather(
        *[redis_task_adapter.submit_rate_limited_task(t, queue_config) for t in tasks]
    )

    accepted = sum(1 for r in results if r)
    assert accepted == limit
    assert (
        await redis_task_adapter.data_store.zcard(redis_task_adapter.QUEUE_RATE_LIMITER(queue="default"))
        == limit
    )
    assert (
        await redis_task_adapter.data_store.zcard(redis_task_adapter.TASKS_BY_QUEUE(queue="default")) == limit
    )


# ── _add_task_to_results ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_add_task_to_results_skips_missing_task(redis_task_adapter):
    """_add_task_to_results returns the list unchanged when the task blob is absent."""
    results: list[Task] = []
    returned = await redis_task_adapter._add_task_to_results(ULID1, results)
    assert returned == []
    assert results == []


@pytest.mark.asyncio
async def test_add_task_to_results_appends_found_task(redis_task_adapter):
    """_add_task_to_results appends the task when it exists."""
    task = make_task()
    await redis_task_adapter.save_task(task)
    results: list[Task] = []
    returned = await redis_task_adapter._add_task_to_results(ULID1, results)
    assert len(returned) == 1
    assert returned[0].id == ULID1
