import asyncio
import contextlib
import datetime as dt
from collections import defaultdict
from unittest.mock import patch

import pytest
import pytest_asyncio
from ulid import ULID

from jobbers import registry
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task
from jobbers.models.task_config import DeadLetterPolicy, TaskConfig
from jobbers.models.task_status import TaskStatus
from jobbers.state_manager import StateManager, UserCancellationError

FROZEN_TIME = dt.datetime.fromisoformat("2021-01-01T00:00:00+00:00")
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")

@pytest_asyncio.fixture
async def state_manager(redis, sqlite_conn):
    """Fixture to provide a StateManager instance with a fake Redis and in-memory SQLite."""
    return StateManager(redis, sqlite_conn)


async def schedule(sm: StateManager, task: Task, run_at: dt.datetime) -> None:
    pipe = sm.data_store.pipeline(transaction=True)
    sm.task_scheduler.stage_add(pipe, task, run_at)
    await pipe.execute()


async def add_to_dlq(sm: StateManager, task: Task, failed_at: dt.datetime) -> None:
    pipe = sm.data_store.pipeline(transaction=True)
    sm.dead_queue.stage_add(pipe, task, failed_at)
    await pipe.execute()


@pytest.mark.asyncio
async def test_get_refresh_tag(state_manager):
    """Test that get_refresh_tag creates and returns a consistent refresh tag for a role."""
    # First call creates the tag
    tag1 = await state_manager.get_refresh_tag("test_role")
    assert tag1 is not None

    # Second call returns the same tag (unchanged)
    tag2 = await state_manager.get_refresh_tag("test_role")
    assert tag1 == tag2


@pytest.mark.asyncio
async def test_get_next_task_returns_task(redis, state_manager):
    """Test that get_next_task retrieves the next task from the queues."""
    task_id = ULID()
    task_obj = Task(id=task_id, name="Test Task", queue="queue1", version=1, status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME)
    await redis.zadd("task-queues:queue1", {task_id.bytes: 1})
    await redis.json().set(f"task:{task_id}", "$", task_obj.to_dict())

    task = await state_manager.get_next_task(["queue1", "queue2"], pop_timeout=1)

    assert task is not None
    assert task.id == task_id
    assert task.name == "Test Task"
    assert task.status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_get_next_task_no_task_found(state_manager):
    """Test that get_next_task returns None if no task is found."""
    task = await state_manager.get_next_task(["queue1", "queue2"], pop_timeout=1)

    assert task is None


@pytest.mark.asyncio
async def test_get_next_task_missing_task_data(redis, state_manager):
    """Test that a task with missing data is moved to dlq-missing-data and None is returned."""
    task_id = ULID()
    await redis.zadd("task-queues:queue1", {task_id.bytes: 1})

    task = await state_manager.get_next_task(["queue1", "queue2"], pop_timeout=1)

    assert task is None
    dlq_members = await redis.zrange("dlq-missing-data", 0, -1)
    assert task_id.bytes in dlq_members


@pytest.mark.asyncio
async def test_get_next_task_skips_missing_data_and_returns_valid(redis, state_manager):
    """When the first queued task has missing data it is skipped and the next valid task is returned."""
    missing_id = ULID()
    valid_id = ULID()
    valid_task = Task(id=valid_id, name="Test Task", queue="queue1", version=1,
                      status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME)

    # missing_id has a lower score so it is popped first
    await redis.zadd("task-queues:queue1", {missing_id.bytes: 1, valid_id.bytes: 2})
    await redis.json().set(f"task:{valid_id}", "$", valid_task.to_dict())

    task = await state_manager.get_next_task(["queue1"], pop_timeout=1)

    assert task is not None
    assert task.id == valid_id
    dlq_members = await redis.zrange("dlq-missing-data", 0, -1)
    assert missing_id.bytes in dlq_members

@pytest.fixture
def rate_limiter(state_manager):
    return state_manager.submission_limiter

@pytest.mark.asyncio
async def test_concurrency_limits_no_limits(rate_limiter):
    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = {
        "queue1": {ULID()},
        "queue2": {ULID()},
    }

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue1", "queue2"}

@pytest.mark.asyncio
async def test_concurrency_limits_with_limits(state_manager, rate_limiter):
    await state_manager.qca.save_queue_config(QueueConfig(name="queue1", max_concurrent=1))
    await state_manager.qca.save_queue_config(QueueConfig(name="queue2", max_concurrent=2))

    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = {
        "queue1": {ULID()},
        "queue2": {ULID()},
    }

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue2"}

@pytest.mark.asyncio
async def test_concurrency_limits_empty_queues(state_manager, rate_limiter):
    await state_manager.qca.save_queue_config(QueueConfig(name="queue1", max_concurrent=1))
    await state_manager.qca.save_queue_config(QueueConfig(name="queue2", max_concurrent=1))

    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = defaultdict(set)

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue1", "queue2"}

@pytest.mark.asyncio
async def test_clean_rate_limit_age(redis, state_manager):
    """Test cleaning tasks from the rate limiter based on rate_limit_age."""
    await state_manager.qca.save_queue_config(QueueConfig(name="queue1"))
    await state_manager.qca.save_queue_config(QueueConfig(name="queue2"))
    await redis.zadd("rate-limiter:queue1", {ULID1.bytes: FROZEN_TIME.timestamp() - 3600})
    await redis.zadd("rate-limiter:queue2", {ULID2.bytes: FROZEN_TIME.timestamp() - 1800})

    # Call the clean method with a rate_limit_age of 1 hour
    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(rate_limit_age=dt.timedelta(hours=1))

    # Verify tasks older than 1 hour were removed
    queue1_tasks = await redis.zrange("rate-limiter:queue1", 0, -1)
    queue2_tasks = await redis.zrange("rate-limiter:queue2", 0, -1)
    assert queue1_tasks == []
    assert queue2_tasks == [ULID2.bytes]

@pytest.mark.asyncio
async def test_clean_min_queue_age(redis, state_manager):
    """Test cleaning tasks from queues based on min_queue_age."""
    await state_manager.qca.save_queue_config(QueueConfig(name="queue1"))
    await redis.zadd("task-queues:queue1", {
        ULID1.bytes: FROZEN_TIME.timestamp() - 3600,
        ULID2.bytes: FROZEN_TIME.timestamp() - 1800,
    })

    # Call the clean method with a min_queue_age of 30 minutes
    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(min_queue_age=FROZEN_TIME-dt.timedelta(minutes=30))

    # Verify tasks older than 30 minutes remain
    queue1_tasks = await redis.zrange("task-queues:queue1", 0, -1, withscores=True)
    assert queue1_tasks == [(ULID1.bytes, FROZEN_TIME.timestamp() - 3600)]

@pytest.mark.asyncio
async def test_clean_max_queue_age(redis, state_manager):
    """Test cleaning tasks from queues based on max_queue_age."""
    await state_manager.qca.save_queue_config(QueueConfig(name="queue1"))
    await redis.zadd("task-queues:queue1", {
        ULID1.bytes: FROZEN_TIME.timestamp() - 3600,
        ULID2.bytes: FROZEN_TIME.timestamp() - 1800,
    })

    # Call the clean method with a max_queue_age of 1 hour
    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(max_queue_age=FROZEN_TIME-dt.timedelta(hours=1))

    # Verify tasks newer than 1 hour remain
    queue1_tasks = await redis.zrange("task-queues:queue1", 0, -1, withscores=True)
    assert queue1_tasks == [(ULID2.bytes, FROZEN_TIME.timestamp() - 1800)]

@pytest.mark.asyncio
async def test_clean_min_and_max_queue_age(redis, state_manager):
    """Test cleaning tasks from queues based on both min_queue_age and max_queue_age."""
    await state_manager.qca.save_queue_config(QueueConfig(name="queue1"))
    await redis.zadd("task-queues:queue1", {
        ULID1.bytes: FROZEN_TIME.timestamp() - 3600,
        ULID2.bytes: FROZEN_TIME.timestamp() - 1800,
    })

    # Call the clean method with a min_queue_age of 30 minutes and max_queue_age of 1 hour
    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(
            min_queue_age=FROZEN_TIME-dt.timedelta(minutes=30),
            max_queue_age=FROZEN_TIME-dt.timedelta(hours=1),
        )

    # Verify only tasks within the age range remain
    queue1_tasks = await redis.zrange("task-queues:queue1", 0, -1, withscores=True)
    assert queue1_tasks == []

@pytest.mark.asyncio
async def test_clean_completed_task_age_removes_old_terminal_tasks(redis, state_manager):
    """completed_task_age deletes terminal task blobs and their index entries when old enough."""
    old_task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.COMPLETED,
                    completed_at=FROZEN_TIME - dt.timedelta(days=8))
    active_task = Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.STARTED,
                       started_at=FROZEN_TIME - dt.timedelta(hours=1))
    await state_manager.ta.save_task(old_task)
    await state_manager.ta.save_task(active_task)
    await state_manager.qca.save_queue_config(QueueConfig(name="default"))

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(completed_task_age=dt.timedelta(days=7))

    assert not await redis.exists(f"task:{ULID1}")
    assert await redis.exists(f"task:{ULID2}")

@pytest.mark.asyncio
async def test_clean_dlq_age_removes_old_entries(redis, state_manager):
    """dlq_age removes DLQ index entries older than the cutoff."""
    await state_manager.qca.save_queue_config(QueueConfig(name="default"))
    old_task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED)
    recent_task = Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.FAILED)
    await add_to_dlq(state_manager, old_task, FROZEN_TIME - dt.timedelta(days=8))
    await add_to_dlq(state_manager, recent_task, FROZEN_TIME - dt.timedelta(days=6))

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(dlq_age=dt.timedelta(days=7))

    assert await redis.zscore("dlq", ULID1.bytes) is None
    assert ULID1.bytes not in await redis.smembers("dlq-queue:default")
    assert await redis.zscore("dlq", ULID2.bytes) is not None

@pytest.mark.asyncio
async def test_clean_dlq_age_keeps_recent_entries(redis, state_manager):
    """dlq_age does not remove DLQ entries within the cutoff window."""
    await state_manager.qca.save_queue_config(QueueConfig(name="default"))
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED)
    await add_to_dlq(state_manager, task, FROZEN_TIME - dt.timedelta(days=6))

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(dlq_age=dt.timedelta(days=7))

    assert await redis.zscore("dlq", ULID1.bytes) is not None

@pytest.mark.asyncio
async def test_clean_stale_time_skips_terminal_tasks(redis, state_manager):
    """A COMPLETED task with a stale heartbeat entry is NOT re-marked STALLED."""
    # Use real timestamps so datetime mocking doesn't interfere with serialization.
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    completed = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.COMPLETED,
                     completed_at=two_hours_ago + dt.timedelta(minutes=30),
                     heartbeat_at=two_hours_ago)
    await state_manager.ta.save_task(completed)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager.qca.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(name="my_task", function=dummy_fn,
                              max_heartbeat_interval=dt.timedelta(minutes=5))
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager.ta.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.COMPLETED

@pytest.mark.asyncio
async def test_clean_stale_time_removes_heartbeat_on_stall(redis, state_manager):
    """When a STARTED task is marked STALLED, its heartbeat sorted-set entry is removed."""
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    started = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED,
                   started_at=two_hours_ago, heartbeat_at=two_hours_ago)
    await state_manager.ta.save_task(started)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager.qca.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(name="my_task", function=dummy_fn,
                              max_heartbeat_interval=dt.timedelta(minutes=5))
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager.ta.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STALLED
    assert await redis.zscore("task-heartbeats:default", ULID1.bytes) is None

@pytest.mark.asyncio
async def test_dispatch_scheduled_task(redis, state_manager):
    """dispatch_scheduled_task moves a due task from the scheduler into its Redis queue."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SUBMITTED, retry_attempt=1)
    run_at = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
    # Pre-store task data (normally set by submit_task/save_task before scheduling)
    await state_manager.ta.save_task(task)
    await schedule(state_manager, task, run_at)

    due = await state_manager.task_scheduler.next_due(["default"])
    assert due is not None
    await state_manager.dispatch_scheduled_task(due)

    # Task must now be in the Redis sorted set for its queue
    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    # Task must be removed from the scheduler
    assert await state_manager.task_scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_dispatch_scheduled_task_skips_cancelled(redis, state_manager):
    """dispatch_scheduled_task does not re-enqueue a task that was cancelled after scheduler acquisition."""
    # Simulate: scheduler acquired the task with a stale SCHEDULED status object, but
    # by the time dispatch runs the cancel path has already written CANCELLED to Redis.
    cancelled = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.CANCELLED, retry_attempt=1)
    await state_manager.ta.save_task(cancelled)

    stale = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SCHEDULED, retry_attempt=1)
    await state_manager.dispatch_scheduled_task(stale)

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) not in queue_members
    saved = await state_manager.ta.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_task_in_registry(state_manager):
    """Test that a task is correctly identified as being in the active tasks registry."""
    task = Task(
        id=ULID2,
        name="No Submitted At",
        status=TaskStatus.SUBMITTED,
        queue="default",
        submitted_at=None,
    )

    # Initially, the task should not be in the registry
    assert task.id not in state_manager.current_tasks_by_queue[task.queue]

    # Add the task to the registry
    with state_manager.task_in_registry(task):
        assert task.id in state_manager.current_tasks_by_queue[task.queue]

    # Now, the task should be removed from the registry
    assert task.id not in state_manager.current_tasks_by_queue[task.queue]

if __name__ == "__main__":
    pytest.main(["-v", "test_state_manager.py"])


# ── Helpers ───────────────────────────────────────────────────────────────────

async def dummy_fn(): # pragma: no cover
    pass


def make_task_config(dead_letter_policy: DeadLetterPolicy = DeadLetterPolicy.NONE) -> TaskConfig:
    return TaskConfig(name="my_task", function=dummy_fn, dead_letter_policy=dead_letter_policy)


# ── fail_task ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fail_task_no_dlq_writes_redis_only(redis, state_manager):
    """fail_task with NONE policy updates Redis but does not touch the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED)
    task.task_config = make_task_config(DeadLetterPolicy.NONE)

    await state_manager.fail_task(task)

    saved = await state_manager.ta.get_task(ULID1)
    assert saved.status == TaskStatus.FAILED
    assert await state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


@pytest.mark.asyncio
async def test_fail_task_with_dlq_writes_both_stores(redis, state_manager):
    """fail_task with SAVE policy updates Redis and writes to the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["oops"])
    task.task_config = make_task_config(DeadLetterPolicy.SAVE)

    await state_manager.fail_task(task)

    saved = await state_manager.ta.get_task(ULID1)
    assert saved.status == TaskStatus.FAILED
    dlq = await state_manager.dead_queue.get_by_ids([str(ULID1)])
    assert len(dlq) == 1
    assert dlq[0].id == ULID1



# ── resubmit_dead_tasks ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_resubmit_dead_tasks_requeues_and_clears_dlq(redis, state_manager):
    """All tasks are enqueued in Redis and removed from the DLQ."""
    task1 = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    task2 = Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e2"])
    await state_manager.ta.save_task(task1)
    await state_manager.ta.save_task(task2)
    await add_to_dlq(state_manager, task1, FROZEN_TIME)
    await add_to_dlq(state_manager, task2, FROZEN_TIME)

    await state_manager.resubmit_dead_tasks([task1, task2])

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert bytes(ULID2) in queue_members
    assert await state_manager.dead_queue.get_by_ids([str(ULID1), str(ULID2)]) == []



@pytest.mark.asyncio
async def test_resubmit_dead_tasks_is_idempotent(redis, state_manager):
    """Re-running resubmit for a task already in Redis does not raise and clears the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    await state_manager.ta.save_task(task)
    await add_to_dlq(state_manager, task, FROZEN_TIME)

    # First run — normal path.
    await state_manager.resubmit_dead_tasks([task])

    # Operator re-adds to DLQ and retries (simulates the crash scenario above recovering).
    task2 = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    await add_to_dlq(state_manager, task2, FROZEN_TIME)
    await state_manager.resubmit_dead_tasks([task2])

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert await state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


# ── schedule_retry_task / dispatch_scheduled_task ─────────────────────────────

@pytest.mark.asyncio
async def test_schedule_retry_task_self_heals_via_dispatch(redis, state_manager):
    """If save_task was missed (crash), the scheduler dispatch recovers by re-enqueueing."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.STARTED, retry_attempt=1)
    run_at = FROZEN_TIME

    # Simulate: task already existed in Redis (it was running) but the status update to
    # SCHEDULED was lost (crash before save_task). The scheduler write did complete.
    await state_manager.ta.save_task(task)
    await schedule(state_manager, task, run_at)

    # Scheduler picks it up and dispatches — this should update Redis.
    due = await state_manager.task_scheduler.next_due(["default"])
    assert due is not None
    await state_manager.dispatch_scheduled_task(due)

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert await state_manager.task_scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_dispatch_acquired_record_not_requeued(redis, state_manager):
    """A task removed from the schedule by next_due is not returned on a subsequent call."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SUBMITTED, retry_attempt=1)
    run_at = FROZEN_TIME

    await state_manager.ta.save_task(task)
    await schedule(state_manager, task, run_at)
    # Acquire the task, simulating dispatch completing the Redis write
    # but crashing before calling remove().
    await state_manager.task_scheduler.next_due(["default"])

    # next_due must not return the already-acquired task.
    assert await state_manager.task_scheduler.next_due(["default"]) is None


# ── request_task_cancellation ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cancel_scheduled_task(redis, state_manager):
    """Cancelling a SCHEDULED task removes it from the scheduler and marks it CANCELLED in Redis."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.SCHEDULED, retry_attempt=1)
    run_at = FROZEN_TIME + dt.timedelta(hours=1)
    await schedule(state_manager, task, run_at)
    await state_manager.save_task(task)

    result = await state_manager.request_task_cancellation(ULID1)

    assert result is not None
    assert result.status == TaskStatus.CANCELLED
    # Removed from the scheduler
    assert await state_manager.task_scheduler.next_due(["default"]) is None
    # Persisted to Redis
    saved = await state_manager.ta.get_task(ULID1)
    assert saved.status == TaskStatus.CANCELLED


# ── TaskAdapter.stage_remove_from_queue ───────────────────────────────────────

@pytest.mark.asyncio
async def test_remove_from_queue_removes_task(redis, state_manager):
    """stage_remove_from_queue removes the task from the queue sorted set and the type index."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME)
    await redis.zadd("task-queues:default", {ULID1.bytes: FROZEN_TIME.timestamp()})
    await redis.sadd("task-type-idx:my_task", ULID1.bytes)

    pipe = state_manager.data_store.pipeline(transaction=True)
    state_manager.ta.stage_remove_from_queue(pipe, task)
    await pipe.execute()

    assert await redis.zscore("task-queues:default", ULID1.bytes) is None
    assert not await redis.sismember("task-type-idx:my_task", ULID1.bytes)


@pytest.mark.asyncio
async def test_remove_from_queue_does_not_affect_other_tasks(redis, state_manager):
    """stage_remove_from_queue only removes the specified task; other tasks in the same queue and index are untouched."""
    task1 = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME)
    _task2 = Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME)
    await redis.zadd("task-queues:default", {ULID1.bytes: FROZEN_TIME.timestamp(), ULID2.bytes: FROZEN_TIME.timestamp()})
    await redis.sadd("task-type-idx:my_task", ULID1.bytes, ULID2.bytes)

    pipe = state_manager.data_store.pipeline(transaction=True)
    state_manager.ta.stage_remove_from_queue(pipe, task1)
    await pipe.execute()

    assert await redis.zscore("task-queues:default", ULID1.bytes) is None
    assert not await redis.sismember("task-type-idx:my_task", ULID1.bytes)
    assert await redis.zscore("task-queues:default", ULID2.bytes) is not None
    assert await redis.sismember("task-type-idx:my_task", ULID2.bytes)


@pytest.mark.asyncio
async def test_remove_from_queue_is_idempotent(redis, state_manager):
    """stage_remove_from_queue on a task not in either structure is a no-op."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME)

    pipe = state_manager.data_store.pipeline(transaction=True)
    state_manager.ta.stage_remove_from_queue(pipe, task)
    await pipe.execute()

    assert await redis.zscore("task-queues:default", ULID1.bytes) is None
    assert not await redis.sismember("task-type-idx:my_task", ULID1.bytes)


# ── monitor_task_cancellation ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_monitor_task_cancellation_raises_on_cancel_message(state_manager, redis):
    """monitor_task_cancellation raises UserCancellationError when a cancel message is published."""
    task_id = ULID1
    monitor = asyncio.create_task(state_manager.monitor_task_cancellation(task_id))
    await asyncio.sleep(0.05)  # let the subscription establish
    await redis.publish(f"task_cancel_{task_id}", "cancel")
    with pytest.raises(UserCancellationError):
        await monitor


@pytest.mark.asyncio
async def test_monitor_task_cancellation_does_not_exit_without_message(state_manager):
    """monitor_task_cancellation keeps running when no cancel message is sent."""
    task_id = ULID1
    monitor = asyncio.create_task(state_manager.monitor_task_cancellation(task_id))
    await asyncio.sleep(0.1)  # let it spin a few poll iterations
    assert not monitor.done(), "monitor should still be running when no cancel message is sent"
    monitor.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await monitor


# ── update_task_heartbeat / remove_task_heartbeat ─────────────────────────────


@pytest.mark.asyncio
async def test_update_task_heartbeat_stores_score(redis, state_manager):
    """update_task_heartbeat writes a score to the heartbeat sorted set."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    await state_manager.update_task_heartbeat(task)
    score = await redis.zscore(state_manager.ta.HEARTBEAT_SCORES(queue="default"), bytes(ULID1))
    assert score is not None


@pytest.mark.asyncio
async def test_remove_task_heartbeat_clears_score(redis, state_manager):
    """remove_task_heartbeat removes the score from the heartbeat sorted set."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    await state_manager.update_task_heartbeat(task)
    await state_manager.remove_task_heartbeat(task)
    score = await redis.zscore(state_manager.ta.HEARTBEAT_SCORES(queue="default"), bytes(ULID1))
    assert score is None


# ── get_active_tasks ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_tasks_returns_tasks_with_heartbeats(redis, state_manager):
    """get_active_tasks returns tasks that have a heartbeat entry."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    await state_manager.ta.save_task(task)
    await state_manager.update_task_heartbeat(task)

    result = await state_manager.get_active_tasks({"default"})
    assert len(result) == 1
    assert result[0].id == ULID1


# ── schedule_retry_task ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_retry_task_adds_to_scheduler(redis, state_manager):
    """schedule_retry_task saves the task and adds it to the scheduled queue."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SCHEDULED)
    run_at = FROZEN_TIME + dt.timedelta(minutes=5)

    result = await state_manager.schedule_retry_task(task, run_at)

    assert result is task
    due = await state_manager.task_scheduler.next_due(["default"])
    assert due is not None
    assert due.id == ULID1


# ── queue_retry_task ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_queue_retry_task_requeues_immediately(redis, state_manager):
    """queue_retry_task sets status to SUBMITTED and enqueues the task."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)

    result = await state_manager.queue_retry_task(task)

    assert result is task
    assert result.status == TaskStatus.SUBMITTED
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in members


# ── request_task_cancellation: task not found ────────────────────────────────


@pytest.mark.asyncio
async def test_request_task_cancellation_returns_none_for_missing_task(state_manager):
    """request_task_cancellation returns None when the task does not exist."""
    result = await state_manager.request_task_cancellation(ULID1)
    assert result is None


# ── dispatch_scheduled_task: WatchError retry ─────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_scheduled_task_skips_cancelled_task(redis, state_manager):
    """dispatch_scheduled_task silently skips tasks that are already CANCELLED."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.CANCELLED)
    await state_manager.ta.save_task(task)

    result = await state_manager.dispatch_scheduled_task(task)

    assert result is task
    # Task should NOT have been re-enqueued
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) not in members
