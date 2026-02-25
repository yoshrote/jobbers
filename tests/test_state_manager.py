import datetime as dt
from collections import defaultdict
from unittest.mock import patch

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from ulid import ULID

from jobbers.models.dead_queue import DeadQueue
from jobbers.models.task import Task
from jobbers.models.task_config import DeadLetterPolicy, TaskConfig
from jobbers.models.task_status import TaskStatus
from jobbers.state_manager import StateManager

FROZEN_TIME = dt.datetime.fromisoformat("2021-01-01T00:00:00+00:00")
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")

@pytest_asyncio.fixture(autouse=True)
async def redis():
    """Fixture to reset the tasks in the mocked Redis before each test."""
    fake_store = fakeredis.FakeRedis()
    yield fake_store
    await fake_store.close()

@pytest.fixture
def state_manager(redis, tmp_path):
    """Fixture to provide a StateManager instance with a fake Redis data store."""
    from jobbers.models.task_scheduler import TaskScheduler
    return StateManager(
        redis,
        task_scheduler=TaskScheduler(tmp_path / "schedule.db"),
        dead_queue=DeadQueue(tmp_path / "dead_queue.db"),
    )

@pytest_asyncio.fixture
async def real_redis():
    """Real Redis fixture for commands not supported by fakeredis (e.g. bzpopmin with multiple keys)."""
    client = aioredis.Redis(host="localhost", port=6379, db=15)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()

@pytest.fixture
def real_state_manager(real_redis, tmp_path):
    """StateManager backed by a real Redis instance."""
    from jobbers.models.task_scheduler import TaskScheduler
    return StateManager(
        real_redis,
        task_scheduler=TaskScheduler(tmp_path / "schedule.db"),
        dead_queue=DeadQueue(tmp_path / "dead_queue.db"),
    )

@pytest.mark.asyncio
async def test_get_refresh_tag(redis, state_manager):
    """Test that get_refresh_tag retrieves the correct refresh tag for a role."""
    # Set up the fake Redis data
    await redis.set("worker-queues:test_role:refresh_tag", bytes(ULID1))

    # Call the method
    refresh_tag = await state_manager.get_refresh_tag("test_role")

    # Assert the result
    assert refresh_tag == ULID1


@pytest.mark.asyncio
async def test_get_next_task_returns_task(real_redis, real_state_manager):
    """Test that get_next_task retrieves the next task from the queues."""
    task_id = ULID()
    task_obj = Task(id=task_id, name="Test Task", queue="queue1", version=1, status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME)
    await real_redis.zadd("task-queues:queue1", {task_id.bytes: 1})
    await real_redis.set(f"task:{task_id}", task_obj.pack())

    task = await real_state_manager.get_next_task(["queue1", "queue2"], pop_timeout=1)

    assert task is not None
    assert task.id == task_id
    assert task.name == "Test Task"
    assert task.status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_get_next_task_no_task_found(real_state_manager):
    """Test that get_next_task returns None if no task is found."""
    task = await real_state_manager.get_next_task(["queue1", "queue2"], pop_timeout=1)

    assert task is None


@pytest.mark.asyncio
async def test_get_next_task_missing_task_data(real_redis, real_state_manager):
    """Test that get_next_task logs a warning and returns None if task data is missing."""
    task_id = ULID()
    await real_redis.zadd("task-queues:queue1", {task_id.bytes: 1})

    task = await real_state_manager.get_next_task(["queue1", "queue2"], pop_timeout=1)

    assert task is None

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
async def test_concurrency_limits_with_limits(redis, rate_limiter):
    await redis.hset("queue-config:queue1",  mapping={b"max_concurrent": "1"})
    await redis.hset("queue-config:queue2",  mapping={b"max_concurrent": "2"})

    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = {
        "queue1": {ULID()},
        "queue2": {ULID()},
    }

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue2"}

@pytest.mark.asyncio
async def test_concurrency_limits_empty_queues(redis, rate_limiter):
    await redis.hset("queue-config:queue1",  mapping={b"max_concurrent": b"1"})
    await redis.hset("queue-config:queue2",  mapping={b"max_concurrent": b"1"})

    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = defaultdict(set)

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue1", "queue2"}

@pytest.mark.asyncio
async def test_clean_rate_limit_age(redis, state_manager):
    """Test cleaning tasks from the rate limiter based on rate_limit_age."""
    # Add tasks to the rate limiter
    await redis.sadd("all-queues", "queue1", "queue2")
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
    # Add tasks to the task queues
    await redis.sadd("all-queues", "queue1")
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
    # Add tasks to the task queues
    await redis.sadd("all-queues", "queue1")
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
    # Add tasks to the task queues
    await redis.sadd("all-queues", "queue1")
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
async def test_dispatch_scheduled_task(redis, state_manager):
    """dispatch_scheduled_task moves a due task from the scheduler into its Redis queue."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SUBMITTED, retry_attempt=1)
    run_at = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
    state_manager.task_scheduler.add(task, run_at)

    due = state_manager.task_scheduler.next_due(None)
    assert due is not None
    await state_manager.dispatch_scheduled_task(due)

    # Task must now be in the Redis sorted set for its queue
    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    # Task must be removed from the scheduler (acquired row cleaned up)
    assert state_manager.task_scheduler.next_due(None) is None


def test_task_in_registry(state_manager):
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

async def dummy_fn():
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
    assert state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


@pytest.mark.asyncio
async def test_fail_task_with_dlq_writes_both_stores(redis, state_manager):
    """fail_task with SAVE policy updates Redis and writes to the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["oops"])
    task.task_config = make_task_config(DeadLetterPolicy.SAVE)

    await state_manager.fail_task(task)

    saved = await state_manager.ta.get_task(ULID1)
    assert saved.status == TaskStatus.FAILED
    dlq = state_manager.dead_queue.get_by_ids([str(ULID1)])
    assert len(dlq) == 1
    assert dlq[0].id == ULID1


@pytest.mark.asyncio
async def test_fail_task_redis_written_before_dlq(redis, state_manager):
    """If the DLQ write crashes, Redis is already updated (write-first ordering)."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["oops"])
    task.task_config = make_task_config(DeadLetterPolicy.SAVE)

    with patch.object(state_manager.dead_queue, "add", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError):
            await state_manager.fail_task(task)

    # Redis must reflect the FAILED status even though the DLQ write never happened.
    saved = await state_manager.ta.get_task(ULID1)
    assert saved.status == TaskStatus.FAILED


# ── resubmit_dead_tasks ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_resubmit_dead_tasks_requeues_and_clears_dlq(redis, state_manager):
    """All tasks are enqueued in Redis and removed from the DLQ."""
    task1 = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    task2 = Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e2"])
    state_manager.dead_queue.add(task1, FROZEN_TIME)
    state_manager.dead_queue.add(task2, FROZEN_TIME)

    await state_manager.resubmit_dead_tasks([task1, task2])

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert bytes(ULID2) in queue_members
    assert state_manager.dead_queue.get_by_ids([str(ULID1), str(ULID2)]) == []


@pytest.mark.asyncio
async def test_resubmit_dead_tasks_redis_written_before_dlq_delete(redis, state_manager):
    """If the batch DLQ delete crashes, all tasks are already live in Redis (safe ordering)."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    state_manager.dead_queue.add(task, FROZEN_TIME)

    with patch.object(state_manager.dead_queue, "remove_many", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError):
            await state_manager.resubmit_dead_tasks([task])

    # Task must be in Redis queue despite the DLQ crash.
    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    # Stale DLQ entry remains — acceptable; re-running resubmit is idempotent.
    assert len(state_manager.dead_queue.get_by_ids([str(ULID1)])) == 1


@pytest.mark.asyncio
async def test_resubmit_dead_tasks_is_idempotent(redis, state_manager):
    """Re-running resubmit for a task already in Redis does not raise and clears the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    state_manager.dead_queue.add(task, FROZEN_TIME)

    # First run — normal path.
    await state_manager.resubmit_dead_tasks([task])

    # Operator re-adds to DLQ and retries (simulates the crash scenario above recovering).
    task2 = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    state_manager.dead_queue.add(task2, FROZEN_TIME)
    await state_manager.resubmit_dead_tasks([task2])

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


# ── schedule_retry_task / dispatch_scheduled_task ─────────────────────────────

@pytest.mark.asyncio
async def test_schedule_retry_task_self_heals_via_dispatch(redis, state_manager):
    """If the Redis write in schedule_retry_task was missed (crash), the scheduler dispatch recovers."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.STARTED, retry_attempt=1)
    run_at = FROZEN_TIME

    # Simulate: only the SQLite write completed (crash before save_task).
    state_manager.task_scheduler.add(task, run_at)

    # Scheduler picks it up and dispatches — this should update Redis.
    due = state_manager.task_scheduler.next_due(None)
    assert due is not None
    await state_manager.dispatch_scheduled_task(due)

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert state_manager.task_scheduler.next_due(None) is None


def test_dispatch_stale_acquired_record_not_requeued(state_manager):
    """A leftover acquired=1 row (crash between Redis write and SQLite delete) is not re-dispatched."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SUBMITTED, retry_attempt=1)
    run_at = FROZEN_TIME

    state_manager.task_scheduler.add(task, run_at)
    # Acquire the row, simulating dispatch_scheduled_task completing the Redis write
    # but crashing before the SQLite delete.
    state_manager.task_scheduler.next_due(None)  # sets acquired=1

    # next_due must not return the already-acquired row.
    assert state_manager.task_scheduler.next_due(None) is None


# ── request_task_cancellation ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cancel_scheduled_task(state_manager):
    """Cancelling a SCHEDULED task removes it from the scheduler and marks it CANCELLED in Redis."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.SCHEDULED, retry_attempt=1)
    run_at = FROZEN_TIME + dt.timedelta(hours=1)
    state_manager.task_scheduler.add(task, run_at)
    await state_manager.save_task(task)

    result = await state_manager.request_task_cancellation(ULID1)

    assert result is not None
    assert result.status == TaskStatus.CANCELLED
    # Removed from the scheduler
    assert state_manager.task_scheduler.next_due(None) is None
    # Persisted to Redis
    saved = await state_manager.ta.get_task(ULID1)
    assert saved.status == TaskStatus.CANCELLED
