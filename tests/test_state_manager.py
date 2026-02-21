import datetime as dt
from collections import defaultdict
from unittest.mock import patch

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.models.dead_queue import DeadQueue
from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus
from jobbers.state_manager import StateManager
from jobbers.utils.serialization import EMPTY_DICT, serialize

FROZEN_TIME = dt.datetime.fromisoformat("2021-01-01T00:00:00+00:00")
ISO_FROZEN_TIME = serialize(FROZEN_TIME)
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
async def test_get_next_task_returns_task(redis, state_manager):
    """Test that get_next_task retrieves the next task from the queues."""
    # Set up the fake Redis data
    task_id = ULID()
    task_data = {
        b"name": b"Test Task",
        b"status": b"submitted",
        b"parameters": EMPTY_DICT,
        b"version": b"1",
        b"submitted_at": ISO_FROZEN_TIME,
    }
    await redis.zadd("task-queues:queue1", {task_id.bytes: 1})
    await redis.hset(f"task:{task_id}", mapping=task_data)

    # Call the method
    task = await state_manager.get_next_task(["queue1", "queue2"])

    # Assert the result
    assert task is not None
    assert task.id == task_id
    assert task.name == "Test Task"
    assert task.status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_get_next_task_no_task_found(redis, state_manager):
    """Test that get_next_task returns None if no task is found."""
    # Call the method with no tasks in the queues
    # Use a timeout to avoid blocking forever
    task = await state_manager.get_next_task(["queue1", "queue2"], timeout=1)

    # Assert the result
    assert task is None


@pytest.mark.asyncio
async def test_get_next_task_missing_task_data(redis, state_manager):
    """Test that get_next_task logs a warning and returns None if task data is missing."""
    # Set up the fake Redis data
    task_id = ULID()
    await redis.zadd("task-queues:queue1", {task_id.bytes: 1})

    # Call the method
    task = await state_manager.get_next_task(["queue1", "queue2"])

    # Assert the result
    assert task is None

@pytest.fixture
def rate_limiter(state_manager):
    return state_manager.submission_limiter

@pytest.mark.asyncio
async def test_rate_limiter_has_room_empty(redis, rate_limiter):
    result = await rate_limiter.has_room_in_queue_queue(QueueConfig(
        name="default",
        # 2 tasks per minute
        rate_numerator=2,
        rate_denominator=1,
        rate_period=RatePeriod.MINUTE
    ))
    assert result is True

@pytest.mark.asyncio
async def test_rate_limiter_has_room_nonempty(redis, rate_limiter):
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp()-1})
    default_queue = QueueConfig(
        name="default",
        # 2 tasks per minute
        rate_numerator=2,
        rate_denominator=1,
        rate_period=RatePeriod.MINUTE
    )

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        result = await rate_limiter.has_room_in_queue_queue(default_queue)
    assert result is True

@pytest.mark.asyncio
async def test_rate_limiter_has_room_older_jobs(redis, rate_limiter):
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp()-60})
    await redis.zadd("rate-limiter:default", {ULID2.bytes: FROZEN_TIME.timestamp()-61})
    default_queue = QueueConfig(
        name="default",
        # 2 tasks per minute
        rate_numerator=2,
        rate_denominator=1,
        rate_period=RatePeriod.MINUTE
    )

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        result = await rate_limiter.has_room_in_queue_queue(default_queue)
    assert result is True

@pytest.mark.asyncio
async def test_rate_limiter_no_room(redis, rate_limiter):
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp()-1})
    await redis.zadd("rate-limiter:default", {ULID2.bytes: FROZEN_TIME.timestamp()-2})
    default_queue = QueueConfig(
        name="default",
        # 2 tasks per minute
        rate_numerator=2,
        rate_denominator=1,
        rate_period=RatePeriod.MINUTE
    )

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        result = await rate_limiter.has_room_in_queue_queue(default_queue)
    assert result is False

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
async def test_add_task_to_queue_adds_task_to_rate_limiter(redis, rate_limiter):
    """Test that add_task_to_queue adds the task to the rate limiter sorted set."""
    task = Task(
        id=ULID1,
        name="Test Task",
        status=TaskStatus.SUBMITTED,
        queue="default",
        submitted_at=FROZEN_TIME,
    )
    pipe = redis.pipeline()
    rate_limiter.add_task_to_queue(task, pipe=pipe)
    await pipe.execute()

    # Check that the task was added to the correct sorted set with the correct score
    members = await redis.zrange("rate-limiter:default", 0, -1, withscores=True)
    assert members == [(bytes(ULID1), FROZEN_TIME.timestamp())]

@pytest.mark.asyncio
async def test_add_task_to_queue_no_submitted_at(redis, rate_limiter):
    """Test that add_task_to_queue does not add the task if submitted_at is None."""
    task = Task(
        id=ULID2,
        name="No Submitted At",
        status=TaskStatus.SUBMITTED,
        queue="default",
        submitted_at=None,
    )
    pipe = redis.pipeline()
    rate_limiter.add_task_to_queue(task, pipe=pipe)
    await pipe.execute()

    # Should not add anything to the sorted set
    members = await redis.zrange("rate-limiter:default", 0, -1, withscores=True)
    assert members == []

@pytest.mark.asyncio
async def test_dispatch_scheduled_task(redis, state_manager):
    """dispatch_scheduled_task moves a due task from the scheduler into its Redis queue."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SUBMITTED, retry_attempt=1)
    run_at = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
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
