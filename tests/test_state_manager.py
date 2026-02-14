import datetime as dt
from collections import defaultdict
from unittest.mock import patch

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from pytest_unordered import unordered
from ulid import ULID

from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task, TaskAdapter, TaskPagination
from jobbers.models.task_status import TaskStatus
from jobbers.state_manager import QueueConfigAdapter, StateManager
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
def state_manager(redis):
    """Fixture to provide a StateManager instance with a fake Redis data store."""
    return StateManager(redis)

@pytest.fixture
def task_adapter(redis):
    """Fixture to provide a TaskAdapter instance with a fake Redis data store."""
    return TaskAdapter(redis)

@pytest.fixture
def queue_config_adapter(redis):
    """Fixture to provide a QueueConfigAdapter instance with a fake Redis data store."""
    return QueueConfigAdapter(redis)

@pytest.fixture
def sample_task():
    """Create a sample task for testing."""
    return Task(
        id=ULID(),
        name="test_task",
        version=1,
        queue="default",
        parameters={},
        status=TaskStatus.STARTED,
        started_at=dt.datetime.now(dt.timezone.utc)
    )

@pytest.mark.asyncio
async def test_submit_task(redis, task_adapter):
    """Test submitting a task to Redis."""
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    await task_adapter.submit_task(task, extra_check=None)
    # Verify the task was added to Redis
    task_list = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in task_list
    task_data = await redis.hgetall(f"task:{ULID1}")
    assert set(task_data.items()) >= set({b"name": b"Test Task", b"status": b"submitted", b"submitted_at": serialize(task.submitted_at)}.items())

@pytest.mark.asyncio
async def test_submit_task_twice_updates_only(redis, task_adapter):
    """Test that submitting a task twice updates the task but does not add it to the task-list again."""
    # Submit the task for the first time
    task = Task(id=ULID1, name="Initial Task", status="unsubmitted")
    await task_adapter.submit_task(task, extra_check=None)

    # Submit the task again with updated details
    updated_task = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await task_adapter.submit_task(updated_task, extra_check=None)

    # Verify the task ID is only added once to the task-list
    task_list = await redis.zrange(f"task-queues:{task.queue}", 0, -1)
    assert task_list == [bytes(ULID1)]

    # Verify the task details were updated
    # submitted_at should not change
    saved_task = await task_adapter.get_task(ULID1)
    assert saved_task.name == "Updated Task"
    assert saved_task.status == TaskStatus.COMPLETED
    assert saved_task.submitted_at == task.submitted_at

@pytest.mark.asyncio
async def test_get_task(redis, task_adapter):
    """Test retrieving a task from Redis."""
    # Add a task to Redis
    await redis.zadd("worker-queues:default", {bytes(ULID1): FROZEN_TIME.timestamp()})
    await redis.hset(f"task:{ULID1}", mapping={b"name": b"Test Task", b"status": b"started", b"submitted_at": ISO_FROZEN_TIME, b"version": 0})
    # Retrieve the task
    task = await task_adapter.get_task(ULID1)
    assert task is not None
    assert task.id == ULID1
    assert task.name == "Test Task"
    assert task.status == TaskStatus.STARTED
    assert task.submitted_at == FROZEN_TIME

@pytest.mark.asyncio
async def test_get_task_not_found(task_adapter):
    """Test retrieving a non-existent task."""
    task = await task_adapter.get_task(ULID1)
    assert task is None

@pytest.mark.asyncio
async def test_task_exists(redis, task_adapter):
    """Test checking if a task exists in Redis."""
    # Add a task to Redis
    await redis.zadd("worker-queues:default", {bytes(ULID1): FROZEN_TIME.timestamp()})
    await redis.hset(f"task:{ULID1}", mapping={"name": "Test Task", "status": "started", "submitted_at": ISO_FROZEN_TIME})
    # Check if the task exists
    exists = await task_adapter.task_exists(ULID1)
    assert exists
    # Check for a non-existent task
    exists = await task_adapter.task_exists(999)
    assert not exists

@pytest.mark.asyncio
async def test_get_all_tasks(redis, task_adapter):
    """Test retrieving all tasks from Redis."""
    queue = "default"
    # Add tasks to Redis
    await redis.zadd(f"task-queues:{queue}", {bytes(ULID1): FROZEN_TIME.timestamp(), bytes(ULID2): FROZEN_TIME.timestamp()})
    await redis.hset(f"task:{ULID1}", mapping={b"name": b"Task 1", b"status": b"started", b"submitted_at": ISO_FROZEN_TIME})
    await redis.hset(f"task:{ULID2}", mapping={b"name": b"Task 2", b"status": b"completed", b"submitted_at": ISO_FROZEN_TIME})
    # Retrieve all tasks
    tasks = await task_adapter.get_all_tasks(TaskPagination(queue=queue))
    assert len(tasks) == 2
    assert tasks == unordered([
        Task(id=ULID1, name="Task 1", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME),
        Task(id=ULID2, name="Task 2", status=TaskStatus.COMPLETED, submitted_at=FROZEN_TIME),
    ])

@pytest.mark.asyncio
async def test_get_all_tasks_empty(task_adapter):
    """Test retrieving tasks when no tasks exist."""
    tasks = await task_adapter.get_all_tasks(TaskPagination(queue="default"))
    assert tasks == []

@pytest.mark.asyncio
async def test_get_queues(redis, queue_config_adapter):
    """Test retrieving queues for a specific role."""
    # Add queues for a role
    await redis.sadd("worker-queues:role1", "queue1", "queue2")
    # Retrieve queues
    queues = await queue_config_adapter.get_queues("role1")
    assert set(queues) == {"queue1", "queue2"}

@pytest.mark.asyncio
async def test_get_queues_empty(redis, queue_config_adapter):
    """Test retrieving queues for a role with no queues."""
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == set()

@pytest.mark.asyncio
async def test_set_queues(redis, queue_config_adapter):
    """Test setting queues for a specific role."""
    # Set queues for a role
    await queue_config_adapter.set_queues("role1", {"queue1", "queue2"})
    # Verify the queues were set
    queues = await redis.smembers("worker-queues:role1")
    assert set(queues) == {b"queue1", b"queue2"}
    all_queues = await redis.smembers("all-queues")
    assert set(all_queues) == {b"queue1", b"queue2"}

@pytest.mark.asyncio
async def test_get_all_queues(redis, queue_config_adapter):
    """Test retrieving all queues across roles."""
    # Add queues for multiple roles
    await redis.sadd("worker-queues:role1", "queue1", "queue2")
    await redis.sadd("worker-queues:role2", "queue3")
    # Retrieve all queues
    queues = await queue_config_adapter.get_all_queues()
    assert set(queues) == {"queue1", "queue2", "queue3"}

@pytest.mark.asyncio
async def test_get_all_queues_empty(redis, queue_config_adapter):
    """Test retrieving all queues when no queues exist."""
    queues = await queue_config_adapter.get_all_queues()
    assert queues == []

@pytest.mark.asyncio
async def test_get_all_roles(redis, queue_config_adapter):
    """Test retrieving all roles."""
    # Add roles with queues
    await redis.sadd("worker-queues:role1", "queue1")
    await redis.sadd("worker-queues:role2", "queue2")
    # Retrieve all roles
    roles = await queue_config_adapter.get_all_roles()
    assert set(roles) == {"role1", "role2"}

@pytest.mark.asyncio
async def test_get_all_roles_empty(queue_config_adapter):
    """Test retrieving all roles when no roles exist."""
    roles = await queue_config_adapter.get_all_roles()
    assert roles == []

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


# Tests for StateManager.get_queue_limits

@pytest.mark.asyncio
async def test_get_queue_limits_empty_set(queue_config_adapter):
    """Test get_queue_limits with an empty set of queues."""
    result = await queue_config_adapter.get_queue_limits(set())
    assert result == {}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_with_limit(redis, queue_config_adapter):
    """Test get_queue_limits with a single queue that has max_concurrent set."""
    # Set up queue config in Redis
    await redis.hset("queue-config:test_queue", mapping={
        "max_concurrent": "5",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"test_queue"})
    assert result == {"test_queue": 5}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_no_limit(redis, queue_config_adapter):
    """Test get_queue_limits with a queue that has no max_concurrent limit."""
    # Set up queue config in Redis with no max_concurrent (defaults to 10)
    await redis.hset("queue-config:unlimited_queue", mapping={
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"unlimited_queue"})
    assert result == {"unlimited_queue": 10}  # Default value


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_zero_limit(redis, queue_config_adapter):
    """Test get_queue_limits with a queue that has max_concurrent set to 0."""
    # Set up queue config in Redis
    await redis.hset("queue-config:zero_limit_queue", mapping={
        "max_concurrent": "0",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"zero_limit_queue"})
    assert result == {"zero_limit_queue": 0}


@pytest.mark.asyncio
async def test_get_queue_limits_multiple_queues(redis, queue_config_adapter):
    """Test get_queue_limits with multiple queues having different limits."""
    # Set up multiple queue configs in Redis
    await redis.hset("queue-config:queue1", mapping={
        "max_concurrent": "3",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    await redis.hset("queue-config:queue2", mapping={
        "max_concurrent": "10",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    await redis.hset("queue-config:queue3", mapping={
        "max_concurrent": "1",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"queue1", "queue2", "queue3"})
    expected = {"queue1": 3, "queue2": 10, "queue3": 1}
    assert result == expected


@pytest.mark.asyncio
async def test_get_queue_limits_with_nonexistent_queue(queue_config_adapter):
    """Test get_queue_limits with a queue that doesn't exist in Redis."""
    # Don't set up any queue config - this should still work with defaults
    result = await queue_config_adapter.get_queue_limits({"nonexistent_queue"})
    assert result == {"nonexistent_queue": 10}  # Default max_concurrent


@pytest.mark.asyncio
async def test_get_queue_limits_mixed_existing_and_nonexistent(redis, queue_config_adapter):
    """Test get_queue_limits with a mix of existing and non-existing queues."""
    # Set up one queue config
    await redis.hset("queue-config:existing_queue", mapping={
        "max_concurrent": "7",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"existing_queue", "nonexistent_queue"})
    expected = {"existing_queue": 7, "nonexistent_queue": 10}
    assert result == expected


@pytest.mark.asyncio
async def test_get_queue_limits_with_rate_limiting_config(redis, queue_config_adapter):
    """Test get_queue_limits with queues that have rate limiting configured (should still return max_concurrent)."""
    # Set up queue config with rate limiting
    await redis.hset("queue-config:rate_limited_queue", mapping={
        "max_concurrent": "15",
        "rate_numerator": serialize(5),
        "rate_denominator": serialize(2),
        "rate_period": "minute"
    })

    result = await queue_config_adapter.get_queue_limits({"rate_limited_queue"})
    assert result == {"rate_limited_queue": 15}


@pytest.mark.asyncio
async def test_get_queue_limits_large_number_of_queues(redis, queue_config_adapter):
    """Test get_queue_limits with a large number of queues to ensure it handles concurrency well."""
    queue_names = [f"queue_{i}" for i in range(20)]

    # Set up configs for all queues
    for i, queue_name in enumerate(queue_names):
        await redis.hset(f"queue-config:{queue_name}", mapping={
            "max_concurrent": str(i + 1),  # Different limit for each queue
            "rate_numerator": serialize(None),
            "rate_denominator": serialize(None),
            "rate_period": ""
        })

    result = await queue_config_adapter.get_queue_limits(set(queue_names))

    # Verify all queues are present with correct limits
    assert len(result) == 20
    for i, queue_name in enumerate(queue_names):
        expected_limit = i + 1
        assert result[queue_name] == expected_limit

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

class TestUpdateTaskHeartbeat:
    """Tests for StateManager.update_task_heartbeat."""

    @pytest.mark.asyncio
    async def test_update_task_heartbeat_sets_timestamp(self, task_adapter, sample_task):
        """Test that heartbeat timestamp is updated in task details."""
        # Arrange
        await task_adapter.submit_task(sample_task, extra_check=None)
        sample_task.heartbeat_at = dt.datetime.now(dt.timezone.utc)

        # Act
        await task_adapter.update_task_heartbeat(sample_task)

        # Assert
        updated_task = await task_adapter.get_task(sample_task.id)
        assert updated_task.heartbeat_at == sample_task.heartbeat_at

    @pytest.mark.asyncio
    async def test_update_task_heartbeat_adds_to_scores(self, task_adapter, sample_task):
        """Test that heartbeat score is added to the sorted set."""
        # Arrange
        await task_adapter.submit_task(sample_task, extra_check=None)
        sample_task.heartbeat_at = dt.datetime.now(dt.timezone.utc)

        # Act
        await task_adapter.update_task_heartbeat(sample_task)

        # Assert
        scores = await task_adapter.data_store.zrange(
            task_adapter.HEARTBEAT_SCORES(queue=sample_task.queue),
            0, -1, withscores=True
        )
        assert any(bytes(sample_task.id) == task_id for task_id, _ in scores)

    @pytest.mark.asyncio
    async def test_update_task_heartbeat_updates_existing_score(self, task_adapter, sample_task):
        """Test that updating heartbeat overwrites the previous score."""
        # Arrange
        await task_adapter.submit_task(sample_task, extra_check=None)
        first_time = dt.datetime.now(dt.timezone.utc)
        sample_task.heartbeat_at = first_time
        await task_adapter.update_task_heartbeat(sample_task)

        # Act
        second_time = first_time + dt.timedelta(seconds=10)
        sample_task.heartbeat_at = second_time
        await task_adapter.update_task_heartbeat(sample_task)

        # Assert
        scores = await task_adapter.data_store.zrange(
            task_adapter.HEARTBEAT_SCORES(queue=sample_task.queue),
            0, -1, withscores=True
        )
        score = next(score for task_id, score in scores if bytes(sample_task.id) == task_id)
        assert score == second_time.timestamp()

class TestGetStaleTasks:
    """Tests for StateManager.get_stale_tasks."""

    @pytest.mark.asyncio
    async def test_get_stale_tasks_returns_stale_tasks(self, task_adapter):
        """Test that stale tasks are returned."""
        # Arrange
        now = dt.datetime.now(dt.timezone.utc)
        stale_time = dt.timedelta(minutes=5)

        # Create a stale task (heartbeat 10 minutes ago)
        stale_task = Task(
            id=ULID(),
            name="stale_task",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=10)
        )
        await task_adapter.submit_task(stale_task, extra_check=None)
        await task_adapter.update_task_heartbeat(stale_task)

        # Act
        stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, stale_time)]

        # Assert
        assert len(stale_tasks) == 1
        assert stale_tasks[0].id == stale_task.id

    @pytest.mark.asyncio
    async def test_get_stale_tasks_excludes_recent_tasks(self, task_adapter):
        """Test that recent tasks are not returned."""
        # Arrange
        now = dt.datetime.now(dt.timezone.utc)
        stale_time = dt.timedelta(minutes=5)

        # Create a recent task (heartbeat 1 minute ago)
        recent_task = Task(
            id=ULID(),
            name="recent_task",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=1)
        )
        await task_adapter.submit_task(recent_task, extra_check=None)
        await task_adapter.update_task_heartbeat(recent_task)

        # Act
        stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, stale_time)]

        # Assert
        assert len(stale_tasks) == 0

    @pytest.mark.asyncio
    async def test_get_stale_tasks_handles_multiple_queues(self, task_adapter):
        """Test that stale tasks from multiple queues are returned."""
        # Arrange
        now = dt.datetime.now(dt.timezone.utc)
        stale_time = dt.timedelta(minutes=5)

        # Create stale tasks in different queues
        stale_task_1 = Task(
            id=ULID(),
            name="task_1",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=10)
        )
        stale_task_2 = Task(
            id=ULID(),
            name="task_2",
            queue="high_priority",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=10)
        )
        await task_adapter.submit_task(stale_task_1, extra_check=None)
        await task_adapter.submit_task(stale_task_2, extra_check=None)
        await task_adapter.update_task_heartbeat(stale_task_1)
        await task_adapter.update_task_heartbeat(stale_task_2)

        # Act
        stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default", "high_priority"}, stale_time)]

        # Assert
        assert len(stale_tasks) == 2
        stale_task_ids = {task.id for task in stale_tasks}
        assert stale_task_1.id in stale_task_ids
        assert stale_task_2.id in stale_task_ids

    @pytest.mark.asyncio
    async def test_get_stale_tasks_returns_none_for_missing_tasks(self, task_adapter, monkeypatch):
        """Test that None results are filtered out."""
        # Arrange
        now = dt.datetime.now(dt.timezone.utc)
        stale_time = dt.timedelta(minutes=5)

        stale_task = Task(
            id=ULID(),
            name="stale_task",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=10)
        )
        await task_adapter.submit_task(stale_task, extra_check=None)
        await task_adapter.update_task_heartbeat(stale_task)

        # Delete the task from storage to simulate a missing task
        await task_adapter.data_store.delete(task_adapter.TASK_DETAILS(task_id=stale_task.id))

        # Act
        stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, stale_time)]

        # Assert
        assert len(stale_tasks) == 0

if __name__ == "__main__":
    pytest.main(["-v", "test_state_manager.py"])
