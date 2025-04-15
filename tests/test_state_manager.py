import datetime
from collections import defaultdict
from unittest.mock import patch

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from pytest_unordered import unordered
from ulid import ULID

from jobbers.serialization import deserialize, serialize
from jobbers.state_manager import RateLimiter, StateManager, Task, TaskStatus

FROZEN_TIME = datetime.datetime.fromisoformat("2021-01-01T00:00:00+00:00")
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

@pytest.mark.asyncio
async def test_submit_task(redis, state_manager):
    """Test submitting a task to Redis."""
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    await state_manager.submit_task(task)
    # Verify the task was added to Redis
    task_list = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in task_list
    task_data = await redis.hget("task-table", bytes(ULID1))
    assert deserialize(task_data).items() >= set({b"name": "Test Task", b"status": b"submitted", b"submitted_at": task.submitted_at}.items())

@pytest.mark.asyncio
async def test_submit_task_twice_updates_only(redis, state_manager):
    """Test that submitting a task twice updates the task but does not add it to the task-list again."""
    # Submit the task for the first time
    task = Task(id=ULID1, name="Initial Task", status="unsubmitted")
    await state_manager.submit_task(task)

    # Submit the task again with updated details
    updated_task = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await state_manager.submit_task(updated_task)

    # Verify the task ID is only added once to the task-list
    task_list = await redis.zrange(f"task-queues:{task.queue}", 0, -1)
    assert task_list == [bytes(ULID1)]

    # Verify the task details were updated
    # submitted_at should not change
    saved_task = await state_manager.get_task(ULID1)
    assert saved_task.name == "Updated Task"
    assert saved_task.status == TaskStatus.COMPLETED
    assert saved_task.submitted_at == task.submitted_at

@pytest.mark.asyncio
async def test_get_task(redis):
    """Test retrieving a task from Redis."""
    # Add a task to Redis
    await redis.zadd("worker-queues:default", {bytes(ULID1): FROZEN_TIME.timestamp()})
    await redis.hset("task-table", bytes(ULID1), serialize({b"name": "Test Task", b"status": b"started", b"submitted_at": FROZEN_TIME, b"version": 0}))
    # Retrieve the task
    task = await StateManager(redis).get_task(ULID1)
    assert task is not None
    assert task.id == ULID1
    assert task.name == "Test Task"
    assert task.status == TaskStatus.STARTED
    assert task.submitted_at == FROZEN_TIME

@pytest.mark.asyncio
async def test_get_task_not_found(state_manager):
    """Test retrieving a non-existent task."""
    task = await state_manager.get_task(ULID1)
    assert task is None

@pytest.mark.asyncio
async def test_task_exists(redis, state_manager):
    """Test checking if a task exists in Redis."""
    # Add a task to Redis
    await redis.zadd("worker-queues:default", {bytes(ULID1): FROZEN_TIME.timestamp()})
    await redis.hset("task-table", bytes(ULID1), serialize({b"name": "Test Task", b"status": b"started", b"submitted_at": FROZEN_TIME}))
    # Check if the task exists
    exists = await state_manager.task_exists(ULID1)
    assert exists
    # Check for a non-existent task
    exists = await state_manager.task_exists(999)
    assert not exists

@pytest.mark.asyncio
async def test_get_all_tasks(redis, state_manager):
    """Test retrieving all tasks from Redis."""
    # Add tasks to Redis
    await redis.zadd("task-queues:default", {bytes(ULID1): FROZEN_TIME.timestamp(), bytes(ULID2): FROZEN_TIME.timestamp()})
    await redis.hset("task-table", bytes(ULID1), serialize({b"name": b"Task 1", b"status": b"started", b"submitted_at": FROZEN_TIME}))
    await redis.hset("task-table", bytes(ULID2), serialize({b"name": b"Task 2", b"status": b"completed", b"submitted_at": FROZEN_TIME}))
    # Retrieve all tasks
    tasks = await state_manager.get_all_tasks()
    assert len(tasks) == 2
    assert tasks == unordered([
        Task(id=ULID1, name="Task 1", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME),
        Task(id=ULID2, name="Task 2", status=TaskStatus.COMPLETED, submitted_at=FROZEN_TIME),
    ])

@pytest.mark.asyncio
async def test_get_all_tasks_empty(state_manager):
    """Test retrieving tasks when no tasks exist."""
    tasks = await state_manager.get_all_tasks()
    assert tasks == []

@pytest.mark.asyncio
async def test_get_queues(redis, state_manager):
    """Test retrieving queues for a specific role."""
    # Add queues for a role
    await redis.sadd("worker-queues:role1", "queue1", "queue2")
    # Retrieve queues
    queues = await state_manager.get_queues("role1")
    assert set(queues) == {"queue1", "queue2"}

@pytest.mark.asyncio
async def test_get_queues_empty(redis, state_manager):
    """Test retrieving queues for a role with no queues."""
    queues = await state_manager.get_queues("role1")
    assert queues == set()

@pytest.mark.asyncio
async def test_set_queues(redis, state_manager):
    """Test setting queues for a specific role."""
    # Set queues for a role
    await state_manager.set_queues("role1", {"queue1", "queue2"})
    # Verify the queues were set
    queues = await redis.smembers("worker-queues:role1")
    assert set(queues) == {b"queue1", b"queue2"}
    all_queues = await redis.smembers("all-queues")
    assert set(all_queues) == {b"queue1", b"queue2"}

@pytest.mark.asyncio
async def test_get_all_queues(redis, state_manager):
    """Test retrieving all queues across roles."""
    # Add queues for multiple roles
    await redis.sadd("worker-queues:role1", "queue1", "queue2")
    await redis.sadd("worker-queues:role2", "queue3")
    # Retrieve all queues
    queues = await state_manager.get_all_queues()
    assert set(queues) == {"queue1", "queue2", "queue3"}

@pytest.mark.asyncio
async def test_get_all_queues_empty(redis, state_manager):
    """Test retrieving all queues when no queues exist."""
    queues = await state_manager.get_all_queues()
    assert queues == []

@pytest.mark.asyncio
async def test_get_all_roles(redis, state_manager):
    """Test retrieving all roles."""
    # Add roles with queues
    await redis.sadd("worker-queues:role1", "queue1")
    await redis.sadd("worker-queues:role2", "queue2")
    # Retrieve all roles
    roles = await state_manager.get_all_roles()
    assert set(roles) == {"role1", "role2"}

@pytest.mark.asyncio
async def test_get_all_roles_empty(state_manager):
    """Test retrieving all roles when no roles exist."""
    roles = await state_manager.get_all_roles()
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
        b"parameters": {},
        b"version": 1,
        b"submitted_at": FROZEN_TIME,
    }
    await redis.zadd("task-queues:queue1", {task_id.bytes: 1})
    await redis.hset("task-table", bytes(task_id), serialize(task_data))

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
def rate_limiter(redis):
    return RateLimiter(redis)

@pytest.mark.asyncio
async def test_rate_limiter_has_room_empty(redis, rate_limiter):
    await redis.hset("queue-config:default", mapping={
        # 2 tasks per minute
        b"rate_numerator": serialize(2),
        b"rate_denominator": serialize(1),
        b"rate_period": b"minute",
    })

    result = await rate_limiter.has_room_in_queue_queue("default")
    assert result is True

@pytest.mark.asyncio
async def test_rate_limiter_has_room_nonempty(redis, rate_limiter):
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp()-1})
    await redis.hset("queue-config:default", mapping={
        # 2 tasks per minute
        b"rate_numerator": serialize(2),
        b"rate_denominator": serialize(1),
        b"rate_period": b"minute",
    })

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        result = await rate_limiter.has_room_in_queue_queue("default")
    assert result is True

@pytest.mark.asyncio
async def test_rate_limiter_has_room_older_jobs(redis, rate_limiter):
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp()-60})
    await redis.zadd("rate-limiter:default", {ULID2.bytes: FROZEN_TIME.timestamp()-61})
    await redis.hset("queue-config:default", mapping={
        # 2 tasks per minute
        b"rate_numerator": serialize(2),
        b"rate_denominator": serialize(1),
        b"rate_period": b"minute",
    })

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        result = await rate_limiter.has_room_in_queue_queue("default")
    assert result is True

@pytest.mark.asyncio
async def test_rate_limiter_no_room(redis, rate_limiter):
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp()-1})
    await redis.zadd("rate-limiter:default", {ULID2.bytes: FROZEN_TIME.timestamp()-2})
    await redis.hset("queue-config:default", mapping={
        # 2 tasks per minute
        b"rate_numerator": serialize(2),
        b"rate_denominator": serialize(1),
        b"rate_period": b"minute",
    })

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        result = await rate_limiter.has_room_in_queue_queue("default")
    assert result is False

@pytest.mark.asyncio
async def test_concurrency_limits_no_limits(rate_limiter):
    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = {
        "queue1": {ULID()},
        "queue2": {ULID()},
    }

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == ["queue1", "queue2"]

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
    assert result == ["queue2"]

@pytest.mark.asyncio
async def test_concurrency_limits_empty_queues(redis, rate_limiter):
    await redis.hset("queue-config:queue1",  mapping={b"max_concurrent": b"1"})
    await redis.hset("queue-config:queue2",  mapping={b"max_concurrent": b"1"})

    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = defaultdict(set)

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == ["queue1", "queue2"]

if __name__ == "__main__":
    pytest.main(["-v", "test_state_manager.py"])
