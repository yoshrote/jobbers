import datetime

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from pytest_unordered import unordered
from ulid import ULID

from jobbers.serialization import serialize
from jobbers.state_manager import StateManager, Task, TaskStatus

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

@pytest.mark.asyncio
async def test_submit_task(redis):
    """Test submitting a task to Redis."""
    task = Task(id=ULID1, name="Test Task", status="submitted")
    await StateManager(redis).submit_task(task)
    # Verify the task was added to Redis
    task_list = await redis.lrange("task-list:default", 0, -1)
    assert bytes(ULID1) in task_list
    task_data = await redis.hgetall(f"task:{ULID1}")
    assert set(task_data.items()) >= set({b"name": b"Test Task", b"status": b"submitted", b"submitted_at": serialize(task.submitted_at)}.items())

@pytest.mark.asyncio
async def test_submit_task_twice_updates_only(redis):
    """Test that submitting a task twice updates the task but does not add it to the task-list again."""
    state_manager = StateManager(redis)

    # Submit the task for the first time
    task = Task(id=ULID1, name="Initial Task", status="submitted")
    await state_manager.submit_task(task)

    # Submit the task again with updated details
    updated_task = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await state_manager.submit_task(updated_task)

    # Verify the task ID is only added once to the task-list
    task_list = await redis.lrange(f"task-list:{task.queue}", 0, -1)
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
    await redis.lpush("task-list:default", bytes(ULID1))
    await redis.hset(f"task:{ULID1}", mapping={b"name": b"Test Task", b"status": b"started", b"submitted_at": ISO_FROZEN_TIME, b"version": 0})
    # Retrieve the task
    task = await StateManager(redis).get_task(ULID1)
    assert task is not None
    assert task.id == ULID1
    assert task.name == "Test Task"
    assert task.status == TaskStatus.STARTED
    assert task.submitted_at == FROZEN_TIME

@pytest.mark.asyncio
async def test_get_task_not_found(redis):
    """Test retrieving a non-existent task."""
    task = await StateManager(redis).get_task(ULID1)
    assert task is None

@pytest.mark.asyncio
async def test_task_exists(redis):
    """Test checking if a task exists in Redis."""
    # Add a task to Redis
    await redis.lpush("task-list:default", bytes(ULID1))
    await redis.hset(f"task:{ULID1}", mapping={"name": "Test Task", "status": "started", "submitted_at": ISO_FROZEN_TIME})
    # Check if the task exists
    exists = await StateManager(redis).task_exists(ULID1)
    assert exists
    # Check for a non-existent task
    exists = await StateManager(redis).task_exists(999)
    assert not exists

@pytest.mark.asyncio
async def test_get_all_tasks(redis):
    """Test retrieving all tasks from Redis."""
    # Add tasks to Redis
    await redis.lpush("task-list:default", bytes(ULID1), bytes(ULID2))
    await redis.hset(f"task:{ULID1}", mapping={b"name": b"Task 1", b"status": b"started", b"submitted_at": ISO_FROZEN_TIME})
    await redis.hset(f"task:{ULID2}", mapping={b"name": b"Task 2", b"status": b"completed", b"submitted_at": ISO_FROZEN_TIME})
    # Retrieve all tasks
    tasks = await StateManager(redis).get_all_tasks()
    assert len(tasks) == 2
    assert tasks == unordered([
        Task(id=ULID1, name="Task 1", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME),
        Task(id=ULID2, name="Task 2", status=TaskStatus.COMPLETED, submitted_at=FROZEN_TIME),
    ])

@pytest.mark.asyncio
async def test_get_all_tasks_empty(redis):
    """Test retrieving tasks when no tasks exist."""
    tasks = await StateManager(redis).get_all_tasks()
    assert tasks == []

@pytest.mark.asyncio
async def test_get_queues(redis):
    """Test retrieving queues for a specific role."""
    # Add queues for a role
    await redis.sadd("worker-queues:role1", "queue1", "queue2")
    # Retrieve queues
    queues = await StateManager(redis).get_queues("role1")
    assert set(queues) == {"queue1", "queue2"}

@pytest.mark.asyncio
async def test_get_queues_empty(redis):
    """Test retrieving queues for a role with no queues."""
    queues = await StateManager(redis).get_queues("role1")
    assert queues == []

@pytest.mark.asyncio
async def test_set_queues(redis):
    """Test setting queues for a specific role."""
    state_manager = StateManager(redis)
    # Set queues for a role
    await state_manager.set_queues("role1", ["queue1", "queue2"])
    # Verify the queues were set
    queues = await redis.smembers("worker-queues:role1")
    assert set(queues) == {b"queue1", b"queue2"}
    all_queues = await redis.smembers("all-queues")
    assert set(all_queues) == {b"queue1", b"queue2"}

@pytest.mark.asyncio
async def test_get_all_queues(redis):
    """Test retrieving all queues across roles."""
    # Add queues for multiple roles
    await redis.sadd("worker-queues:role1", "queue1", "queue2")
    await redis.sadd("worker-queues:role2", "queue3")
    # Retrieve all queues
    queues = await StateManager(redis).get_all_queues()
    assert set(queues) == {"queue1", "queue2", "queue3"}

@pytest.mark.asyncio
async def test_get_all_queues_empty(redis):
    """Test retrieving all queues when no queues exist."""
    queues = await StateManager(redis).get_all_queues()
    assert queues == []

@pytest.mark.asyncio
async def test_get_all_roles(redis):
    """Test retrieving all roles."""
    # Add roles with queues
    await redis.sadd("worker-queues:role1", "queue1")
    await redis.sadd("worker-queues:role2", "queue2")
    # Retrieve all roles
    roles = await StateManager(redis).get_all_roles()
    assert set(roles) == {"role1", "role2"}

@pytest.mark.asyncio
async def test_get_all_roles_empty(redis):
    """Test retrieving all roles when no roles exist."""
    roles = await StateManager(redis).get_all_roles()
    assert roles == []

if __name__ == "__main__":
    pytest.main(["-v", "test_state_manager.py"])
