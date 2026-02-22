import datetime as dt

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from pytest_unordered import unordered
from ulid import ULID

from jobbers.models.task import Task, TaskAdapter, TaskPagination, TaskStatus
from jobbers.models.task_config import TaskConfig
from jobbers.utils.serialization import EMPTY_DICT, NONE, serialize

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
def task_adapter(redis):
    """Fixture to provide a TaskAdapter instance with a fake Redis data store."""
    return TaskAdapter(redis)

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


def test_task_serialization_and_deserialization():
    """Test that a Task can be serialized to Redis and deserialized back correctly."""
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={"key": "value"},
        results={"result_key": "result_value"},
        errors=[],
        status=TaskStatus.STARTED,
        submitted_at=dt.datetime(2025, 4, 4, 12, 0, 0),
        started_at=dt.datetime(2025, 4, 4, 12, 5, 0),
        heartbeat_at=None,
        completed_at=None,
    )

    # Serialize the task to Redis format
    redis_data = task.to_redis()

    # Deserialize the task back from Redis format
    deserialized_task = Task.from_redis(task_id, redis_data)

    # Assert that the original task and deserialized task are equal
    assert task == deserialized_task

def test_valid_params():
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={},
        results={},
        errors=[],
        status=TaskStatus.UNSUBMITTED,
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
    )

    def task_function(foo: str, bar: int|None=5) -> None:
        pass

    task.task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
    )
    task.parameters = {"foo": "spam", "bar": 5}
    assert task.valid_task_params()

    task.parameters = {"foo": "spam", "bar": None}
    assert task.valid_task_params()

    task.parameters = {"foo": "spam", "bar": "baz"}
    assert not task.valid_task_params()


def test_task_serialization_with_none_values():
    """Test that Task serialization handles None values properly."""
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={},
        results={},
        status=TaskStatus.UNSUBMITTED,
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
    )

    # Serialize the task to Redis format
    redis_data = task.to_redis()

    # Assert that None values are serialized as expected
    assert redis_data[b"parameters"] == EMPTY_DICT
    assert redis_data[b"results"] == EMPTY_DICT
    assert redis_data[b"errors"] == serialize([])
    assert redis_data[b"started_at"] == NONE
    assert redis_data[b"heartbeat_at"] == NONE
    assert redis_data[b"completed_at"] == NONE

    # Deserialize the task back from Redis format
    deserialized_task = Task.from_redis(task_id, redis_data)

    # Assert that the original task and deserialized task are equal
    assert task == deserialized_task


def test_task_serialization_with_non_none_values():
    """Test that Task serialization handles non-None values properly."""
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={"key": "value"},
        results={"result_key": "result_value"},
        errors=["Some error occurred"],
        status=TaskStatus.COMPLETED,
        submitted_at=dt.datetime(2025, 4, 4, 12, 0, 0),
        started_at=dt.datetime(2025, 4, 4, 12, 5, 0),
        heartbeat_at=dt.datetime(2025, 4, 4, 12, 10, 0),
        completed_at=dt.datetime(2025, 4, 4, 12, 15, 0),
    )

    # Serialize the task to Redis format
    redis_data = task.to_redis()

    # Assert that non-None values are serialized as expected
    assert redis_data[b"parameters"] == b"\x81\xa3key\xa5value"
    assert redis_data[b"results"] == b"\x81\xaaresult_key\xacresult_value"
    assert redis_data[b"errors"] == serialize(["Some error occurred"])
    assert redis_data[b"status"] == b"completed"
    assert redis_data[b"submitted_at"] == b"\xc7\x13\x012025-04-04T12:00:00"
    assert redis_data[b"started_at"] == b"\xc7\x13\x012025-04-04T12:05:00"
    assert redis_data[b"heartbeat_at"] == b"\xc7\x13\x012025-04-04T12:10:00"
    assert redis_data[b"completed_at"] == b"\xc7\x13\x012025-04-04T12:15:00"

    # Deserialize the task back from Redis format
    deserialized_task = Task.from_redis(task_id, redis_data)

    # Assert that the original task and deserialized task are equal
    assert task == deserialized_task


@pytest.mark.asyncio
async def test_submit_task(redis, task_adapter):
    """Test submitting a task to Redis."""
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    task.set_status(TaskStatus.SUBMITTED)  # SM sets this before calling the adapter
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
    task.set_status(TaskStatus.SUBMITTED)  # SM sets this before calling the adapter
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


class TestUpdateTaskHeartbeat:
    """Tests for TaskAdapter.update_task_heartbeat."""

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
    """Tests for TaskAdapter.get_stale_tasks."""

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
