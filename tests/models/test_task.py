import datetime as dt

from ulid import ULID

from jobbers.models.task import Task, TaskStatus
from jobbers.serialization import EMPTY_DICT, NONE, deserialize


def test_task_serialization_and_deserialization():
    """Test that a Task can be serialized to Redis and deserialized back correctly."""
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={"key": "value"},
        results={"result_key": "result_value"},
        error=None,
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


def test_task_serialization_with_none_values():
    """Test that Task serialization handles None values properly."""
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={},
        results={},
        error=None,
        status=TaskStatus.UNSUBMITTED,
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
    )

    # Serialize the task to Redis format
    raw_data = task.to_redis()
    redis_data = deserialize(raw_data)

    # Assert that None values are serialized as expected
    assert redis_data[b"parameters"] == {}
    assert redis_data[b"results"] == {}
    assert redis_data[b"error"] == None
    assert redis_data[b"started_at"] == None
    assert redis_data[b"heartbeat_at"] == None
    assert redis_data[b"completed_at"] == None

    # Deserialize the task back from Redis format
    deserialized_task = Task.from_redis(task_id, raw_data)

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
        error="Some error occurred",
        status=TaskStatus.COMPLETED,
        submitted_at=dt.datetime(2025, 4, 4, 12, 0, 0),
        started_at=dt.datetime(2025, 4, 4, 12, 5, 0),
        heartbeat_at=dt.datetime(2025, 4, 4, 12, 10, 0),
        completed_at=dt.datetime(2025, 4, 4, 12, 15, 0),
    )

    # Serialize the task to Redis format
    raw_data = task.to_redis()
    redis_data = deserialize(raw_data)

    # Assert that non-None values are serialized as expected
    assert redis_data[b"parameters"] == {"key": "value"}
    assert redis_data[b"results"] == {"result_key": "result_value"}
    assert redis_data[b"error"] == "Some error occurred"
    assert redis_data[b"status"] == b"completed"
    assert redis_data[b"submitted_at"] == dt.datetime(2025, 4, 4, 12, 0, 0)
    assert redis_data[b"started_at"] ==dt.datetime(2025, 4, 4, 12, 5, 0)
    assert redis_data[b"heartbeat_at"] ==dt.datetime(2025, 4, 4, 12, 10, 0)
    assert redis_data[b"completed_at"] == dt.datetime(2025, 4, 4, 12, 15, 0)

    # Deserialize the task back from Redis format
    deserialized_task = Task.from_redis(task_id, raw_data)

    # Assert that the original task and deserialized task are equal
    assert task == deserialized_task
