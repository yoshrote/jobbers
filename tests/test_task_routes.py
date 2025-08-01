import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
import redis.asyncio as redis
from httpx import ASGITransport, AsyncClient
from ulid import ULID

from jobbers.models import Task
from jobbers.models.task_config import TaskConfig
from jobbers.state_manager import TaskAdapter
from jobbers.task_routes import app

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")

@pytest_asyncio.fixture
async def redis_db():
    # Connect to a test Redis instance (e.g., managed by pytest-redis)
    client = redis.Redis(host='localhost', port=6379, db=1)
    yield client
    # Clean up after the test (e.g., flush the database)
    await client.flushdb()
    await client.close()

@pytest.fixture(autouse=True)
def patch_redis(redis_db):
     with patch("jobbers.task_routes.db.get_client", return_value=redis_db):
         yield

@pytest.mark.asyncio
async def test_main_page(redis_db):
    """
    Test the task submission and status endpoints.

    This task may flake out if there is a worker listening to the queue
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/")

    response_data = response.json()
    assert response.status_code == 200
    # check that the task details are the same as task_data other than status and submitted_at
    assert response_data["message"] ==  "Welcome to Task Manager!"
    assert response_data["tasks"] == []

@pytest.mark.asyncio
async def test_submit_valid_task(redis_db):
    """
    Test the task submission and status endpoints.

    This task may flake out if there is a worker listening to the queue
    """
    async def task_function() -> None:
        pass

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", status="unsubmitted", parameters={})

    with patch("jobbers.registry.get_task_config", return_value=test_task_config):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/submit-task", data=task_data.model_dump_json(exclude_unset=True))

    response_data = response.json()
    assert response.status_code == 200
    # check that the task details are the same as task_data other than status and submitted_at
    assert response_data["message"] == "Task submitted successfully"

    response_task = Task.model_validate(response_data["task"])

    def simplify(task: Task) -> dict[str, Any]:
        return task.model_dump(exclude=[
            # changed since submission
            "status", "submitted_at",
        ])

    assert simplify(response_task) == simplify(task_data)
    # Check that it actually exists in redis
    assert simplify(task_data) == simplify(await TaskAdapter(redis_db).get_task(task_data.id))

@pytest.mark.asyncio
# @pytest.mark.skip(reason="Need to add a registered task with a param to hit this error")
async def test_submit_invalid_task(redis_db):
    """Test the task submission fails when given bad input."""
    # jobber_registry.register_task("test_task", test_task_function, parameters=["foo"])
    # add a task config with a function that requires a parameter to the jobber registry
    async def task_function() -> None:
        pass

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", status="unsubmitted", parameters={"foo": "bar"})
    # try to submit a task without the required parameter
    with patch("jobbers.registry.get_task_config", return_value=test_task_config):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/submit-task", data=task_data.model_dump_json(exclude_unset=True))
    # this should raise a validation error
    assert response.status_code == 400

@pytest.mark.asyncio
async def test_get_task_status_found(redis_db):
    """Test retrieving the status of an existing task."""
    # Status must != "unsubmitted" or submit_task will change Task details
    task_data = Task(id=ULID1, name="Test Task", status="submitted", submitted_at=dt.datetime.now(dt.timezone.utc), parameters={})
    await TaskAdapter(redis_db).submit_task(task_data, extra_check=None)

    # Check that it exists via API
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/task-status/{ULID1}")

    assert response.status_code == 200
    assert Task.model_validate(response.json()) == task_data

@pytest.mark.asyncio
async def test_get_task_status_not_found():
    """Test retrieving the status of a non-existent task."""
    mock_task_adapter = AsyncMock()
    mock_task_adapter.get_task.return_value = None

    with patch("jobbers.task_routes.TaskAdapter", return_value=mock_task_adapter):

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/task-status/{ULID1}")

        assert response.status_code == 404
        assert response.json() == {"detail": "Task not found"}
        mock_task_adapter.get_task.assert_called_once_with(ULID1)

@pytest.mark.asyncio
async def test_get_task_list():
    """Test retrieving the list of all tasks."""
    mock_task_adapter = AsyncMock()
    mock_task_adapter.get_all_tasks.return_value = [
        {"id": str(ULID1), "name": "Task 1", "status": "started"},
        {"id": str(ULID2), "name": "Task 2", "status": "completed"},
    ]

    with patch("jobbers.task_routes.TaskAdapter", return_value=mock_task_adapter):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/task-list")

        assert response.status_code == 200
        assert response.json() == {
            "tasks": [
                {"id": str(ULID1), "name": "Task 1", "status": "started"},
                {"id": str(ULID2), "name": "Task 2", "status": "completed"},
            ]
        }
        mock_task_adapter.get_all_tasks.assert_called_once()

@pytest.mark.asyncio
async def test_get_queues():
    """Test retrieving the list of all queues for a given role."""
    mock_queue_adapter = AsyncMock()
    mock_queue_adapter.get_queues.return_value = {"queue1", "queue2"}

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_queue_adapter):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues/role1")

        assert response.status_code == 200
        assert response.json() == {"queues": ["queue1", "queue2"]}
        mock_queue_adapter.get_queues.assert_called_once_with("role1")

@pytest.mark.asyncio
async def test_set_queues():
    """Test setting the list of all queues for a given role."""
    mock_queue_adapter = AsyncMock()

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_queue_adapter):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/queues/role1", json=["queue1", "queue2"])

        assert response.status_code == 200
        assert response.json() == {"message": "Queues set successfully"}
        mock_queue_adapter.set_queues.assert_called_once_with("role1", {"queue1", "queue2"})

@pytest.mark.asyncio
async def test_get_all_queues():
    """Test retrieving the list of all queues."""
    mock_queue_adapter = AsyncMock()
    mock_queue_adapter.get_all_queues.return_value = ["queue1", "queue2", "queue3"]

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_queue_adapter):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues")

        assert response.status_code == 200
        assert response.json() == {"queues": ["queue1", "queue2", "queue3"]}
        mock_queue_adapter.get_all_queues.assert_called_once()

@pytest.mark.asyncio
async def test_get_all_roles():
    """Test retrieving the list of all roles."""
    mock_queue_adapter = AsyncMock()
    mock_queue_adapter.get_all_roles.return_value = ["role1", "role2"]

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_queue_adapter):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/roles")

        assert response.status_code == 200
        assert response.json() == {"roles": ["role1", "role2"]}
        mock_queue_adapter.get_all_roles.assert_called_once()
