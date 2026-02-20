import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from ulid import ULID

from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.state_manager import TaskAdapter
from jobbers.task_routes import app

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")

@pytest_asyncio.fixture
async def redis_db():
    # Connect to a test Redis instance (e.g., managed by pytest-redis)
    client = fakeredis.aioredis.FakeRedis()
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
    async def task_function(foo: int) -> None:
        pass

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", status="unsubmitted", parameters={"foo": 42})

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
    async def task_function(foo: int) -> None:
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
            response = await client.get("/task-list", params={"queue": "default"})

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

@pytest.mark.asyncio
async def test_get_scheduled_tasks_no_optional_filters():
    """Test fetching scheduled tasks with only queue uses all-None optional filters."""
    task1 = Task(id=ULID1, name="Task 1", status="submitted", parameters={})
    task2 = Task(id=ULID2, name="Task 2", status="submitted", parameters={})

    mock_sm = MagicMock()
    mock_sm.task_scheduler.get_by_filter.return_value = [task1, task2]

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/scheduled-tasks", params={"queue": "default"})

    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 2
    mock_sm.task_scheduler.get_by_filter.assert_called_once_with(
        queue="default",
        task_name=None,
        task_version=None,
        limit=10,
        start_after=None,
    )


@pytest.mark.asyncio
async def test_get_scheduled_tasks_by_filter():
    """Test fetching scheduled tasks with queue, task_name, task_version, and limit filters."""
    task = Task(id=ULID1, name="My Task", status="submitted", parameters={})

    mock_sm = MagicMock()
    mock_sm.task_scheduler.get_by_filter.return_value = [task]

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/scheduled-tasks",
                params={"queue": "high", "task_name": "My Task", "task_version": 2, "limit": 10},
            )

    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1
    mock_sm.task_scheduler.get_by_filter.assert_called_once_with(
        queue="high",
        task_name="My Task",
        task_version=2,
        limit=10,
        start_after=None,
    )


@pytest.mark.asyncio
async def test_get_scheduled_tasks_empty():
    """Test fetching scheduled tasks returns an empty list when none match."""
    mock_sm = MagicMock()
    mock_sm.task_scheduler.get_by_filter.return_value = []

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/scheduled-tasks", params={"queue": "nonexistent"})

    assert response.status_code == 200
    assert response.json() == {"tasks": []}


@pytest.mark.asyncio
async def test_get_scheduled_tasks_with_start_cursor():
    """Test that the start ULID is forwarded as start_after for cursor pagination."""
    task = Task(id=ULID2, name="Task 2", status="submitted", parameters={})

    mock_sm = MagicMock()
    mock_sm.task_scheduler.get_by_filter.return_value = [task]

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/scheduled-tasks",
                params={"queue": "default", "start": str(ULID1)},
            )

    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1
    mock_sm.task_scheduler.get_by_filter.assert_called_once_with(
        queue="default",
        task_name=None,
        task_version=None,
        limit=10,
        start_after=str(ULID1),
    )


@pytest.mark.asyncio
async def test_resubmit_from_dlq_by_ids():
    """Test resubmitting DLQ tasks by explicit task ID list."""
    task = Task(id=ULID1, name="Test Task", status="submitted", parameters={})

    mock_sm = MagicMock()
    mock_sm.dead_queue.get_by_ids.return_value = [task]
    mock_sm.resubmit_dead_tasks = AsyncMock(return_value=[task])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/dead-letter-queue/resubmit",
                json={"task_ids": [str(ULID1)]},
            )

    assert response.status_code == 200
    data = response.json()
    assert data["resubmitted"] == 1
    assert len(data["tasks"]) == 1
    mock_sm.dead_queue.get_by_ids.assert_called_once_with([str(ULID1)])
    mock_sm.resubmit_dead_tasks.assert_called_once_with([task], reset_retry_count=True)

@pytest.mark.asyncio
async def test_resubmit_from_dlq_by_filter():
    """Test resubmitting DLQ tasks by filter criteria."""
    task = Task(id=ULID2, name="My Task", status="submitted", parameters={})

    mock_sm = MagicMock()
    mock_sm.dead_queue.get_by_filter.return_value = [task]
    mock_sm.resubmit_dead_tasks = AsyncMock(return_value=[task])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/dead-letter-queue/resubmit",
                json={"queue": "default", "task_name": "My Task"},
            )

    assert response.status_code == 200
    data = response.json()
    assert data["resubmitted"] == 1
    mock_sm.dead_queue.get_by_filter.assert_called_once_with(
        queue="default",
        task_name="My Task",
        task_version=None,
        limit=100,
    )
    mock_sm.resubmit_dead_tasks.assert_called_once_with([task], reset_retry_count=True)

@pytest.mark.asyncio
async def test_resubmit_from_dlq_no_filter_returns_400():
    """Test that omitting all filter criteria returns 400."""
    mock_sm = MagicMock()

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/dead-letter-queue/resubmit", json={})

    assert response.status_code == 400
    assert "Provide task_ids" in response.json()["detail"]
    mock_sm.dead_queue.get_by_ids.assert_not_called()
    mock_sm.dead_queue.get_by_filter.assert_not_called()

@pytest.mark.asyncio
async def test_resubmit_from_dlq_reset_retry_false():
    """Test that reset_retry_count=False is forwarded to resubmit_dead_tasks."""
    task = Task(id=ULID1, name="Test Task", status="submitted", parameters={})

    mock_sm = MagicMock()
    mock_sm.dead_queue.get_by_ids.return_value = [task]
    mock_sm.resubmit_dead_tasks = AsyncMock(return_value=[task])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/dead-letter-queue/resubmit",
                json={"task_ids": [str(ULID1)], "reset_retry_count": False},
            )

    assert response.status_code == 200
    mock_sm.resubmit_dead_tasks.assert_called_once_with([task], reset_retry_count=False)
