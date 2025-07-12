from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from jobbers.models import Task
from jobbers.task_routes import app

ULID1 = "01JQC31AJP7TSA9X8AEP64XG08"
ULID2 = "01JQC31BHQ5AXV0JK23ZWSS5NA"

@pytest.mark.asyncio
async def test_submit_task():
    """Test the task submission endpoint."""
    mock_task_adapter = AsyncMock()
    with patch("jobbers.task_routes.TaskAdapter", return_value=mock_task_adapter), \
         patch("jobbers.task_routes.db.get_client") as mock_get_client:

        mock_get_client.return_value = AsyncMock()
        task_data = Task(id=ULID1, name="Test Task", status="submitted")
        summarized_data = task_data.summarized()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/submit-task", data=task_data.model_dump_json())

        assert response.status_code == 200
        assert response.json() == {"message": "Task submitted successfully", "task": summarized_data}
        mock_task_adapter.submit_task.assert_called_once_with(task_data)

@pytest.mark.asyncio
async def test_get_task_status_found():
    """Test retrieving the status of an existing task."""
    mock_task_adapter = AsyncMock()
    mock_task_adapter.get_task.return_value = Task(id=ULID1, name="Test Task", status="started")

    with patch("jobbers.task_routes.TaskAdapter", return_value=mock_task_adapter), \
         patch("jobbers.task_routes.db.get_client") as mock_get_client:

        mock_get_client.return_value = AsyncMock()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/task-status/{ULID1}")

        assert response.status_code == 200
        assert response.json() == {"task_id": ULID1, "status": "started"}
        mock_task_adapter.get_task.assert_called_once_with(ULID1)

@pytest.mark.asyncio
async def test_get_task_status_not_found():
    """Test retrieving the status of a non-existent task."""
    mock_task_adapter = AsyncMock()
    mock_task_adapter.get_task.return_value = None

    with patch("jobbers.task_routes.TaskAdapter", return_value=mock_task_adapter), \
         patch("jobbers.task_routes.db.get_client") as mock_get_client:

        mock_get_client.return_value = AsyncMock()

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
        {"id": ULID1, "name": "Task 1", "status": "started"},
        {"id": ULID2, "name": "Task 2", "status": "completed"},
    ]

    with patch("jobbers.task_routes.TaskAdapter", return_value=mock_task_adapter), \
         patch("jobbers.task_routes.db.get_client") as mock_get_client:

        mock_get_client.return_value = AsyncMock()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/task-list")

        assert response.status_code == 200
        assert response.json() == {
            "tasks": [
                {"id": ULID1, "name": "Task 1", "status": "started"},
                {"id": ULID2, "name": "Task 2", "status": "completed"},
            ]
        }
        mock_task_adapter.get_all_tasks.assert_called_once()

@pytest.mark.asyncio
async def test_get_queues():
    """Test retrieving the list of all queues for a given role."""
    mock_queue_adapter = AsyncMock()
    mock_queue_adapter.get_queues.return_value = {"queue1", "queue2"}

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_queue_adapter), \
         patch("jobbers.task_routes.db.get_client") as mock_get_client:

        mock_get_client.return_value = AsyncMock()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues/role1")

        assert response.status_code == 200
        assert response.json() == {"queues": ["queue1", "queue2"]}
        mock_queue_adapter.get_queues.assert_called_once_with("role1")

@pytest.mark.asyncio
async def test_set_queues():
    """Test setting the list of all queues for a given role."""
    mock_queue_adapter = AsyncMock()

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_queue_adapter), \
         patch("jobbers.task_routes.db.get_client") as mock_get_client:

        mock_get_client.return_value = AsyncMock()

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

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_queue_adapter), \
         patch("jobbers.task_routes.db.get_client") as mock_get_client:

        mock_get_client.return_value = AsyncMock()

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

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_queue_adapter), \
         patch("jobbers.task_routes.db.get_client") as mock_get_client:

        mock_get_client.return_value = AsyncMock()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/roles")

        assert response.status_code == 200
        assert response.json() == {"roles": ["role1", "role2"]}
        mock_queue_adapter.get_all_roles.assert_called_once()
