from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from jobbers.state_manager import Task
from jobbers.task_routes import app

ULID1 = "01JQC31AJP7TSA9X8AEP64XG08"
ULID2 = "01JQC31BHQ5AXV0JK23ZWSS5NA"

@pytest.mark.asyncio
async def test_submit_task():
    """Test the task submission endpoint."""
    mock_state_manager = AsyncMock()
    with patch("jobbers.state_manager.build_sm", return_value=mock_state_manager):
        task_data = Task(id=ULID1, name="Test Task", status="submitted")
        summarized_data = task_data.summarized()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/submit-task", data=task_data.model_dump_json())

        assert response.status_code == 200
        assert response.json() == {"message": "Task submitted successfully", "task": summarized_data}
        mock_state_manager.submit_task.assert_called_once_with(task_data)

@pytest.mark.asyncio
async def test_get_task_status_found():
    """Test retrieving the status of an existing task."""
    mock_state_manager = AsyncMock()
    mock_state_manager.get_task.return_value = Task(id=ULID1, name="Test Task", status="started")

    with patch("jobbers.state_manager.build_sm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/task-status/{ULID1}")

        assert response.status_code == 200
        assert response.json() == {"task_id": ULID1, "status": "started"}
        mock_state_manager.get_task.assert_called_once_with(ULID1)

@pytest.mark.asyncio
async def test_get_task_status_not_found():
    """Test retrieving the status of a non-existent task."""
    mock_state_manager = AsyncMock()
    mock_state_manager.get_task.return_value = None

    with patch("jobbers.state_manager.build_sm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/task-status/{ULID1}")

        assert response.status_code == 404
        assert response.json() == {"detail": "Task not found"}
        mock_state_manager.get_task.assert_called_once_with(ULID1)

@pytest.mark.asyncio
async def test_get_task_list():
    """Test retrieving the list of all tasks."""
    mock_state_manager = AsyncMock()
    mock_state_manager.get_all_tasks.return_value = [
        {"id": ULID1, "name": "Task 1", "status": "started"},
        {"id": ULID2, "name": "Task 2", "status": "completed"},
    ]

    with patch("jobbers.state_manager.build_sm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/task-list")

        assert response.status_code == 200
        assert response.json() == {
            "tasks": [
                {"id": ULID1, "name": "Task 1", "status": "started"},
                {"id": ULID2, "name": "Task 2", "status": "completed"},
            ]
        }
        mock_state_manager.get_all_tasks.assert_called_once()

@pytest.mark.asyncio
async def test_get_queues():
    """Test retrieving the list of all queues for a given role."""
    mock_state_manager = AsyncMock()
    mock_state_manager.get_queues.return_value = {"queue1", "queue2"}

    with patch("jobbers.state_manager.build_sm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues/role1")

        assert response.status_code == 200
        assert response.json() == {"queues": ["queue1", "queue2"]}
        mock_state_manager.get_queues.assert_called_once_with("role1")

@pytest.mark.asyncio
async def test_set_queues():
    """Test setting the list of all queues for a given role."""
    mock_state_manager = AsyncMock()

    with patch("jobbers.state_manager.build_sm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/queues/role1", json=["queue1", "queue2"])

        assert response.status_code == 200
        assert response.json() == {"message": "Queues set successfully"}
        mock_state_manager.set_queues.assert_called_once_with("role1", {"queue1", "queue2"})

@pytest.mark.asyncio
async def test_get_all_queues():
    """Test retrieving the list of all queues."""
    mock_state_manager = AsyncMock()
    mock_state_manager.get_all_queues.return_value = ["queue1", "queue2", "queue3"]

    with patch("jobbers.state_manager.build_sm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues")

        assert response.status_code == 200
        assert response.json() == {"queues": ["queue1", "queue2", "queue3"]}
        mock_state_manager.get_all_queues.assert_called_once()

@pytest.mark.asyncio
async def test_get_all_roles():
    """Test retrieving the list of all roles."""
    mock_state_manager = AsyncMock()
    mock_state_manager.get_all_roles.return_value = ["role1", "role2"]

    with patch("jobbers.state_manager.build_sm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/roles")

        assert response.status_code == 200
        assert response.json() == {"roles": ["role1", "role2"]}
        mock_state_manager.get_all_roles.assert_called_once()
