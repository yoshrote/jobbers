from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from playground.state_manager import Task
from playground.task_routes import app

ULID1 = "01JQC31AJP7TSA9X8AEP64XG08"
ULID2 = "01JQC31BHQ5AXV0JK23ZWSS5NA"

@pytest.mark.asyncio
async def test_submit_task():
    """Test the task submission endpoint."""
    mock_state_manager = AsyncMock()
    with patch("playground.task_routes.build_tm", return_value=mock_state_manager):
        task_data = Task(id=ULID1, name="Test Task", status="pending")
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
    mock_state_manager.get_task.return_value = Task(id=ULID1, name="Test Task", status="running")

    with patch("playground.task_routes.build_tm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/task-status/{ULID1}")

        assert response.status_code == 200
        assert response.json() == {"task_id": ULID1, "status": "running"}
        mock_state_manager.get_task.assert_called_once_with(ULID1)

@pytest.mark.asyncio
async def test_get_task_status_not_found():
    """Test retrieving the status of a non-existent task."""
    mock_state_manager = AsyncMock()
    mock_state_manager.get_task.return_value = None

    with patch("playground.task_routes.build_tm", return_value=mock_state_manager):
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
        {"id": ULID1, "name": "Task 1", "status": "running"},
        {"id": ULID2, "name": "Task 2", "status": "completed"},
    ]

    with patch("playground.task_routes.build_tm", return_value=mock_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/task-list")

        assert response.status_code == 200
        assert response.json() == {
            "tasks": [
                {"id": ULID1, "name": "Task 1", "status": "running"},
                {"id": ULID2, "name": "Task 2", "status": "completed"},
            ]
        }
        mock_state_manager.get_all_tasks.assert_called_once()
