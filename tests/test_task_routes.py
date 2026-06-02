import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import IntegrityError
from ulid import ULID

from jobbers.adapters.sql import SQLQueueConfigAdapter
from jobbers.models.dag import DAGRunPagination
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.models.task_routing import RoutingConfig, RoutingStrategy
from jobbers.state_manager import TaskException
from jobbers.task_routes import app

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")
FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)


@pytest_asyncio.fixture(autouse=True)
async def setup(session_factory, state_manager):
    """Seed the 'default' queue into both SQL (for task-routing CRUD tests) and the routing backend."""
    await SQLQueueConfigAdapter(session_factory).save_queue_config(QueueConfig(name="default"))
    await state_manager.routing.save_queue_config(QueueConfig(name="default"))
    patches = [
        patch("jobbers.task_routes.db.get_session_factory", return_value=session_factory),
        patch("jobbers.task_routes.db.get_state_manager", return_value=state_manager),
        patch("jobbers.db.get_task_adapter", return_value=state_manager.task_state),
    ]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()


@pytest.mark.asyncio
async def test_main_page():
    """GET / returns a welcome message and an empty task list."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/")

    response_data = response.json()
    assert response.status_code == 200
    # check that the task details are the same as task_data other than status and submitted_at
    assert response_data["message"] == "Welcome to Task Manager!"
    assert response_data["tasks"] == []


@pytest.mark.asyncio
async def test_submit_valid_task(state_manager):
    """A valid task is accepted, stored, and returned with SUBMITTED status."""

    async def task_function(foo: int) -> None: ...

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", status="unsubmitted", parameters={"foo": 42})

    with patch("jobbers.registry.get_task_config", return_value=test_task_config):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/submit-task", json=task_data.model_dump(mode="json", exclude_unset=True)
            )

    response_data = response.json()
    assert response.status_code == 200
    # check that the task details are the same as task_data other than status and submitted_at
    assert response_data["message"] == "Task submitted successfully"

    response_task = Task.model_validate(response_data["task"])

    def simplify(task: Task) -> dict[str, Any]:
        return task.model_dump(
            exclude=[
                # changed since submission
                "status",
                "submitted_at",
            ]
        )

    assert simplify(response_task) == simplify(task_data)
    # Check that it actually exists in redis
    assert simplify(task_data) == simplify(await state_manager.task_state.get_task(task_data.id))


@pytest.mark.asyncio
async def test_submit_invalid_task():
    """A task whose parameters fail type validation is rejected with a 400."""

    async def task_function(foo: int) -> None: ...

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", status="unsubmitted", parameters={"foo": "bar"})
    with patch("jobbers.registry.get_task_config", return_value=test_task_config):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/submit-task", json=task_data.model_dump(mode="json", exclude_unset=True)
            )
    # this should raise a validation error
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_task_status_found(state_manager):
    """Test retrieving the status of an existing task."""
    # Status must != "unsubmitted" or submit_task will change Task details
    task_data = Task(
        id=ULID1, name="Test Task", status="submitted", submitted_at=dt.datetime.now(dt.UTC), parameters={}
    )
    await state_manager.task_state.save_task(task_data)

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

    with patch("jobbers.db.get_task_adapter", return_value=mock_task_adapter):
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

    with patch("jobbers.db.get_task_adapter", return_value=mock_task_adapter):
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
    mock_sm = MagicMock()
    mock_sm.get_queues = AsyncMock(return_value={"queue1", "queue2"})

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues/role1")

        assert response.status_code == 200
        assert response.json() == {"queues": ["queue1", "queue2"]}
        mock_sm.get_queues.assert_called_once_with("role1")


@pytest.mark.asyncio
async def test_set_queues():
    """Test setting the list of all queues for a given role."""
    mock_sm = MagicMock()
    mock_sm.get_all_queues = AsyncMock(return_value=["queue1", "queue2"])
    mock_sm.save_role = AsyncMock()

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/queues/role1", json=["queue1", "queue2"])

        assert response.status_code == 200
        assert response.json() == {"message": "Queues set successfully"}
        mock_sm.save_role.assert_called_once_with("role1", {"queue1", "queue2"})


@pytest.mark.asyncio
async def test_set_queues_rolls_back_on_invalid_queue(session_factory):
    """Adding a nonexistent queue to a role rolls back: original queues are preserved."""
    qca = SQLQueueConfigAdapter(session_factory)
    await qca.save_queue_config(QueueConfig(name="extra_queue"))
    await qca.save_role("myrole", {"default", "extra_queue"})

    with pytest.raises(IntegrityError):
        await qca.save_role("myrole", {"default", "nonexistent_queue"})

    assert await qca.get_queues("myrole") == {"default", "extra_queue"}


@pytest.mark.asyncio
async def test_get_all_queues():
    """Test retrieving the list of all queues."""
    mock_sm = MagicMock()
    mock_sm.get_all_queues = AsyncMock(return_value=["queue1", "queue2", "queue3"])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues")

        assert response.status_code == 200
        assert response.json() == {"queues": ["queue1", "queue2", "queue3"]}
        mock_sm.get_all_queues.assert_called_once()


@pytest.mark.asyncio
async def test_get_all_roles():
    """Test retrieving the list of all roles."""
    mock_sm = MagicMock()
    mock_sm.get_all_roles = AsyncMock(return_value=["role1", "role2"])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/roles")

        assert response.status_code == 200
        assert response.json() == {"roles": ["role1", "role2"]}
        mock_sm.get_all_roles.assert_called_once()


@pytest.mark.asyncio
async def test_get_scheduled_tasks_no_optional_filters():
    """Test fetching scheduled tasks with only queue uses all-None optional filters."""
    task1 = Task(id=ULID1, name="Task 1", status="submitted", parameters={})
    task2 = Task(id=ULID2, name="Task 2", status="submitted", parameters={})
    run_at = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)

    mock_sm = MagicMock()
    mock_sm.task_scheduler.get_by_filter = AsyncMock(return_value=[(task1, run_at), (task2, run_at)])

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
    run_at = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)

    mock_sm = MagicMock()
    mock_sm.task_scheduler.get_by_filter = AsyncMock(return_value=[(task, run_at)])

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
    mock_sm.task_scheduler.get_by_filter = AsyncMock(return_value=[])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/scheduled-tasks", params={"queue": "nonexistent"})

    assert response.status_code == 200
    assert response.json() == {"tasks": []}


@pytest.mark.asyncio
async def test_get_scheduled_tasks_with_start_cursor():
    """Test that the start ULID is forwarded as start_after for cursor pagination."""
    task = Task(id=ULID2, name="Task 2", status="submitted", parameters={})
    run_at = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)

    mock_sm = MagicMock()
    mock_sm.task_scheduler.get_by_filter = AsyncMock(return_value=[(task, run_at)])

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
    mock_sm.dead_queue.get_by_ids = AsyncMock(return_value=[task])
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
    mock_sm.dead_queue.get_by_filter = AsyncMock(return_value=[task])
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
    mock_sm.dead_queue.get_by_ids = AsyncMock(return_value=[task])
    mock_sm.resubmit_dead_tasks = AsyncMock(return_value=[task])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/dead-letter-queue/resubmit",
                json={"task_ids": [str(ULID1)], "reset_retry_count": False},
            )

    assert response.status_code == 200
    mock_sm.resubmit_dead_tasks.assert_called_once_with([task], reset_retry_count=False)


# --- Cancellation tests ---


@pytest.mark.asyncio
async def test_cancel_task_submitted():
    """Test cancelling a task in SUBMITTED status returns 200."""
    task = Task(id=ULID1, name="Test Task", status="submitted", parameters={})

    mock_sm = MagicMock()
    mock_sm.request_task_cancellation = AsyncMock(return_value=task)

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/task/{ULID1}/cancel")

    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Cancellation request sent"
    assert data["task"]["id"] == str(ULID1)
    mock_sm.request_task_cancellation.assert_called_once_with(ULID1)


@pytest.mark.asyncio
async def test_cancel_task_started():
    """Test cancelling a task in STARTED status returns 200."""
    task = Task(id=ULID1, name="Test Task", status="started", parameters={})

    mock_sm = MagicMock()
    mock_sm.request_task_cancellation = AsyncMock(return_value=task)

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/task/{ULID1}/cancel")

    assert response.status_code == 200
    assert response.json()["message"] == "Cancellation request sent"


@pytest.mark.asyncio
async def test_cancel_task_not_found():
    """Test cancelling a non-existent task returns 404."""
    mock_sm = MagicMock()
    mock_sm.request_task_cancellation = AsyncMock(return_value=None)

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/task/{ULID1}/cancel")

    assert response.status_code == 404
    assert response.json() == {"detail": "Task not found"}


@pytest.mark.asyncio
async def test_cancel_task_not_cancellable():
    """Test cancelling a completed task returns 409."""
    mock_sm = MagicMock()
    mock_sm.request_task_cancellation = AsyncMock(
        side_effect=TaskException("Task has status 'completed' and cannot be cancelled.")
    )

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/task/{ULID1}/cancel")

    assert response.status_code == 409
    assert "cannot be cancelled" in response.json()["detail"]


@pytest.mark.asyncio
async def test_cancel_tasks_all_success():
    """Test bulk cancellation when all tasks are cancellable."""
    task1 = Task(id=ULID1, name="Task 1", status="submitted", parameters={})
    task2 = Task(id=ULID2, name="Task 2", status="started", parameters={})

    mock_sm = MagicMock()
    mock_sm.request_task_cancellation = AsyncMock(side_effect=[task1, task2])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/tasks/cancel",
                json={"task_ids": [str(ULID1), str(ULID2)]},
            )

    assert response.status_code == 200
    results = response.json()["results"]
    assert len(results) == 2
    assert all(r["status"] == "cancellation_requested" for r in results)


@pytest.mark.asyncio
async def test_cancel_tasks_mixed_results():
    """Test bulk cancellation with a mix of success, not found, and not cancellable."""
    task1 = Task(id=ULID1, name="Task 1", status="submitted", parameters={})

    mock_sm = MagicMock()
    mock_sm.request_task_cancellation = AsyncMock(
        side_effect=[
            task1,
            None,
            TaskException("Task has status 'completed' and cannot be cancelled."),
        ]
    )

    ulid3 = ULID.from_str("01KJ6CGYRS5WXNW66WEYQ7QTHV")

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/tasks/cancel",
                json={"task_ids": [str(ULID1), str(ULID2), str(ulid3)]},
            )

    assert response.status_code == 200
    results = response.json()["results"]
    assert results[0]["status"] == "cancellation_requested"
    assert results[1]["status"] == "not_found"
    assert results[2]["status"] == "error"
    assert "cannot be cancelled" in results[2]["detail"]


# ── Queue CRUD routes ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_queue():
    """POST /queues creates a new queue."""
    mock_sm = MagicMock()
    mock_sm.get_queue_config = AsyncMock(return_value=None)
    mock_sm.save_queue_config = AsyncMock()

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/queues", json={"name": "myqueue"})

    assert response.status_code == 201
    mock_sm.save_queue_config.assert_called_once()


@pytest.mark.asyncio
async def test_create_queue_conflict_returns_409():
    """POST /queues returns 409 when the queue already exists."""
    mock_sm = MagicMock()
    mock_sm.get_queue_config = AsyncMock(return_value=QueueConfig(name="myqueue"))

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/queues", json={"name": "myqueue"})

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_get_queue_config_found():
    """GET /queues/{name}/config returns the queue config."""
    mock_sm = MagicMock()
    mock_sm.get_queue_config = AsyncMock(return_value=QueueConfig(name="myqueue"))

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues/myqueue/config")

    assert response.status_code == 200
    assert response.json()["queue"]["name"] == "myqueue"


@pytest.mark.asyncio
async def test_get_queue_config_not_found():
    """GET /queues/{name}/config returns 404 when queue doesn't exist."""
    mock_sm = MagicMock()
    mock_sm.get_queue_config = AsyncMock(return_value=None)

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues/noqueue/config")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_queue(state_manager):
    """PUT /queues/{name} saves config via StateManager (which bumps refresh_tags and invalidates cache)."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/queues/myqueue", json={"name": "myqueue", "max_concurrent": 7})

    assert response.status_code == 200
    saved = await state_manager.routing.get_queue_config("myqueue")
    assert saved is not None
    assert saved.max_concurrent == 7


@pytest.mark.asyncio
async def test_delete_queue_found():
    """DELETE /queues/{name} removes the queue."""
    mock_sm = MagicMock()
    mock_sm.get_all_queues = AsyncMock(return_value=["myqueue"])
    mock_sm.delete_queue = AsyncMock()

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/queues/myqueue")

    assert response.status_code == 200
    mock_sm.delete_queue.assert_called_once_with("myqueue")


@pytest.mark.asyncio
async def test_delete_queue_not_found():
    """DELETE /queues/{name} returns 404 when the queue doesn't exist."""
    mock_sm = MagicMock()
    mock_sm.get_all_queues = AsyncMock(return_value=[])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/queues/noqueue")

    assert response.status_code == 404


# ── DLQ routes ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dead_letter_queue(state_manager):
    """GET /dead-letter-queue returns tasks from the DLQ."""
    task = Task(id=ULID1, name="failed_task", version=1, queue="default", status="failed")
    state_manager.dead_queue.get_by_filter = AsyncMock(return_value=[task])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/dead-letter-queue")

    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1


@pytest.mark.asyncio
async def test_get_dlq_task_history(state_manager):
    """GET /dead-letter-queue/{id}/history returns the error history."""
    state_manager.dead_queue.get_history = AsyncMock(return_value=[{"attempt": 0, "error": "boom"}])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/dead-letter-queue/{ULID1}/history")

    assert response.status_code == 200
    assert response.json()["history"][0]["error"] == "boom"


@pytest.mark.asyncio
async def test_remove_from_dlq(state_manager):
    """DELETE /dead-letter-queue removes the specified task IDs."""
    state_manager.dead_queue.remove_many = AsyncMock()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.request("DELETE", "/dead-letter-queue", json={"task_ids": [str(ULID1)]})

    assert response.status_code == 200
    assert response.json()["removed"] == 1
    state_manager.dead_queue.remove_many.assert_called_once()


# ── Active tasks route ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_tasks_with_queue_filter(state_manager):
    """GET /active-tasks?queue=default returns active tasks for that queue."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status="started")
    state_manager.get_active_tasks = AsyncMock(return_value=[task])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/active-tasks", params={"queue": "default"})

    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1


@pytest.mark.asyncio
async def test_get_active_tasks_no_filter_uses_all_queues(state_manager):
    """GET /active-tasks without a queue filter uses all queues from SQLite."""
    state_manager.get_active_tasks = AsyncMock(return_value=[])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/active-tasks")

    assert response.status_code == 200


# ── Role CRUD routes ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_role():
    """POST /roles creates a new role."""
    mock_sm = MagicMock()
    mock_sm.get_queues = AsyncMock(return_value=set())
    mock_sm.get_all_queues = AsyncMock(return_value=["default"])
    mock_sm.save_role = AsyncMock()

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/roles", json={"name": "myrole", "queues": ["default"]})

    assert response.status_code == 201
    mock_sm.save_role.assert_called_once()


@pytest.mark.asyncio
async def test_create_role_conflict_returns_409():
    """POST /roles returns 409 when the role already exists."""
    mock_sm = MagicMock()
    mock_sm.get_queues = AsyncMock(return_value={"default"})

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/roles", json={"name": "myrole", "queues": ["default"]})

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_get_role_found():
    """GET /roles/{name} returns the queues for the role."""
    mock_sm = MagicMock()
    mock_sm.get_all_roles = AsyncMock(return_value=["myrole"])
    mock_sm.get_queues = AsyncMock(return_value={"default"})

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/roles/myrole")

    assert response.status_code == 200
    assert response.json()["queues"] == ["default"]


@pytest.mark.asyncio
async def test_get_role_not_found():
    """GET /roles/{name} returns 404 when the role doesn't exist."""
    mock_sm = MagicMock()
    mock_sm.get_all_roles = AsyncMock(return_value=[])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/roles/norole")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_role_found():
    """PUT /roles/{name} replaces the queues for the role."""
    mock_sm = MagicMock()
    mock_sm.get_all_roles = AsyncMock(return_value=["myrole"])
    mock_sm.get_all_queues = AsyncMock(return_value=["default"])
    mock_sm.save_role = AsyncMock()

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put("/roles/myrole", json=["default"])

    assert response.status_code == 200
    mock_sm.save_role.assert_called_once_with("myrole", {"default"})


@pytest.mark.asyncio
async def test_update_role_not_found():
    """PUT /roles/{name} returns 404 when the role doesn't exist."""
    mock_sm = MagicMock()
    mock_sm.get_all_roles = AsyncMock(return_value=[])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put("/roles/norole", json=["default"])

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_role_found():
    """DELETE /roles/{name} removes the role."""
    mock_sm = MagicMock()
    mock_sm.get_all_roles = AsyncMock(return_value=["myrole"])
    mock_sm.delete_role = AsyncMock()

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/roles/myrole")

    assert response.status_code == 200
    mock_sm.delete_role.assert_called_once_with("myrole")


@pytest.mark.asyncio
async def test_delete_role_not_found():
    """DELETE /roles/{name} returns 404 when the role doesn't exist."""
    mock_sm = MagicMock()
    mock_sm.get_all_roles = AsyncMock(return_value=[])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/roles/norole")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_refresh_role_found(state_manager):
    """POST /roles/{name}/refresh returns 200 with a new refresh_tag."""
    await state_manager.routing.save_role("myrole", {"default"})
    tag_before = await state_manager.get_refresh_tag("myrole")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/roles/myrole/refresh")

    assert response.status_code == 200
    data = response.json()
    assert data["role"] == "myrole"
    assert "refresh_tag" in data
    tag_after = await state_manager.get_refresh_tag("myrole")
    assert tag_after != tag_before


@pytest.mark.asyncio
async def test_refresh_role_not_found():
    """POST /roles/{name}/refresh returns 404 when the role doesn't exist."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/roles/doesnotexist/refresh")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_queue_bumps_refresh_tag_for_containing_roles(state_manager):
    """PUT /queues/{name} bumps refresh_tag for every role that includes the queue."""
    await state_manager.routing.save_role("role_a", {"default", "q1"})
    await state_manager.routing.save_role("role_b", {"default"})
    tag_a_before = await state_manager.get_refresh_tag("role_a")
    tag_b_before = await state_manager.get_refresh_tag("role_b")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/queues/q1", json={"name": "q1", "max_concurrent": 3})

    assert response.status_code == 200
    tag_a_after = await state_manager.get_refresh_tag("role_a")
    tag_b_after = await state_manager.get_refresh_tag("role_b")
    assert tag_a_after != tag_a_before, "role_a should have a new refresh_tag (contains q1)"
    assert tag_b_after == tag_b_before, "role_b should be unchanged (does not contain q1)"


@pytest.mark.asyncio
async def test_update_task_routing_bumps_routing_version(state_manager, redis):
    """PUT /task-routing updates routing:version to a new ULID in Redis."""
    version_before = await redis.get("routing:version")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(
            "/task-routing/my_task/1",
            json={"strategy": "single", "queues": ["default"]},
        )

    assert response.status_code == 200
    version_after = await redis.get("routing:version")
    assert version_after is not None
    assert version_before != version_after


@pytest.mark.asyncio
async def test_delete_task_routing_bumps_routing_version(state_manager, redis):
    """DELETE /task-routing updates routing:version to a new ULID in Redis."""
    await state_manager.routing.save_routing_config(
        RoutingConfig(
            task_name="my_task", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["default"]
        )
    )
    version_before = await redis.get("routing:version")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/task-routing/my_task/1")

    assert response.status_code == 200
    version_after = await redis.get("routing:version")
    assert version_after is not None
    assert version_before != version_after


# ── submit_task: TaskException branch ────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_task_raises_400_on_task_exception():
    """POST /submit-task returns 400 when the state manager raises TaskException."""
    mock_sm = MagicMock()
    mock_sm.get_routing_config = AsyncMock(return_value=None)
    mock_sm.get_queue_config = AsyncMock(return_value=QueueConfig(name="default"))
    mock_sm.submit_task = AsyncMock(side_effect=TaskException("bad params"))

    async def task_function(foo: int) -> None: ...

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", status="unsubmitted", parameters={"foo": 42})

    with (
        patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm),
        patch("jobbers.registry.get_task_config", return_value=test_task_config),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/submit-task", json=task_data.model_dump(mode="json", exclude_unset=True)
            )

    assert response.status_code == 400
    assert "bad params" in response.json()["detail"]


# ── schedule_task route ───────────────────────────────────────────────────────

RUN_AT = dt.datetime(2026, 3, 18, 12, 0, 0, tzinfo=dt.UTC)


@pytest.mark.asyncio
async def test_schedule_task_valid(state_manager):
    """POST /schedule-task schedules the task and returns 200 with task summary and run_at."""

    async def task_function(foo: int) -> None: ...

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", queue="default", parameters={"foo": 1})

    with patch("jobbers.registry.get_task_config", return_value=test_task_config):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/schedule-task",
                json={
                    "task": task_data.model_dump(mode="json", exclude_unset=True),
                    "run_at": RUN_AT.isoformat(),
                },
            )

    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Task scheduled successfully"
    assert data["task"]["id"] == str(ULID1)
    assert data["run_at"] == RUN_AT.isoformat()

    scheduled = await state_manager.task_scheduler.get_by_filter(queue="default")
    assert len(scheduled) == 1
    assert scheduled[0][0].id == ULID1


@pytest.mark.asyncio
async def test_schedule_task_invalid_params_returns_400():
    """POST /schedule-task returns 400 when task parameters fail validation."""

    async def task_function(foo: int) -> None: ...

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", queue="default", parameters={"foo": "not_an_int"})

    with patch("jobbers.registry.get_task_config", return_value=test_task_config):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/schedule-task",
                json={
                    "task": task_data.model_dump(mode="json", exclude_unset=True),
                    "run_at": RUN_AT.isoformat(),
                },
            )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_schedule_task_unknown_task_returns_400():
    """POST /schedule-task returns 400 when the task name is not registered."""
    task_data = Task(id=ULID1, name="nonexistent_task", queue="default", parameters={})

    with patch("jobbers.registry.get_task_config", return_value=None):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/schedule-task",
                json={
                    "task": task_data.model_dump(mode="json", exclude_unset=True),
                    "run_at": RUN_AT.isoformat(),
                },
            )

    assert response.status_code == 400
    assert "nonexistent_task" in response.json()["detail"]


# ── DAG run listing and detail routes ─────────────────────────────────────────

DAG_RUN_ID = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
SUBMITTED_AT = dt.datetime(2026, 4, 12, 10, 0, 0, tzinfo=dt.UTC)


@pytest.mark.asyncio
async def test_list_dags_empty(state_manager):
    """GET /dags returns an empty list when no DAG runs exist."""
    state_manager.list_dag_runs = AsyncMock(return_value=([], 0))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/dags")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["dags"] == []


@pytest.mark.asyncio
async def test_list_dags_returns_runs(state_manager):
    """GET /dags returns DAG runs with dag_run_id and submitted_at."""
    state_manager.list_dag_runs = AsyncMock(return_value=([(DAG_RUN_ID, SUBMITTED_AT)], 1))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/dags")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["dags"]) == 1
    assert data["dags"][0]["dag_run_id"] == str(DAG_RUN_ID)
    assert data["dags"][0]["submitted_at"] == SUBMITTED_AT.isoformat()


@pytest.mark.asyncio
async def test_list_dags_passes_pagination(state_manager):
    """GET /dags forwards offset and limit query params as a DAGRunPagination object."""
    state_manager.list_dag_runs = AsyncMock(return_value=([], 0))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/dags", params={"offset": 10, "limit": 25})

    assert response.status_code == 200
    state_manager.list_dag_runs.assert_called_once_with(DAGRunPagination(offset=10, limit=25))


@pytest.mark.asyncio
async def test_get_dag_found(state_manager):
    """GET /dags/{dag_run_id} returns run details and task IDs."""
    state_manager.get_dag_run = AsyncMock(return_value=(SUBMITTED_AT, [ULID1, ULID2]))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/dags/{DAG_RUN_ID}")

    assert response.status_code == 200
    data = response.json()
    assert data["dag_run_id"] == str(DAG_RUN_ID)
    assert data["submitted_at"] == SUBMITTED_AT.isoformat()
    assert data["task_ids"] == [str(ULID1), str(ULID2)]


@pytest.mark.asyncio
async def test_get_dag_not_found(state_manager):
    """GET /dags/{dag_run_id} returns 404 when the run does not exist."""
    state_manager.get_dag_run = AsyncMock(return_value=None)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/dags/{DAG_RUN_ID}")

    assert response.status_code == 404


# ── task routing endpoints ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_task_routing_not_found():
    """GET /task-routing returns 404 when no routing config exists."""
    mock_sm = MagicMock()
    mock_sm.get_routing_config = AsyncMock(return_value=None)

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/task-routing/echo_task/1")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_task_routing_found(state_manager):
    """GET /task-routing returns the routing config when it exists."""
    config = RoutingConfig(
        task_name="echo_task", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"]
    )
    await state_manager.routing.save_routing_config(config)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/task-routing/echo_task/1")

    assert response.status_code == 200
    data = response.json()["routing"]
    assert data["strategy"] == "single"
    assert data["queues"] == ["fast"]


@pytest.mark.asyncio
async def test_put_task_routing_creates(state_manager):
    """PUT /task-routing creates a new routing config."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(
            "/task-routing/echo_task/1",
            json={"strategy": "single", "queues": ["fast"]},
        )

    assert response.status_code == 200
    assert response.json()["routing"]["strategy"] == "single"

    saved = await state_manager.routing.get_routing_config("echo_task", 1)
    assert saved is not None
    assert saved.strategy == RoutingStrategy.SINGLE
    assert saved.queues == ["fast"]


@pytest.mark.asyncio
async def test_put_task_routing_path_overrides_body(state_manager):
    """PUT uses path task_name/task_version even when body contains different values."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(
            "/task-routing/real_name/3",
            json={"task_name": "body_name", "task_version": 99, "strategy": "single", "queues": ["q"]},
        )

    assert response.status_code == 200
    saved = await state_manager.routing.get_routing_config("real_name", 3)
    assert saved is not None
    assert saved.task_name == "real_name"
    assert saved.task_version == 3


@pytest.mark.asyncio
async def test_delete_task_routing_removes_config(state_manager):
    """DELETE /task-routing removes the config and returns 200."""
    await state_manager.routing.save_routing_config(
        RoutingConfig(task_name="echo_task", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/task-routing/echo_task/1")

    assert response.status_code == 200
    assert await state_manager.routing.get_routing_config("echo_task", 1) is None


@pytest.mark.asyncio
async def test_delete_task_routing_not_found():
    """DELETE /task-routing returns 404 when config does not exist."""
    mock_sm = MagicMock()
    mock_sm.delete_routing_config = AsyncMock(return_value=False)

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/task-routing/ghost/99")

    assert response.status_code == 404


# ── Queue existence validation ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_set_queues_rejects_unknown_queue():
    """POST /queues/{role} returns 400 when any requested queue does not exist."""
    mock_sm = MagicMock()
    mock_sm.get_all_queues = AsyncMock(return_value=["default"])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/queues/myrole", json=["default", "ghost_queue"])

    assert response.status_code == 400
    assert "ghost_queue" in response.json()["detail"]
    mock_sm.save_role.assert_not_called()


@pytest.mark.asyncio
async def test_create_role_rejects_unknown_queue():
    """POST /roles returns 400 when any requested queue does not exist."""
    mock_sm = MagicMock()
    mock_sm.get_queues = AsyncMock(return_value=set())
    mock_sm.get_all_queues = AsyncMock(return_value=["default"])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/roles", json={"name": "newrole", "queues": ["default", "nope"]})

    assert response.status_code == 400
    assert "nope" in response.json()["detail"]
    mock_sm.save_role.assert_not_called()


@pytest.mark.asyncio
async def test_update_role_rejects_unknown_queue():
    """PUT /roles/{name} returns 400 when any requested queue does not exist."""
    mock_sm = MagicMock()
    mock_sm.get_all_roles = AsyncMock(return_value=["myrole"])
    mock_sm.get_all_queues = AsyncMock(return_value=["default"])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put("/roles/myrole", json=["default", "missing"])

    assert response.status_code == 400
    assert "missing" in response.json()["detail"]
    mock_sm.save_role.assert_not_called()


# ── task-status additional branches ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_task_status_scheduled_includes_scheduled_at(state_manager):
    """GET /task-status/{id} includes scheduled_at when the task is SCHEDULED."""
    run_at = dt.datetime(2025, 6, 1, tzinfo=dt.UTC)
    task = Task(id=ULID1, name="my_task", queue="default", status="scheduled", submitted_at=FROZEN_TIME)
    await state_manager.task_state.save_task(task)
    # Seed into the scheduler so get_run_at returns a value
    pipe = state_manager.job_store.pipeline(transaction=True)
    state_manager.task_scheduler.stage_add(pipe, task, run_at)
    await pipe.execute()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/task-status/{ULID1}")

    assert response.status_code == 200
    data = response.json()
    assert "scheduled_at" in data
    assert data["scheduled_at"] == run_at.isoformat()


@pytest.mark.asyncio
async def test_get_task_status_dag_root_includes_diagram(state_manager):
    """GET /task-status/{id} includes dag_diagram when the task has dag_callbacks set."""
    from jobbers.models.dag import DAGTaskSpec, SimpleCallback

    child_spec = DAGTaskSpec(name="child_task", queue="default")
    task = Task(
        id=ULID1,
        name="root_task",
        queue="default",
        status="submitted",
        submitted_at=FROZEN_TIME,
        dag_callbacks=[SimpleCallback(task=child_spec)],
    )
    await state_manager.task_state.save_task(task)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/task-status/{ULID1}")

    assert response.status_code == 200
    data = response.json()
    assert "dag_diagram" in data
    assert "root_task" in data["dag_diagram"]


# ── DLQ resubmit with task_version filter ────────────────────────────────────


@pytest.mark.asyncio
async def test_resubmit_from_dlq_by_filter_with_task_version():
    """POST /dead-letter-queue/resubmit passes task_version to get_by_filter."""
    task = Task(id=ULID1, name="My Task", version=2, status="failed", parameters={})

    mock_sm = MagicMock()
    mock_sm.dead_queue.get_by_filter = AsyncMock(return_value=[task])
    mock_sm.resubmit_dead_tasks = AsyncMock(return_value=[task])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/dead-letter-queue/resubmit",
                json={"queue": "default", "task_name": "My Task", "task_version": 2},
            )

    assert response.status_code == 200
    mock_sm.dead_queue.get_by_filter.assert_called_once_with(
        queue="default",
        task_name="My Task",
        task_version=2,
        limit=100,
    )


# ── submit-dag with unregistered task ────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_dag_with_unregistered_task_returns_400():
    """POST /submit-dag returns 400 when the diagram references a task not in the registry."""
    diagram = 'flowchart TD\n  A["totally_unregistered_xyz@1"]'

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/submit-dag", json={"diagram": diagram})

    assert response.status_code == 400
    assert "totally_unregistered_xyz" in response.json()["detail"]


# ── PUT /cron-dags/{id} branches ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_cron_dag_not_found_returns_404():
    """PUT /cron-dags/{id} returns 404 when the cron DAG does not exist."""
    mock_sm = MagicMock()
    mock_sm.cron_dag_scheduler.get = AsyncMock(return_value=None)

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                f"/cron-dags/{ULID1}",
                json={
                    "name": "updated",
                    "cron_expr": "0 * * * *",
                    "diagram": 'flowchart TD\n  A["my_task@1"]',
                },
            )

    assert response.status_code == 404
    assert str(ULID1) in response.json()["detail"]


@pytest.mark.asyncio
async def test_update_cron_dag_invalid_cron_expr_returns_400():
    """PUT /cron-dags/{id} returns 400 when cron_expr is not a valid cron expression."""
    from jobbers.models.cron_dag import CronDAGEntry
    from jobbers.models.dag import DAGTaskSpec

    existing = CronDAGEntry(
        name="existing",
        cron_expr="0 * * * *",
        dag_spec=DAGTaskSpec(name="my_task"),
    )
    mock_sm = MagicMock()
    mock_sm.cron_dag_scheduler.get = AsyncMock(return_value=existing)

    with patch("jobbers.task_routes.db.get_state_manager", return_value=mock_sm):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                f"/cron-dags/{existing.id}",
                json={
                    "name": "updated",
                    "cron_expr": "not a cron expression",
                    "diagram": 'flowchart TD\n  A["my_task@1"]',
                },
            )

    assert response.status_code == 400
    assert "cron" in response.json()["detail"].lower()
