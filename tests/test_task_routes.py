import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from ulid import ULID

from jobbers.adapters import MsgpackTaskAdapter as TaskAdapter
from jobbers.models.queue_config import QueueConfig, QueueConfigAdapter
from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.state_manager import StateManager
from jobbers.task_routes import app

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")


@pytest_asyncio.fixture
async def sqlite_with_default_queue(sqlite_conn):
    """Seed the in-memory SQLite DB with a 'default' queue."""
    await QueueConfigAdapter(sqlite_conn).save_queue_config(QueueConfig(name="default"))
    return sqlite_conn


@pytest_asyncio.fixture(autouse=True)
async def patch_sqlite(sqlite_with_default_queue):
    """Patch get_sqlite_conn for validation with the seeded in-memory SQLite DB."""
    with patch("jobbers.validation.db.get_sqlite_conn", return_value=sqlite_with_default_queue):
        yield sqlite_with_default_queue


@pytest_asyncio.fixture(autouse=True)
async def patch_state_manager(redis, patch_sqlite):
    """Provide a real StateManager backed by the test redis and sqlite connections."""
    ta = TaskAdapter(redis)
    sm = StateManager(redis, patch_sqlite, task_adapter=ta)
    with (
        patch("jobbers.task_routes.db.get_state_manager", return_value=sm),
        patch("jobbers.db.get_task_adapter", return_value=ta),
    ):
        yield sm


@pytest.mark.asyncio
async def test_main_page():
    """
    Test the task submission and status endpoints.

    This task may flake out if there is a worker listening to the queue
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/")

    response_data = response.json()
    assert response.status_code == 200
    # check that the task details are the same as task_data other than status and submitted_at
    assert response_data["message"] == "Welcome to Task Manager!"
    assert response_data["tasks"] == []


@pytest.mark.asyncio
async def test_submit_valid_task(redis):
    """
    Test the task submission and status endpoints.

    This task may flake out if there is a worker listening to the queue
    """

    async def task_function(foo: int) -> None: # pragma: no cover
        pass

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
    assert simplify(task_data) == simplify(await TaskAdapter(redis).get_task(task_data.id))


@pytest.mark.asyncio
# @pytest.mark.skip(reason="Need to add a registered task with a param to hit this error")
async def test_submit_invalid_task():
    """Test the task submission fails when given bad input."""

    # jobber_registry.register_task("test_task", test_task_function, parameters=["foo"])
    # add a task config with a function that requires a parameter to the jobber registry
    async def task_function(foo: int) -> None: # pragma: no cover
        pass

    test_task_config = TaskConfig(name="Test Task", function=task_function)
    task_data = Task(id=ULID1, name="Test Task", status="unsubmitted", parameters={"foo": "bar"})
    # try to submit a task without the required parameter
    with patch("jobbers.registry.get_task_config", return_value=test_task_config):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/submit-task", json=task_data.model_dump(mode="json", exclude_unset=True)
            )
    # this should raise a validation error
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_task_status_found(redis):
    """Test retrieving the status of an existing task."""
    # Status must != "unsubmitted" or submit_task will change Task details
    task_data = Task(
        id=ULID1, name="Test Task", status="submitted", submitted_at=dt.datetime.now(dt.UTC), parameters={}
    )
    await TaskAdapter(redis).submit_task(task_data)

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
        mock_queue_adapter.save_role.assert_called_once_with("role1", {"queue1", "queue2"})


@pytest.mark.asyncio
async def test_set_queues_rolls_back_on_invalid_queue(patch_sqlite):
    """Adding a nonexistent queue to a role rolls back: original queues are preserved."""
    import aiosqlite

    qca = QueueConfigAdapter(patch_sqlite)
    await qca.save_queue_config(QueueConfig(name="extra_queue"))
    await qca.save_role("myrole", {"default", "extra_queue"})

    with pytest.raises(aiosqlite.IntegrityError):
        await qca.save_role("myrole", {"default", "nonexistent_queue"})

    assert await qca.get_queues("myrole") == {"default", "extra_queue"}


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
    mock_sm.task_scheduler.get_by_filter = AsyncMock(return_value=[task1, task2])

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
    mock_sm.task_scheduler.get_by_filter = AsyncMock(return_value=[task])

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

    mock_sm = MagicMock()
    mock_sm.task_scheduler.get_by_filter = AsyncMock(return_value=[task])

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
    from jobbers.state_manager import TaskException

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
    from jobbers.state_manager import TaskException

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
    mock_qca = AsyncMock()
    mock_qca.get_queue_config.return_value = None

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/queues", json={"name": "myqueue"})

    assert response.status_code == 201
    mock_qca.save_queue_config.assert_called_once()


@pytest.mark.asyncio
async def test_create_queue_conflict_returns_409():
    """POST /queues returns 409 when the queue already exists."""
    mock_qca = AsyncMock()
    mock_qca.get_queue_config.return_value = QueueConfig(name="myqueue")

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/queues", json={"name": "myqueue"})

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_get_queue_config_found():
    """GET /queues/{name}/config returns the queue config."""
    mock_qca = AsyncMock()
    mock_qca.get_queue_config.return_value = QueueConfig(name="myqueue")

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues/myqueue/config")

    assert response.status_code == 200
    assert response.json()["queue"]["name"] == "myqueue"


@pytest.mark.asyncio
async def test_get_queue_config_not_found():
    """GET /queues/{name}/config returns 404 when queue doesn't exist."""
    mock_qca = AsyncMock()
    mock_qca.get_queue_config.return_value = None

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/queues/noqueue/config")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_queue():
    """PUT /queues/{name} updates the queue config."""
    mock_qca = AsyncMock()

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put("/queues/myqueue", json={"name": "myqueue"})

    assert response.status_code == 200
    mock_qca.save_queue_config.assert_called_once()


@pytest.mark.asyncio
async def test_delete_queue_found():
    """DELETE /queues/{name} removes the queue."""
    mock_qca = AsyncMock()
    mock_qca.get_all_queues.return_value = ["myqueue"]

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/queues/myqueue")

    assert response.status_code == 200
    mock_qca.delete_queue.assert_called_once_with("myqueue")


@pytest.mark.asyncio
async def test_delete_queue_not_found():
    """DELETE /queues/{name} returns 404 when the queue doesn't exist."""
    mock_qca = AsyncMock()
    mock_qca.get_all_queues.return_value = []

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/queues/noqueue")

    assert response.status_code == 404


# ── DLQ routes ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dead_letter_queue(patch_state_manager):
    """GET /dead-letter-queue returns tasks from the DLQ."""
    task = Task(id=ULID1, name="failed_task", version=1, queue="default", status="failed")
    patch_state_manager.dead_queue.get_by_filter = AsyncMock(return_value=[task])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=patch_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/dead-letter-queue")

    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1


@pytest.mark.asyncio
async def test_get_dlq_task_history(patch_state_manager):
    """GET /dead-letter-queue/{id}/history returns the error history."""
    patch_state_manager.dead_queue.get_history = AsyncMock(return_value=[{"attempt": 0, "error": "boom"}])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=patch_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/dead-letter-queue/{ULID1}/history")

    assert response.status_code == 200
    assert response.json()["history"][0]["error"] == "boom"


@pytest.mark.asyncio
async def test_remove_from_dlq(patch_state_manager):
    """DELETE /dead-letter-queue removes the specified task IDs."""
    patch_state_manager.dead_queue.remove_many = AsyncMock()

    with patch("jobbers.task_routes.db.get_state_manager", return_value=patch_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.request("DELETE", "/dead-letter-queue", json={"task_ids": [str(ULID1)]})

    assert response.status_code == 200
    assert response.json()["removed"] == 1
    patch_state_manager.dead_queue.remove_many.assert_called_once()


# ── Active tasks route ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_tasks_with_queue_filter(patch_state_manager):
    """GET /active-tasks?queue=default returns active tasks for that queue."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status="started")
    patch_state_manager.get_active_tasks = AsyncMock(return_value=[task])

    with patch("jobbers.task_routes.db.get_state_manager", return_value=patch_state_manager):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/active-tasks", params={"queue": "default"})

    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1


@pytest.mark.asyncio
async def test_get_active_tasks_no_filter_uses_all_queues(patch_state_manager, patch_sqlite):
    """GET /active-tasks without a queue filter uses all queues from SQLite."""
    patch_state_manager.get_active_tasks = AsyncMock(return_value=[])

    with (
        patch("jobbers.task_routes.db.get_state_manager", return_value=patch_state_manager),
        patch("jobbers.task_routes.db.get_sqlite_conn", return_value=patch_sqlite),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/active-tasks")

    assert response.status_code == 200


# ── Role CRUD routes ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_role():
    """POST /roles creates a new role."""
    mock_qca = AsyncMock()
    mock_qca.get_queues.return_value = set()

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/roles", json={"name": "myrole", "queues": ["default"]})

    assert response.status_code == 201
    mock_qca.save_role.assert_called_once()


@pytest.mark.asyncio
async def test_create_role_conflict_returns_409():
    """POST /roles returns 409 when the role already exists."""
    mock_qca = AsyncMock()
    mock_qca.get_queues.return_value = {"default"}

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/roles", json={"name": "myrole", "queues": ["default"]})

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_get_role_found():
    """GET /roles/{name} returns the queues for the role."""
    mock_qca = AsyncMock()
    mock_qca.get_all_roles.return_value = ["myrole"]
    mock_qca.get_queues.return_value = {"default"}

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/roles/myrole")

    assert response.status_code == 200
    assert response.json()["queues"] == ["default"]


@pytest.mark.asyncio
async def test_get_role_not_found():
    """GET /roles/{name} returns 404 when the role doesn't exist."""
    mock_qca = AsyncMock()
    mock_qca.get_all_roles.return_value = []

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/roles/norole")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_role_found():
    """PUT /roles/{name} replaces the queues for the role."""
    mock_qca = AsyncMock()
    mock_qca.get_all_roles.return_value = ["myrole"]

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put("/roles/myrole", json=["default"])

    assert response.status_code == 200
    mock_qca.save_role.assert_called_once_with("myrole", {"default"})


@pytest.mark.asyncio
async def test_update_role_not_found():
    """PUT /roles/{name} returns 404 when the role doesn't exist."""
    mock_qca = AsyncMock()
    mock_qca.get_all_roles.return_value = []

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put("/roles/norole", json=["default"])

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_role_found():
    """DELETE /roles/{name} removes the role."""
    mock_qca = AsyncMock()
    mock_qca.get_all_roles.return_value = ["myrole"]

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/roles/myrole")

    assert response.status_code == 200
    mock_qca.delete_role.assert_called_once_with("myrole")


@pytest.mark.asyncio
async def test_delete_role_not_found():
    """DELETE /roles/{name} returns 404 when the role doesn't exist."""
    mock_qca = AsyncMock()
    mock_qca.get_all_roles.return_value = []

    with patch("jobbers.task_routes.QueueConfigAdapter", return_value=mock_qca):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.delete("/roles/norole")

    assert response.status_code == 404


# ── submit_task: TaskException branch ────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_task_raises_400_on_task_exception():
    """POST /submit-task returns 400 when the state manager raises TaskException."""
    from jobbers.state_manager import TaskException

    mock_sm = MagicMock()
    mock_sm.submit_task = AsyncMock(side_effect=TaskException("bad params"))

    async def task_function(foo: int) -> None: # pragma: no cover
        pass

    from jobbers.models.task_config import TaskConfig

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
