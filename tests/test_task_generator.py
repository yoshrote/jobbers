import datetime as dt
from unittest.mock import AsyncMock

import pytest
from ulid import ULID

from jobbers.models.task import Task, TaskStatus
from jobbers.state_manager import StateManager
from jobbers.worker_proc import TaskGenerator


@pytest.mark.asyncio
async def test_find_queues_default_role():
    """Test that TaskGenerator finds default queues for the 'default' role."""
    state_manager = AsyncMock()
    task_generator = TaskGenerator(state_manager, role="default")

    queues = await task_generator.find_queues()

    assert queues == {"default"}


@pytest.mark.asyncio
async def test_find_queues_custom_role():
    """Test that TaskGenerator finds queues for a custom role."""
    state_manager = AsyncMock()
    state_manager.get_queues.return_value = {"queue1", "queue2"}
    task_generator = TaskGenerator(state_manager, role="custom_role")

    queues = await task_generator.find_queues()

    assert queues == {"queue1", "queue2"}
    state_manager.get_queues.assert_called_once_with("custom_role")


@pytest.mark.asyncio
async def test_find_queues_custom_role_no_queues():
    """Test that TaskGenerator falls back to default queues if no queues are found for a custom role."""
    state_manager = AsyncMock()
    state_manager.get_queues.return_value = None
    task_generator = TaskGenerator(state_manager, role="custom_role")

    queues = await task_generator.find_queues()

    assert queues == set()
    state_manager.get_queues.assert_called_once_with("custom_role")


@pytest.mark.asyncio
async def test_task_generator_iteration():
    """Test that TaskGenerator iterates over tasks."""
    state_manager = AsyncMock()
    task = Task(
        id=ULID(),
        name="test_task",
        version=1,
        status=TaskStatus.UNSUBMITTED,
    )
    state_manager.get_next_task.return_value = task
    task_generator = TaskGenerator(state_manager, role="default")

    async for generated_task in task_generator:
        assert generated_task == task
        break  # Stop after the first task for testing purposes

    state_manager.get_next_task.assert_called_once_with({"default"})


@pytest.mark.asyncio
async def test_task_generator_stops_iteration():
    """Test that TaskGenerator stops iteration when no tasks are available."""
    state_manager = AsyncMock()
    state_manager.get_next_task.return_value = None
    task_generator = TaskGenerator(state_manager, role="default")

    async for _ in task_generator:
        pass

    state_manager.get_next_task.assert_called_once_with({"default"})


@pytest.mark.asyncio
async def test_is_local_cache_stale_initial_load():
    """Test that is_local_cache_stale refreshes on initial load."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=60)
    task_generator.task_queues = None  # Simulate initial load

    needed_refresh = False
    assert task_generator.ttl.last_refreshed is None, "the last_refreshed should be None before loading"
    async with task_generator.ttl as needs_refresh:
        needed_refresh = needs_refresh

    assert needed_refresh, "Refresh should be needed on initial load"
    assert task_generator.ttl.last_refreshed is not None, "Last refreshed timestamp should be updated"

@pytest.mark.asyncio
async def test_is_local_cache_stale_ttl_expired():
    """Test that is_local_cache_stale refreshes when TTL has expired."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=60)
    original_refresh = task_generator.ttl.last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=61)

    needed_refresh = False
    async with task_generator.ttl as needs_refresh:
        needed_refresh = needs_refresh

    assert needed_refresh, "Refresh should be needed when TTL has expired"
    # assert task_generator.refresh_tag == "new_tag", "Refresh tag should be updated when TTL expires"
    assert task_generator.ttl.last_refreshed != original_refresh, "Last refreshed timestamp should be updated"

@pytest.mark.asyncio
async def test_is_local_cache_stale_ttl_disabled():
    """Test that is_local_cache_stale refreshes when TTL is disabled."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=0)
    original_refresh = task_generator.ttl.last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=61)

    needed_refresh = False
    async with task_generator.ttl as needs_refresh:
        needed_refresh = needs_refresh

    assert needed_refresh, "Refresh should be needed when TTL is disabled"
    # assert task_generator.refresh_tag == "new_tag", "Refresh tag should be updated when TTL expires"
    assert task_generator.ttl.last_refreshed != original_refresh, "Last refreshed timestamp should be updated"

@pytest.mark.asyncio
async def test_is_local_cache_stale_no_refresh_needed():
    """Test that is_local_cache_stale does not refresh when TTL has not expired."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=60)
    original_refresh = task_generator.ttl.last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=30)
    # task_generator.task_queues = {"default"}
    # task_generator.refresh_tag = "old_tag"
    # state_manager.get_refresh_tag = AsyncMock(return_value="old_tag")

    needed_refresh = False
    async with task_generator.ttl as needs_refresh:
        needed_refresh = needs_refresh

    assert not needed_refresh, "Refresh should not be needed when TTL has not expired"
    # assert task_generator.refresh_tag == "old_tag", "Refresh tag should not change when no refresh is needed"
    # assert task_generator.ttl.last_refreshed is not None, "Last refreshed timestamp should remain unchanged"
    assert task_generator.ttl.last_refreshed == original_refresh, "Last refreshed timestamp should remain unchanged"

@pytest.mark.asyncio
async def test_task_generator_stops_after_max_tasks():
    """Test that TaskGenerator stops yielding after emitting max_tasks tasks."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, max_tasks=2)

    # Mock get_next_task to return a new Task
    state_manager.get_next_task = AsyncMock(
        side_effect=[
            Task(id=ULID(), name="task1", version=1),
            Task(id=ULID(), name="task2", version=1),
            Task(id=ULID(), name="task3", version=1),
        ]
    )

    tasks = []
    async for task in task_generator:
        tasks.append(task)

    assert len(tasks) == 2, "TaskGenerator should yield exactly max_tasks tasks"
    assert tasks[0].name == "task1"
    assert tasks[1].name == "task2"
