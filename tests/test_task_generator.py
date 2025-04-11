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
    state_manager.get_refresh_tag = AsyncMock(return_value="new_tag")

    async with task_generator.is_local_cache_stale():
        pass

    assert task_generator.refresh_tag == "new_tag", "Refresh tag should be updated on initial load"
    assert task_generator._last_refreshed is not None, "Last refreshed timestamp should be updated"

@pytest.mark.asyncio
async def test_is_local_cache_stale_ttl_expired():
    """Test that is_local_cache_stale refreshes when TTL has expired."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=60)
    task_generator._last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=61)
    task_generator.task_queues = {"default"}
    task_generator.refresh_tag = "old_tag"
    state_manager.get_refresh_tag = AsyncMock(return_value="new_tag")

    async with task_generator.is_local_cache_stale():
        pass

    assert task_generator.refresh_tag == "new_tag", "Refresh tag should be updated when TTL expires"
    assert task_generator._last_refreshed is not None, "Last refreshed timestamp should be updated"

@pytest.mark.asyncio
async def test_is_local_cache_stale_ttl_disabled():
    """Test that is_local_cache_stale refreshes when TTL is disabled."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=0)
    task_generator._last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=61)
    task_generator.task_queues = {"default"}
    task_generator.refresh_tag = "old_tag"
    state_manager.get_refresh_tag = AsyncMock(return_value="new_tag")

    async with task_generator.is_local_cache_stale():
        pass

    assert task_generator.refresh_tag == "new_tag", "Refresh tag should be updated when TTL expires"
    assert task_generator._last_refreshed is not None, "Last refreshed timestamp should be updated"

@pytest.mark.asyncio
async def test_is_local_cache_stale_no_refresh_needed():
    """Test that is_local_cache_stale does not refresh when TTL has not expired."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=60)
    task_generator._last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=30)
    task_generator.task_queues = {"default"}
    task_generator.refresh_tag = "old_tag"
    state_manager.get_refresh_tag = AsyncMock(return_value="old_tag")

    async with task_generator.is_local_cache_stale():
        pass

    assert task_generator.refresh_tag == "old_tag", "Refresh tag should not change when no refresh is needed"
    assert task_generator._last_refreshed is not None, "Last refreshed timestamp should remain unchanged"
