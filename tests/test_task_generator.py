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
async def test_should_reload_queues_refresh_tag_changed():
    """Test that TaskGenerator reloads queues when the refresh tag changes."""
    state_manager = AsyncMock()
    state_manager.get_refresh_tag.return_value = "new_refresh_tag"
    task_generator = TaskGenerator(state_manager, role="custom_role")
    task_generator.refresh_tag = "old_refresh_tag"

    should_reload = await task_generator.should_reload_queues()

    assert should_reload is True
    assert task_generator.refresh_tag == "new_refresh_tag"
    state_manager.get_refresh_tag.assert_called_once_with("custom_role")


@pytest.mark.asyncio
async def test_should_reload_queues_refresh_tag_unchanged():
    """Test that TaskGenerator does not reload queues when the refresh tag has not changed."""
    state_manager = AsyncMock()
    state_manager.get_refresh_tag.return_value = "same_refresh_tag"
    task_generator = TaskGenerator(state_manager, role="custom_role")
    task_generator.refresh_tag = "same_refresh_tag"

    should_reload = await task_generator.should_reload_queues()

    assert should_reload is False
    state_manager.get_refresh_tag.assert_called_once_with("custom_role")


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
async def test_should_reload_queues_due_to_ttl():
    """Test that should_reload_queues returns True when now - _last_refreshed >= config_ttl."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=60)

    # Set _last_refreshed to 61 seconds ago
    task_generator._last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=61)

    should_reload = await task_generator.should_reload_queues()

    state_manager.get_refresh_tag.assert_called_once()
    assert should_reload is True, "should_reload_queues should return True when TTL has expired"

@pytest.mark.asyncio
async def test_should_reload_queues_under_ttl():
    """Test that should_reload_queues returns False when now - _last_refreshed < config_ttl."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=60)

    # Set _last_refreshed to 61 seconds ago
    task_generator._last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=30)

    should_reload = await task_generator.should_reload_queues()

    # We should not check state when under the TTL
    state_manager.get_refresh_tag.assert_not_called()
    assert should_reload is False, "should_reload_queues should return True when TTL has expired"

@pytest.mark.asyncio
async def test_should_reload_queues_on_intial_load():
    """Test that should_reload_queues returns False when now - _last_refreshed < config_ttl."""
    state_manager = AsyncMock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, config_ttl=60)

    # Set _last_refreshed to 61 seconds ago
    task_generator._last_refreshed = None

    should_reload = await task_generator.should_reload_queues()

    state_manager.get_refresh_tag.assert_called_once()
    assert should_reload is True, "should_reload_queues should return True when TTL has expired"
