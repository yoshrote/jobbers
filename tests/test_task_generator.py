from unittest.mock import AsyncMock, Mock

import pytest
from ulid import ULID

from jobbers.models.task import Task, TaskStatus
from jobbers.state_manager import QueueConfigAdapter, StateManager
from jobbers.task_generator import TaskGenerator

EXHAUSTED = object()

@pytest.mark.asyncio
async def test_find_queues_default_role():
    """Test that TaskGenerator finds default queues for the 'default' role."""
    state_manager = Mock(spec=StateManager)
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    task_generator = TaskGenerator(state_manager, queue_config_adapter, role="default")

    queues = await task_generator.find_queues()

    assert queues == {"default"}


@pytest.mark.asyncio
async def test_find_queues_custom_role():
    """Test that TaskGenerator finds queues for a custom role."""
    state_manager = Mock(spec=StateManager)
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queues.return_value = {"queue1", "queue2"}
    task_generator = TaskGenerator(state_manager, queue_config_adapter, role="custom_role")

    queues = await task_generator.find_queues()

    assert queues == {"queue1", "queue2"}
    queue_config_adapter.get_queues.assert_called_once_with("custom_role")


@pytest.mark.asyncio
async def test_find_queues_custom_role_no_queues():
    """Test that TaskGenerator falls back to default queues if no queues are found for a custom role."""
    state_manager = Mock(spec=StateManager)
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queues.return_value = None
    task_generator = TaskGenerator(state_manager, queue_config_adapter, role="custom_role")

    queues = await task_generator.find_queues()

    assert queues == set()
    queue_config_adapter.get_queues.assert_called_once_with("custom_role")

@pytest.mark.asyncio
async def test_task_generator_iteration():
    """Test that TaskGenerator iterates over tasks."""
    state_manager = Mock(spec=StateManager)
    task = Task(
        id=ULID(),
        name="test_task",
        version=1,
        status=TaskStatus.UNSUBMITTED,
    )
    state_manager.get_next_task.return_value = task
    state_manager.active_tasks_per_queue = {}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {}
    task_generator = TaskGenerator(state_manager, queue_config_adapter, role="default")

    assert await anext(task_generator, EXHAUSTED) == task

    state_manager.get_next_task.assert_called_once_with({"default"})

@pytest.mark.asyncio
async def test_task_generator_stops_iteration():
    """Test that TaskGenerator stops iteration when no tasks are available."""
    state_manager = AsyncMock(spec=StateManager)
    state_manager.get_next_task.return_value = None
    state_manager.active_tasks_per_queue = {}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {}
    task_generator = TaskGenerator(state_manager, queue_config_adapter, role="default")

    assert await anext(task_generator, EXHAUSTED) == EXHAUSTED

    state_manager.get_next_task.assert_called_once_with({"default"})

@pytest.mark.asyncio
async def test_task_generator_stops_after_max_tasks():
    """Test that TaskGenerator stops yielding after emitting max_tasks tasks."""
    state_manager = AsyncMock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {}
    task_generator = TaskGenerator(state_manager, queue_config_adapter, max_tasks=2)

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
