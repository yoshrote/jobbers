from datetime import datetime, timezone
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
        status=TaskStatus.SUBMITTED,
        submitted_at=datetime.now(tz=timezone.utc)
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
            Task(id=ULID(), name="task1", version=1, submitted_at=datetime.now(tz=timezone.utc)),
            Task(id=ULID(), name="task2", version=1, submitted_at=datetime.now(tz=timezone.utc)),
            Task(id=ULID(), name="task3", version=1, submitted_at=datetime.now(tz=timezone.utc)),
        ]
    )

    tasks = []
    async for task in task_generator:
        tasks.append(task)

    assert len(tasks) == 2, "TaskGenerator should yield exactly max_tasks tasks"
    assert tasks[0].name == "task1"
    assert tasks[1].name == "task2"


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_empty_queues():
    """Test that filter_by_worker_queue_capacity returns empty set when given empty queues."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    result = await task_generator.filter_by_worker_queue_capacity(set())

    assert result == set()


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_no_limits():
    """Test that filter_by_worker_queue_capacity returns all queues when no limits are set."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 5, "queue2": 10}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {"queue1": None, "queue2": None}
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1", "queue2"}
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_zero_limit():
    """Test that filter_by_worker_queue_capacity returns all queues when limit is 0 (no limit)."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 5, "queue2": 10}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {"queue1": 0, "queue2": 0}
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1", "queue2"}
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_under_limit():
    """Test that filter_by_worker_queue_capacity includes queues under their limits."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 3, "queue2": 7}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {"queue1": 5, "queue2": 10}
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    # Both queues should be included since active tasks (3, 7) < limits (5, 10)
    assert result == {"queue1", "queue2"}
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_at_limit():
    """Test that filter_by_worker_queue_capacity excludes queues at their limits."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 5, "queue2": 10}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {"queue1": 5, "queue2": 10}
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    # Both queues should be excluded since active tasks (5, 10) == limits (5, 10)
    assert result == set()
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_over_limit():
    """Test that filter_by_worker_queue_capacity excludes queues over their limits."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 8, "queue2": 15}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {"queue1": 5, "queue2": 10}
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    # Both queues should be excluded since active tasks (8, 15) > limits (5, 10)
    assert result == set()
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_mixed_scenarios():
    """Test that filter_by_worker_queue_capacity handles mixed scenarios correctly."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {
        "queue1": 3,  # Under limit (5)
        "queue2": 10, # At limit (10)
        "queue3": 15, # Over limit (12)
        "queue4": 2,  # No active tasks recorded, should default to 0
    }
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {
        "queue1": 5,   # Limit set
        "queue2": 10,  # Limit set
        "queue3": 12,  # Limit set
        "queue4": 5,   # Limit set, no active tasks
        "queue5": 0,   # No limit (0 means unlimited)
    }
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"queue1", "queue2", "queue3", "queue4", "queue5"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    # Only queue1 (3 < 5), queue4 (2 < 5), and queue5 (unlimited) should be included
    assert result == {"queue1", "queue4", "queue5"}
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_no_active_tasks():
    """Test that filter_by_worker_queue_capacity includes queues with no active tasks."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}  # No active tasks
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {"queue1": 5, "queue2": 3}
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    # Both queues should be included since 0 active tasks < any positive limit
    assert result == {"queue1", "queue2"}
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_missing_queue_data():
    """Test that filter_by_worker_queue_capacity handles missing data gracefully."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 3}  # Only queue1 has active tasks
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {"queue1": 5}  # Only queue1 has limits
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"queue1", "queue2", "queue3"}  # queue2 and queue3 missing from both dicts
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    # queue1: 3 < 5 (included)
    # queue2: 0 active (default), 0 limit (default) -> no limit means unlimited (included)
    # queue3: 0 active (default), 0 limit (default) -> no limit means unlimited (included)
    assert result == {"queue1", "queue2", "queue3"}
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_regression_test():
    """Regression test for the bug where > was used instead of < in the comparison."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"busy_queue": 8, "available_queue": 2}
    queue_config_adapter = Mock(spec=QueueConfigAdapter)
    queue_config_adapter.get_queue_limits.return_value = {"busy_queue": 5, "available_queue": 5}
    task_generator = TaskGenerator(state_manager, queue_config_adapter)

    queues = {"busy_queue", "available_queue"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    # This is the key regression test:
    # - busy_queue has 8 active tasks with limit 5 -> should be EXCLUDED (8 >= 5)
    # - available_queue has 2 active tasks with limit 5 -> should be INCLUDED (2 < 5)
    # The bug was using > instead of <, which would have incorrectly included busy_queue
    assert result == {"available_queue"}
    queue_config_adapter.get_queue_limits.assert_called_once_with(queues)
