import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from ulid import ULID

from jobbers.models.task import Task, TaskStatus
from jobbers.state_manager import StateManager
from jobbers.task_generator import TaskGenerator

EXHAUSTED = object()


@pytest.mark.asyncio
async def test_find_queues_default_role():
    """Test that TaskGenerator finds default queues for the 'default' role."""
    state_manager = Mock(spec=StateManager)
    task_generator = TaskGenerator(state_manager, role="default")

    queues = await task_generator.find_queues()

    assert queues == {"default"}


@pytest.mark.asyncio
async def test_find_queues_custom_role():
    """Test that TaskGenerator finds queues for a custom role."""
    state_manager = Mock(spec=StateManager)
    state_manager.get_queues = AsyncMock(return_value={"queue1", "queue2"})
    task_generator = TaskGenerator(state_manager, role="custom_role")

    queues = await task_generator.find_queues()

    assert queues == {"queue1", "queue2"}
    state_manager.get_queues.assert_called_once_with("custom_role")


@pytest.mark.asyncio
async def test_find_queues_custom_role_no_queues():
    """Test that TaskGenerator falls back to default queues if no queues are found for a custom role."""
    state_manager = Mock(spec=StateManager)
    state_manager.get_queues = AsyncMock(return_value=None)
    task_generator = TaskGenerator(state_manager, role="custom_role")

    queues = await task_generator.find_queues()

    assert queues == set()
    state_manager.get_queues.assert_called_once_with("custom_role")


@pytest.mark.asyncio
async def test_task_generator_iteration():
    """Test that TaskGenerator iterates over tasks."""
    state_manager = Mock(spec=StateManager)
    task = Task(
        id=ULID(), name="test_task", version=1, status=TaskStatus.SUBMITTED, submitted_at=datetime.now(tz=UTC)
    )
    state_manager.get_next_task.return_value = task
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    task_generator = TaskGenerator(state_manager, role="default")

    assert await anext(task_generator, EXHAUSTED) == task

    state_manager.get_next_task.assert_called_once_with({"default"})


@pytest.mark.asyncio
async def test_task_generator_stops_iteration():
    """Test that TaskGenerator stops iteration when no tasks are available."""
    state_manager = AsyncMock(spec=StateManager)
    state_manager.get_next_task.return_value = None
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    task_generator = TaskGenerator(state_manager, role="default")

    assert await anext(task_generator, EXHAUSTED) == EXHAUSTED

    state_manager.get_next_task.assert_called_once_with({"default"})


@pytest.mark.asyncio
async def test_task_generator_stops_after_max_tasks():
    """Test that TaskGenerator stops yielding after emitting max_tasks tasks."""
    state_manager = AsyncMock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    task_generator = TaskGenerator(state_manager, max_tasks=2)

    # Mock get_next_task to return a new Task
    state_manager.get_next_task = AsyncMock(
        side_effect=[
            Task(id=ULID(), name="task1", version=1, submitted_at=datetime.now(tz=UTC)),
            Task(id=ULID(), name="task2", version=1, submitted_at=datetime.now(tz=UTC)),
            Task(id=ULID(), name="task3", version=1, submitted_at=datetime.now(tz=UTC)),
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
    task_generator = TaskGenerator(state_manager)

    result = await task_generator.filter_by_worker_queue_capacity(set())

    assert result == set()


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_no_limits():
    """Test that filter_by_worker_queue_capacity returns all queues when no limits are set."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 5, "queue2": 10}
    state_manager.get_queue_limits = AsyncMock(return_value={"queue1": None, "queue2": None})
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1", "queue2"}
    state_manager.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_zero_limit():
    """Test that filter_by_worker_queue_capacity returns all queues when limit is 0 (no limit)."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 5, "queue2": 10}
    state_manager.get_queue_limits = AsyncMock(return_value={"queue1": 0, "queue2": 0})
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1", "queue2"}
    state_manager.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_under_limit():
    """Test that filter_by_worker_queue_capacity includes queues under their limits."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 3, "queue2": 7}
    state_manager.get_queue_limits = AsyncMock(return_value={"queue1": 5, "queue2": 10})
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1", "queue2"}
    state_manager.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_at_limit():
    """Test that filter_by_worker_queue_capacity excludes queues at their limits."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 5, "queue2": 10}
    state_manager.get_queue_limits = AsyncMock(return_value={"queue1": 5, "queue2": 10})
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == set()
    state_manager.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_over_limit():
    """Test that filter_by_worker_queue_capacity excludes queues over their limits."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 8, "queue2": 15}
    state_manager.get_queue_limits = AsyncMock(return_value={"queue1": 5, "queue2": 10})
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == set()
    state_manager.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_mixed_scenarios():
    """Test that filter_by_worker_queue_capacity handles mixed scenarios correctly."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {
        "queue1": 3,  # Under limit (5)
        "queue2": 10,  # At limit (10)
        "queue3": 15,  # Over limit (12)
        "queue4": 2,  # No active tasks recorded, should default to 0
    }
    state_manager.get_queue_limits = AsyncMock(
        return_value={
            "queue1": 5,
            "queue2": 10,
            "queue3": 12,
            "queue4": 5,
            "queue5": 0,
        }
    )
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1", "queue2", "queue3", "queue4", "queue5"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1", "queue4", "queue5"}
    state_manager.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_no_active_tasks():
    """Test that filter_by_worker_queue_capacity includes queues with no active tasks."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={"queue1": 5, "queue2": 3})
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1", "queue2"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1", "queue2"}
    state_manager.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_missing_queue_data():
    """Test that filter_by_worker_queue_capacity handles missing data gracefully."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 3}
    state_manager.get_queue_limits = AsyncMock(return_value={"queue1": 5})
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1", "queue2", "queue3"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1", "queue2", "queue3"}
    state_manager.get_queue_limits.assert_called_once_with(queues)


@pytest.mark.asyncio
async def test_filter_by_worker_queue_capacity_regression_test():
    """Regression test for the bug where > was used instead of < in the comparison."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"busy_queue": 8, "available_queue": 2}
    state_manager.get_queue_limits = AsyncMock(return_value={"busy_queue": 5, "available_queue": 5})
    task_generator = TaskGenerator(state_manager)

    queues = {"busy_queue", "available_queue"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"available_queue"}
    state_manager.get_queue_limits.assert_called_once_with(queues)


# ── stop() ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stop_prevents_further_iteration():
    """Calling stop() sets _run=False; the next __anext__ raises StopAsyncIteration."""
    state_manager = Mock(spec=StateManager)
    task_generator = TaskGenerator(state_manager)

    task_generator.stop()
    result = await anext(task_generator, EXHAUSTED)
    assert result is EXHAUSTED


# ── CancelledError handling ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancelled_error_propagates():
    """CancelledError from get_next_task is re-raised after the except block."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    task_generator = TaskGenerator(state_manager)

    state_manager.get_next_task.side_effect = asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await task_generator.__anext__()


# ── missing submitted_at ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_missing_submitted_at_raises_runtime_error():
    """A task returned by get_next_task with no submitted_at raises RuntimeError."""
    from ulid import ULID as _ULID

    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})

    task = Task(id=_ULID(), name="test_task", version=1)  # submitted_at=None by default
    state_manager.get_next_task.return_value = task

    task_generator = TaskGenerator(state_manager)

    with pytest.raises(RuntimeError, match="Pulled a task that was never submitted"):
        await task_generator.__anext__()
