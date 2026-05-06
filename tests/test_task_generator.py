import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from ulid import ULID

from jobbers.models.task import Task, TaskStatus
from jobbers.state_manager import StateManager
from jobbers.task_generator import _CAPACITY_BACKOFF_SECS, TaskGenerator

EXHAUSTED = object()


def _silent_pubsub(task_generator: TaskGenerator) -> AsyncMock:
    """Pre-seed task_generator._refresh_pubsub with a no-message mock so queues() doesn't crash."""
    pubsub = AsyncMock()
    pubsub.get_message = AsyncMock(return_value=None)
    task_generator._refresh_pubsub = pubsub
    return pubsub


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
    state_manager.get_refresh_tag = AsyncMock(return_value=ULID())
    task_generator = TaskGenerator(state_manager, role="default")
    _silent_pubsub(task_generator)

    assert await anext(task_generator, EXHAUSTED) == task

    state_manager.get_next_task.assert_called_once_with({"default"})


@pytest.mark.asyncio
async def test_task_generator_sleeps_and_retries_on_capacity_filter():
    """When get_next_task returns None (all queues filtered), the generator sleeps and retries."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    state_manager.get_refresh_tag = AsyncMock(return_value=ULID())

    task = Task(
        id=ULID(),
        name="retry_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        submitted_at=datetime.now(tz=UTC),
    )
    state_manager.get_next_task = AsyncMock(side_effect=[None, task])

    task_generator = TaskGenerator(state_manager, role="default")
    _silent_pubsub(task_generator)

    with patch("jobbers.task_generator.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        result = await task_generator.__anext__()

    assert result is task
    mock_sleep.assert_awaited_once_with(_CAPACITY_BACKOFF_SECS)
    assert state_manager.get_next_task.await_count == 2


@pytest.mark.asyncio
async def test_task_generator_cancelled_during_backoff_sleep():
    """CancelledError raised during backoff sleep propagates without requeueing."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    state_manager.get_next_task = AsyncMock(return_value=None)
    state_manager.get_refresh_tag = AsyncMock(return_value=ULID())

    task_generator = TaskGenerator(state_manager, role="default")
    _silent_pubsub(task_generator)

    with patch(
        "jobbers.task_generator.asyncio.sleep", new_callable=AsyncMock, side_effect=asyncio.CancelledError
    ):
        with pytest.raises(asyncio.CancelledError):
            await task_generator.__anext__()


@pytest.mark.asyncio
async def test_task_generator_stops_after_max_tasks():
    """Test that TaskGenerator stops yielding after emitting max_tasks tasks."""
    state_manager = AsyncMock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    state_manager.get_refresh_tag = AsyncMock(return_value=ULID())
    task_generator = TaskGenerator(state_manager, max_tasks=2)
    _silent_pubsub(task_generator)

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
async def test_filter_by_worker_queue_capacity_unknown_queue():
    """Test that queues absent from the limits dict are treated as unlimited."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {"queue1": 99}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    task_generator = TaskGenerator(state_manager)

    queues = {"queue1"}
    result = await task_generator.filter_by_worker_queue_capacity(queues)

    assert result == {"queue1"}
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
    state_manager.get_refresh_tag = AsyncMock(return_value=ULID())
    task_generator = TaskGenerator(state_manager)
    _silent_pubsub(task_generator)

    state_manager.get_next_task.side_effect = asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await task_generator.__anext__()


# ── missing submitted_at ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_missing_submitted_at_raises_runtime_error():
    """A task returned by get_next_task with no submitted_at raises RuntimeError."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    state_manager.get_refresh_tag = AsyncMock(return_value=ULID())

    task = Task(id=ULID(), name="test_task", version=1)  # submitted_at=None by default
    state_manager.get_next_task.return_value = task

    task_generator = TaskGenerator(state_manager)
    _silent_pubsub(task_generator)

    with pytest.raises(RuntimeError, match="Pulled a task that was never submitted"):
        await task_generator.__anext__()


# ── queues(): version-based cache invalidation ────────────────────────────────


@pytest.mark.asyncio
async def test_queues_invalidates_routing_config_on_version_change():
    """When routing_version changes between polls, invalidate_all_routing_config is called."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    tag = ULID()
    state_manager.get_refresh_tag = AsyncMock(return_value=tag)
    old_version = ULID()
    new_version = ULID()
    state_manager.get_routing_version = AsyncMock(return_value=new_version)

    task_generator = TaskGenerator(state_manager, role="default")
    task_generator.routing_version = old_version  # stale version
    task_generator.refresh_tag = tag  # pre-warm to suppress queue reload
    _silent_pubsub(task_generator)

    await task_generator.queues()

    state_manager.invalidate_all_routing_config.assert_called_once()
    assert task_generator.routing_version == new_version


@pytest.mark.asyncio
async def test_queues_no_routing_invalidation_when_version_unchanged():
    """When routing_version is the same, invalidate_all_routing_config is NOT called."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    tag = ULID()
    state_manager.get_refresh_tag = AsyncMock(return_value=tag)
    version = ULID()
    state_manager.get_routing_version = AsyncMock(return_value=version)

    task_generator = TaskGenerator(state_manager, role="default")
    task_generator.routing_version = version  # already current
    task_generator.refresh_tag = tag  # pre-warm
    _silent_pubsub(task_generator)

    await task_generator.queues()

    state_manager.invalidate_all_routing_config.assert_not_called()


@pytest.mark.asyncio
async def test_queues_invalidates_queue_config_on_refresh_tag_change():
    """When refresh_tag changes, invalidate_queue_config is called for each queue in the role."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    version = ULID()
    state_manager.get_routing_version = AsyncMock(return_value=version)
    new_tag = ULID()
    state_manager.get_refresh_tag = AsyncMock(return_value=new_tag)

    task_generator = TaskGenerator(state_manager, role="default")
    task_generator.routing_version = version  # pre-warm
    task_generator.refresh_tag = ULID()  # old tag, different from new_tag
    _silent_pubsub(task_generator)

    await task_generator.queues()

    # Default role → queues = {"default"}
    state_manager.invalidate_queue_config.assert_called_once_with("default")
    assert task_generator.refresh_tag == new_tag


# ── pub/sub immediate refresh ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_queues_pubsub_message_forces_immediate_refresh():
    """A pub/sub message causes queues() to reset refresh_tag and re-read from state_manager."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    version = ULID()
    state_manager.get_routing_version = AsyncMock(return_value=version)
    new_tag = ULID()
    state_manager.get_refresh_tag = AsyncMock(return_value=new_tag)

    task_generator = TaskGenerator(state_manager, role="default")
    task_generator.routing_version = version
    task_generator.refresh_tag = new_tag  # pre-warm with same tag so it wouldn't refresh otherwise

    pubsub = AsyncMock()
    pubsub.get_message = AsyncMock(return_value={"type": "message", "data": str(new_tag).encode()})
    task_generator._refresh_pubsub = pubsub  # inject active pubsub with a pending message

    await task_generator.queues()

    # Refresh triggered by pub/sub message even though the tag hadn't changed
    state_manager.invalidate_queue_config.assert_called_once_with("default")
    assert task_generator.refresh_tag == new_tag


@pytest.mark.asyncio
async def test_queues_records_refresh_metrics():
    """queue_config_refreshes counter and refresh_lag_ms histogram are recorded on a tag change."""
    state_manager = Mock(spec=StateManager)
    state_manager.active_tasks_per_queue = {}
    state_manager.get_queue_limits = AsyncMock(return_value={})
    version = ULID()
    state_manager.get_routing_version = AsyncMock(return_value=version)
    new_tag = ULID()
    state_manager.get_refresh_tag = AsyncMock(return_value=new_tag)

    task_generator = TaskGenerator(state_manager, role="default")
    task_generator.routing_version = version
    task_generator.refresh_tag = ULID()  # old tag — triggers refresh
    _silent_pubsub(task_generator)

    from jobbers import task_generator as tg_module

    with (
        patch.object(tg_module.queue_config_refreshes, "add") as mock_counter,
        patch.object(tg_module.refresh_lag_ms, "record") as mock_histogram,
    ):
        await task_generator.queues()

    mock_counter.assert_called_once_with(1, {"role": "default"})
    mock_histogram.assert_called_once()
    lag_value = mock_histogram.call_args[0][0]
    assert lag_value >= 0
