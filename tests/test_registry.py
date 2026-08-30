import datetime as dt
from typing import Annotated
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobbers.models.dag import DAGNode
from jobbers.models.task_config import TaskConfig, TaskExecutionMode
from jobbers.registry import TaskWrapper, _task_function_map, get_task_config, register_task
from jobbers.utils.di import Depends


@pytest.fixture(autouse=True)
def setup():
    """Reset the global task registry before and after each test for isolation."""
    _task_function_map.clear()
    yield
    _task_function_map.clear()


def test_register_task_success():
    """Test successful registration of a task function."""

    @register_task(name="test_task", version=1)
    def test_function():  # pragma: no cover
        pass

    assert isinstance(test_function, TaskWrapper)
    task_config = get_task_config("test_task", 1)
    assert task_config is not None
    assert task_config.name == "test_task"
    assert task_config.version == 1
    assert task_config.function == test_function._func


def test_register_task_re_registration():
    """Test re-registration of the same function for the same name and version."""

    @register_task(name="test_task", version=1)
    @register_task(name="test_task", version=1)
    def test_function():  # pragma: no cover
        pass

    task_config = get_task_config("test_task", 1)
    assert task_config is not None
    assert task_config.function == test_function._func


def test_register_task_returns_wrapper():
    """Decorator returns a TaskWrapper that is callable."""

    @register_task(name="test_task", version=1)
    async def test_function(**kwargs):  # pragma: no cover
        return kwargs

    assert isinstance(test_function, TaskWrapper)
    assert callable(test_function)


def test_task_wrapper_node():
    """TaskWrapper.node() returns a DAGNode with the correct task name and version."""

    @register_task(name="test_task", version=2)
    async def test_function(**kwargs):  # pragma: no cover
        return kwargs

    node = test_function.node(queue="myqueue", x=1)
    assert isinstance(node, DAGNode)
    assert node._name == "test_task"
    assert node._version == 2
    assert node._queue == "myqueue"
    assert node._parameters == {"x": 1}


def test_register_task_different_function_same_name_version():
    """Test registering a different function with the same name and version raises an exception."""

    @register_task(name="test_task", version=1)
    def test_function_1():  # pragma: no cover
        pass

    with pytest.raises(
        ValueError, match="Task test_task version 1 is already registered to another function"
    ):

        @register_task(name="test_task", version=1)
        def test_function_2():  # pragma: no cover
            pass


def test_register_task_non_callable():
    """Test registering a non-callable object raises a ValueError."""
    with pytest.raises(ValueError, match="Task function must be callable"):
        register_task(name="test_task", version=1)(None)


def test_get_task_config_found():
    """Test retrieving a registered task configuration."""

    @register_task(name="test_task", version=1)
    def test_function():  # pragma: no cover
        pass

    task_config = get_task_config("test_task", 1)
    assert task_config is not None
    assert isinstance(task_config, TaskConfig)
    assert task_config.name == "test_task"
    assert task_config.version == 1


def test_get_task_config_not_found():
    """Test retrieving a non-existent task configuration."""
    task_config = get_task_config("non_existent_task", 1)
    assert task_config is None


def test_register_task_sync_function_gets_sync_subworker_execution_mode():
    """A plain (non-async) function is auto-detected as execution_mode=sync_subworker."""

    @register_task(name="sync_task", version=1)
    def sync_function():  # pragma: no cover
        pass

    task_config = get_task_config("sync_task", 1)
    assert task_config is not None
    assert task_config.execution_mode == TaskExecutionMode.SYNC_SUBWORKER


def test_register_task_async_function_gets_async_execution_mode():
    """An async def function is auto-detected as execution_mode=async."""

    @register_task(name="async_task", version=1)
    async def async_function(**kwargs):  # pragma: no cover
        return kwargs

    task_config = get_task_config("async_task", 1)
    assert task_config is not None
    assert task_config.execution_mode == TaskExecutionMode.ASYNC


def test_register_task_sync_function_with_depends_raises():
    """A sync function that declares a Depends() parameter is rejected at registration time."""

    async def get_val() -> int:
        return 1

    with pytest.raises(ValueError, match="cannot declare Depends"):

        @register_task(name="sync_di_task", version=1)
        def sync_function(v: Annotated[int, Depends(get_val)]):  # pragma: no cover
            pass

    assert get_task_config("sync_di_task", 1) is None


def test_register_task_async_function_with_depends_still_allowed():
    """Depends() remains fine for async tasks; only sync_subworker tasks reject it."""

    async def get_val() -> int:
        return 1

    @register_task(name="async_di_task", version=1)
    async def async_function(v: Annotated[int, Depends(get_val)]):  # pragma: no cover
        return v

    task_config = get_task_config("async_di_task", 1)
    assert task_config is not None
    assert task_config.execution_mode == TaskExecutionMode.ASYNC
    assert len(task_config.dependency_graph) == 1


async def _test_function(**kwargs):  # pragma: no cover
    return kwargs


@pytest.mark.asyncio
async def test_task_wrapper_submit_creates_and_submits_task():
    """TaskWrapper.submit() builds a Task and hands it to StateManager.submit_task."""
    wrapper = TaskWrapper(_test_function, "test_task", 1)
    mock_sm = MagicMock()
    mock_sm.submit_task = AsyncMock()

    with patch("jobbers.registry.db.get_state_manager", return_value=mock_sm):
        task = await wrapper.submit(queue="myqueue", x=1)

    assert task.name == "test_task"
    assert task.version == 1
    assert task.queue == "myqueue"
    assert task.parameters == {"x": 1}
    mock_sm.submit_task.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_task_wrapper_submit_defaults_to_default_queue():
    """TaskWrapper.submit() without a queue argument targets the 'default' queue."""
    wrapper = TaskWrapper(_test_function, "test_task", 1)
    mock_sm = MagicMock()
    mock_sm.submit_task = AsyncMock()

    with patch("jobbers.registry.db.get_state_manager", return_value=mock_sm):
        task = await wrapper.submit(x=1)

    assert task.queue == "default"


@pytest.mark.asyncio
async def test_task_wrapper_schedule_creates_and_schedules_task():
    """TaskWrapper.schedule() builds a Task and hands it to StateManager.schedule_new_task."""
    wrapper = TaskWrapper(_test_function, "test_task", 1)
    run_at = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
    mock_sm = MagicMock()
    mock_sm.schedule_new_task = AsyncMock()

    with patch("jobbers.registry.db.get_state_manager", return_value=mock_sm):
        task = await wrapper.schedule(run_at, queue="myqueue", x=1)

    assert task.name == "test_task"
    assert task.version == 1
    assert task.queue == "myqueue"
    assert task.parameters == {"x": 1}
    mock_sm.schedule_new_task.assert_called_once_with(task, run_at)
