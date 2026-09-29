import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobbers.models.dag import DAGNode
from jobbers.models.task_config import TaskConfig
from jobbers.registry import (
    TaskWrapper,
    _router_function_map,
    _task_function_map,
    clear_registry,
    get_router_config,
    get_routers,
    get_task_config,
    get_tasks,
    register_router,
    register_task,
)


@pytest.fixture(autouse=True)
def setup():
    """Reset the global task/router registries before and after each test for isolation."""
    _task_function_map.clear()
    _router_function_map.clear()
    yield
    _task_function_map.clear()
    _router_function_map.clear()


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

    node = test_function.node(lane="myqueue", x=1)
    assert isinstance(node, DAGNode)
    assert node._name == "test_task"
    assert node._version == 2
    assert node._lane == "myqueue"
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


async def _test_function(**kwargs):  # pragma: no cover
    return kwargs


@pytest.mark.asyncio
async def test_task_wrapper_submit_creates_and_submits_task():
    """TaskWrapper.submit() builds a Task and hands it to StateManager.submit_task."""
    wrapper = TaskWrapper(_test_function, "test_task", 1)
    mock_sm = MagicMock()
    mock_sm.submit_task = AsyncMock()

    with patch("jobbers.registry.db.get_state_manager", return_value=mock_sm):
        task = await wrapper.submit(lane="myqueue", x=1)

    assert task.name == "test_task"
    assert task.version == 1
    assert task.lane == "myqueue"
    assert task.parameters == {"x": 1}
    mock_sm.submit_task.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_task_wrapper_submit_defaults_to_default_queue():
    """TaskWrapper.submit() without a lane argument targets the 'default' lane."""
    wrapper = TaskWrapper(_test_function, "test_task", 1)
    mock_sm = MagicMock()
    mock_sm.submit_task = AsyncMock()

    with patch("jobbers.registry.db.get_state_manager", return_value=mock_sm):
        task = await wrapper.submit(x=1)

    assert task.lane == "default"


@pytest.mark.asyncio
async def test_task_wrapper_schedule_creates_and_schedules_task():
    """TaskWrapper.schedule() builds a Task and hands it to StateManager.schedule_new_task."""
    wrapper = TaskWrapper(_test_function, "test_task", 1)
    run_at = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
    mock_sm = MagicMock()
    mock_sm.schedule_new_task = AsyncMock()

    with patch("jobbers.registry.db.get_state_manager", return_value=mock_sm):
        task = await wrapper.schedule(run_at, lane="myqueue", x=1)

    assert task.name == "test_task"
    assert task.version == 1
    assert task.lane == "myqueue"
    assert task.parameters == {"x": 1}
    mock_sm.schedule_new_task.assert_called_once_with(task, run_at)


# ── register_router ───────────────────────────────────────────────────────────


def test_register_router_stores_config():
    clear_registry()

    @register_router(name="route_by_tier", version=1)
    def router(results):  # pragma: no cover
        return "a"

    config = get_router_config("route_by_tier", 1)
    assert config is not None
    assert config.name == "route_by_tier"
    assert config.version == 1
    assert config.function is router
    assert ("route_by_tier", 1) in list(get_routers())


def test_register_router_returns_the_plain_function():
    """Unlike register_task, the decorator hands back the function unwrapped."""
    clear_registry()

    @register_router(name="r", version=1)
    def router(results):
        return results["target"]

    assert router({"target": "chosen"}) == "chosen"


def test_register_router_rejects_async_function():
    clear_registry()
    with pytest.raises(ValueError, match="must be a plain 'def'"):

        @register_router(name="async_router", version=1)
        async def router(results):  # pragma: no cover
            return "a"


def test_register_router_rejects_different_function_same_name_version():
    clear_registry()

    @register_router(name="r", version=1)
    def first(results):  # pragma: no cover
        return "a"

    with pytest.raises(ValueError, match="already registered to another function"):

        @register_router(name="r", version=1)
        def second(results):  # pragma: no cover
            return "b"


def test_register_router_allows_reregistering_same_function():
    clear_registry()

    def router(results):  # pragma: no cover
        return "a"

    register_router(name="r", version=1)(router)
    register_router(name="r", version=1)(router)
    assert get_router_config("r", 1) is not None


def test_get_router_config_missing_returns_none():
    clear_registry()
    assert get_router_config("nope", 1) is None


def test_routers_are_versioned_independently():
    clear_registry()

    @register_router(name="r", version=1)
    def v1(results):  # pragma: no cover
        return "a"

    @register_router(name="r", version=2)
    def v2(results):  # pragma: no cover
        return "b"

    assert get_router_config("r", 1).function is v1
    assert get_router_config("r", 2).function is v2


def test_clear_registry_clears_routers_too():
    clear_registry()

    @register_task(name="t", version=1)
    async def task():  # pragma: no cover
        return None

    @register_router(name="r", version=1)
    def router(results):  # pragma: no cover
        return "a"

    clear_registry()
    assert get_router_config("r", 1) is None
    assert list(get_routers()) == []
    assert list(get_tasks()) == []
