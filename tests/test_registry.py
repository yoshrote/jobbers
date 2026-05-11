import pytest

from jobbers.models.dag import DAGNode
from jobbers.models.task_config import TaskConfig
from jobbers.registry import TaskWrapper, _task_function_map, get_task_config, register_task


@pytest.fixture(autouse=True)
def setup():
    """Fixture to reset the tasks in the mocked Redis before each test."""
    # Clear the internal task function map before each test to ensure isolation
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
