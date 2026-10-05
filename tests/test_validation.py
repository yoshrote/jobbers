from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.validation import ValidationError, validate_task

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")


def _mock_sm(queue_config=None):
    sm = AsyncMock()
    sm.get_queue_config = AsyncMock(return_value=queue_config)
    return sm


@pytest.mark.asyncio
async def test_validate_task_unregistered():
    """Unregistered task raises ValidationError without any Redis calls."""
    task = Task(id=ULID1, name="unknown_task", parameters={})
    with patch("jobbers.registry.get_task_config", return_value=None):
        with pytest.raises(ValidationError, match="Unknown task"):
            await validate_task(task, _mock_sm())


@pytest.mark.asyncio
async def test_validate_task_invalid_params():
    """Task with wrong parameter type raises ValidationError."""

    async def task_function(foo: int) -> None: ...

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", parameters={"foo": "bar"})
    with patch("jobbers.registry.get_task_config", return_value=task_config):
        with pytest.raises(ValidationError, match="Invalid parameters"):
            await validate_task(task, _mock_sm())


@pytest.mark.asyncio
async def test_validate_task_valid_sets_task_config():
    """Valid task passes validation and task_config is set on the task."""

    async def task_function(foo: int) -> None: ...

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", parameters={"foo": 42})

    with patch("jobbers.registry.get_task_config", return_value=task_config):
        await validate_task(task, _mock_sm(queue_config=QueueConfig(name="default")))

    assert task.task_config is task_config


# ── queue validity ────────────────────────────────────────────────────────────


def _task_config():
    async def task_function(foo: int) -> None: ...

    return TaskConfig(name="Test Task", function=task_function)


@pytest.mark.asyncio
async def test_validate_task_unknown_queue():
    """A task naming a queue that does not exist raises ValidationError."""
    task = Task(id=ULID1, name="Test Task", queue="unknown_queue", parameters={"foo": 42})
    with patch("jobbers.registry.get_task_config", return_value=_task_config()):
        with pytest.raises(ValidationError, match="Unknown queue unknown_queue"):
            await validate_task(task, _mock_sm(queue_config=None))


@pytest.mark.asyncio
async def test_validate_task_queue_valid_when_it_exists():
    """The queue named on the task is the one checked -- there is no resolution step."""
    task = Task(id=ULID1, name="Test Task", queue="heavy", parameters={"foo": 42})
    sm = _mock_sm(queue_config=QueueConfig(name="heavy"))
    with patch("jobbers.registry.get_task_config", return_value=_task_config()):
        await validate_task(task, sm)
    sm.get_queue_config.assert_awaited_once_with("heavy")


@pytest.mark.asyncio
async def test_validate_task_polls_for_config_changes():
    """
    validate_task refreshes stale config before checking the queue.

    get_queue_config caches per process, negative results included, so without the poll
    this process can keep rejecting a queue another Manager replica has already created.
    """

    async def task_function(foo: int) -> None: ...

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", queue="default", parameters={"foo": 42})
    sm = _mock_sm(queue_config=QueueConfig(name="default"))

    with patch("jobbers.registry.get_task_config", return_value=task_config):
        await validate_task(task, sm)

    sm.refresh_config_if_stale.assert_awaited_once()
