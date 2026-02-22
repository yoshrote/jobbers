from unittest.mock import patch

import pytest
from ulid import ULID

from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.validation import ValidationError, validate_task

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")


def test_validate_task_unregistered():
    """Unregistered task raises ValidationError without any Redis calls."""
    task = Task(id=ULID1, name="unknown_task", parameters={})
    with patch("jobbers.registry.get_task_config", return_value=None):
        with pytest.raises(ValidationError, match="Unknown task"):
            validate_task(task)


def test_validate_task_invalid_params():
    """Task with wrong parameter type raises ValidationError."""
    async def task_function(foo: int) -> None:
        pass

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", parameters={"foo": "bar"})
    with patch("jobbers.registry.get_task_config", return_value=task_config):
        with pytest.raises(ValidationError, match="Invalid parameters"):
            validate_task(task)


def test_validate_task_valid_sets_task_config():
    """Valid task passes validation and task_config is set on the task."""
    async def task_function(foo: int) -> None:
        pass

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", parameters={"foo": 42})

    with patch("jobbers.registry.get_task_config", return_value=task_config):
        validate_task(task)

    assert task.task_config is task_config
