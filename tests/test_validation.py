from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.models.queue_config import QueueConfig, create_schema
from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.state_manager import StateManager
from jobbers.validation import ValidationError, validate_task

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")


@pytest_asyncio.fixture
async def sqlite_conn():
    """In-memory SQLite connection with schema applied."""
    async with aiosqlite.connect(":memory:") as conn:
        await conn.execute("PRAGMA foreign_keys = ON")
        await create_schema(conn)
        yield conn

@pytest_asyncio.fixture
async def state_manager(redis, sqlite_conn):
    """Fixture to provide a StateManager instance with a fake Redis and in-memory SQLite."""
    return StateManager(redis, sqlite_conn)

@pytest.mark.asyncio
async def test_validate_task_unregistered():
    """Unregistered task raises ValidationError without any Redis calls."""
    task = Task(id=ULID1, name="unknown_task", parameters={})
    with patch("jobbers.registry.get_task_config", return_value=None):
        with pytest.raises(ValidationError, match="Unknown task"):
            await validate_task(task)


@pytest.mark.asyncio
async def test_validate_task_invalid_params():
    """Task with wrong parameter type raises ValidationError."""
    async def task_function(foo: int) -> None:
        pass

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", parameters={"foo": "bar"})
    with patch("jobbers.registry.get_task_config", return_value=task_config):
        with pytest.raises(ValidationError, match="Invalid parameters"):
            await validate_task(task)


@pytest.mark.asyncio
async def test_validate_task_valid_sets_task_config():
    """Valid task passes validation and task_config is set on the task."""
    async def task_function(foo: int) -> None:
        pass

    task_config = TaskConfig(name="Test Task", function=task_function)
    queue_config = QueueConfig(name="default")
    task = Task(id=ULID1, name="Test Task", parameters={"foo": 42})

    with patch("jobbers.registry.get_task_config", return_value=task_config):
        with patch("jobbers.validation.db.get_sqlite_conn", return_value=MagicMock()):
            with patch("jobbers.validation.QueueConfigAdapter") as MockAdapter:
                MockAdapter.return_value.get_queue_config = AsyncMock(return_value=queue_config)
                await validate_task(task)

    assert task.task_config is task_config


@pytest.mark.asyncio
async def test_validate_task_missing_queue_config():
    """Task targeting an unconfigured queue raises ValidationError."""
    async def task_function(foo: int) -> None:
        pass

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", queue="unknown-queue", parameters={"foo": 42})

    with patch("jobbers.registry.get_task_config", return_value=task_config):
        with patch("jobbers.validation.db.get_sqlite_conn", return_value=MagicMock()):
            with patch("jobbers.validation.QueueConfigAdapter") as MockAdapter:
                MockAdapter.return_value.get_queue_config = AsyncMock(return_value=None)
                with pytest.raises(ValidationError, match="Unknown queue unknown-queue"):
                    await validate_task(task)
