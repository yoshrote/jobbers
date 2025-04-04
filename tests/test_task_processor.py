import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from jobbers.models.task import Task, TaskStatus
from jobbers.registry import TaskConfig
from jobbers.worker_proc import TaskProcessor


@pytest.mark.asyncio
async def test_task_processor_success():
    """Test that TaskProcessor successfully processes a task."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = AsyncMock()
    async def task_function(param1: str):
        return {"result": "success"}

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
    )

    with patch("jobbers.worker_proc.get_task_config", return_value=task_config):
        processor = TaskProcessor(task, state_manager)
        result_task = await processor.process()

    assert result_task.status == TaskStatus.COMPLETED
    assert result_task.results == {"result": "success"}
    state_manager.submit_task.assert_called_once_with(result_task)


@pytest.mark.asyncio
async def test_task_processor_dropped_task():
    """Test that TaskProcessor handles a dropped task."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="unknown_task",
        version=1,
        status=TaskStatus.UNSUBMITTED,
    )
    state_manager = AsyncMock()

    with patch("jobbers.worker_proc.get_task_config", return_value=None):
        processor = TaskProcessor(task, state_manager)
        result_task = await processor.process()

    assert result_task.status == TaskStatus.DROPPED
    assert result_task.completed_at is not None
    state_manager.submit_task.assert_called_once_with(result_task)


@pytest.mark.asyncio
async def test_task_processor_expected_exception():
    """Test that TaskProcessor handles an expected exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.UNSUBMITTED,
        retry_attempt=0,
    )
    state_manager = AsyncMock()
    task_function = AsyncMock(side_effect=ValueError("Expected error"))
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
        expected_exceptions=(ValueError,),  # Specify expected exceptions for retry logic
    )

    with patch("jobbers.worker_proc.get_task_config", return_value=task_config):
        processor = TaskProcessor(task, state_manager)
        result_task = await processor.process()

    assert result_task.status == TaskStatus.UNSUBMITTED
    assert result_task.retry_attempt == 1
    assert "Expected error" in result_task.error
    state_manager.submit_task.assert_called_once_with(result_task)


@pytest.mark.asyncio
async def test_task_processor_unexpected_exception():
    """Test that TaskProcessor handles an unexpected exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.UNSUBMITTED,
    )
    state_manager = AsyncMock()
    task_function = AsyncMock(side_effect=RuntimeError("Unexpected error"))
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
    )

    with patch("jobbers.worker_proc.get_task_config", return_value=task_config):
        processor = TaskProcessor(task, state_manager)
        result_task = await processor.process()

    assert result_task.status == TaskStatus.FAILED
    assert "Unexpected error" in result_task.error
    state_manager.submit_task.assert_called_once_with(result_task)


@pytest.mark.asyncio
async def test_task_processor_timeout_with_retry():
    """Test that TaskProcessor handles a timeout exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = AsyncMock()
    task_function = AsyncMock(side_effect=asyncio.TimeoutError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=3,
    )

    with patch("jobbers.worker_proc.get_task_config", return_value=task_config):
        processor = TaskProcessor(task, state_manager)
        result_task = await processor.process()

    assert result_task.status == TaskStatus.UNSUBMITTED
    assert "timed out" in result_task.error
    state_manager.submit_task.assert_called_once_with(result_task)

@pytest.mark.asyncio
async def test_task_processor_timeout_without_retry():
    """Test that TaskProcessor handles a timeout exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = AsyncMock()
    task_function = AsyncMock(side_effect=asyncio.TimeoutError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=0,  # No retries for this task
    )

    with patch("jobbers.worker_proc.get_task_config", return_value=task_config):
        processor = TaskProcessor(task, state_manager)
        result_task = await processor.process()

    assert result_task.status == TaskStatus.FAILED
    assert result_task.completed_at is not None, "Failed tasks should have a completed_at timestamp"
    assert "timed out" in result_task.error
    state_manager.submit_task.assert_called_once_with(result_task)

@pytest.mark.asyncio
async def test_task_processor_cancelled():
    """Test that TaskProcessor handles a timeout exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = AsyncMock()
    task_function = AsyncMock(side_effect=asyncio.CancelledError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=0,  # No retries for this task
    )

    with patch("jobbers.worker_proc.get_task_config", return_value=task_config):
        processor = TaskProcessor(task, state_manager)
        result_task = await processor.process()

    assert result_task.status == TaskStatus.CANCELLED
    assert result_task.completed_at is not None, "Cancelled tasks should have a completed_at timestamp"
    state_manager.submit_task.assert_called_once_with(result_task)
