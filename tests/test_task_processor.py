import asyncio
import datetime as dt
from unittest.mock import ANY, AsyncMock, call, patch

import pytest

from jobbers.models.task import Task, TaskStatus
from jobbers.models.task_config import BackoffStrategy
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.registry import TaskConfig, clear_registry, register_task
from jobbers.state_manager import StateManager
from jobbers.task_processor import TaskProcessor


@pytest.fixture(autouse=True)
def register_test_task():
    @register_task(name="test_task", version=1)
    def test_function(): # pragma: no cover
        pass

    yield

    clear_registry()

@pytest.mark.asyncio
async def test_task_processor_success():
    """Test that TaskProcessor successfully processes a task."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
        queue="test_queue",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value={"result": "success"})

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    assert result_task.status == TaskStatus.COMPLETED
    assert result_task.results == {"result": "success"}
    # save_task called once when starting; complete_task called when done
    state_manager.save_task.assert_called_once_with(task)
    state_manager.complete_task.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_task_processor_dropped_task():
    """Test that TaskProcessor handles a dropped task."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="unknown_task",
        version=1,
        status=TaskStatus.UNSUBMITTED,
    )
    state_manager = _make_state_manager()

    with patch("jobbers.task_processor.get_task_config", return_value=None):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    assert result_task.status == TaskStatus.DROPPED
    assert result_task.completed_at is not None
    state_manager.save_task.assert_called_once_with(task)



@pytest.mark.asyncio
async def test_task_processor_expected_exception_with_retry():
    """Test that TaskProcessor handles an expected exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.UNSUBMITTED,
        retry_attempt=0,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=ValueError("Expected error"))
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
        expected_exceptions=(ValueError,),  # Specify expected exceptions for retry logic
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    assert result_task.status == TaskStatus.SUBMITTED
    assert result_task.retry_attempt == 1
    assert any("Expected error" in e for e in result_task.errors)
    # save_task called when starting; queue_retry_task called for immediate retry (no retry_delay)
    state_manager.save_task.assert_called_once_with(task)
    state_manager.queue_retry_task.assert_called_once_with(task)

@pytest.mark.asyncio
async def test_task_processor_expected_exception_without_retry():
    """Test that TaskProcessor handles an expected exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.UNSUBMITTED,
        retry_attempt=0,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=ValueError("Expected error"))
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        expected_exceptions=(ValueError,),  # Specify expected exceptions for retry logic
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    assert result_task.status == TaskStatus.FAILED
    assert result_task.retry_attempt == 0
    assert any("Expected error" in e for e in result_task.errors)
    # save_task called when starting; fail_task called when failing
    state_manager.save_task.assert_called_once_with(task)
    state_manager.fail_task.assert_called_once_with(task)


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
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=RuntimeError("Unexpected error"))
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
        expected_exceptions=(ValueError,),  # Specify expected exceptions for retry logic
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    assert result_task.status == TaskStatus.FAILED
    assert any("Unexpected error" in e for e in result_task.errors)
    # save_task called when starting; fail_task called when failing
    state_manager.save_task.assert_called_once_with(task)
    state_manager.fail_task.assert_called_once_with(task)



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
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=asyncio.TimeoutError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=3,
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    assert result_task.status == TaskStatus.SUBMITTED
    assert any("timed out" in e for e in result_task.errors)
    # save_task called when starting; queue_retry_task called for immediate retry (no retry_delay)
    state_manager.save_task.assert_called_once_with(task)
    state_manager.queue_retry_task.assert_called_once_with(task)


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
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=asyncio.TimeoutError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=0,  # No retries for this task
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    assert result_task.status == TaskStatus.FAILED
    assert result_task.completed_at is not None, "Failed tasks should have a completed_at timestamp"
    assert any("timed out" in e for e in result_task.errors)
    # save_task called when starting; fail_task called when failing
    state_manager.save_task.assert_called_once_with(task)
    state_manager.fail_task.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_task_processor_stalled():
    """Test that TaskProcessor handles a timeout exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=asyncio.CancelledError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=0,  # No retries for this task
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        with pytest.raises(asyncio.CancelledError):
            await processor.process(task)

    # the task should have been updated via side effects
    assert task.status == TaskStatus.STALLED
    assert task.completed_at is not None, "Cancelled tasks should have a completed_at timestamp"
    # save_task called when starting and when handling cancellation
    state_manager.save_task.assert_has_calls([call(task), call(task)])


@pytest.mark.asyncio
async def test_task_processor_stalled_with_stop_policy():
    """Test that TaskProcessor handles CancelledError with TaskShutdownPolicy.STOP."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=asyncio.CancelledError())

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.STOP,
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        with pytest.raises(asyncio.CancelledError):
            await processor.process(task)

    # Task should be marked as stalled due to STOP policy
    assert task.status == TaskStatus.STALLED
    assert task.completed_at is not None
    # save_task called when starting and when handling cancellation
    state_manager.save_task.assert_has_calls([call(task), call(task)])


@pytest.mark.asyncio
async def test_task_processor_cancelled_with_resubmit_policy():
    """Test that TaskProcessor handles CancelledError with TaskShutdownPolicy.RESUBMIT."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=asyncio.CancelledError())

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.RESUBMIT,
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        with pytest.raises(asyncio.CancelledError):
            await processor.process(task)

    # Task should be marked for resubmission due to RESUBMIT policy
    assert task.status == TaskStatus.UNSUBMITTED
    assert task.completed_at is None  # Should not be completed when resubmitted
    # save_task called when starting and when handling cancellation
    state_manager.save_task.assert_has_calls([call(task), call(task)])


@pytest.mark.asyncio
async def test_task_processor_cancelled_with_continue_policy():
    """Test that TaskProcessor handles CancelledError with TaskShutdownPolicy.CONTINUE."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=asyncio.CancelledError())

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.CONTINUE,
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        with pytest.raises(asyncio.CancelledError):
            await processor.process(task)

    # With CONTINUE policy, shutdown() is a NOOP so task remains in STARTED state
    assert task.status == TaskStatus.STARTED  # Should remain as started (set before cancellation happened)
    assert task.completed_at is None  # Should not be completed for CONTINUE policy
    # save_task called when starting and when handling cancellation
    state_manager.save_task.assert_has_calls([call(task), call(task)])


@pytest.mark.asyncio
async def test_task_processor_cancelled_with_continue_policy_uses_shield():
    """Test that TaskProcessor uses asyncio.shield() when TaskShutdownPolicy.CONTINUE is set."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = _make_state_manager()

    # Mock the task function to succeed
    task_function = AsyncMock(return_value={"result": "success"})

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.CONTINUE,
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config), \
         patch("asyncio.shield", wraps=asyncio.shield) as mock_shield:

        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    # Verify that asyncio.shield was called when CONTINUE policy is used
    mock_shield.assert_called_once()

    # Task should complete successfully
    assert result_task.status == TaskStatus.COMPLETED
    assert result_task.results == {"result": "success"}

    # save_task called when starting; complete_task called when done
    state_manager.save_task.assert_called_once_with(task)
    state_manager.complete_task.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_task_processor_cancelled_with_stop_policy_no_shield():
    """Test that TaskProcessor does NOT use asyncio.shield() when TaskShutdownPolicy.STOP is set."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = _make_state_manager()

    # Mock the task function to succeed
    task_function = AsyncMock(return_value={"result": "success"})

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.STOP,
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config), \
         patch("asyncio.shield") as mock_shield:

        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    # Verify that asyncio.shield was NOT called when STOP policy is used
    mock_shield.assert_not_called()

    # Task should complete successfully
    assert result_task.status == TaskStatus.COMPLETED
    assert result_task.results == {"result": "success"}

    # save_task called when starting; complete_task called when done
    state_manager.save_task.assert_called_once_with(task)
    state_manager.complete_task.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_task_processor_cancelled_with_resubmit_policy_no_shield():
    """Test that TaskProcessor does NOT use asyncio.shield() when TaskShutdownPolicy.RESUBMIT is set."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={"param1": "value1"},
        status=TaskStatus.SUBMITTED,
    )
    state_manager = _make_state_manager()

    # Mock the task function to succeed
    task_function = AsyncMock(return_value={"result": "success"})

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.RESUBMIT,
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config), \
         patch("asyncio.shield") as mock_shield:

        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    # Verify that asyncio.shield was NOT called when RESUBMIT policy is used
    mock_shield.assert_not_called()

    # Task should complete successfully
    assert result_task.status == TaskStatus.COMPLETED
    assert result_task.results == {"result": "success"}

    # save_task called when starting; complete_task called when done
    state_manager.save_task.assert_called_once_with(task)
    state_manager.complete_task.assert_called_once_with(task)


# ── scheduled-retry tests (TaskScheduler present + retry_delay configured) ───

def _make_state_manager():
    """Return a mock StateManager whose retry_task mirrors the real SM behaviour."""
    state_manager = AsyncMock(spec=StateManager)
    state_manager.task_scheduler = AsyncMock(spec=TaskScheduler)

    async def _schedule_retry_task(task: Task, run_at: dt.datetime) -> Task:
        """Mimic StateManager.schedule_retry_task: add to scheduler with the provided run_at."""
        state_manager.task_scheduler.add(task, run_at)
        return task

    async def _queue_retry_task(task: Task) -> Task:
        """Mimic StateManager.queue_retry_task: set SUBMITTED and requeue."""
        task.set_status(TaskStatus.SUBMITTED)
        return task

    state_manager.schedule_retry_task.side_effect = _schedule_retry_task
    state_manager.queue_retry_task.side_effect = _queue_retry_task
    return state_manager


def _retryable_config(backoff_strategy=BackoffStrategy.CONSTANT, max_retries=3):
    async def task_function(**_):  # pragma: no cover
        pass

    return TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=max_retries,
        retry_delay=5,
        backoff_strategy=backoff_strategy,
        expected_exceptions=(ValueError,),
    )


@pytest.mark.asyncio
async def test_expected_exception_scheduled_with_backoff():
    """With a scheduler + retry_delay, a retryable exception → SCHEDULED, not UNSUBMITTED."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.UNSUBMITTED,
        retry_attempt=0,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=ValueError("boom"))
    task_config = _retryable_config()
    task_config = task_config.model_copy(update={"function": task_function})
    scheduler = state_manager.task_scheduler

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.SCHEDULED
    assert result.retry_attempt == 1
    state_manager.schedule_retry_task.assert_called_once_with(task, ANY)
    scheduler.add.assert_called_once()
    scheduled_task, run_at = scheduler.add.call_args.args
    assert scheduled_task.id == task.id


@pytest.mark.asyncio
async def test_timeout_scheduled_with_backoff():
    """With a scheduler + retry_delay, a retryable timeout → SCHEDULED, not UNSUBMITTED."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        retry_attempt=0,
    )
    state_manager = _make_state_manager()

    task_function = AsyncMock(side_effect=asyncio.TimeoutError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=3,
        retry_delay=5,
        backoff_strategy=BackoffStrategy.CONSTANT,
    )
    scheduler = state_manager.task_scheduler

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.SCHEDULED
    assert result.retry_attempt == 1
    state_manager.schedule_retry_task.assert_called_once_with(task, ANY)
    scheduler.add.assert_called_once()


@pytest.mark.asyncio
async def test_expected_exception_max_retries_fails_even_with_scheduler():
    """At max_retries, the task is FAILED regardless of whether a scheduler is present."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.UNSUBMITTED,
        retry_attempt=3,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=ValueError("boom"))
    task_config = _retryable_config(max_retries=3)
    task_config = task_config.model_copy(update={"function": task_function})
    scheduler = state_manager.task_scheduler

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.FAILED
    assert result.retry_attempt == 3
    state_manager.fail_task.assert_called_once_with(task)
    scheduler.add.assert_not_called()


@pytest.mark.asyncio
async def test_timeout_max_retries_fails_even_with_scheduler():
    """At max_retries, a timed-out task is FAILED regardless of scheduler."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        retry_attempt=3,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=asyncio.TimeoutError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=3,
        retry_delay=5,
        backoff_strategy=BackoffStrategy.CONSTANT,
    )
    scheduler = state_manager.task_scheduler

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.FAILED
    assert result.completed_at is not None
    state_manager.fail_task.assert_called_once_with(task)
    scheduler.add.assert_not_called()
