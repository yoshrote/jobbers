import asyncio
import contextlib
import datetime as dt
from unittest.mock import ANY, AsyncMock, call, patch

import fakeredis.aioredis as fakeredis
import pytest
from ulid import ULID

from jobbers.models.dag import DAGNode, DAGTaskSpec, DynamicFanOut, SimpleCallback, TaskResult
from jobbers.models.task import Task, TaskStatus
from jobbers.models.task_config import BackoffStrategy
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.registry import TaskConfig, clear_registry, register_task
from jobbers.schedulers.task_scheduler import TaskScheduler
from jobbers.state_manager import StateManager, UserCancellationError
from jobbers.task_processor import TaskProcessor


@pytest.fixture(autouse=True)
def register_test_task():
    @register_task(name="test_task", version=1)
    def test_function():  # pragma: no cover
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
    task_function = AsyncMock(return_value=TaskResult(results={"result": "success"}))

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
    state_manager.save_task.assert_has_calls([call(task), call(task)])


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
    task_function = AsyncMock(return_value=None)

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.CONTINUE,
    )

    with (
        patch("jobbers.task_processor.get_task_config", return_value=task_config),
        patch("asyncio.shield", wraps=asyncio.shield) as mock_shield,
    ):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    # Verify that asyncio.shield was called when CONTINUE policy is used
    mock_shield.assert_called_once()

    # Task should complete successfully
    assert result_task.status == TaskStatus.COMPLETED

    # save_task called when starting; complete_task called when done
    state_manager.save_task.assert_has_calls([call(task), call(task)])


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
    task_function = AsyncMock(return_value=None)

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.STOP,
    )

    with (
        patch("jobbers.task_processor.get_task_config", return_value=task_config),
        patch("asyncio.shield") as mock_shield,
    ):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    # Verify that asyncio.shield was NOT called when STOP policy is used
    mock_shield.assert_not_called()

    # Task should complete successfully
    assert result_task.status == TaskStatus.COMPLETED

    # save_task called when starting; complete_task called when done
    state_manager.save_task.assert_has_calls([call(task), call(task)])


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
    task_function = AsyncMock(return_value=None)

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=0,
        on_shutdown=TaskShutdownPolicy.RESUBMIT,
    )

    with (
        patch("jobbers.task_processor.get_task_config", return_value=task_config),
        patch("asyncio.shield") as mock_shield,
    ):
        processor = TaskProcessor(state_manager)
        result_task = await processor.process(task)

    # Verify that asyncio.shield was NOT called when RESUBMIT policy is used
    mock_shield.assert_not_called()

    # Task should complete successfully
    assert result_task.status == TaskStatus.COMPLETED

    # save_task called when starting; complete_task called when done
    state_manager.save_task.assert_has_calls([call(task), call(task)])


# ── scheduled-retry tests (TaskScheduler present + retry_delay configured) ───


def _make_state_manager():
    """Return a mock StateManager whose retry_task mirrors the real SM behaviour."""
    state_manager = AsyncMock(spec=StateManager)
    state_manager.task_scheduler = AsyncMock(spec=TaskScheduler)
    state_manager.job_store = fakeredis.FakeRedis()
    state_manager.ta = AsyncMock()

    async def _schedule_retry_task(task: Task, run_at: dt.datetime) -> Task:
        return task

    async def _queue_retry_task(task: Task) -> Task:
        """Mimic StateManager.queue_retry_task: set SUBMITTED and requeue."""
        task.set_status(TaskStatus.SUBMITTED)
        return task

    state_manager.schedule_retry_task.side_effect = _schedule_retry_task
    state_manager.queue_retry_task.side_effect = _queue_retry_task
    return state_manager


def _retryable_config(backoff_strategy=BackoffStrategy.CONSTANT, max_retries=3):
    """Build a TaskConfig with retry_delay=5 for scheduled-retry tests."""

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
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.SCHEDULED
    assert result.retry_attempt == 1
    state_manager.schedule_retry_task.assert_called_once_with(task, ANY)


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
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.SCHEDULED
    assert result.retry_attempt == 1
    state_manager.schedule_retry_task.assert_called_once_with(task, ANY)


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
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.FAILED
    assert result.retry_attempt == 3
    state_manager.fail_task.assert_called_once_with(task)
    state_manager.schedule_retry_task.assert_not_called()


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
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.FAILED
    assert result.completed_at is not None
    state_manager.fail_task.assert_called_once_with(task)
    state_manager.schedule_retry_task.assert_not_called()


# ── TaskGroup cleanup (monitor cancelled when process finishes) ───────────────


@pytest.mark.asyncio
async def test_run_cancels_monitor_when_process_succeeds():
    """When process() returns normally, the monitor task is cancelled and awaited."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="test_queue",
    )
    state_manager = _make_state_manager()
    monitor_cancelled = asyncio.Event()

    async def slow_monitor(_task_id: str) -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            monitor_cancelled.set()
            raise

    state_manager.monitor_task_cancellation.side_effect = slow_monitor

    task_function = AsyncMock(return_value=None)
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.run(task)

    assert monitor_cancelled.is_set(), "Monitor should be cancelled when process completes successfully"
    assert task.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_run_cancels_monitor_when_process_fails():
    """When the task function raises (process() returns with FAILED status), the monitor is cancelled."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="test_queue",
    )
    state_manager = _make_state_manager()
    monitor_cancelled = asyncio.Event()

    async def slow_monitor(_task_id: str) -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            monitor_cancelled.set()
            raise

    state_manager.monitor_task_cancellation.side_effect = slow_monitor

    task_function = AsyncMock(side_effect=RuntimeError("boom"))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10, max_retries=0)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.run(task)

    assert monitor_cancelled.is_set(), "Monitor should be cancelled when process completes with a failure"
    assert task.status == TaskStatus.FAILED


# ── event-based cancellation ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_task_processor_run_exits_early_on_cancel_signal():
    """
    TaskProcessor.run exits cleanly when a cancel signal is delivered to the monitor.

    Sequence:
    1. monitor raises UserCancellationError → handle_user_cancelled_task sets CANCELLED.
    2. TaskGroup cancels process → CancelledError caught, but status is already CANCELLED so
       handle_system_cancelled_task is skipped; process() returns cleanly.
    3. TaskGroup raises ExceptionGroup([UserCancellationError]) → run() catches and suppresses it.
    run() returns normally (no exception); the task status is CANCELLED.
    """
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={},
        status=TaskStatus.SUBMITTED,
        queue="test_queue",
    )

    task_started = asyncio.Event()

    async def slow_task():
        task_started.set()
        await asyncio.sleep(30)  # long-running; will be cancelled before this finishes
        return {}  # pragma: no cover

    task_config = TaskConfig(name="test_task", version=1, function=slow_task, timeout=60)
    state_manager = _make_state_manager()

    async def event_monitor(_task_id: ULID) -> None:
        await task_started.wait()
        raise UserCancellationError(str(_task_id))

    state_manager.monitor_task_cancellation.side_effect = event_monitor

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        # run() catches the ExceptionGroup([UserCancellationError]) raised by the
        # TaskGroup and exits cleanly — no exception should propagate here.
        await processor.run(task)

    assert task.status == TaskStatus.CANCELLED
    # State was saved at least twice: once when started, once when interrupted.
    assert state_manager.save_task.call_count >= 2


@pytest.mark.asyncio
async def test_process_does_not_overwrite_cancelled_status_on_system_cancel():
    """
    process() skips handle_system_cancelled_task when status is already CANCELLED.

    This prevents the TaskGroup-injected CancelledError (a side-effect of user cancellation)
    from overwriting CANCELLED with STALLED.
    """
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={},
        status=TaskStatus.SUBMITTED,
        queue="test_queue",
    )
    state_manager = _make_state_manager()

    async def cancel_immediately() -> dict[str, object]:
        # Simulate handle_user_cancelled_task having set status before CancelledError propagates
        task.set_status(TaskStatus.CANCELLED)
        raise asyncio.CancelledError()

    task_config = TaskConfig(name="test_task", version=1, function=cancel_immediately, timeout=60)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        with patch.object(processor, "handle_system_cancelled_task", new_callable=AsyncMock) as mock_sys:
            await processor.process(task)

    mock_sys.assert_not_called()
    assert task.status == TaskStatus.CANCELLED


# ── _handle_dynamic_fanout ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_submits_children_and_presaves_collector():
    """Normal fan-out: children are submitted atomically via pipeline and collector is pre-saved."""
    dag_run_id = ULID()
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
    )
    c1 = DAGNode("worker_a")
    c2 = DAGNode("worker_b")
    collector = DAGNode("aggregator")
    fanout = DynamicFanOut(children=[c1, c2], collector=collector)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()

    # Set up get_queue_config to report non-rate-limited queues
    state_manager.get_queue_config = AsyncMock(return_value=None)

    processor = TaskProcessor(state_manager)
    await processor._handle_dynamic_fanout(parent, fanout)

    # init_fan_in called once with correct key and both child IDs
    fan_in_key = f"dag:fan-in:{collector.id}"
    state_manager.init_fan_in.assert_awaited_once_with(fan_in_key, {c1.id, c2.id}, ttl=fanout.fan_in_ttl)
    # collector pre-saved once with parent_ids == all child IDs
    state_manager.save_task.assert_awaited_once()
    saved_task = state_manager.save_task.call_args[0][0]
    assert saved_task.id == collector.id
    assert set(saved_task.parent_ids) == {c1.id, c2.id}

    # children submitted via submit_tasks_batch, not submit_task
    state_manager.submit_task.assert_not_awaited()
    state_manager.submit_tasks_batch.assert_awaited_once()
    child_tasks = state_manager.submit_tasks_batch.call_args[0][0]
    assert {ct.id for ct in child_tasks} == {c1.id, c2.id}
    for ct in child_tasks:
        assert ct.parent_ids == [parent.id]
        assert ct.dag_run_id == dag_run_id
    assert saved_task.dag_run_id == dag_run_id


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_no_children_submits_collector_immediately():
    """Degenerate fan-out with no children submits the collector directly."""
    dag_run_id = ULID()
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
    )
    collector = DAGNode("aggregator")
    fanout = DynamicFanOut(children=[], collector=collector)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()

    processor = TaskProcessor(state_manager)
    await processor._handle_dynamic_fanout(parent, fanout)

    state_manager.init_fan_in.assert_not_called()
    state_manager.submit_task.assert_awaited_once()
    submitted = state_manager.submit_task.call_args[0][0]
    assert submitted.id == collector.id
    assert submitted.dag_run_id == dag_run_id


@pytest.mark.asyncio
async def test_task_processor_stores_task_result_on_success():
    """TaskProcessor stores results from a TaskResult return value."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=TaskResult(results={"answer": 42}))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.results == {"answer": 42}
    assert result.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_monitor_task_cancellation_calls_handle_user_cancelled_task():
    # StateManager raises UserCancellationError directly — not inside a TaskGroup,
    # so it never arrives wrapped in an ExceptionGroup. The except clause in
    # TaskProcessor.monitor_task_cancellation must match UserCancellationError,
    # not ExceptionGroup, otherwise handle_user_cancelled_task is never called.
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={},
        status=TaskStatus.SUBMITTED,
        queue="test_queue",
    )
    state_manager = _make_state_manager()
    state_manager.monitor_task_cancellation.side_effect = UserCancellationError("cancelled")

    processor = TaskProcessor(state_manager)
    with patch.object(processor, "handle_user_cancelled_task", new_callable=AsyncMock) as mock_handle:
        with pytest.raises((UserCancellationError, ExceptionGroup)):
            await processor.monitor_task_cancellation(task)

    mock_handle.assert_called_once_with(task)


# ── post_process with dag_callbacks ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_post_process_triggers_dag_callbacks():
    """post_process submits callbacks produced by generate_callbacks when has_callbacks() is True."""
    # Build a parent task with a SimpleCallback so has_callbacks() → True
    child_spec = DAGTaskSpec(name="child_task", queue="default")
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_callbacks=[SimpleCallback(task=child_spec)],
    )

    state_manager = _make_state_manager()

    # generate_callbacks now receives state_manager.ta directly
    mock_ta = AsyncMock()
    mock_ta.fan_in_complete.return_value = 0
    mock_ta.get_fan_in_members.return_value = []
    state_manager.ta = mock_ta

    processor = TaskProcessor(state_manager)
    await processor.post_process(parent)

    # Exactly one child task should have been submitted (one SimpleCallback)
    state_manager.submit_tasks_batch.assert_awaited_once()
    submitted = state_manager.submit_tasks_batch.call_args[0][0][0]
    assert submitted.name == child_spec.name


@pytest.mark.asyncio
async def test_post_process_with_dynamic_fanout_calls_handle_dynamic_fanout():
    """post_process delegates to _handle_dynamic_fanout when a DynamicFanOut is passed."""
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
    )
    fanout = DynamicFanOut(children=[DAGNode("child")], collector=DAGNode("collect"))

    state_manager = _make_state_manager()
    processor = TaskProcessor(state_manager)

    with patch.object(processor, "_handle_dynamic_fanout", new_callable=AsyncMock) as mock_fanout:
        await processor.post_process(parent, dynamic_fanout=fanout)

    mock_fanout.assert_awaited_once_with(parent, fanout)


@pytest.mark.asyncio
async def test_task_result_parent_ids_copied_to_task():
    """When a TaskResult carries parent_ids, processor copies them onto the task."""
    parent_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=TaskResult(results={}, parent_ids=[parent_id]))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.parent_ids == [parent_id]


@pytest.mark.asyncio
async def test_end_to_end_latency_recorded_when_submitted_at_set():
    """end_to_end_latency metric is recorded when task has submitted_at and completed_at."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        submitted_at=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=TaskResult(results={}))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert result.submitted_at is not None
    assert result.completed_at is not None


# ── run() re-raises non-UserCancellationError ─────────────────────────────────


@pytest.mark.asyncio
async def test_run_reraises_non_user_cancellation_error():
    """run() re-raises exceptions from process() that are not UserCancellationError."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
    )
    state_manager = _make_state_manager()

    processor = TaskProcessor(state_manager)

    boom = RuntimeError("unexpected failure")

    async def failing_process(_task):
        raise boom

    with patch.object(processor, "process", side_effect=failing_process):
        with pytest.raises((RuntimeError, ExceptionGroup)):
            await processor.run(task)


# ── post_process_error ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_post_process_error_submits_error_callbacks():
    """post_process_error submits tasks returned by generate_error_callbacks."""
    error_spec = DAGTaskSpec(name="error_handler")
    child_spec = DAGTaskSpec(name="child")
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        dag_callbacks=[SimpleCallback(task=child_spec, error_callback=error_spec)],
    )
    state_manager = _make_state_manager()

    processor = TaskProcessor(state_manager)
    await processor.post_process_error(task)

    state_manager.submit_tasks_batch.assert_awaited_once()
    submitted = state_manager.submit_tasks_batch.call_args[0][0][0]
    assert submitted.id == error_spec.id
    assert submitted.parent_ids == [task.id]


@pytest.mark.asyncio
async def test_post_process_error_no_error_callbacks_does_nothing():
    """post_process_error does not call submit_task when no error callbacks are set."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        dag_callbacks=[SimpleCallback(task=DAGTaskSpec(name="child"))],
    )
    state_manager = _make_state_manager()

    processor = TaskProcessor(state_manager)
    await processor.post_process_error(task)

    state_manager.submit_tasks_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_task_triggers_error_callback():
    """When a task fails with an unexpected exception, error callbacks are submitted."""
    error_spec = DAGTaskSpec(name="error_handler")
    child_spec = DAGTaskSpec(name="child")
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        dag_callbacks=[SimpleCallback(task=child_spec, error_callback=error_spec)],
    )
    state_manager = _make_state_manager()

    task_function = AsyncMock(side_effect=RuntimeError("boom"))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10, max_retries=0)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.FAILED
    state_manager.submit_tasks_batch.assert_awaited_once()
    submitted = state_manager.submit_tasks_batch.call_args[0][0][0]
    assert submitted.id == error_spec.id


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger_status", ["stalled", "cancelled", "dropped"])
async def test_non_failed_terminal_statuses_do_not_trigger_error_callback(trigger_status):
    """
    STALLED, CANCELLED, and DROPPED tasks must not fire error callbacks.

    Only FAILED represents a task that ran and produced an actionable error.
    The other terminal statuses are control-flow outcomes (user stopped it,
    system stopped it, or it was never registered) where firing an error
    callback would be surprising and is explicitly not supported.
    """
    error_spec = DAGTaskSpec(name="error_handler")
    child_spec = DAGTaskSpec(name="child")
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        dag_callbacks=[SimpleCallback(task=child_spec, error_callback=error_spec)],
    )
    state_manager = _make_state_manager()

    if trigger_status == "stalled":
        # CancelledError from the system with STOP policy → STALLED
        task_function = AsyncMock(side_effect=asyncio.CancelledError)
        task_config = TaskConfig(
            name="test_task",
            version=1,
            function=task_function,
            timeout=10,
            on_shutdown=TaskShutdownPolicy.STOP,
        )
        with patch("jobbers.task_processor.get_task_config", return_value=task_config):
            processor = TaskProcessor(state_manager)
            with contextlib.suppress(asyncio.CancelledError):
                await processor.process(task)
        assert task.status == TaskStatus.STALLED
    elif trigger_status == "cancelled":
        # Simulate user cancellation: the cancellation monitor sets status to
        # CANCELLED before CancelledError reaches the process() handler.
        async def user_cancelled_fn(**_):
            task.set_status(TaskStatus.CANCELLED)
            raise asyncio.CancelledError

        task_config = TaskConfig(name="test_task", version=1, function=user_cancelled_fn, timeout=10)
        with patch("jobbers.task_processor.get_task_config", return_value=task_config):
            processor = TaskProcessor(state_manager)
            with contextlib.suppress(asyncio.CancelledError):
                await processor.process(task)
        assert task.status == TaskStatus.CANCELLED
    else:
        # Unknown task type → DROPPED
        with patch("jobbers.task_processor.get_task_config", return_value=None):
            processor = TaskProcessor(state_manager)
            await processor.process(task)
        assert task.status == TaskStatus.DROPPED

    state_manager.submit_tasks_batch.assert_not_awaited()


# ── handle_success cron_id branch ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_handle_success_without_cron_id_calls_save_task():
    """handle_success saves the task directly when cron_id is None."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.STARTED,
        queue="default",
        cron_id=None,
    )
    state_manager = _make_state_manager()
    processor = TaskProcessor(state_manager)
    await processor.handle_success(task)

    state_manager.save_task.assert_awaited_once_with(task)
    assert task.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_handle_success_with_cron_id_uses_complete_cron_task():
    """handle_success delegates to complete_cron_task (not save_task) when cron_id is set."""
    cron_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.STARTED,
        queue="default",
        cron_id=cron_id,
    )
    state_manager = _make_state_manager()

    processor = TaskProcessor(state_manager)
    await processor.handle_success(task)

    state_manager.save_task.assert_not_awaited()
    state_manager.complete_cron_task.assert_awaited_once_with(task)
    assert task.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_attempt", "backoff", "base", "expected_delay_seconds"),
    [
        # EXPONENTIAL: delay = base * 2^attempt (pre-increment)
        # Bug produced base * 2^(attempt+1), doubling the intended delay.
        (0, BackoffStrategy.EXPONENTIAL, 60, 60.0),  # bug gave 120s
        (1, BackoffStrategy.EXPONENTIAL, 60, 120.0),  # bug gave 240s
        # LINEAR: delay = base * attempt (pre-increment)
        # Bug produced base * (attempt+1), adding one extra step.
        (1, BackoffStrategy.LINEAR, 30, 30.0),  # bug gave 60s
        (2, BackoffStrategy.LINEAR, 30, 60.0),  # bug gave 90s
    ],
)
async def test_scheduled_retry_delay_uses_pre_increment_attempt(
    retry_attempt: int,
    backoff: BackoffStrategy,
    base: int,
    expected_delay_seconds: float,
) -> None:
    """
    schedule_retry_task must receive a run_at derived from compute_retry_at(retry_attempt).

    Regression: set_status(SCHEDULED) used to run before compute_retry_at, so the
    first retry would use attempt=1 instead of attempt=0, making every delay one
    step higher on the backoff curve than configured.
    """
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        retry_attempt=retry_attempt,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=ValueError("boom"))
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=retry_attempt + 2,  # always has retries remaining
        retry_delay=base,
        backoff_strategy=backoff,
        expected_exceptions=(ValueError,),
    )

    before = dt.datetime.now(dt.UTC)
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        await TaskProcessor(state_manager).process(task)

    state_manager.schedule_retry_task.assert_called_once()
    _, run_at = state_manager.schedule_retry_task.call_args[0]
    actual_delay = (run_at - before).total_seconds()
    assert abs(actual_delay - expected_delay_seconds) < 1.0, (
        f"Expected delay ≈ {expected_delay_seconds}s, got {actual_delay:.1f}s. "
        "compute_retry_at may have been called with the post-increment retry_attempt."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_attempt", "backoff", "base", "expected_delay_seconds"),
    [
        (0, BackoffStrategy.EXPONENTIAL, 60, 60.0),
        (1, BackoffStrategy.EXPONENTIAL, 60, 120.0),
    ],
)
async def test_timeout_retry_delay_uses_pre_increment_attempt(
    retry_attempt: int,
    backoff: BackoffStrategy,
    base: int,
    expected_delay_seconds: float,
) -> None:
    """Same regression check for the timeout path in handle_timeout_exception."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        retry_attempt=retry_attempt,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=asyncio.TimeoutError)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=1,
        max_retries=retry_attempt + 2,
        retry_delay=base,
        backoff_strategy=backoff,
    )

    before = dt.datetime.now(dt.UTC)
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        await TaskProcessor(state_manager).process(task)

    state_manager.schedule_retry_task.assert_called_once()
    _, run_at = state_manager.schedule_retry_task.call_args[0]
    actual_delay = (run_at - before).total_seconds()
    assert abs(actual_delay - expected_delay_seconds) < 1.0, (
        f"Expected delay ≈ {expected_delay_seconds}s, got {actual_delay:.1f}s. "
        "compute_retry_at may have been called with the post-increment retry_attempt."
    )


@pytest.mark.asyncio
async def test_retried_task_does_not_trigger_error_callback():
    """A task being retried (SCHEDULED) does not fire error callbacks."""
    error_spec = DAGTaskSpec(name="error_handler")
    child_spec = DAGTaskSpec(name="child")
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        dag_callbacks=[SimpleCallback(task=child_spec, error_callback=error_spec)],
    )
    state_manager = _make_state_manager()
    state_manager.submit_task = AsyncMock()

    task_config = _retryable_config(max_retries=3)
    task_config.function = AsyncMock(
        side_effect=task_config.expected_exceptions[0]("retrying")
        if task_config.expected_exceptions
        else ValueError("retrying")
    )

    # Use an expected exception so retry logic kicks in
    async def failing_fn(**_):
        raise ValueError("retry me")

    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=failing_fn,
        timeout=10,
        max_retries=3,
        retry_delay=5,
        expected_exceptions=(ValueError,),
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.SCHEDULED
    state_manager.submit_task.assert_not_awaited()


# ── inject_parent_results ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_inject_parent_results_passes_results_as_kwarg():
    """When inject_parent_results=True and parent_ids is set, the task function receives parent_results."""
    parent_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        parent_ids=[parent_id],
        inject_parent_results=True,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=TaskResult(results={}))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        with patch.object(task.__class__, "parent_results", new_callable=AsyncMock, return_value={"val": 99}):
            processor = TaskProcessor(state_manager)
            result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    task_function.assert_awaited_once()
    _, call_kwargs = task_function.call_args
    assert call_kwargs.get("parent_results") == {"val": 99}


@pytest.mark.asyncio
async def test_no_injection_when_flag_is_false():
    """When inject_parent_results=False, the task function is called with only task.parameters."""
    parent_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        parameters={"x": 1},
        parent_ids=[parent_id],
        inject_parent_results=False,
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=TaskResult(results={}))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    task_function.assert_awaited_once_with(x=1)
