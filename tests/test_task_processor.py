import asyncio
import contextlib
import datetime as dt
import logging
from typing import Annotated
from unittest.mock import ANY, AsyncMock, call, patch

import pytest
from ulid import ULID

from jobbers.adapters.redis import RedisTaskScheduler
from jobbers.models.dag import (
    DAGNode,
    DAGTaskSpec,
    DynamicFanOut,
    DynamicFanOutCallback,
    FanInCallback,
    FromParent,
    SimpleCallback,
    TaskResult,
)
from jobbers.models.task import Task, TaskStatus
from jobbers.models.task_config import BackoffStrategy
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.registry import TaskConfig, clear_registry, register_task
from jobbers.state_manager import StateManager, TaskRateLimitedError, UserCancellationError
from jobbers.task_processor import TaskProcessor, _spec_to_dag_node


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
async def test_execution_time_metric_excludes_retry_backoff_wait():
    """
    Uses retried_at (start of the final attempt), not started_at (start of the very first attempt).

    Regression test: a retried task's recorded execution_time previously spanned every retry's
    backoff wait (completed_at - started_at), not just the attempt that actually finished.
    """
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={},
        status=TaskStatus.UNSUBMITTED,
        retry_attempt=0,
    )
    state_manager = _make_state_manager()
    failing_function = AsyncMock(side_effect=ValueError("boom"))
    failing_config = TaskConfig(
        name="test_task",
        version=1,
        function=failing_function,
        timeout=10,
        max_retries=3,
        expected_exceptions=(ValueError,),
    )

    with patch("jobbers.task_processor.get_task_config", return_value=failing_config):
        retried_task = await TaskProcessor(state_manager).process(task)

    assert retried_task.status == TaskStatus.SUBMITTED
    # Simulate a long backoff wait between the first (failed) attempt and the retry: a bug
    # using started_at instead of retried_at would show up as a huge (wrong) execution_time.
    retried_task.started_at = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)

    succeeding_function = AsyncMock(return_value=TaskResult(results={"ok": True}))
    succeeding_config = TaskConfig(
        name="test_task", version=1, function=succeeding_function, timeout=10, max_retries=3
    )
    with (
        patch("jobbers.task_processor.get_task_config", return_value=succeeding_config),
        patch("jobbers.task_processor.execution_time") as mock_execution_time,
    ):
        completed_task = await TaskProcessor(state_manager).process(retried_task)

    assert completed_task.status == TaskStatus.COMPLETED
    mock_execution_time.record.assert_called_once()
    recorded_ms = mock_execution_time.record.call_args[0][0]
    assert recorded_ms < 5000  # well under the ~1 hour backoff gap; only the 2nd attempt counted


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

    # Task should be marked SUBMITTED and re-enqueued due to RESUBMIT policy
    assert task.status == TaskStatus.SUBMITTED
    assert task.completed_at is None  # Should not be completed when resubmitted
    assert task.retry_attempt == 0  # resubmit-on-shutdown is not a retry
    # save_task called once when starting; requeue_task called when handling cancellation
    state_manager.save_task.assert_called_once_with(task)
    state_manager.requeue_task.assert_called_once_with(task)


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
    state_manager.task_scheduler = AsyncMock(spec=RedisTaskScheduler)
    state_manager.task_state = AsyncMock()
    # Default to "not cancelling" so the post_process/_handle_retry DAG-cancellation
    # gates don't short-circuit tests that aren't exercising that feature.
    state_manager.is_dag_run_cancelling = AsyncMock(return_value=False)

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


# ── _handle_retry: DAG-cancellation gate ──────────────────────────────────────


@pytest.mark.asyncio
async def test_expected_exception_cancelled_instead_of_scheduled_when_dag_run_cancelling():
    """
    A retryable failure that would normally be SCHEDULED is CANCELLED instead when the DAG is cancelling.

    Cancellation wins over "retries remaining" -- see "Cancelling DAG runs" in docs/interacting-with-dags.md.
    """
    dag_run_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        retry_attempt=0,
        dag_run_id=dag_run_id,
    )
    state_manager = _make_state_manager()
    state_manager.is_dag_run_cancelling = AsyncMock(return_value=True)
    task_function = AsyncMock(side_effect=ValueError("boom"))
    task_config = _retryable_config()  # has retry_delay=5 -> would normally schedule
    task_config = task_config.model_copy(update={"function": task_function})

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.CANCELLED
    state_manager.schedule_retry_task.assert_not_called()
    state_manager.queue_retry_task.assert_not_called()
    state_manager.fail_task.assert_not_called()
    state_manager.finalize_dag_run_task.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_expected_exception_cancelled_instead_of_queued_when_dag_run_cancelling():
    """A retryable failure that would normally be re-queued immediately is CANCELLED instead when cancelling."""
    dag_run_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        retry_attempt=0,
        dag_run_id=dag_run_id,
    )
    state_manager = _make_state_manager()
    state_manager.is_dag_run_cancelling = AsyncMock(return_value=True)
    task_function = AsyncMock(side_effect=ValueError("boom"))
    # No retry_delay -> would normally be an immediate requeue (queue_retry_task).
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
        expected_exceptions=(ValueError,),
    )

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.CANCELLED
    state_manager.queue_retry_task.assert_not_called()
    state_manager.schedule_retry_task.assert_not_called()


@pytest.mark.asyncio
async def test_expected_exception_retries_normally_when_dag_run_not_cancelling():
    """Sanity check: an explicit is_dag_run_cancelling=False still retries normally (gate is a no-op)."""
    dag_run_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        retry_attempt=0,
        dag_run_id=dag_run_id,
    )
    state_manager = _make_state_manager()
    state_manager.is_dag_run_cancelling = AsyncMock(return_value=False)
    task_function = AsyncMock(side_effect=ValueError("boom"))
    task_config = _retryable_config()
    task_config = task_config.model_copy(update={"function": task_function})

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.SCHEDULED
    state_manager.schedule_retry_task.assert_called_once_with(task, ANY)


@pytest.mark.asyncio
async def test_expected_exception_skips_cancelling_check_for_non_dag_task():
    """A standalone (non-DAG) task's retry path never calls is_dag_run_cancelling."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
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
    state_manager.is_dag_run_cancelling.assert_not_awaited()


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
    fanout = DynamicFanOut(arms=[c1, c2], collector=collector)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()

    # Set up get_queue_config to report non-rate-limited queues
    state_manager.get_queue_config = AsyncMock(return_value=None)

    processor = TaskProcessor(state_manager)
    await processor._handle_dynamic_fanout(parent, fanout, [])

    # init_fan_in called once with the run's dag_run_id, correct key, and both child IDs
    fan_in_key = f"dag:fan-in:{collector.id}"
    state_manager.init_fan_in.assert_awaited_once_with(
        dag_run_id, fan_in_key, {c1.id, c2.id}, ttl=fanout.fan_in_ttl
    )
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
    fanout = DynamicFanOut(arms=[], collector=collector)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()

    processor = TaskProcessor(state_manager)
    await processor._handle_dynamic_fanout(parent, fanout, [])

    state_manager.init_fan_in.assert_not_called()
    state_manager.submit_task.assert_awaited_once()
    submitted = state_manager.submit_task.call_args[0][0]
    assert submitted.id == collector.id
    assert submitted.dag_run_id == dag_run_id


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_no_children_logs_and_does_not_raise_on_rate_limit(caplog):
    """A rate-limited degenerate-collector submission is logged, not propagated."""
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
    fanout = DynamicFanOut(arms=[], collector=collector)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock(side_effect=TaskRateLimitedError("queue is full"))

    processor = TaskProcessor(state_manager)
    with caplog.at_level(logging.ERROR):
        await processor._handle_dynamic_fanout(parent, fanout, [])  # must not raise

    assert any("rejected by rate" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_no_children_with_outer_fan_in_still_delegates():
    """
    Degenerate (zero-arm) fan-out must still delegate outer fan-in callbacks to the solo collector.

    Regression test: the zero-arm early return used to submit the collector without
    ever calling delegate_fan_in, so an outer fan-in waiting on the parent would
    never be closed and would hang forever.
    """
    dag_run_id = ULID()
    outer_fan_in_key = "dag:fan-in:outer-collector"
    outer_fan_in_cb = FanInCallback(
        task=DAGTaskSpec(name="outer_collect", queue="default"),
        fan_in_key=outer_fan_in_key,
    )
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        dag_callbacks=[outer_fan_in_cb],
    )
    collector = DAGNode("aggregator")
    fanout = DynamicFanOut(arms=[], collector=collector, propagate_fan_in=True)

    state_manager = _make_state_manager()
    state_manager.submit_task = AsyncMock()
    state_manager.task_state.delegate_fan_in = AsyncMock()

    processor = TaskProcessor(state_manager)
    await processor._handle_dynamic_fanout(parent, fanout, [outer_fan_in_cb])

    state_manager.task_state.delegate_fan_in.assert_awaited_once_with(
        dag_run_id, outer_fan_in_key, parent.id, collector.id
    )
    state_manager.submit_task.assert_awaited_once()
    submitted = state_manager.submit_task.call_args[0][0]
    assert submitted.id == collector.id
    assert outer_fan_in_cb in submitted.dag_callbacks


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_multi_step_arms_wires_fan_in_to_terminals():
    """Fan-in is attached to terminal (leaf) nodes, not arm roots, for multi-step arms."""
    dag_run_id = ULID()
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
    )
    # arm: root_a → step_a (terminal); root_b is single-step (its own terminal)
    root_a = DAGNode("root_a")
    step_a = DAGNode("step_a")
    root_a.then(step_a)
    root_b = DAGNode("root_b")
    collector = DAGNode("aggregator")
    fanout = DynamicFanOut(arms=[root_a, root_b], collector=collector)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()
    state_manager.get_queue_config = AsyncMock(return_value=None)

    processor = TaskProcessor(state_manager)
    await processor._handle_dynamic_fanout(parent, fanout, [])

    # Collector fan-in must wait for {step_a, root_b} — the *terminals* — not {root_a, root_b}.
    collector_fan_in_key = f"dag:fan-in:{collector.id}"
    init_calls = {call[0][1]: call[0][2] for call in state_manager.init_fan_in.call_args_list}
    assert all(call[0][0] == dag_run_id for call in state_manager.init_fan_in.call_args_list)
    assert collector_fan_in_key in init_calls
    assert init_calls[collector_fan_in_key] == {step_a.id, root_b.id}

    # Arm roots are submitted (not terminals); step_a's callbacks wire it to the collector.
    arm_tasks = state_manager.submit_tasks_batch.call_args[0][0]
    assert {t.id for t in arm_tasks} == {root_a.id, root_b.id}

    # Verify the callback chain is baked correctly into root_a_task so that at
    # execution time root_a → (submits) step_a → (decrements fan-in) → collector.
    arm_tasks_by_id = {t.id: t for t in arm_tasks}
    root_a_task = arm_tasks_by_id[root_a.id]
    assert len(root_a_task.dag_callbacks) == 1
    assert isinstance(root_a_task.dag_callbacks[0], SimpleCallback)
    step_a_spec = root_a_task.dag_callbacks[0].task
    assert step_a_spec.id == step_a.id
    fan_in_cbs = [cb for cb in step_a_spec.dag_callbacks if isinstance(cb, FanInCallback)]
    assert len(fan_in_cbs) == 1
    assert fan_in_cbs[0].fan_in_key == collector_fan_in_key


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_arm_with_static_diamond_inits_both_fan_ins():
    """An arm with a static diamond sub-graph initialises both the inner and outer fan-in sets."""
    dag_run_id = ULID()
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
    )
    # arm_A: arm_root → (b1, b2) → merge_node   (static diamond within the arm)
    arm_root = DAGNode("arm_root")
    b1 = DAGNode("branch_1")
    b2 = DAGNode("branch_2")
    merge_node = DAGNode("merge_node")
    arm_root.then(b1, b2)
    DAGNode.merge(b1, b2, into=merge_node)
    # arm_B: single-step
    arm_b = DAGNode("arm_b")
    collector = DAGNode("collector")
    fanout = DynamicFanOut(arms=[arm_root, arm_b], collector=collector)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()
    state_manager.get_queue_config = AsyncMock(return_value=None)

    processor = TaskProcessor(state_manager)
    await processor._handle_dynamic_fanout(parent, fanout, [])

    outer_key = f"dag:fan-in:{collector.id}"
    inner_key = f"dag:fan-in:{merge_node.id}"
    init_calls = {c[0][1]: c[0][2] for c in state_manager.init_fan_in.call_args_list}
    assert all(c[0][0] == dag_run_id for c in state_manager.init_fan_in.call_args_list)

    # Outer fan-in: collector waits for the terminal of arm_A (merge_node) and arm_b.
    assert init_calls[outer_key] == {merge_node.id, arm_b.id}
    # Inner (static) fan-in within arm_A: merge_node waits for b1 and b2.
    assert init_calls[inner_key] == {b1.id, b2.id}

    # Only arm roots are submitted — the branch and merge nodes are interior.
    arm_tasks = state_manager.submit_tasks_batch.call_args[0][0]
    assert {t.id for t in arm_tasks} == {arm_root.id, arm_b.id}

    # Verify the full callback chain baked into arm_root_task:
    #   arm_root → SimpleCallback(b1), SimpleCallback(b2)
    #   b1 → FanInCallback(inner_key, merge_node)
    #   merge_node → FanInCallback(outer_key, collector)
    arm_tasks_by_id = {t.id: t for t in arm_tasks}
    arm_root_task = arm_tasks_by_id[arm_root.id]
    child_ids = {cb.task.id for cb in arm_root_task.dag_callbacks if isinstance(cb, SimpleCallback)}
    assert child_ids == {b1.id, b2.id}

    b1_spec = next(
        cb.task
        for cb in arm_root_task.dag_callbacks
        if isinstance(cb, SimpleCallback) and cb.task.id == b1.id
    )
    inner_cbs = [cb for cb in b1_spec.dag_callbacks if isinstance(cb, FanInCallback)]
    assert len(inner_cbs) == 1
    assert inner_cbs[0].fan_in_key == inner_key

    merge_spec = inner_cbs[0].task
    assert merge_spec.id == merge_node.id
    outer_cbs = [cb for cb in merge_spec.dag_callbacks if isinstance(cb, FanInCallback)]
    assert len(outer_cbs) == 1
    assert outer_cbs[0].fan_in_key == outer_key


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_with_outer_fan_in_delegates_to_collector():
    """When the parent has outer FanInCallbacks the collector inherits them and delegate_fan_in is called."""
    dag_run_id = ULID()
    outer_fan_in_key = "dag:fan-in:outer-collector"
    outer_fan_in_cb = FanInCallback(
        task=DAGTaskSpec(name="outer_collect", queue="default"),
        fan_in_key=outer_fan_in_key,
    )
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        dag_callbacks=[outer_fan_in_cb],
    )
    arm = DAGNode("arm_task")
    collector = DAGNode("inner_collect")
    fanout = DynamicFanOut(arms=[arm], collector=collector, propagate_fan_in=True)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()
    state_manager.get_queue_config = AsyncMock(return_value=None)
    state_manager.task_state.delegate_fan_in = AsyncMock()

    processor = TaskProcessor(state_manager)
    await processor._handle_dynamic_fanout(parent, fanout, [outer_fan_in_cb])

    # delegate_fan_in must swap parent.id → collector.id in the outer fan-in set.
    state_manager.task_state.delegate_fan_in.assert_awaited_once_with(
        dag_run_id, outer_fan_in_key, parent.id, collector.id
    )

    # The collector task must carry the outer fan-in callback.
    saved_collector = state_manager.save_task.call_args[0][0]
    assert saved_collector.id == collector.id
    assert outer_fan_in_cb in saved_collector.dag_callbacks


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_propagate_fan_in_false_skips_delegation():
    """propagate_fan_in=False means outer fan-in callbacks are NOT transferred to the collector."""
    dag_run_id = ULID()
    outer_fan_in_key = "dag:fan-in:outer-collector"
    outer_fan_in_cb = FanInCallback(
        task=DAGTaskSpec(name="outer_collect", queue="default"),
        fan_in_key=outer_fan_in_key,
    )
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        dag_callbacks=[outer_fan_in_cb],
    )
    arm = DAGNode("arm_task")
    collector = DAGNode("inner_collect")
    # opt-out: the parent task's FanInCallback should fire immediately, not be delegated
    fanout = DynamicFanOut(arms=[arm], collector=collector, propagate_fan_in=False)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()
    state_manager.get_queue_config = AsyncMock(return_value=None)
    state_manager.task_state.delegate_fan_in = AsyncMock()

    processor = TaskProcessor(state_manager)
    # With propagate_fan_in=False, post_process passes outer_fan_in_cbs=[]
    await processor._handle_dynamic_fanout(parent, fanout, [])

    # No delegation — delegate_fan_in must not be called.
    state_manager.task_state.delegate_fan_in.assert_not_awaited()

    # The collector must NOT carry the outer fan-in callback.
    saved_collector = state_manager.save_task.call_args[0][0]
    assert outer_fan_in_cb not in saved_collector.dag_callbacks


# ── declarative fan-out / _spec_to_dag_node (nested fan-out) ─────────────────


def test_spec_to_dag_node_preserves_nested_dynamic_fanout_callback():
    """
    _spec_to_dag_node must reattach a DynamicFanOutCallback found on a spec node.

    Regression test: nested declarative fan-outs (a mermaid arm task that is itself
    a dispatcher) were previously lost when _handle_declarative_fanout rebuilt a
    DAGNode tree from the stored DAGTaskSpec — the rebuilt arm task's dag_callbacks
    no longer contained the inner DynamicFanOutCallback, so the nested fan-out never
    fired when that arm task actually executed.
    """
    arm_spec = DAGTaskSpec(id=ULID(), name="inner_arm", queue="default", version=1, parameters={})
    collector_spec = DAGTaskSpec(id=ULID(), name="inner_collector", queue="default", version=1, parameters={})
    inner_fanout_cb = DynamicFanOutCallback(arm_root=arm_spec, collector=collector_spec, items_key="items")

    dispatcher_spec = DAGTaskSpec(
        id=ULID(),
        name="dispatcher",
        queue="default",
        version=1,
        parameters={},
        dag_callbacks=[inner_fanout_cb],
    )

    node = _spec_to_dag_node(dispatcher_spec)
    task = node.to_task()

    fanout_cbs = [cb for cb in task.dag_callbacks if isinstance(cb, DynamicFanOutCallback)]
    assert len(fanout_cbs) == 1
    assert fanout_cbs[0].arm_root.name == "inner_arm"
    assert fanout_cbs[0].collector.name == "inner_collector"


@pytest.mark.asyncio
async def test_handle_declarative_fanout_preserves_nested_dispatch_on_arm_task():
    """
    An arm task that is itself a nested dispatcher keeps its own DynamicFanOutCallback.

    End-to-end regression test for the canonical nested-fanout mermaid example
    (A -->> B; B -->> R; R --o D; D --o C): when outer dispatcher A completes and
    _handle_declarative_fanout rebuilds/submits arm B, B's submitted task must still
    carry the inner DynamicFanOutCallback — otherwise B's own dispatch to R/D is
    silently dropped when B executes.
    """
    dag_run_id = ULID()
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="outer_dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        results={"items": [{}]},
    )

    inner_arm_spec = DAGTaskSpec(id=ULID(), name="inner_worker", queue="default", version=1, parameters={})
    inner_collector_spec = DAGTaskSpec(
        id=ULID(), name="inner_collector", queue="default", version=1, parameters={}
    )
    inner_fanout_cb = DynamicFanOutCallback(
        arm_root=inner_arm_spec, collector=inner_collector_spec, items_key="items"
    )

    # The outer arm template ("process_batch") is itself a nested dispatcher.
    outer_arm_root = DAGTaskSpec(
        id=ULID(),
        name="process_batch",
        queue="default",
        version=1,
        parameters={},
        dag_callbacks=[inner_fanout_cb],
    )
    outer_collector_spec = DAGTaskSpec(
        id=ULID(), name="aggregate_all", queue="default", version=1, parameters={}
    )
    outer_cb = DynamicFanOutCallback(
        arm_root=outer_arm_root, collector=outer_collector_spec, items_key="items"
    )

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_tasks_batch = AsyncMock()
    state_manager.get_queue_config = AsyncMock(return_value=None)

    processor = TaskProcessor(state_manager)
    await processor._handle_declarative_fanout(parent, outer_cb)

    state_manager.submit_tasks_batch.assert_awaited_once()
    submitted_arms = state_manager.submit_tasks_batch.call_args[0][0]
    assert len(submitted_arms) == 1
    arm_task = submitted_arms[0]
    nested_cbs = [cb for cb in arm_task.dag_callbacks if isinstance(cb, DynamicFanOutCallback)]
    assert len(nested_cbs) == 1, "the arm task must still carry its own nested DynamicFanOutCallback"
    assert nested_cbs[0].arm_root.name == "inner_worker"


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

    mock_ta = AsyncMock()
    mock_ta.fan_in_complete.return_value = 0
    mock_ta.get_fan_in_members.return_value = []
    state_manager.task_state = mock_ta

    processor = TaskProcessor(state_manager)
    await processor.post_process(parent)

    # Exactly one child task should have been submitted (one SimpleCallback)
    state_manager.submit_tasks_batch.assert_awaited_once()
    submitted = state_manager.submit_tasks_batch.call_args[0][0][0]
    assert submitted.name == child_spec.name


@pytest.mark.asyncio
async def test_post_process_skipped_when_dag_run_cancelling():
    """post_process spawns no descendants when the task's DAG run is marked cancelling."""
    dag_run_id = ULID()
    child_spec = DAGTaskSpec(name="child_task", queue="default")
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        dag_callbacks=[SimpleCallback(task=child_spec)],
    )
    state_manager = _make_state_manager()
    state_manager.is_dag_run_cancelling = AsyncMock(return_value=True)

    processor = TaskProcessor(state_manager)
    await processor.post_process(parent)

    state_manager.submit_tasks_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_process_skips_cancelling_check_for_non_dag_task():
    """A standalone (non-DAG) task's post_process never calls is_dag_run_cancelling."""
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

    processor = TaskProcessor(state_manager)
    await processor.post_process(parent)

    state_manager.is_dag_run_cancelling.assert_not_awaited()
    state_manager.submit_tasks_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_completed_task_records_post_process_failure_without_raising():
    """A post_process failure is logged, recorded on the task, and does not propagate."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={},
        status=TaskStatus.SUBMITTED,
        queue="test_queue",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=TaskResult(results={}))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with (
        patch("jobbers.task_processor.get_task_config", return_value=task_config),
        patch("jobbers.task_processor.post_process_failures") as mock_counter,
    ):
        processor = TaskProcessor(state_manager)
        with patch.object(processor, "post_process", AsyncMock(side_effect=RuntimeError("boom"))):
            result_task = await processor.process(task)  # must not raise

    assert result_task.status == TaskStatus.COMPLETED
    assert any("post_process failed" in e and "boom" in e for e in result_task.errors)
    assert state_manager.save_task.call_count >= 2  # once on start, once from the failure handler
    mock_counter.add.assert_called_once()


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
    fanout = DynamicFanOut(arms=[DAGNode("child")], collector=DAGNode("collect"))

    state_manager = _make_state_manager()
    processor = TaskProcessor(state_manager)

    with patch.object(processor, "_handle_dynamic_fanout", new_callable=AsyncMock) as mock_fanout:
        await processor.post_process(parent, dynamic_fanout=fanout)

    mock_fanout.assert_awaited_once_with(parent, fanout, [])


@pytest.mark.asyncio
async def test_post_process_declarative_fanout_skips_delegated_outer_fan_in():
    """
    An outer fan-in member that is also a declarative dispatcher must not re-decrement its delegated key.

    Regression test: post_process's skip_fan_in_keys was only ever computed from
    the programmatic `dynamic_fanout` parameter, never from a declarative
    DynamicFanOutCallback's own propagate_fan_in — so a nested-declarative
    dispatcher that was also an outer fan-in member always fell through to
    generate_callbacks, hit the already-renamed key, and logged a spurious
    "skipping collector" warning plus a wasted round trip on every completion.
    """
    dag_run_id = ULID()
    outer_fan_in_key = "dag:fan-in:outer-collector"
    outer_fan_in_cb = FanInCallback(
        task=DAGTaskSpec(name="outer_collect", queue="default"),
        fan_in_key=outer_fan_in_key,
    )
    inner_fanout_cb = DynamicFanOutCallback(
        arm_root=DAGTaskSpec(name="inner_worker", queue="default"),
        collector=DAGTaskSpec(name="inner_collector", queue="default"),
        items_key="items",
    )
    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        dag_callbacks=[outer_fan_in_cb, inner_fanout_cb],
    )

    state_manager = _make_state_manager()
    state_manager.task_state.fan_in_complete = AsyncMock()

    processor = TaskProcessor(state_manager)
    with patch.object(processor, "_handle_declarative_fanout", new_callable=AsyncMock) as mock_declarative:
        await processor.post_process(parent)

    mock_declarative.assert_awaited_once_with(parent, inner_fanout_cb)
    # The outer FanInCallback's key must be skipped here -- _handle_declarative_fanout
    # (mocked above, but exercised for real in test_handle_declarative_fanout_*) is
    # responsible for delegating it via delegate_fan_in instead.
    state_manager.task_state.fan_in_complete.assert_not_awaited()


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
async def test_task_processor_stores_plain_dict_result():
    """Returning a plain dict saves the result data (not discarded as empty dict)."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value={"rows": 42})
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.results == {"rows": 42}
    assert result.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_task_processor_plain_dict_deduplicates_parent_ids():
    """Returning a plain dict deduplicates any existing parent_ids on the task."""
    parent_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        parent_ids=[parent_id, parent_id],
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value={"ok": True})
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.parent_ids == [parent_id]


@pytest.mark.asyncio
async def test_task_processor_none_result_stores_empty_dict():
    """Returning None is valid and stores an empty results dict."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=None)
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.results == {}
    assert result.status == TaskStatus.COMPLETED


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
async def test_process_failed_task_records_post_process_error_failure_without_raising():
    """A post_process_error failure is logged, recorded on the task, and does not propagate."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        parameters={},
        status=TaskStatus.SUBMITTED,
        queue="test_queue",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(side_effect=ValueError("boom"))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10, max_retries=0)

    with (
        patch("jobbers.task_processor.get_task_config", return_value=task_config),
        patch("jobbers.task_processor.post_process_failures") as mock_counter,
    ):
        processor = TaskProcessor(state_manager)
        with patch.object(processor, "post_process_error", AsyncMock(side_effect=RuntimeError("kaboom"))):
            result_task = await processor.process(task)  # must not raise

    assert result_task.status == TaskStatus.FAILED
    assert any("post_process failed" in e and "kaboom" in e for e in result_task.errors)
    mock_counter.add.assert_called_once()


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


# ── FromParent ────────────────────────────────────────────────────────────────


def _make_from_parent_task(parent_ids: list[ULID], parameters: dict[str, object] | None = None) -> Task:
    return Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        parent_ids=parent_ids,
        parameters=parameters or {},
    )


@pytest.mark.asyncio
async def test_from_parent_single_parent_key_present_injects_scalar():
    """A single parent producing the key resolves to a plain scalar value."""
    parent_id = ULID()
    task = _make_from_parent_task([parent_id])
    state_manager = _make_state_manager()
    calls: list[int] = []

    async def fn(rows: Annotated[int, FromParent("rows")], **kwargs):
        calls.append(rows)
        return {"seen": rows}

    # task_config.function must be the real function, not a Mock wrapper: FromParent
    # resolution relies on get_type_hints(), which can't see through a Mock's annotations.
    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        with patch.object(
            task.__class__, "parent_results", new_callable=AsyncMock, return_value={parent_id: {"rows": 42}}
        ):
            processor = TaskProcessor(state_manager)
            result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert calls == [42]


@pytest.mark.asyncio
async def test_from_parent_single_parent_key_absent_no_default_raises_type_error():
    """Key absent + no Python default -- the kwarg is omitted and the call itself raises."""
    parent_id = ULID()
    task = _make_from_parent_task([parent_id])
    state_manager = _make_state_manager()

    async def fn(rows: Annotated[int, FromParent("rows")], **kwargs):
        return {"seen": rows}  # pragma: no cover -- never reached, call itself raises first

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        with patch.object(
            task.__class__, "parent_results", new_callable=AsyncMock, return_value={parent_id: {}}
        ):
            processor = TaskProcessor(state_manager)
            with pytest.raises(TypeError):
                await processor.process(task)


@pytest.mark.asyncio
async def test_from_parent_single_parent_key_absent_with_python_default_uses_it():
    """Key absent + the function's own Python default -- FromParent never sets the kwarg, so the default applies."""
    parent_id = ULID()
    task = _make_from_parent_task([parent_id])
    state_manager = _make_state_manager()
    calls: list[int] = []

    async def fn(rows: Annotated[int, FromParent("rows")] = 0, **kwargs):
        calls.append(rows)
        return {"seen": rows}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        with patch.object(
            task.__class__, "parent_results", new_callable=AsyncMock, return_value={parent_id: {}}
        ):
            processor = TaskProcessor(state_manager)
            result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert calls == [0]


@pytest.mark.asyncio
async def test_from_parent_wrong_parent_count_always_raises():
    """many=False requires exactly one parent -- always raises, regardless of what the parents' data contains."""
    p1, p2 = ULID(), ULID()
    task = _make_from_parent_task([p1, p2])
    state_manager = _make_state_manager()
    calls: list[int] = []

    async def fn(rows: Annotated[int, FromParent("rows")], **kwargs):
        calls.append(rows)  # pragma: no cover -- never reached, resolution raises first
        return {"seen": rows}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    # Neither, one, nor both parents having the key changes the outcome -- it's a shape mismatch.
    for parent_results in ({p1: {}, p2: {}}, {p1: {"rows": 1}, p2: {}}, {p1: {"rows": 1}, p2: {"rows": 2}}):
        with patch("jobbers.task_processor.get_task_config", return_value=task_config):
            with patch.object(
                task.__class__, "parent_results", new_callable=AsyncMock, return_value=parent_results
            ):
                processor = TaskProcessor(state_manager)
                with pytest.raises(ValueError, match="requires exactly one parent"):
                    await processor.process(task)
    assert calls == []


@pytest.mark.asyncio
async def test_from_parent_many_collects_list_across_parents():
    """many=True always returns a list -- collected from every parent that produced the key."""
    p1, p2 = ULID(), ULID()
    task = _make_from_parent_task([p1, p2])
    state_manager = _make_state_manager()
    calls: list[list[int]] = []

    async def fn(rows: Annotated[list[int], FromParent("rows", many=True)], **kwargs):
        calls.append(rows)
        return {"total": sum(rows)}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        with patch.object(
            task.__class__,
            "parent_results",
            new_callable=AsyncMock,
            return_value={p1: {"rows": 1}, p2: {"rows": 2}},
        ):
            processor = TaskProcessor(state_manager)
            result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert sorted(calls[0]) == [1, 2]


@pytest.mark.asyncio
async def test_from_parent_many_empty_when_none_match():
    """many=True resolves to an empty list, not an error, when no parent produced the key."""
    p1, p2 = ULID(), ULID()
    task = _make_from_parent_task([p1, p2])
    state_manager = _make_state_manager()
    calls: list[list[int]] = []

    async def fn(rows: Annotated[list[int], FromParent("rows", many=True)], **kwargs):
        calls.append(rows)
        return {"total": sum(rows)}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        with patch.object(
            task.__class__, "parent_results", new_callable=AsyncMock, return_value={p1: {}, p2: {}}
        ):
            processor = TaskProcessor(state_manager)
            result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert calls == [[]]


@pytest.mark.asyncio
async def test_from_parent_key_defaults_to_param_name():
    """FromParent() with no key argument resolves using the annotated parameter's own name."""
    parent_id = ULID()
    task = _make_from_parent_task([parent_id])
    state_manager = _make_state_manager()
    calls: list[int] = []

    async def fn(rows: Annotated[int, FromParent()], **kwargs):
        calls.append(rows)
        return {"seen": rows}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        with patch.object(
            task.__class__, "parent_results", new_callable=AsyncMock, return_value={parent_id: {"rows": 7}}
        ):
            processor = TaskProcessor(state_manager)
            result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert calls == [7]


@pytest.mark.asyncio
async def test_from_parent_root_node_singular_falls_back_to_python_default():
    """Zero parents is not a shape violation -- a root node uses the function's own default instead of raising."""
    task = _make_from_parent_task([])
    state_manager = _make_state_manager()
    calls: list[int] = []

    async def fn(rows: Annotated[int, FromParent("rows")] = 9, **kwargs):
        calls.append(rows)
        return {"seen": rows}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert calls == [9]


@pytest.mark.asyncio
async def test_from_parent_root_node_singular_uses_submitted_parameter():
    """A root node can override FromParent with a directly-submitted parameter of the same name."""
    task = _make_from_parent_task([], parameters={"rows": 5})
    state_manager = _make_state_manager()
    calls: list[int] = []

    async def fn(rows: Annotated[int, FromParent("rows")], **kwargs):
        calls.append(rows)
        return {"seen": rows}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert calls == [5]


@pytest.mark.asyncio
async def test_from_parent_root_node_many_uses_submitted_parameter_not_empty_list():
    """A root node with many=True does not clobber a submitted list with []."""
    task = _make_from_parent_task([], parameters={"rows": [1, 2, 3]})
    state_manager = _make_state_manager()
    calls: list[list[int]] = []

    async def fn(rows: Annotated[list[int], FromParent("rows", many=True)], **kwargs):
        calls.append(rows)
        return {"seen": rows}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert calls == [[1, 2, 3]]


@pytest.mark.asyncio
async def test_from_parent_root_node_many_falls_back_to_python_default():
    """Root node, many=True, no submitted parameter -- the function's own default applies, not []."""
    task = _make_from_parent_task([])
    state_manager = _make_state_manager()
    calls: list[list[int]] = []

    async def fn(rows: Annotated[list[int], FromParent("rows", many=True)] = (), **kwargs):  # type: ignore[assignment]
        calls.append(list(rows))
        return {"seen": list(rows)}

    task_config = TaskConfig(name="test_task", version=1, function=fn, timeout=10)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.COMPLETED
    assert calls == [[]]


# ── _maybe_cleanup ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_maybe_cleanup_standalone_deletes_on_matching_status():
    """Standalone task with cleanup_on={COMPLETED} is deleted after successful completion."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=None)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        cleanup_on=frozenset({TaskStatus.COMPLETED}),
    )
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.process(task)

    assert task.status == TaskStatus.COMPLETED
    state_manager.delete_task.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_maybe_cleanup_standalone_does_not_delete_when_status_not_in_cleanup_on():
    """Standalone task with cleanup_on={FAILED} is NOT deleted on COMPLETED."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=None)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        cleanup_on=frozenset({TaskStatus.FAILED}),
    )
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.process(task)

    assert task.status == TaskStatus.COMPLETED
    state_manager.delete_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_maybe_cleanup_standalone_no_cleanup_on():
    """Standalone task without cleanup_on is never auto-deleted."""
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
    )
    state_manager = _make_state_manager()
    task_function = AsyncMock(return_value=None)
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10)
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.process(task)

    state_manager.delete_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_maybe_cleanup_dag_task_delegates_to_state_manager():
    """
    DAG tasks delegate to StateManager.finalize_dag_run_task.

    All the DAG-run bookkeeping (aggregate-status recording, pending-counter close,
    sibling sweep, and the ordering between them) now lives on StateManager behind
    this single method — TaskProcessor just needs to call it exactly once per
    DAG-task completion. See test_state_manager.py for coverage of what
    finalize_dag_run_task itself does.
    """
    dag_run_id = ULID()
    task_id = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")

    task = Task(
        id=task_id,
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        dag_run_id=dag_run_id,
    )

    state_manager = _make_state_manager()

    task_function = AsyncMock(return_value=None)
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        cleanup_on=frozenset({TaskStatus.COMPLETED}),
    )
    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.process(task)

    assert task.status == TaskStatus.COMPLETED
    state_manager.finalize_dag_run_task.assert_awaited_once_with(task)
    state_manager.delete_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_maybe_cleanup_skipped_for_non_terminal_status():
    """
    A DAG task that retries (non-terminal status) must NOT close itself out of the DAG run's pending set.

    Regression test: process() used to call _maybe_cleanup unconditionally, so a
    task hitting a retryable exception (status SCHEDULED/UNSUBMITTED, not done)
    would still call close_dag_run_task_and_sweep, permanently marking it closed
    before it had actually finished.
    """
    dag_run_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        dag_run_id=dag_run_id,
    )

    task_function = AsyncMock(side_effect=ValueError("transient"))
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
        expected_exceptions=(ValueError,),
        cleanup_on=frozenset({TaskStatus.COMPLETED}),
    )

    state_manager = _make_state_manager()

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.process(task)

    # No retry_delay configured means immediate retry: _handle_retry sets UNSUBMITTED,
    # then queue_retry_task re-queues it as SUBMITTED — either way, not terminal.
    assert task.status == TaskStatus.SUBMITTED
    state_manager.finalize_dag_run_task.assert_not_awaited()
    state_manager.delete_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_maybe_cleanup_failed_dag_task_does_not_close_pending():
    """
    A DAG task that permanently FAILS must NOT close itself out of the DAG run's pending set.

    A FAILED task never calls generate_callbacks(), so any FanInCallback/
    DynamicFanOutCallback it carries never fires and the DAG can't complete on its
    own. Closing it out of DAG_RUN_PENDING here would let the run's pending count
    reach zero and trigger the sweep even though the DAG never actually finished —
    the run must stay open (fan-in tracking and sibling records preserved within
    their TTLs) instead.
    """
    dag_run_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        dag_run_id=dag_run_id,
    )

    # No expected_exceptions configured, so any raised exception is unexpected and
    # goes straight to handle_unexpected_exception -> FAILED, regardless of retries.
    task_function = AsyncMock(side_effect=ValueError("boom"))
    task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        cleanup_on=frozenset({TaskStatus.COMPLETED}),
    )

    state_manager = _make_state_manager()

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.process(task)

    assert task.status == TaskStatus.FAILED
    state_manager.finalize_dag_run_task.assert_awaited_once_with(task)
    state_manager.delete_task.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [TaskStatus.CANCELLED, TaskStatus.DROPPED])
async def test_maybe_cleanup_stuck_dag_task_does_not_close_pending(status):
    """
    A DAG task that ends CANCELLED or DROPPED must NOT close itself out of the DAG run's pending set.

    Both share FAILED/STALLED's defect (now reflected in stuck_statuses()): the task
    never calls generate_callbacks(), so any FanInCallback/DynamicFanOutCallback it
    carries never fires. Closing it out of DAG_RUN_PENDING here would let the run's
    pending count reach zero and trigger the sibling sweep even though the collector
    this task belonged to can now never fire.
    """
    dag_run_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        status=status,
        queue="default",
        dag_run_id=dag_run_id,
    )
    state_manager = _make_state_manager()

    processor = TaskProcessor(state_manager)
    await processor._maybe_cleanup(task)

    state_manager.finalize_dag_run_task.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_maybe_cleanup_runs_after_dynamic_fanout_registers_arms():
    """
    Regression test for an ordering hazard with _maybe_cleanup.

    _maybe_cleanup must run after post_process spawns fan-out arms, so a dispatcher
    only closes itself out of the DAG run's pending set once its own arms are
    already registered in that same set. If cleanup ran first (as it used to), a
    dispatcher that looked like the last active task could trigger cleanup before
    the arms it was about to spawn even existed.
    """
    dag_run_id = ULID()
    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        dag_run_id=dag_run_id,
    )

    def _dispatch(**kwargs):
        arm = DAGNode("worker")
        collector = DAGNode("aggregator")
        return TaskResult(results={}, fanout=DynamicFanOut(arms=[arm], collector=collector))

    task_function = AsyncMock(side_effect=_dispatch)
    task_config = TaskConfig(
        name="dispatcher",
        version=1,
        function=task_function,
        timeout=10,
        cleanup_on=frozenset({TaskStatus.COMPLETED}),
    )

    state_manager = _make_state_manager()
    state_manager.get_queue_config = AsyncMock(return_value=None)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        await processor.process(task)

    assert task.status == TaskStatus.COMPLETED
    call_names = [c[0] for c in state_manager.mock_calls]
    submit_index = call_names.index("submit_tasks_batch")
    close_index = call_names.index("finalize_dag_run_task")
    assert submit_index < close_index, "arms must be registered before the dispatcher closes out of the run"
