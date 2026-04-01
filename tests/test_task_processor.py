import asyncio
import contextlib
import datetime as dt
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

import fakeredis.aioredis as fakeredis
import pytest

from jobbers.models.task import Task, TaskStatus
from jobbers.models.task_config import BackoffStrategy
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.registry import TaskConfig, clear_registry, register_task
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
    from jobbers.models.dag import TaskResult

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


# ── pubsub cancellation ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_task_processor_run_exits_early_on_pubsub_cancel():
    """
    TaskProcessor.run exits cleanly when a cancel signal is published to the task's pubsub channel.

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

    fake_store = fakeredis.FakeRedis()
    task_started = asyncio.Event()

    async def slow_task():
        task_started.set()
        await asyncio.sleep(30)  # long-running; will be cancelled before this finishes
        return {}  # pragma: no cover

    task_config = TaskConfig(name="test_task", version=1, function=slow_task, timeout=60)
    state_manager = _make_state_manager()

    async def pubsub_monitor(task_id: str) -> None:
        """Real pubsub listener — loops until a cancel message arrives."""
        async with fake_store.pubsub() as pubsub:
            await pubsub.subscribe(f"task_cancel_{task_id}")
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True)
                if msg is not None:
                    raise UserCancellationError(str(task_id))
                await asyncio.sleep(0.01)

    state_manager.monitor_task_cancellation.side_effect = pubsub_monitor

    async def publish_cancel() -> None:
        await task_started.wait()
        await fake_store.publish(f"task_cancel_{task.id}", "cancel")

    publisher = asyncio.create_task(publish_cancel())

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        # run() catches the ExceptionGroup([UserCancellationError]) raised by the
        # TaskGroup and exits cleanly — no exception should propagate here.
        await processor.run(task)

    publisher.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await publisher

    await fake_store.aclose()

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
    from jobbers.models.dag import DAGNode, DynamicFanOut

    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
    )
    c1 = DAGNode("worker_a")
    c2 = DAGNode("worker_b")
    collector = DAGNode("aggregator")
    fanout = DynamicFanOut(children=[c1, c2], collector=collector)

    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()

    # Set up qca to report non-rate-limited queues
    state_manager.qca = AsyncMock()
    state_manager.qca.get_queue_config = AsyncMock(return_value=None)

    # Set up pipeline: stage_submit_task is sync; execute() is awaitable
    mock_pipe = AsyncMock()
    state_manager.job_store = MagicMock()
    state_manager.job_store.pipeline.return_value = mock_pipe

    # Capture stage_submit_task calls
    staged_tasks: list[Task] = []
    state_manager.ta = MagicMock()
    state_manager.ta.stage_submit_task.side_effect = lambda pipe, task: staged_tasks.append(task)

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

    # children submitted via pipeline, not submit_task
    state_manager.submit_task.assert_not_awaited()
    assert {ct.id for ct in staged_tasks} == {c1.id, c2.id}
    for ct in staged_tasks:
        assert ct.parent_ids == [parent.id]
        assert ct.status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_handle_dynamic_fanout_no_children_submits_collector_immediately():
    """Degenerate fan-out with no children submits the collector directly."""
    from jobbers.models.dag import DAGNode, DynamicFanOut

    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
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


@pytest.mark.asyncio
async def test_task_processor_stores_task_result_on_success():
    """TaskProcessor stores results from a TaskResult return value."""
    from jobbers.models.dag import TaskResult

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
    from unittest.mock import AsyncMock

    from jobbers.models.dag import DAGTaskSpec, SimpleCallback

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
    state_manager.submit_task = AsyncMock()

    # generate_callbacks now receives state_manager.ta directly
    mock_ta = AsyncMock()
    mock_ta.fan_in_complete.return_value = 0
    mock_ta.get_fan_in_members.return_value = []
    state_manager.ta = mock_ta

    processor = TaskProcessor(state_manager)
    await processor.post_process(parent)

    # At least one child task should have been submitted
    assert state_manager.submit_task.await_count >= 1


@pytest.mark.asyncio
async def test_post_process_with_dynamic_fanout_calls_handle_dynamic_fanout():
    """post_process delegates to _handle_dynamic_fanout when a DynamicFanOut is passed."""
    from unittest.mock import AsyncMock, patch

    from jobbers.models.dag import DAGNode, DynamicFanOut

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
    from ulid import ULID

    from jobbers.models.dag import TaskResult

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
    from jobbers.models.dag import TaskResult

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
    from jobbers.models.dag import DAGTaskSpec, SimpleCallback

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
    state_manager.submit_task = AsyncMock()

    processor = TaskProcessor(state_manager)
    await processor.post_process_error(task)

    state_manager.submit_task.assert_awaited_once()
    submitted = state_manager.submit_task.call_args[0][0]
    assert submitted.id == error_spec.id
    assert submitted.parent_ids == [task.id]


@pytest.mark.asyncio
async def test_post_process_error_no_error_callbacks_does_nothing():
    """post_process_error does not call submit_task when no error callbacks are set."""
    from jobbers.models.dag import DAGTaskSpec, SimpleCallback

    task = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="test_task",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        dag_callbacks=[SimpleCallback(task=DAGTaskSpec(name="child"))],
    )
    state_manager = _make_state_manager()
    state_manager.submit_task = AsyncMock()

    processor = TaskProcessor(state_manager)
    await processor.post_process_error(task)

    state_manager.submit_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_task_triggers_error_callback():
    """When a task fails with an unexpected exception, error callbacks are submitted."""
    from jobbers.models.dag import DAGTaskSpec, SimpleCallback

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

    task_function = AsyncMock(side_effect=RuntimeError("boom"))
    task_config = TaskConfig(name="test_task", version=1, function=task_function, timeout=10, max_retries=0)

    with patch("jobbers.task_processor.get_task_config", return_value=task_config):
        processor = TaskProcessor(state_manager)
        result = await processor.process(task)

    assert result.status == TaskStatus.FAILED
    state_manager.submit_task.assert_awaited_once()
    submitted = state_manager.submit_task.call_args[0][0]
    assert submitted.id == error_spec.id


@pytest.mark.asyncio
async def test_retried_task_does_not_trigger_error_callback():
    """A task being retried (SCHEDULED) does not fire error callbacks."""
    from jobbers.models.dag import DAGTaskSpec, SimpleCallback

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


# ── _handle_diagram_fanout ────────────────────────────────────────────────────


def _make_fanout_state_manager():
    """Return a mock StateManager wired for fanout pipeline tests."""
    state_manager = _make_state_manager()
    state_manager.init_fan_in = AsyncMock()
    state_manager.save_task = AsyncMock()
    state_manager.submit_task = AsyncMock()
    state_manager.qca = AsyncMock()
    state_manager.qca.get_queue_config = AsyncMock(return_value=None)
    mock_pipe = AsyncMock()
    state_manager.job_store = MagicMock()
    state_manager.job_store.pipeline.return_value = mock_pipe
    staged_tasks: list[Task] = []
    state_manager.ta = MagicMock()
    state_manager.ta.stage_submit_task.side_effect = lambda pipe, task: staged_tasks.append(task)
    state_manager._staged_tasks = staged_tasks
    return state_manager


@pytest.mark.asyncio
async def test_handle_diagram_fanout_submits_children_and_presaves_collector():
    """Diagram-based fanout: routing fn called, children submitted, collector pre-saved."""
    from jobbers.models.dag import DAGTaskSpec, DynamicFanOutCallback
    from jobbers.registry import register_router

    @register_router("test_router")
    def _router(parent_results, **kw):
        return [{"item": "x"}, {"item": "y"}]

    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        results={"items": ["x", "y"]},
    )
    template = DAGTaskSpec(name="process_item")
    collector = DAGTaskSpec(name="aggregate")
    cb = DynamicFanOutCallback(router_name="test_router", child_template=template, collector=collector)

    state_manager = _make_fanout_state_manager()
    processor = TaskProcessor(state_manager)
    await processor._handle_diagram_fanout(parent, cb)

    # init_fan_in called once (for the 2 leaf IDs)
    state_manager.init_fan_in.assert_awaited_once()
    fan_in_key, leaf_ids = state_manager.init_fan_in.call_args[0][:2]
    assert "dag:fan-in:" in fan_in_key
    assert len(leaf_ids) == 2

    # collector pre-saved
    state_manager.save_task.assert_awaited_once()
    saved = state_manager.save_task.call_args[0][0]
    assert saved.name == "aggregate"

    # two child tasks staged via pipeline
    staged = state_manager._staged_tasks
    assert len(staged) == 2
    assert all(ct.status == TaskStatus.SUBMITTED for ct in staged)
    # per-child params merged in
    items = {ct.parameters.get("item") for ct in staged}
    assert items == {"x", "y"}


@pytest.mark.asyncio
async def test_handle_diagram_fanout_empty_routing_submits_collector():
    """Empty router result → collector submitted immediately, no fan-in init."""
    from jobbers.models.dag import DAGTaskSpec, DynamicFanOutCallback
    from jobbers.registry import register_router

    @register_router("empty_router")
    def _router(parent_results, **kw):
        return []

    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
    )
    template = DAGTaskSpec(name="process_item")
    collector = DAGTaskSpec(name="aggregate")
    cb = DynamicFanOutCallback(router_name="empty_router", child_template=template, collector=collector)

    state_manager = _make_fanout_state_manager()
    processor = TaskProcessor(state_manager)
    await processor._handle_diagram_fanout(parent, cb)

    state_manager.init_fan_in.assert_not_awaited()
    state_manager.submit_task.assert_awaited_once()
    submitted = state_manager.submit_task.call_args[0][0]
    assert submitted.name == "aggregate"


@pytest.mark.asyncio
async def test_handle_diagram_fanout_unknown_router_marks_dropped():
    """Unknown router name marks the parent task as DROPPED."""
    from jobbers.models.dag import DAGTaskSpec, DynamicFanOutCallback

    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
    )
    template = DAGTaskSpec(name="process_item")
    collector = DAGTaskSpec(name="aggregate")
    cb = DynamicFanOutCallback(router_name="no_such_router", child_template=template, collector=collector)

    state_manager = _make_fanout_state_manager()
    processor = TaskProcessor(state_manager)
    await processor._handle_diagram_fanout(parent, cb)

    assert parent.status == TaskStatus.DROPPED
    state_manager.save_task.assert_awaited_once_with(parent)
    state_manager.init_fan_in.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_diagram_fanout_branch_chain():
    """Branch template chain (B --> E --o C): leaf IDs in fan-in set are E's, not B's."""
    from jobbers.models.dag import DAGTaskSpec, DynamicFanOutCallback, SimpleCallback
    from jobbers.registry import register_router

    @register_router("chain_router")
    def _router(parent_results, **kw):
        return [{"val": 1}]

    enrich = DAGTaskSpec(name="enrich_result")
    process = DAGTaskSpec(name="process_chunk", dag_callbacks=[SimpleCallback(task=enrich)])
    collector = DAGTaskSpec(name="aggregate")
    cb = DynamicFanOutCallback(router_name="chain_router", child_template=process, collector=collector)

    parent = Task(
        id="01JQC31AJP7TSA9X8AEP64XG08",
        name="dispatcher",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
    )

    state_manager = _make_fanout_state_manager()
    processor = TaskProcessor(state_manager)
    await processor._handle_diagram_fanout(parent, cb)

    # One child staged (the branch root = process_chunk)
    staged = state_manager._staged_tasks
    assert len(staged) == 1
    assert staged[0].name == "process_chunk"

    # The staged task's dag_callbacks must include a FanInCallback to the collector
    # (the inner chain: process_chunk --> enrich_result --o aggregate).
    # process_chunk has a SimpleCallback to enrich_result; enrich_result has FanInCallback to aggregate.
    assert len(staged[0].dag_callbacks) == 1  # SimpleCallback to enrich_result
    inner_cb = staged[0].dag_callbacks[0]
    assert inner_cb.task.name == "enrich_result"
    assert len(inner_cb.task.dag_callbacks) == 1  # FanInCallback to aggregate
    fan_in_cb = inner_cb.task.dag_callbacks[0]
    assert fan_in_cb.task.name == "aggregate"
