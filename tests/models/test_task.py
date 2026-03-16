import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_config import TaskConfig
from jobbers.models.task_status import TaskStatus

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")


def test_valid_params():
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={},
        results={},
        errors=[],
        status=TaskStatus.UNSUBMITTED,
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
    )

    def task_function(foo: str, bar: int | None = 5) -> None:  # pragma: no cover
        pass

    task.task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
    )
    task.parameters = {"foo": "spam", "bar": 5}
    assert task.valid_task_params()

    task.parameters = {"foo": "spam", "bar": None}
    assert task.valid_task_params()

    task.parameters = {"foo": "spam", "bar": "baz"}
    assert not task.valid_task_params()


# ── valid_task_params: no task_config ─────────────────────────────────────────


def test_valid_task_params_no_task_config():
    """valid_task_params returns True immediately when task_config is None."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    assert task.task_config is None
    assert task.valid_task_params() is True


# ── shutdown ───────────────────────────────────────────────────────────────────


def test_shutdown_no_task_config_is_noop():
    """shutdown() returns immediately when task_config is None."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    task.shutdown()  # should not raise
    assert task.status == TaskStatus.STARTED


def test_shutdown_continue_policy_is_noop():
    """shutdown() with CONTINUE policy leaves the task status unchanged."""
    from jobbers.models.task_shutdown_policy import TaskShutdownPolicy

    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    task.task_config = TaskConfig(name="t", function=noop, on_shutdown=TaskShutdownPolicy.CONTINUE)
    task.shutdown()
    assert task.status == TaskStatus.STARTED


def test_shutdown_resubmit_policy_sets_status_unsubmitted():
    """shutdown() with RESUBMIT policy sets status to UNSUBMITTED without incrementing retry_attempt."""
    from jobbers.models.task_shutdown_policy import TaskShutdownPolicy

    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    task.task_config = TaskConfig(name="t", function=noop, on_shutdown=TaskShutdownPolicy.RESUBMIT)
    before = task.retry_attempt
    task.shutdown()
    assert task.status == TaskStatus.UNSUBMITTED
    assert task.retry_attempt == before  # direct assignment, not set_status


# ── should_retry / should_schedule ───────────────────────────────────────────


def test_should_retry_true_when_retries_remain():
    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, max_retries=3)
    task.retry_attempt = 1
    assert task.should_retry() is True


def test_should_retry_false_when_exhausted():
    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, max_retries=3)
    task.retry_attempt = 3
    assert task.should_retry() is False


def test_should_schedule_true_when_retry_delay_set():
    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, retry_delay=10)
    assert task.should_schedule() is True


def test_should_schedule_false_when_no_retry_delay():
    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, retry_delay=None)
    assert task.should_schedule() is False


# ── summarized ────────────────────────────────────────────────────────────────


def test_summarized_includes_last_error_when_errors_present():
    task = Task(
        id=ULID1,
        name="t",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        errors=["first", "last error"],
    )
    summary = task.summarized()
    assert summary["last_error"] == "last error"


def test_summarized_omits_last_error_when_no_errors():
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    summary = task.summarized()
    assert "last_error" not in summary


# ── heartbeat ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_heartbeat_sets_heartbeat_at_and_calls_adapter():
    """heartbeat() timestamps heartbeat_at and delegates to the task adapter.

    Patches at the jobbers.db level so the real _ta property body (lines 105-107)
    executes, covering both the property and the heartbeat method.
    """
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    mock_adapter = AsyncMock()
    with patch("jobbers.db.get_client", return_value=MagicMock()):
        with patch("jobbers.db.create_task_adapter", return_value=mock_adapter):
            await task.heartbeat()
    assert task.heartbeat_at is not None
    mock_adapter.update_task_heartbeat.assert_awaited_once_with(task)


# ── set_status ────────────────────────────────────────────────────────────────


def test_set_status_started_sets_retried_at_when_already_started():
    """Second STARTED transition sets retried_at instead of started_at."""
    task = Task(
        id=ULID1,
        name="t",
        version=1,
        queue="default",
        status=TaskStatus.SUBMITTED,
        started_at=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
    )
    task.set_status(TaskStatus.STARTED)
    assert task.retried_at is not None


def test_set_status_scheduled_increments_retry_attempt():
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    before = task.retry_attempt
    task.set_status(TaskStatus.SCHEDULED)
    assert task.retry_attempt == before + 1
    assert task.status == TaskStatus.SCHEDULED


def test_set_status_unsubmitted_increments_retry_attempt():
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    before = task.retry_attempt
    task.set_status(TaskStatus.UNSUBMITTED)
    assert task.retry_attempt == before + 1


# ── TaskPagination ─────────────────────────────────────────────────────────────


def test_serialize_start_with_ulid():
    """serialize_start returns the ULID as a string when start is not None."""
    pagination = TaskPagination(queue="default", start=ULID1)
    result = pagination.model_dump(mode="json")
    assert result["start"] == str(ULID1)


def test_serialize_start_with_none():
    """serialize_start returns None when start is None."""
    pagination = TaskPagination(queue="default")
    result = pagination.model_dump(mode="json")
    assert result["start"] is None
