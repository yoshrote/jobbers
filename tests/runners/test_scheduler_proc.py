"""Unit tests for jobbers/runners/scheduler_proc.py."""

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from jobbers.models.cron_dag import CronDAGEntry
from jobbers.models.dag import DAGTaskSpec
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus
from jobbers.runners.scheduler_proc import main

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)


def make_task() -> Task:
    return Task(id=ULID(), name="retry_task", version=1, queue="default", status=TaskStatus.SCHEDULED)


def make_cron_entry(*, enabled: bool = True, cron_expr: str = "0 * * * *") -> CronDAGEntry:
    return CronDAGEntry(
        name="test_cron",
        cron_expr=cron_expr,
        dag_spec=DAGTaskSpec(name="test_task", queue="default"),
        enabled=enabled,
    )


def make_state_manager_with_tasks(
    tasks: list[Task], cron_entries: list | None = None
) -> MagicMock:
    task_side_effect = [(t, PAST) for t in tasks]
    state_manager = MagicMock()
    state_manager.task_scheduler.next_due_bulk = AsyncMock(side_effect=[task_side_effect, []])
    if cron_entries is not None:
        state_manager.cron_dag_scheduler.next_due_bulk = AsyncMock(side_effect=[cron_entries, []])
    else:
        state_manager.cron_dag_scheduler.next_due_bulk = AsyncMock(return_value=[])
    state_manager.get_queues = AsyncMock(return_value={"default"})
    state_manager.dispatch_scheduled_task = AsyncMock(side_effect=lambda t: t)
    state_manager.dispatch_cron_dag = AsyncMock()
    return state_manager


@pytest.mark.asyncio
async def test_scheduler_dispatches_due_task():
    """When next_due() returns a task the runner dispatches it and does not sleep first."""
    task = make_task()
    state_manager = make_state_manager_with_tasks([task])

    sleep_calls: list[float] = []

    async def fake_sleep(interval: float) -> None:
        sleep_calls.append(interval)
        raise asyncio.CancelledError  # stop the loop after first idle

    with (
        patch("jobbers.runners.scheduler_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep),
    ):
        try:
            await main(
                poll_interval=1.0, config_interval=dt.timedelta(minutes=5), role="default", batch_size=1
            )
        except asyncio.CancelledError:
            pass

    state_manager.dispatch_scheduled_task.assert_called_once_with(task)
    # Sleep should only happen after the task was dispatched and the next poll found nothing
    assert sleep_calls == [1.0]


@pytest.mark.asyncio
async def test_scheduler_sleeps_when_no_task():
    """When next_due() returns None the runner sleeps for poll_interval."""
    state_manager = make_state_manager_with_tasks([])

    sleep_calls: list[float] = []

    async def fake_sleep(interval: float) -> None:
        sleep_calls.append(interval)
        raise asyncio.CancelledError  # stop after first sleep

    with (
        patch("jobbers.runners.scheduler_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep),
    ):
        try:
            await main(
                poll_interval=7.5, config_interval=dt.timedelta(minutes=5), role="default", batch_size=1
            )
        except asyncio.CancelledError:
            pass

    state_manager.dispatch_scheduled_task.assert_not_called()
    assert sleep_calls == [7.5]


@pytest.mark.asyncio
async def test_scheduler_runs_multiple_iterations():
    """Mixing due tasks and idle cycles works correctly over several iterations."""
    task1 = make_task()
    task2 = make_task()
    # Sequence: task, task, None (sleep → cancel)
    state_manager = make_state_manager_with_tasks([task1, task2])

    async def fake_sleep(interval: float) -> None:
        raise asyncio.CancelledError  # stop on first idle

    with (
        patch("jobbers.runners.scheduler_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep),
    ):
        try:
            await main(
                poll_interval=1.0, config_interval=dt.timedelta(minutes=5), role="default", batch_size=1
            )
        except asyncio.CancelledError:
            pass

    assert state_manager.dispatch_scheduled_task.call_count == 2


@pytest.mark.asyncio
async def test_scheduler_uses_env_var_poll_interval(monkeypatch):
    """SCHEDULER_POLL_INTERVAL env var controls the sleep duration passed to main()."""
    monkeypatch.setenv("SCHEDULER_POLL_INTERVAL", "42.0")

    state_manager = make_state_manager_with_tasks([])

    received_intervals: list[float] = []

    async def fake_sleep(interval: float) -> None:
        received_intervals.append(interval)
        raise asyncio.CancelledError

    import os

    with (
        patch("jobbers.runners.scheduler_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep),
    ):
        try:
            poll_interval = float(os.environ.get("SCHEDULER_POLL_INTERVAL", "5.0"))
            await main(
                poll_interval=poll_interval,
                config_interval=dt.timedelta(minutes=5),
                role="default",
                batch_size=1,
            )
        except asyncio.CancelledError:
            pass

    assert received_intervals == [42.0]


# ── cron DAG dispatch ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_scheduler_dispatches_enabled_cron_entry():
    """Enabled cron entries are dispatched via dispatch_cron_dag."""
    entry = make_cron_entry(enabled=True)
    state_manager = make_state_manager_with_tasks([], cron_entries=[(entry, PAST)])

    async def fake_sleep(_: float) -> None:
        raise asyncio.CancelledError

    with (
        patch("jobbers.runners.scheduler_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep),
    ):
        try:
            await main(poll_interval=1.0, config_interval=dt.timedelta(minutes=5), role="default", batch_size=1)
        except asyncio.CancelledError:
            pass

    state_manager.dispatch_cron_dag.assert_called_once_with(entry, PAST)


@pytest.mark.asyncio
async def test_scheduler_reschedules_disabled_cron_entry():
    """Disabled cron entries are rescheduled without being dispatched."""
    entry = make_cron_entry(enabled=False, cron_expr="0 * * * *")
    mock_pipe = AsyncMock()
    state_manager = make_state_manager_with_tasks([], cron_entries=[(entry, PAST)])
    state_manager.job_store.pipeline.return_value = mock_pipe

    async def fake_sleep(_: float) -> None:
        raise asyncio.CancelledError

    with (
        patch("jobbers.runners.scheduler_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep),
    ):
        try:
            await main(poll_interval=1.0, config_interval=dt.timedelta(minutes=5), role="default", batch_size=1)
        except asyncio.CancelledError:
            pass

    state_manager.dispatch_cron_dag.assert_not_called()
    state_manager.cron_dag_scheduler.stage_reschedule.assert_called_once()
    mock_pipe.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_scheduler_handles_mixed_enabled_and_disabled_cron_entries():
    """Enabled entries are dispatched and disabled entries are rescheduled in one iteration."""
    enabled_entry = make_cron_entry(enabled=True)
    disabled_entry = make_cron_entry(enabled=False, cron_expr="0 * * * *")
    mock_pipe = AsyncMock()
    state_manager = make_state_manager_with_tasks(
        [], cron_entries=[(enabled_entry, PAST), (disabled_entry, PAST)]
    )
    state_manager.job_store.pipeline.return_value = mock_pipe

    async def fake_sleep(_: float) -> None:
        raise asyncio.CancelledError

    with (
        patch("jobbers.runners.scheduler_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep),
    ):
        try:
            await main(poll_interval=1.0, config_interval=dt.timedelta(minutes=5), role="default", batch_size=1)
        except asyncio.CancelledError:
            pass

    state_manager.dispatch_cron_dag.assert_called_once_with(enabled_entry, PAST)
    state_manager.cron_dag_scheduler.stage_reschedule.assert_called_once()
    mock_pipe.execute.assert_awaited_once()
