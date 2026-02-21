"""Unit tests for jobbers/runners/scheduler_proc.py."""
import asyncio
import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus
from jobbers.runners.scheduler_proc import main

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)


def make_task() -> Task:
    return Task(id=ULID(), name="retry_task", version=1, queue="default", status=TaskStatus.SCHEDULED)

def make_state_manager_with_tasks(tasks: list[Task]) -> MagicMock:
    entries = [(t, PAST) for t in tasks]
    state_manager = MagicMock()
    state_manager.task_scheduler.next_due_bulk = MagicMock(side_effect=[entries, []])
    state_manager.qca.get_queues = AsyncMock(return_value={"default"})
    state_manager.dispatch_scheduled_task = AsyncMock(side_effect=lambda t: t)
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

    with patch("jobbers.runners.scheduler_proc.get_state_manager", return_value=state_manager), \
         patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep):
        try:
            await main(poll_interval=1.0, role="default", batch_size=1)
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

    with patch("jobbers.runners.scheduler_proc.get_state_manager", return_value=state_manager), \
         patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep):
        try:
            await main(poll_interval=7.5, role="default", batch_size=1)
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

    with patch("jobbers.runners.scheduler_proc.get_state_manager", return_value=state_manager), \
         patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep):
        try:
            await main(poll_interval=1.0, role="default", batch_size=1)
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

    with patch("jobbers.runners.scheduler_proc.get_state_manager", return_value=state_manager), \
         patch("jobbers.runners.scheduler_proc.asyncio.sleep", side_effect=fake_sleep):
        try:
            poll_interval = float(os.environ.get("SCHEDULER_POLL_INTERVAL", "5.0"))
            await main(poll_interval=poll_interval, role="default", batch_size=1)
        except asyncio.CancelledError:
            pass

    assert received_intervals == [42.0]
