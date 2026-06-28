"""Tests for synchronous (CPU-bound) task support — see jobbers/sync_runner.py."""

import asyncio
import importlib
import time

import pytest
from ulid import ULID

from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus
from jobbers.registry import clear_registry
from jobbers.state_manager import UserCancellationError
from jobbers.task_processor import TaskProcessor
from tests.fixtures import sync_tasks
from tests.test_task_processor import _make_state_manager

TASK_MODULE = "tests.fixtures.sync_tasks"


@pytest.fixture(autouse=True)
def ensure_sync_tasks_registered():
    """
    Defensively re-register the fixture module's tasks before each test.

    Other test files clear the global registry in their own teardown
    (`registry.clear_registry()`); since module import only runs decorators once per
    process, clear + reload guarantees these tasks are present regardless of test order.
    """
    clear_registry()
    importlib.reload(sync_tasks)
    return


@pytest.mark.asyncio
async def test_sync_task_happy_path():
    task = Task(id=ULID(), name="sync_happy", version=1, parameters={"x": 21}, status=TaskStatus.SUBMITTED)
    state_manager = _make_state_manager()
    processor = TaskProcessor(state_manager, task_module=TASK_MODULE, sync_semaphore=asyncio.Semaphore(2))

    result = await asyncio.wait_for(processor.process(task), timeout=15)

    assert result.status == TaskStatus.COMPLETED
    assert result.results == {"doubled": 42}


@pytest.mark.asyncio
async def test_sync_task_heartbeat_drained_by_parent():
    task = Task(id=ULID(), name="sync_heartbeat", version=1, status=TaskStatus.SUBMITTED)
    state_manager = _make_state_manager()
    processor = TaskProcessor(state_manager, task_module=TASK_MODULE, sync_semaphore=asyncio.Semaphore(2))

    result = await asyncio.wait_for(processor.process(task), timeout=15)

    assert result.status == TaskStatus.COMPLETED
    # Called once at task start, plus at least once more from the explicit ctx.heartbeat()
    # call inside the subprocess, drained by the parent's heartbeat queue.
    assert state_manager.update_task_heartbeat.call_count >= 2


@pytest.mark.asyncio
async def test_sync_task_cooperative_cancellation():
    """A task that checks ctx.cancelled exits promptly once cancellation is requested."""
    task = Task(id=ULID(), name="sync_cooperative_cancel", version=1, status=TaskStatus.SUBMITTED)
    state_manager = _make_state_manager()

    async def _cancel_after(_task_id: object) -> None:
        await asyncio.sleep(0.3)
        raise UserCancellationError("cancelled")

    state_manager.monitor_task_cancellation.side_effect = _cancel_after
    processor = TaskProcessor(state_manager, task_module=TASK_MODULE, sync_semaphore=asyncio.Semaphore(2))

    start = time.monotonic()
    await asyncio.wait_for(processor.run(task), timeout=15)
    elapsed = time.monotonic() - start

    assert task.status == TaskStatus.CANCELLED
    # The task polls every 0.05s, so it should notice cancellation well under the 10s
    # worst case (200 iterations) if cooperative cancellation is actually working.
    assert elapsed < 5


@pytest.mark.asyncio
async def test_sync_task_forced_termination_when_cancellation_ignored():
    """A task that ignores ctx.cancelled still gets killed via proc.terminate()/.kill()."""
    task = Task(id=ULID(), name="sync_ignores_cancel", version=1, status=TaskStatus.SUBMITTED)
    state_manager = _make_state_manager()

    async def _cancel_after(_task_id: object) -> None:
        await asyncio.sleep(0.3)
        raise UserCancellationError("cancelled")

    state_manager.monitor_task_cancellation.side_effect = _cancel_after
    processor = TaskProcessor(state_manager, task_module=TASK_MODULE, sync_semaphore=asyncio.Semaphore(2))

    start = time.monotonic()
    # If forced termination didn't work, the tight `while True: pass` loop would run
    # forever and this would hang until the outer wait_for timeout fires.
    await asyncio.wait_for(processor.run(task), timeout=15)
    elapsed = time.monotonic() - start

    assert task.status == TaskStatus.CANCELLED
    assert elapsed < 10
