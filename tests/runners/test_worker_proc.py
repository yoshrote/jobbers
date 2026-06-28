"""Unit tests for jobbers/runners/worker_proc.py."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.models.task_status import TaskStatus
from jobbers.runners.worker_proc import main

# ── main ──────────────────────────────────────────────────────────────────────


async def _run_cancel_listener_forever() -> None:
    await asyncio.sleep(10000)


def _make_state_manager() -> MagicMock:
    sm = MagicMock()
    sm.run_cancel_listener = MagicMock(side_effect=_run_cancel_listener_forever)
    return sm


@pytest.mark.asyncio
async def test_main_processes_tasks_until_exhausted():
    """main() pulls tasks from the generator and processes each one."""
    task = Task(id=ULID(), name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)

    process_calls: list[object] = []

    async def fake_run(t: Task) -> None:
        process_calls.append(t)

    mock_processor = MagicMock()
    mock_processor.run = fake_run

    state_manager = _make_state_manager()

    with (
        patch("jobbers.runners.worker_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.worker_proc.TaskGenerator") as MockGen,
        patch("jobbers.runners.worker_proc.TaskProcessor", return_value=mock_processor),
    ):
        gen_instance = MagicMock()
        gen_instance.queues = AsyncMock(return_value={"default"})
        gen_instance.stop = MagicMock()

        gen_instance.__anext__ = AsyncMock(side_effect=[task, StopAsyncIteration()])
        MockGen.return_value = gen_instance

        await main("dummy_task_module")

    assert len(process_calls) == 1
    assert process_calls[0] is task


@pytest.mark.asyncio
async def test_main_respects_worker_ttl_env_var(monkeypatch):
    """WORKER_TTL is passed as max_tasks to TaskGenerator."""
    monkeypatch.setenv("WORKER_TTL", "7")
    state_manager = _make_state_manager()

    with (
        patch("jobbers.runners.worker_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.worker_proc.TaskGenerator") as MockGen,
    ):
        gen_instance = MagicMock()
        gen_instance.queues = AsyncMock(return_value={"default"})
        gen_instance.stop = MagicMock()
        gen_instance.__anext__ = AsyncMock(side_effect=StopAsyncIteration)
        MockGen.return_value = gen_instance

        await main("dummy_task_module")

    _, kwargs = MockGen.call_args
    assert kwargs.get("max_tasks") == 7 or MockGen.call_args[0][2] == 7


@pytest.mark.asyncio
async def test_main_cancels_active_tasks_on_stop():
    """When the generator is exhausted active asyncio tasks are gathered."""
    state_manager = _make_state_manager()

    with (
        patch("jobbers.runners.worker_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.worker_proc.TaskGenerator") as MockGen,
    ):
        gen_instance = MagicMock()
        gen_instance.queues = AsyncMock(return_value={"default"})
        gen_instance.stop = MagicMock()
        gen_instance.__anext__ = AsyncMock(side_effect=StopAsyncIteration)
        MockGen.return_value = gen_instance

        await main("dummy_task_module")  # should not raise

    gen_instance.stop.assert_called_once()


# ── sync-task backpressure (requeue when no subprocess slot is free) ──────────


def _sync_task_config() -> TaskConfig:
    async def _noop(**_kwargs: object) -> None:  # pragma: no cover
        pass

    return TaskConfig(name="sync_t", version=1, function=_noop, is_sync=True)


def _async_task_config() -> TaskConfig:
    async def _noop(**_kwargs: object) -> None:  # pragma: no cover
        pass

    return TaskConfig(name="async_t", version=1, function=_noop, is_sync=False)


@pytest.mark.asyncio
async def test_sync_task_requeued_when_no_subprocess_slot_free(monkeypatch):
    """A sync task pulled while WORKER_SYNC_PROCESSES is saturated is requeued, not run."""
    monkeypatch.setenv("WORKER_CONCURRENT_TASKS", "1")
    monkeypatch.setenv("WORKER_SYNC_PROCESSES", "0")  # Semaphore(0) is always locked

    task_a = Task(id=ULID(), name="sync_t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    task_b = Task(id=ULID(), name="sync_t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    state_manager = _make_state_manager()
    state_manager.requeue_task = AsyncMock()

    with (
        patch("jobbers.runners.worker_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.worker_proc.TaskGenerator") as MockGen,
        patch("jobbers.runners.worker_proc.TaskProcessor") as MockProcessor,
        patch("jobbers.runners.worker_proc.get_task_config", return_value=_sync_task_config()),
        patch("jobbers.runners.worker_proc.asyncio.sleep", new=AsyncMock()) as mock_sleep,
    ):
        gen_instance = MagicMock()
        gen_instance.queues = AsyncMock(return_value={"default"})
        gen_instance.stop = MagicMock()
        gen_instance.__anext__ = AsyncMock(side_effect=[task_a, task_b, StopAsyncIteration()])
        MockGen.return_value = gen_instance

        # If the outer semaphore weren't released on the requeue path, the second
        # acquire() would hang forever with WORKER_CONCURRENT_TASKS=1 — bound the
        # whole call so a regression fails the test instead of hanging the suite.
        await asyncio.wait_for(main("dummy_task_module"), timeout=5)

    assert state_manager.requeue_task.await_count == 2
    state_manager.requeue_task.assert_any_await(task_a)
    state_manager.requeue_task.assert_any_await(task_b)
    MockProcessor.assert_not_called()
    assert mock_sleep.await_count == 2


@pytest.mark.asyncio
async def test_async_task_runs_normally_without_requeue():
    """A non-sync task is dispatched to TaskProcessor as before; no requeue happens."""
    task = Task(id=ULID(), name="async_t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    state_manager = _make_state_manager()
    state_manager.requeue_task = AsyncMock()

    process_calls: list[object] = []

    async def fake_run(t: Task) -> None:
        process_calls.append(t)

    mock_processor = MagicMock()
    mock_processor.run = fake_run

    with (
        patch("jobbers.runners.worker_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.worker_proc.TaskGenerator") as MockGen,
        patch("jobbers.runners.worker_proc.TaskProcessor", return_value=mock_processor),
        patch("jobbers.runners.worker_proc.get_task_config", return_value=_async_task_config()),
    ):
        gen_instance = MagicMock()
        gen_instance.queues = AsyncMock(return_value={"default"})
        gen_instance.stop = MagicMock()
        gen_instance.__anext__ = AsyncMock(side_effect=[task, StopAsyncIteration()])
        MockGen.return_value = gen_instance

        await main("dummy_task_module")

    assert process_calls == [task]
    state_manager.requeue_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_task_runs_normally_when_slot_is_free():
    """A sync task proceeds to TaskProcessor when WORKER_SYNC_PROCESSES has room."""
    task = Task(id=ULID(), name="sync_t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    state_manager = _make_state_manager()
    state_manager.requeue_task = AsyncMock()

    process_calls: list[object] = []

    async def fake_run(t: Task) -> None:
        process_calls.append(t)

    mock_processor = MagicMock()
    mock_processor.run = fake_run

    with (
        patch("jobbers.runners.worker_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.worker_proc.TaskGenerator") as MockGen,
        patch("jobbers.runners.worker_proc.TaskProcessor", return_value=mock_processor),
        patch("jobbers.runners.worker_proc.get_task_config", return_value=_sync_task_config()),
    ):
        gen_instance = MagicMock()
        gen_instance.queues = AsyncMock(return_value={"default"})
        gen_instance.stop = MagicMock()
        gen_instance.__anext__ = AsyncMock(side_effect=[task, StopAsyncIteration()])
        MockGen.return_value = gen_instance

        await main("dummy_task_module")  # default WORKER_SYNC_PROCESSES=2 has room

    assert process_calls == [task]
    state_manager.requeue_task.assert_not_awaited()
