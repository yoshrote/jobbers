"""Unit tests for jobbers/runners/worker_proc.py."""

import asyncio
import os
import signal
import sys
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.models.task_status import TaskStatus
from jobbers.runners.worker_proc import _load_task_module, _run_cancel_listener_supervised, main, run

# ── _load_task_module ─────────────────────────────────────────────────────────


def test_load_task_module_by_dotted_name():
    """A dotted module name is imported via importlib.import_module."""
    with patch("importlib.import_module") as mock_import:
        _load_task_module("some.module.path")
    mock_import.assert_called_once_with("some.module.path")


def test_load_task_module_by_file_path():
    """An absolute .py path is loaded as a module from file."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
        f.write("LOADED = True\n")
        path = f.name

    _load_task_module(path)
    assert "_user_tasks" in sys.modules
    assert sys.modules["_user_tasks"].LOADED is True

    del sys.modules["_user_tasks"]


def test_load_task_module_by_relative_py_extension():
    """A path ending in .py (relative) is also loaded from file."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
        f.write("VALUE = 42\n")
        path = f.name

    _load_task_module(path)
    assert sys.modules["_user_tasks"].VALUE == 42
    del sys.modules["_user_tasks"]


def test_load_task_module_invalid_path_raises():
    """A non-existent absolute path raises ImportError."""
    with pytest.raises((ImportError, FileNotFoundError)):
        _load_task_module("/nonexistent/path/tasks.py")


# ── _run_cancel_listener_supervised ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_cancel_listener_supervised_restarts_after_unexpected_failure(caplog):
    """
    The listener is restarted (with a logged error) if it dies with anything but CancelledError.

    Regression test: previously the worker created the cancel-listener task and never
    supervised it, so an unexpected failure (e.g. a dropped Redis pub/sub connection) silently
    disabled task cancellation for the rest of the worker's life with no log and no restart.
    """
    calls = 0

    async def flaky_listener() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("pubsub connection dropped")
        raise asyncio.CancelledError

    state_manager = MagicMock()
    state_manager.run_cancel_listener = AsyncMock(side_effect=flaky_listener)

    with (
        patch("jobbers.runners.worker_proc.asyncio.sleep", new=AsyncMock()),
        pytest.raises(asyncio.CancelledError),
    ):
        await _run_cancel_listener_supervised(state_manager)

    assert calls == 2
    assert "Cancellation listener crashed" in caplog.text


@pytest.mark.asyncio
async def test_run_cancel_listener_supervised_propagates_cancellation_immediately():
    """A CancelledError on the first attempt propagates without being treated as a crash."""
    state_manager = MagicMock()
    state_manager.run_cancel_listener = AsyncMock(side_effect=asyncio.CancelledError)

    with pytest.raises(asyncio.CancelledError):
        await _run_cancel_listener_supervised(state_manager)

    state_manager.run_cancel_listener.assert_awaited_once()


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

        await main()

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

        await main()

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

        await main()  # should not raise


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform == "win32", reason="loop.add_signal_handler is POSIX-only")
async def test_main_sigterm_respects_on_shutdown_policy():
    """
    On SIGTERM shutdown, active tasks are cancelled unless on_shutdown is CONTINUE.

    CONTINUE-policy tasks are left running to completion instead.
    """
    stop_task = Task(id=ULID(), name="stop_task", version=1, queue="default", status=TaskStatus.STARTED)
    stop_task.task_config = TaskConfig(
        name="stop_task", version=1, function=AsyncMock(), on_shutdown=TaskShutdownPolicy.STOP
    )
    continue_task = Task(
        id=ULID(), name="continue_task", version=1, queue="default", status=TaskStatus.STARTED
    )
    continue_task.task_config = TaskConfig(
        name="continue_task", version=1, function=AsyncMock(), on_shutdown=TaskShutdownPolicy.CONTINUE
    )

    stop_cancelled = asyncio.Event()
    continue_completed = asyncio.Event()
    both_active = asyncio.Event()
    seen: set[object] = set()

    async def fake_run(task: Task) -> None:
        seen.add(task.id)
        if len(seen) == 2:
            both_active.set()
        if task is stop_task:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                stop_cancelled.set()
                raise
        else:
            await asyncio.sleep(0.2)
            continue_completed.set()

    mock_processor = MagicMock()
    mock_processor.run = fake_run

    state_manager = _make_state_manager()
    remaining = iter([stop_task, continue_task])

    async def fake_anext() -> Task:
        try:
            return next(remaining)
        except StopIteration:
            await asyncio.sleep(10000)  # simulate blocking on an empty queue
            raise AssertionError("should have been cancelled by shutdown")  # pragma: no cover

    gen_instance = MagicMock()
    gen_instance.queues = AsyncMock(return_value={"default"})
    gen_instance.stop = MagicMock()
    gen_instance.__anext__ = fake_anext

    with (
        patch("jobbers.runners.worker_proc.db.init_state_manager", return_value=state_manager),
        patch("jobbers.runners.worker_proc.TaskGenerator", return_value=gen_instance),
        patch("jobbers.runners.worker_proc.TaskProcessor", return_value=mock_processor),
    ):
        main_task = asyncio.create_task(main())
        await asyncio.wait_for(both_active.wait(), timeout=2)
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(main_task, timeout=2)

    assert stop_cancelled.is_set()
    assert continue_completed.is_set()

    gen_instance.stop.assert_called_once()


# ── run() otel shutdown ───────────────────────────────────────────────────────


def test_run_calls_shutdown_otel_even_on_failure():
    """run() must flush/shut down otel providers even if asyncio.run() raises."""
    with (
        patch("sys.argv", ["jobbers_worker", "os"]),
        patch("jobbers.runners.worker_proc.enable_otel"),
        patch("jobbers.runners.worker_proc.asyncio.run", side_effect=RuntimeError("boom")),
        patch("jobbers.runners.worker_proc.shutdown_otel") as mock_shutdown,
        pytest.raises(RuntimeError, match="boom"),
    ):
        run()

    mock_shutdown.assert_called_once()
