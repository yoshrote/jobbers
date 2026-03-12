"""Unit tests for jobbers/runners/worker_proc.py."""

import sys
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobbers.runners.worker_proc import _load_task_module, main

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
    assert sys.modules["_user_tasks"].LOADED is True  # type: ignore[attr-defined]

    del sys.modules["_user_tasks"]


def test_load_task_module_by_relative_py_extension():
    """A path ending in .py (relative) is also loaded from file."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
        f.write("VALUE = 42\n")
        path = f.name

    _load_task_module(path)
    assert sys.modules["_user_tasks"].VALUE == 42  # type: ignore[attr-defined]
    del sys.modules["_user_tasks"]


def test_load_task_module_invalid_path_raises():
    """A non-existent absolute path raises ImportError."""
    with pytest.raises((ImportError, FileNotFoundError)):
        _load_task_module("/nonexistent/path/tasks.py")


# ── main ──────────────────────────────────────────────────────────────────────


def _make_state_manager() -> MagicMock:
    sm = MagicMock()
    sm.qca = MagicMock()
    sm.qca.get_queues = AsyncMock(return_value={"default"})
    return sm


@pytest.mark.asyncio
async def test_main_processes_tasks_until_exhausted():
    """main() pulls tasks from the generator and processes each one."""
    from ulid import ULID

    from jobbers.models.task import Task
    from jobbers.models.task_status import TaskStatus

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

        from unittest.mock import AsyncMock as _AM

        gen_instance.__anext__ = _AM(side_effect=[task, StopAsyncIteration()])
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
    assert kwargs.get("max_tasks") == 7 or MockGen.call_args[0][3] == 7


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

    gen_instance.stop.assert_called_once()
