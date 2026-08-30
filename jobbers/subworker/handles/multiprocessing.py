"""
The vanilla, Python-only ``SubworkerHandleProtocol`` implementation.

Wraps ``multiprocessing.Process`` (spawn start method) and ``multiprocessing.Pipe()``,
with ``pickle`` on the wire — a direct port of ``sync-task-subworker-design.md``
§4.2/§4.5, expressed against ``SubworkerHandleProtocol``. See
``.claude/plans/subworker-handle-protocol-design.md`` §3.

``dispatch()``/``recv()`` exchange already-decoded Python values; pickling is entirely
internal to this module and never crosses the protocol boundary.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import multiprocessing
import multiprocessing.connection
import os
import signal
import tempfile
import traceback
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from jobbers.registry import get_task_config
from jobbers.subworker import status_file
from jobbers.subworker.context import _current_heartbeat_sender
from jobbers.subworker.errors import TaskCancelledError
from jobbers.subworker.protocols import (
    HeartbeatMsg,
    ResultMsg,
    SubworkerExited,
    SubworkerMessage,
    SubworkerTaskError,
)
from jobbers.utils.module_loading import load_task_module

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from multiprocessing.process import BaseProcess
    from types import FrameType

logger = logging.getLogger(__name__)

# Windows has no arbitrary per-process signal delivery; the soft-cancel phase is skipped
# there entirely and cancellation goes straight to the grace-period kill() backstop.
# `getattr` (rather than `signal.SIGUSR1` directly) because the attribute doesn't exist
# in the `signal` module at all on Windows, not just at runtime but for static typing too.
_SIGUSR1: int | None = getattr(signal, "SIGUSR1", None)


class _DispatchMsg(NamedTuple):
    """Wire message, parent -> child. Never exposed outside this module."""

    request_id: str
    task_name: str
    task_version: int
    kwargs: dict[str, Any]


class _RetireMsg(NamedTuple):
    """Poison pill, parent -> child: exit 0 once idle instead of waiting for another dispatch."""


class MultiprocessingSubworkerHandle:
    """One dedicated ``multiprocessing.Process`` + ``Pipe()``, reused across dispatches."""

    def __init__(self, task_module: str) -> None:
        """``task_module`` is re-imported in the child, mirroring ``worker_proc.py``'s own load step."""
        self._task_module = task_module
        self._ctx = multiprocessing.get_context("spawn")
        # `Connection` vs. `PipeConnection` differ by platform in typeshed (POSIX socket-backed
        # vs. Windows named-pipe-backed); `Any` sidesteps that split rather than fighting it.
        self._conn: Any = None
        self._process: BaseProcess | None = None
        self._reaped = False
        fd, self._status_file = tempfile.mkstemp(prefix="jobbers-subworker-status-", suffix=".txt")
        os.close(fd)
        status_file.write_status(self._status_file, None)

    @property
    def pid(self) -> int | None:
        if self._process is None or self._reaped:
            return None
        return self._process.pid

    async def start(self) -> None:
        parent_conn, child_conn = self._ctx.Pipe(duplex=True)
        process = self._ctx.Process(
            target=_bootstrap_main,
            args=(child_conn, self._task_module, self._status_file),
            daemon=True,
        )
        process.start()
        child_conn.close()  # the child owns this end now; drop the parent's duplicate handle
        self._conn = parent_conn
        self._process = process

    async def current_request_id(self) -> str | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, status_file.read_status, self._status_file)

    async def dispatch(
        self, request_id: str, task_name: str, task_version: int, kwargs: dict[str, Any]
    ) -> None:
        conn = self._conn
        if conn is None:
            raise RuntimeError("start() must be called before dispatch()")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, conn.send, _DispatchMsg(request_id, task_name, task_version, kwargs))

    async def recv(self) -> SubworkerMessage:
        if self._conn is None or self._process is None:
            raise RuntimeError("start() must be called before recv()")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._recv_blocking)

    def _recv_blocking(self) -> SubworkerMessage:
        conn = self._conn
        process = self._process
        if conn is None or process is None:
            raise RuntimeError("start() must be called before recv()")
        ready = multiprocessing.connection.wait([conn, process.sentinel])
        if conn in ready:
            try:
                msg = conn.recv()
            except (EOFError, OSError):
                return self._mark_exited()
            if isinstance(msg, HeartbeatMsg | ResultMsg):
                return msg
            raise RuntimeError(f"unexpected message from subworker: {msg!r}")
        # Only the sentinel fired: the process exited without a final message in flight.
        return self._mark_exited()

    def _mark_exited(self) -> SubworkerExited:
        process = self._process
        if process is None:
            raise RuntimeError("no process to reap")
        # The pipe closing (EOF) can be observed a hair before the OS finishes tearing
        # down the process; join() without a timeout blocks only as long as that gap,
        # which is why this is safe to call from a recv() the caller is already awaiting.
        process.join()
        self._reaped = True
        with contextlib.suppress(OSError):
            os.remove(self._status_file)
        return SubworkerExited(exit_code=process.exitcode)

    async def cancel(self, request_id: str) -> None:
        del request_id  # each subworker runs exactly one task at a time; no target ambiguity
        if self._process is None or self._reaped or _SIGUSR1 is None:
            return
        pid = self._process.pid
        if pid is None:
            return
        with contextlib.suppress(ProcessLookupError, OSError):
            os.kill(pid, _SIGUSR1)

    async def retire(self) -> None:
        conn = self._conn
        if conn is None or self._reaped:
            return
        loop = asyncio.get_running_loop()
        with contextlib.suppress(OSError, BrokenPipeError):
            await loop.run_in_executor(None, conn.send, _RetireMsg())

    async def kill(self, grace_period: float) -> int | None:
        process = self._process
        if process is None:
            return None
        loop = asyncio.get_running_loop()
        if process.is_alive():
            process.terminate()
            await loop.run_in_executor(None, process.join, grace_period)
        if process.is_alive():
            process.kill()
            await loop.run_in_executor(None, process.join)
        self._reaped = True
        if self._conn is not None:
            with contextlib.suppress(OSError):
                self._conn.close()
        with contextlib.suppress(OSError):
            os.remove(self._status_file)
        return process.exitcode


def _bootstrap_main(conn: Any, task_module: str, status_file_path: str) -> None:
    """Child process entry point (the ``spawn`` target) — must stay importable at module level."""
    state: dict[str, str | None] = {"current_request_id": None}

    def _on_cancel_signal(signum: int, frame: FrameType | None) -> None:
        del signum, frame
        if state["current_request_id"] is not None:
            raise TaskCancelledError("cancelled by parent")

    if _SIGUSR1 is not None:
        signal.signal(_SIGUSR1, _on_cancel_signal)

    try:
        load_task_module(task_module)
    except Exception:
        logger.exception("Subworker failed to load task module %r", task_module)
        conn.close()
        return

    while True:
        try:
            msg = conn.recv()
        except EOFError:
            break
        if isinstance(msg, _RetireMsg):
            break
        if not isinstance(msg, _DispatchMsg):
            logger.warning("Ignoring unexpected message from parent: %r", msg)
            continue
        state["current_request_id"] = msg.request_id
        status_file.write_status(status_file_path, msg.request_id)

        def _send_heartbeat(rid: str = msg.request_id) -> None:
            conn.send(HeartbeatMsg(rid))

        heartbeat_token = _current_heartbeat_sender.set(_send_heartbeat)
        try:
            result = _run_task(msg.task_name, msg.task_version, msg.kwargs)
        except TaskCancelledError as exc:
            conn.send(
                ResultMsg(
                    msg.request_id,
                    False,
                    None,
                    SubworkerTaskError("TaskCancelledError", str(exc), traceback.format_exc(), True),
                )
            )
        except Exception as exc:
            conn.send(
                ResultMsg(
                    msg.request_id,
                    False,
                    None,
                    SubworkerTaskError(type(exc).__name__, str(exc), traceback.format_exc(), None),
                )
            )
        else:
            conn.send(ResultMsg(msg.request_id, True, result, None))
        finally:
            _current_heartbeat_sender.reset(heartbeat_token)
            state["current_request_id"] = None
            status_file.write_status(status_file_path, None)
    conn.close()


def _run_task(task_name: str, task_version: int, kwargs: dict[str, Any]) -> Any:
    task_config = get_task_config(task_name, task_version)
    if task_config is None:
        raise LookupError(f"Unknown task {task_name} v{task_version}")
    result: Any = task_config.function(**kwargs)
    if inspect.isawaitable(result):
        result = asyncio.run(cast("Coroutine[Any, Any, Any]", result))
    return result
