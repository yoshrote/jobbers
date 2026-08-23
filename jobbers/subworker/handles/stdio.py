"""
The language-agnostic ``SubworkerHandleProtocol`` implementation.

Wraps ``subprocess.Popen``-style stdio (via ``asyncio.create_subprocess_exec``) with
length-prefixed MessagePack frames — a direct port of
``cross-language-subworker-stdio-design.md`` §3-§7, expressed against
``SubworkerHandleProtocol``. See ``.claude/plans/subworker-handle-protocol-design.md`` §3.

The child may be any executable that follows the wire protocol (``jobbers.subworker.wire``)
and the SDK contract in that doc's §8; ``jobbers.subworker.bootstrap.stdio_main`` is this
project's own reference Python implementation of that contract.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from jobbers.subworker import wire
from jobbers.subworker.protocols import (
    HeartbeatMsg,
    ResultMsg,
    SubworkerExited,
    SubworkerMessage,
    SubworkerTaskError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

logger = logging.getLogger(__name__)


class StdioSubworkerHandle:
    """One dedicated child process, addressed exclusively via its own stdin/stdout/stderr."""

    def __init__(
        self, executable: str, args: Sequence[str] = (), *, env: Mapping[str, str] | None = None
    ) -> None:
        self._executable = executable
        self._args = list(args)
        self._env = dict(env) if env is not None else None
        self._proc: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._exited = False

    @property
    def pid(self) -> int | None:
        if self._proc is None or self._exited:
            return None
        return self._proc.pid

    async def start(self) -> None:
        self._proc = await asyncio.create_subprocess_exec(
            self._executable,
            *self._args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._env,
        )
        self._stderr_task = asyncio.create_task(self._drain_stderr())

    async def _drain_stderr(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        while True:
            line = await proc.stderr.readline()
            if not line:
                return
            logger.info("subworker[pid=%s] stderr: %s", proc.pid, line.decode(errors="replace").rstrip())

    async def dispatch(
        self, request_id: str, task_name: str, task_version: int, kwargs: dict[str, Any]
    ) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise RuntimeError("start() must be called before dispatch()")
        proc.stdin.write(
            wire.pack_frame(
                {
                    "type": "dispatch",
                    "request_id": request_id,
                    "task_name": task_name,
                    "task_version": task_version,
                    "kwargs": kwargs,
                }
            )
        )
        await proc.stdin.drain()

    async def recv(self) -> SubworkerMessage:
        proc = self._proc
        if proc is None or proc.stdout is None:
            raise RuntimeError("start() must be called before recv()")
        try:
            payload = await wire.read_frame_async(proc.stdout)
        except ConnectionError:
            payload = None
        if payload is None:
            return await self._on_exit()
        return _decode_message(payload)

    async def _on_exit(self) -> SubworkerExited:
        proc = self._proc
        if proc is None:
            raise RuntimeError("no process to reap")
        self._exited = True
        exit_code = await proc.wait()
        return SubworkerExited(exit_code=exit_code)

    async def cancel(self, request_id: str) -> None:
        proc = self._proc
        if proc is None or self._exited or proc.stdin is None or proc.stdin.is_closing():
            return
        with contextlib.suppress(ConnectionError, OSError):
            proc.stdin.write(wire.pack_frame({"type": "cancel", "request_id": request_id}))
            await proc.stdin.drain()

    async def retire(self) -> None:
        proc = self._proc
        if proc is None or self._exited or proc.stdin is None:
            return
        with contextlib.suppress(OSError):
            proc.stdin.close()

    async def kill(self, grace_period: float) -> int | None:
        proc = self._proc
        if proc is None:
            return None
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=grace_period)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                await proc.wait()
        self._exited = True
        stderr_task = self._stderr_task
        if stderr_task is not None:
            stderr_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stderr_task
        return proc.returncode


def _decode_message(payload: dict[str, Any]) -> SubworkerMessage:
    msg_type = payload.get("type")
    if msg_type == "heartbeat":
        return HeartbeatMsg(request_id=payload["request_id"])
    if msg_type == "result":
        error_payload = payload.get("error")
        error = (
            SubworkerTaskError(
                error_type=error_payload["error_type"],
                message=error_payload["message"],
                traceback_text=error_payload["traceback_text"],
                retryable=error_payload.get("retryable"),
            )
            if error_payload is not None
            else None
        )
        return ResultMsg(
            request_id=payload["request_id"],
            ok=payload["ok"],
            result=payload.get("result"),
            error=error,
        )
    raise RuntimeError(f"unexpected message from subworker: {payload!r}")
