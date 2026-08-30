"""
Reference Python implementation of the stdio subworker SDK contract.

See ``.claude/plans/cross-language-subworker-stdio-design.md`` §8. Run as a child
process by ``StdioSubworkerHandle``::

    python -m jobbers.subworker.bootstrap.stdio_main <task_module>

A non-Python subworker must follow the same wire protocol (``jobbers.subworker.wire``) and
SDK contract but is not implemented here.

Cooperative cancellation needs the process to keep reading stdin *while* a task is
running synchronously on the main thread, so a background thread owns all reads;
dispatches are handed to the main thread over a queue, and a ``cancel`` message for the
in-flight request is translated into ``SIGUSR2`` (POSIX only, per that doc's §7 signal
choice) to interrupt the blocked main thread — mirroring
``MultiprocessingSubworkerHandle``'s own ``SIGUSR1`` mechanism. A task calls
``jobbers.subworker.context.heartbeat()`` to send a heartbeat; only the main thread ever
writes to stdout (heartbeats included), so it's safe to send synchronously from within
the task's own call stack without coordinating with the reader thread.

If ``SUBWORKER_STATUS_FILE`` is set in the environment, the currently-processing request
id (or ``"0"`` while idle) is also written there via ``jobbers.subworker.status_file`` --
an out-of-band signal ``StdioSubworkerHandle.current_request_id()`` reads independently of
stdout, so the parent can confirm a cancel actually took effect even if the ``ResultMsg``
is delayed or never arrives. Optional: a non-Python subworker that skips this just means
its parent always waits out the full grace period before killing it on cancel/timeout.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import os
import queue
import signal
import sys
import threading
import traceback
from typing import TYPE_CHECKING, Any, cast

from jobbers.registry import get_task_config
from jobbers.subworker import status_file, wire
from jobbers.subworker.context import _current_heartbeat_sender
from jobbers.subworker.errors import TaskCancelledError
from jobbers.utils.module_loading import load_task_module

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from types import FrameType

logger = logging.getLogger(__name__)

# `getattr` (rather than `signal.SIGUSR2` directly) because the attribute doesn't exist
# in the `signal` module at all on Windows, not just at runtime but for static typing too.
_SIGUSR2: int | None = getattr(signal, "SIGUSR2", None)
_RETIRE = object()  # sentinel pushed onto the dispatch queue on EOF


def _run_task(task_name: str, task_version: int, kwargs: dict[str, Any]) -> Any:
    task_config = get_task_config(task_name, task_version)
    if task_config is None:
        raise LookupError(f"Unknown task {task_name} v{task_version}")
    result: Any = task_config.function(**kwargs)
    if inspect.isawaitable(result):
        result = asyncio.run(cast("Coroutine[Any, Any, Any]", result))
    return result


def _reader_loop(stdin: Any, dispatch_queue: queue.Queue[Any], state: dict[str, str | None]) -> None:
    while True:
        try:
            payload = wire.read_frame_sync(stdin)
        except ConnectionError:
            payload = None
        if payload is None:
            # EOF: the parent retired us. Let the main thread finish any in-flight
            # dispatch first — it only sees this sentinel once it's idle again.
            dispatch_queue.put(_RETIRE)
            return
        msg_type = payload.get("type")
        if msg_type == "dispatch":
            dispatch_queue.put(payload)
        elif msg_type == "cancel":
            if _SIGUSR2 is not None and payload.get("request_id") == state["current_request_id"]:
                os.kill(os.getpid(), _SIGUSR2)
        else:
            logger.warning("Ignoring unknown message type: %r", msg_type)


def _send_result(stdout: Any, request_id: str, ok: bool, result: Any, error: dict[str, Any] | None) -> None:
    wire.write_frame_sync(
        stdout, {"type": "result", "request_id": request_id, "ok": ok, "result": result, "error": error}
    )


def main(task_module: str) -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    load_task_module(task_module)

    status_file_path = os.environ.get("SUBWORKER_STATUS_FILE")

    state: dict[str, str | None] = {"current_request_id": None}

    def _on_cancel_signal(signum: int, frame: FrameType | None) -> None:
        del signum, frame
        if state["current_request_id"] is not None:
            raise TaskCancelledError("cancelled by parent")

    if _SIGUSR2 is not None:
        signal.signal(_SIGUSR2, _on_cancel_signal)

    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    dispatch_queue: queue.Queue[Any] = queue.Queue()
    reader = threading.Thread(target=_reader_loop, args=(stdin, dispatch_queue, state), daemon=True)
    reader.start()

    while True:
        item = dispatch_queue.get()
        if item is _RETIRE:
            break
        request_id = item["request_id"]
        state["current_request_id"] = request_id
        if status_file_path is not None:
            status_file.write_status(status_file_path, request_id)

        def _send_heartbeat(rid: str = request_id) -> None:
            wire.write_frame_sync(stdout, {"type": "heartbeat", "request_id": rid})

        heartbeat_token = _current_heartbeat_sender.set(_send_heartbeat)
        try:
            result = _run_task(item["task_name"], item["task_version"], item.get("kwargs") or {})
        except TaskCancelledError as exc:
            _send_result(
                stdout,
                request_id,
                False,
                None,
                {
                    "error_type": "TaskCancelledError",
                    "message": str(exc),
                    "traceback_text": traceback.format_exc(),
                    "retryable": True,
                },
            )
        except Exception as exc:
            _send_result(
                stdout,
                request_id,
                False,
                None,
                {
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                    "traceback_text": traceback.format_exc(),
                    "retryable": None,
                },
            )
        else:
            _send_result(stdout, request_id, True, result, None)
        finally:
            _current_heartbeat_sender.reset(heartbeat_token)
            state["current_request_id"] = None
            if status_file_path is not None:
                status_file.write_status(status_file_path, None)


def run() -> None:
    parser = argparse.ArgumentParser(description="Jobbers stdio subworker (reference Python implementation)")
    parser.add_argument("task_module", help="Task module to load (dotted name or file path)")
    args = parser.parse_args()
    main(args.task_module)


if __name__ == "__main__":
    run()
