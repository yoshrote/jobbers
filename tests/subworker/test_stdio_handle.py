"""Integration tests for StdioSubworkerHandle — spawns the reference Python bootstrap as a real child."""

import asyncio
import os
import signal
import sys
import uuid

import pytest

from jobbers.subworker.handles.stdio import StdioSubworkerHandle
from jobbers.subworker.protocols import ResultMsg, SubworkerExited

TASK_MODULE = os.path.join(os.path.dirname(__file__), "..", "fixtures", "subworker_tasks.py")


def _make_handle() -> StdioSubworkerHandle:
    return StdioSubworkerHandle(sys.executable, ["-m", "jobbers.subworker.bootstrap.stdio_main", TASK_MODULE])


@pytest.mark.asyncio
async def test_dispatch_returns_result():
    handle = _make_handle()
    await handle.start()
    try:
        assert handle.pid is not None
        await handle.dispatch(str(uuid.uuid4()), "subworker_add", 1, {"a": 2, "b": 3})
        msg = await handle.recv()
        assert isinstance(msg, ResultMsg)
        assert msg.ok is True
        assert msg.result == 5
        assert msg.error is None
    finally:
        await handle.kill(grace_period=2)


@pytest.mark.asyncio
async def test_dispatch_error_is_marshalled():
    handle = _make_handle()
    await handle.start()
    try:
        request_id = str(uuid.uuid4())
        await handle.dispatch(request_id, "subworker_fail", 1, {"message": "boom"})
        msg = await handle.recv()
        assert isinstance(msg, ResultMsg)
        assert msg.request_id == request_id
        assert msg.ok is False
        assert msg.error is not None
        assert msg.error.error_type == "ValueError"
        assert msg.error.message == "boom"
    finally:
        await handle.kill(grace_period=2)


@pytest.mark.asyncio
async def test_handle_is_reused_across_dispatches():
    handle = _make_handle()
    await handle.start()
    pid = handle.pid
    try:
        for i in range(3):
            await handle.dispatch(str(uuid.uuid4()), "subworker_add", 1, {"a": i, "b": 1})
            msg = await handle.recv()
            assert isinstance(msg, ResultMsg)
            assert msg.result == i + 1
        assert handle.pid == pid
    finally:
        await handle.kill(grace_period=2)


@pytest.mark.asyncio
async def test_retire_exits_cleanly_once_idle():
    handle = _make_handle()
    await handle.start()
    await handle.retire()
    msg = await handle.recv()
    assert isinstance(msg, SubworkerExited)
    assert msg.exit_code == 0
    assert handle.pid is None


@pytest.mark.asyncio
async def test_kill_terminates_idle_process():
    handle = _make_handle()
    await handle.start()
    exit_code = await handle.kill(grace_period=2)
    assert exit_code is not None
    assert handle.pid is None


@pytest.mark.asyncio
async def test_pid_is_none_before_start():
    handle = _make_handle()
    assert handle.pid is None


@pytest.mark.asyncio
async def test_recv_detects_external_crash():
    handle = _make_handle()
    await handle.start()
    pid = handle.pid
    assert pid is not None
    await handle.dispatch(str(uuid.uuid4()), "subworker_sleep", 1, {"seconds": 5})
    os.kill(pid, signal.SIGTERM)
    msg = await handle.recv()
    assert isinstance(msg, SubworkerExited)
    assert handle.pid is None


@pytest.mark.skipif(not hasattr(signal, "SIGUSR2"), reason="SIGUSR2 accelerant is POSIX-only")
@pytest.mark.asyncio
async def test_cancel_interrupts_in_flight_task():
    handle = _make_handle()
    await handle.start()
    try:
        request_id = str(uuid.uuid4())
        await handle.dispatch(request_id, "subworker_sleep", 1, {"seconds": 5})
        await asyncio.sleep(0.3)
        await handle.cancel(request_id)
        msg = await handle.recv()
        assert isinstance(msg, ResultMsg)
        assert msg.ok is False
        assert msg.error is not None
        assert msg.error.error_type == "TaskCancelledError"
    finally:
        await handle.kill(grace_period=2)


@pytest.mark.asyncio
async def test_unknown_task_raises_lookup_error_marshalled():
    handle = _make_handle()
    await handle.start()
    try:
        await handle.dispatch(str(uuid.uuid4()), "does_not_exist", 1, {})
        msg = await handle.recv()
        assert isinstance(msg, ResultMsg)
        assert msg.ok is False
        assert msg.error is not None
        assert msg.error.error_type == "LookupError"
    finally:
        await handle.kill(grace_period=2)
