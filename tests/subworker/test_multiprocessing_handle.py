"""Integration tests for MultiprocessingSubworkerHandle — spawns real subprocesses."""

import asyncio
import os
import signal
import uuid

import pytest

from jobbers.subworker.handles.multiprocessing import MultiprocessingSubworkerHandle
from jobbers.subworker.protocols import HeartbeatMsg, ResultMsg, SubworkerExited

TASK_MODULE = os.path.join(os.path.dirname(__file__), "..", "fixtures", "subworker_tasks.py")


@pytest.mark.asyncio
async def test_dispatch_returns_result():
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
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
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
    await handle.start()
    try:
        request_id = str(uuid.uuid4())
        await handle.dispatch(request_id, "subworker_fail", 1, {"message": "boom"})
        msg = await handle.recv()
        assert isinstance(msg, ResultMsg)
        assert msg.request_id == request_id
        assert msg.ok is False
        assert msg.result is None
        assert msg.error is not None
        assert msg.error.error_type == "ValueError"
        assert msg.error.message == "boom"
    finally:
        await handle.kill(grace_period=2)


@pytest.mark.asyncio
async def test_handle_is_reused_across_dispatches():
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
    await handle.start()
    pid = handle.pid
    try:
        for i in range(3):
            await handle.dispatch(str(uuid.uuid4()), "subworker_add", 1, {"a": i, "b": 1})
            msg = await handle.recv()
            assert isinstance(msg, ResultMsg)
            assert msg.result == i + 1
        assert handle.pid == pid  # same process the whole time
    finally:
        await handle.kill(grace_period=2)


@pytest.mark.asyncio
async def test_retire_exits_cleanly_once_idle():
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
    await handle.start()
    await handle.retire()
    msg = await handle.recv()
    assert isinstance(msg, SubworkerExited)
    assert msg.exit_code == 0
    assert handle.pid is None


@pytest.mark.asyncio
async def test_kill_terminates_idle_process():
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
    await handle.start()
    exit_code = await handle.kill(grace_period=2)
    assert exit_code is not None
    assert handle.pid is None


@pytest.mark.asyncio
async def test_pid_is_none_before_start():
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
    assert handle.pid is None


@pytest.mark.asyncio
async def test_recv_detects_external_crash():
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
    await handle.start()
    pid = handle.pid
    assert pid is not None
    await handle.dispatch(str(uuid.uuid4()), "subworker_sleep", 1, {"seconds": 5})
    os.kill(pid, signal.SIGTERM)
    msg = await handle.recv()
    assert isinstance(msg, SubworkerExited)
    assert handle.pid is None


@pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="SIGUSR1 is POSIX-only")
@pytest.mark.asyncio
async def test_cancel_interrupts_in_flight_task():
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
    await handle.start()
    try:
        request_id = str(uuid.uuid4())
        await handle.dispatch(request_id, "subworker_sleep", 1, {"seconds": 5})
        # Give the child a moment to actually be inside time.sleep() before signalling.
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
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
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


@pytest.mark.asyncio
async def test_heartbeat_calls_are_delivered_before_the_result():
    """A task calling jobbers.subworker.context.heartbeat() sends HeartbeatMsg frames first."""
    handle = MultiprocessingSubworkerHandle(TASK_MODULE)
    await handle.start()
    try:
        request_id = str(uuid.uuid4())
        await handle.dispatch(request_id, "subworker_heartbeat_then_done", 1, {"count": 3})
        msgs = [await handle.recv() for _ in range(4)]
        assert msgs[:3] == [HeartbeatMsg(request_id)] * 3
        assert isinstance(msgs[3], ResultMsg)
        assert msgs[3].ok is True
        assert msgs[3].result == "done"
    finally:
        await handle.kill(grace_period=2)

