"""
Unit tests for SubworkerPool orchestration logic, against an in-memory fake handle.

Fast and deterministic — exercises occupancy tracking, TTL recycling, heartbeat
forwarding, crash-triggered respawn, and cancel/shutdown escalation without spawning
real subprocesses. See test_pool_integration.py for end-to-end coverage against the two
real SubworkerHandleProtocol implementations.
"""

import asyncio
import itertools
from typing import Any

import pytest

from jobbers.subworker.errors import SubworkerCrashedError, SubworkerTaskFailure
from jobbers.subworker.pool import SubworkerPool
from jobbers.subworker.protocols import (
    HeartbeatMsg,
    ResultMsg,
    SubworkerExited,
    SubworkerMessage,
    SubworkerTaskError,
)

_pid_counter = itertools.count(1)


class FakeSubworkerHandle:
    """In-memory SubworkerHandleProtocol implementation for deterministic pool tests."""

    def __init__(self) -> None:
        self.started = False
        self.retired = False
        self.killed = False
        self.dispatches: list[tuple[str, str, int, dict[str, Any]]] = []
        self.cancels: list[str] = []
        self._pid = next(_pid_counter)
        self._exited = False
        self._inbox: asyncio.Queue[SubworkerMessage] = asyncio.Queue()

    @property
    def pid(self) -> int | None:
        return None if self._exited else self._pid

    async def start(self) -> None:
        self.started = True

    async def dispatch(
        self, request_id: str, task_name: str, task_version: int, kwargs: dict[str, Any]
    ) -> None:
        self.dispatches.append((request_id, task_name, task_version, kwargs))

    async def recv(self) -> SubworkerMessage:
        msg = await self._inbox.get()
        if isinstance(msg, SubworkerExited):
            self._exited = True
        return msg

    async def cancel(self, request_id: str) -> None:
        self.cancels.append(request_id)

    async def retire(self) -> None:
        self.retired = True
        if not self._exited:
            self._inbox.put_nowait(SubworkerExited(exit_code=0))

    async def kill(self, grace_period: float) -> int | None:
        self.killed = True
        if not self._exited:
            self._inbox.put_nowait(SubworkerExited(exit_code=137))
        return 137

    # Test-only helpers driving recv() output; not part of the protocol.
    def push_result(
        self, request_id: str, ok: bool, result: Any = None, error: SubworkerTaskError | None = None
    ) -> None:
        self._inbox.put_nowait(ResultMsg(request_id, ok, result, error))

    def push_heartbeat(self, request_id: str) -> None:
        self._inbox.put_nowait(HeartbeatMsg(request_id))

    def crash(self, exit_code: int | None = 1) -> None:
        self._inbox.put_nowait(SubworkerExited(exit_code=exit_code))


def _factory(created: list[FakeSubworkerHandle]):
    def make() -> FakeSubworkerHandle:
        handle = FakeSubworkerHandle()
        created.append(handle)
        return handle

    return make


@pytest.mark.asyncio
async def test_dispatch_returns_result():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()

    task = asyncio.create_task(pool.dispatch("add", 1, {"a": 1, "b": 2}))
    await asyncio.sleep(0)  # let dispatch() reach handle.dispatch()
    request_id, task_name, task_version, kwargs = created[0].dispatches[0]
    assert (task_name, task_version, kwargs) == ("add", 1, {"a": 1, "b": 2})

    created[0].push_result(request_id, True, result=3)
    assert await task == 3
    assert pool.free_slots == 1


@pytest.mark.asyncio
async def test_dispatch_error_raises_subworker_task_failure():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()

    task = asyncio.create_task(pool.dispatch("fail", 1, {}))
    await asyncio.sleep(0)
    request_id = created[0].dispatches[0][0]
    error = SubworkerTaskError("ValueError", "boom", "traceback", None)
    created[0].push_result(request_id, False, error=error)

    with pytest.raises(SubworkerTaskFailure) as exc_info:
        await task
    assert exc_info.value.error == error


@pytest.mark.asyncio
async def test_second_dispatch_waits_for_free_slot():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()

    first = asyncio.create_task(pool.dispatch("a", 1, {}))
    await asyncio.sleep(0)
    second = asyncio.create_task(pool.dispatch("b", 1, {}))
    await asyncio.sleep(0)

    # Only one handle was ever created (size=1), and it only has the first dispatch queued.
    assert len(created) == 1
    assert len(created[0].dispatches) == 1
    assert pool.free_slots == 0

    request_id_1 = created[0].dispatches[0][0]
    created[0].push_result(request_id_1, True, result="first")
    assert await first == "first"

    await asyncio.sleep(0)  # let the freed slot get picked up by the second dispatch
    assert len(created[0].dispatches) == 2
    request_id_2 = created[0].dispatches[1][0]
    created[0].push_result(request_id_2, True, result="second")
    assert await second == "second"


@pytest.mark.asyncio
async def test_heartbeat_forwarded_and_keeps_task_in_flight():
    created: list[FakeSubworkerHandle] = []
    heartbeats: list[str] = []
    pool = SubworkerPool(_factory(created), size=1, on_heartbeat=heartbeats.append)
    await pool.start()

    task = asyncio.create_task(pool.dispatch("slow", 1, {}))
    await asyncio.sleep(0)
    request_id = created[0].dispatches[0][0]

    created[0].push_heartbeat(request_id)
    created[0].push_heartbeat(request_id)
    await asyncio.sleep(0)
    assert not task.done()
    assert heartbeats == [request_id, request_id]

    created[0].push_result(request_id, True, result="done")
    assert await task == "done"


@pytest.mark.asyncio
async def test_crash_while_in_flight_raises_and_respawns():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()

    task = asyncio.create_task(pool.dispatch("slow", 1, {}))
    await asyncio.sleep(0)
    created[0].crash(exit_code=139)

    with pytest.raises(SubworkerCrashedError) as exc_info:
        await task
    assert exc_info.value.exit_code == 139
    assert len(created) == 2  # the dead slot was replaced
    assert pool.free_slots == 1


@pytest.mark.asyncio
async def test_ttl_recycles_handle_after_n_dispatches():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1, ttl=2)
    await pool.start()

    for _ in range(2):
        task = asyncio.create_task(pool.dispatch("a", 1, {}))
        await asyncio.sleep(0)
        request_id = created[0].dispatches[-1][0]
        created[0].push_result(request_id, True, result="ok")
        await task

    assert created[0].retired is True
    assert len(created) == 2  # recycled once the TTL was hit

    task = asyncio.create_task(pool.dispatch("a", 1, {}))
    await asyncio.sleep(0)
    assert len(created[1].dispatches) == 1  # the new dispatch went to the replacement
    request_id = created[1].dispatches[0][0]
    created[1].push_result(request_id, True, result="ok2")
    assert await task == "ok2"


@pytest.mark.asyncio
async def test_cancel_succeeds_cooperatively_within_grace_period():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()

    task = asyncio.create_task(pool.dispatch("slow", 1, {}))
    await asyncio.sleep(0)
    request_id = created[0].dispatches[0][0]

    cancel_task = asyncio.create_task(pool.cancel(request_id, grace_period=1))
    await asyncio.sleep(0)
    assert created[0].cancels == [request_id]

    error = SubworkerTaskError("TaskCancelledError", "cancelled by parent", "", True)
    created[0].push_result(request_id, False, error=error)

    await cancel_task
    assert created[0].killed is False  # cooperative cancel succeeded; no escalation needed
    with pytest.raises(SubworkerTaskFailure):
        await task


@pytest.mark.asyncio
async def test_cancel_escalates_to_kill_on_timeout():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()

    task = asyncio.create_task(pool.dispatch("slow", 1, {}))
    await asyncio.sleep(0)
    request_id = created[0].dispatches[0][0]

    await pool.cancel(request_id, grace_period=0.01)  # nothing ever replies; times out
    assert created[0].killed is True
    with pytest.raises(SubworkerCrashedError):
        await task


@pytest.mark.asyncio
async def test_cancel_of_unknown_request_is_a_noop():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()
    await pool.cancel("does-not-exist", grace_period=1)  # must not raise


@pytest.mark.asyncio
async def test_shutdown_retires_idle_handles():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=2)
    await pool.start()

    await pool.shutdown(grace_period=1)
    assert all(h.retired for h in created)


@pytest.mark.asyncio
async def test_shutdown_cancels_and_kills_busy_handles():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()

    task = asyncio.create_task(pool.dispatch("slow", 1, {}))
    await asyncio.sleep(0)
    request_id = created[0].dispatches[0][0]

    await pool.shutdown(grace_period=1)
    assert created[0].cancels == [request_id]
    assert created[0].killed is True
    with pytest.raises(SubworkerCrashedError):
        await task


@pytest.mark.asyncio
async def test_dispatch_after_shutdown_raises():
    created: list[FakeSubworkerHandle] = []
    pool = SubworkerPool(_factory(created), size=1)
    await pool.start()
    await pool.shutdown(grace_period=1)

    with pytest.raises(RuntimeError):
        await pool.dispatch("a", 1, {})


@pytest.mark.asyncio
async def test_start_twice_raises():
    pool = SubworkerPool(_factory([]), size=1)
    await pool.start()
    with pytest.raises(RuntimeError):
        await pool.start()


def test_size_must_be_positive():
    with pytest.raises(ValueError, match="size must be at least 1"):
        SubworkerPool(_factory([]), size=0)
