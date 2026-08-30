"""Integration tests for SubworkerPool against the two real SubworkerHandleProtocol implementations."""

import asyncio
import os
import sys
from collections.abc import Callable

import pytest

from jobbers.subworker.errors import SubworkerCrashedError, SubworkerTaskFailure
from jobbers.subworker.handles.multiprocessing import MultiprocessingSubworkerHandle
from jobbers.subworker.handles.stdio import StdioSubworkerHandle
from jobbers.subworker.pool import SubworkerPool
from jobbers.subworker.protocols import SubworkerHandleProtocol

TASK_MODULE = os.path.join(os.path.dirname(__file__), "..", "fixtures", "subworker_tasks.py")


def _multiprocessing_factory() -> SubworkerHandleProtocol:
    return MultiprocessingSubworkerHandle(TASK_MODULE)


def _stdio_factory() -> SubworkerHandleProtocol:
    return StdioSubworkerHandle(sys.executable, ["-m", "jobbers.subworker.bootstrap.stdio_main", TASK_MODULE])


@pytest.fixture(params=[_multiprocessing_factory, _stdio_factory], ids=["multiprocessing", "stdio"])
def handle_factory(request: pytest.FixtureRequest) -> Callable[[], SubworkerHandleProtocol]:
    return request.param  # type: ignore[no-any-return]


@pytest.mark.asyncio
async def test_pool_dispatch_round_trip(handle_factory: Callable[[], SubworkerHandleProtocol]) -> None:
    pool = SubworkerPool(handle_factory, size=1)
    await pool.start()
    try:
        result = await pool.dispatch("subworker_add", 1, {"a": 2, "b": 3})
        assert result == 5
    finally:
        await pool.shutdown(grace_period=2)


@pytest.mark.asyncio
async def test_pool_dispatch_error_raises_task_failure(
    handle_factory: Callable[[], SubworkerHandleProtocol],
) -> None:
    pool = SubworkerPool(handle_factory, size=1)
    await pool.start()
    try:
        with pytest.raises(SubworkerTaskFailure) as exc_info:
            await pool.dispatch("subworker_fail", 1, {"message": "boom"})
        assert exc_info.value.error.error_type == "ValueError"
        assert exc_info.value.error.message == "boom"
    finally:
        await pool.shutdown(grace_period=2)


@pytest.mark.asyncio
async def test_pool_reuses_handle_across_dispatches(
    handle_factory: Callable[[], SubworkerHandleProtocol],
) -> None:
    pool = SubworkerPool(handle_factory, size=1)
    await pool.start()
    try:
        for i in range(3):
            result = await pool.dispatch("subworker_add", 1, {"a": i, "b": 1})
            assert result == i + 1
    finally:
        await pool.shutdown(grace_period=2)


@pytest.mark.asyncio
async def test_pool_cancel_escalates_to_kill_and_respawns(
    handle_factory: Callable[[], SubworkerHandleProtocol],
) -> None:
    pool = SubworkerPool(handle_factory, size=1)
    await pool.start()
    try:
        dispatch_task = asyncio.create_task(pool.dispatch("subworker_sleep", 1, {"seconds": 5}))
        await asyncio.sleep(0.3)
        # dispatch() only returns the eventual result, not the request_id; reach into the
        # pool's own bookkeeping here since this test is specifically about that bookkeeping.
        (request_id,) = pool._futures.keys()
        await pool.cancel(request_id, grace_period=0.3)
        with pytest.raises((SubworkerTaskFailure, SubworkerCrashedError)):
            await dispatch_task

        # the pool replaced the dead/killed slot; it should still be usable afterward.
        result = await pool.dispatch("subworker_add", 1, {"a": 1, "b": 1})
        assert result == 2
    finally:
        await pool.shutdown(grace_period=2)


@pytest.mark.asyncio
async def test_pool_free_slots_and_shutdown(handle_factory: Callable[[], SubworkerHandleProtocol]) -> None:
    pool = SubworkerPool(handle_factory, size=2)
    await pool.start()
    assert pool.free_slots == 2

    dispatch_task = asyncio.create_task(pool.dispatch("subworker_sleep", 1, {"seconds": 5}))
    await asyncio.sleep(0.3)
    assert pool.free_slots == 1

    await pool.shutdown(grace_period=2)
    with pytest.raises((SubworkerTaskFailure, SubworkerCrashedError)):
        await dispatch_task
