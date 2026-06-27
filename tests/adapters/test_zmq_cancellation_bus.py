"""Tests for ZmqCancellationBus using inproc:// transport (no network needed)."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
import zmq
import zmq.asyncio
from ulid import ULID

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

from jobbers.adapters.zmq import ZmqBus, ZmqCancellationBus

pytestmark = pytest.mark.asyncio


def _make_addr() -> str:
    return f"inproc://test-cancel-{uuid.uuid4().hex}"


@pytest_asyncio.fixture
async def ctx() -> AsyncGenerator[zmq.asyncio.Context, None]:
    context: zmq.asyncio.Context = zmq.asyncio.Context()
    yield context
    context.term()


@pytest_asyncio.fixture
async def zmq_bus(ctx: zmq.asyncio.Context) -> AsyncGenerator[ZmqBus, None]:
    """ZmqBus with PUB socket already bound (inproc:// requires bind before connect)."""
    pub_addr = _make_addr()
    router_addr = _make_addr()
    bus = ZmqBus(pub_addr, pub_addr, router_addr, router_addr, context=ctx)
    await bus._get_pub_sock()
    yield bus
    bus.close()


@pytest_asyncio.fixture
async def bus(zmq_bus: ZmqBus) -> ZmqCancellationBus:
    return ZmqCancellationBus(zmq_bus)


async def _collect(b: ZmqCancellationBus, n: int) -> list[ULID]:
    results: list[ULID] = []
    async for task_id in b.listen_cancellations():
        results.append(task_id)
        if len(results) >= n:
            break
    return results


async def test_round_trip(bus: ZmqCancellationBus) -> None:
    expected = ULID()
    task = asyncio.create_task(_collect(bus, 1))
    await asyncio.sleep(0.01)  # allow SUB socket to connect
    await bus.publish_cancellation(expected)
    result = await asyncio.wait_for(task, timeout=2.0)
    assert result == [expected]


async def test_multiple_messages(bus: ZmqCancellationBus) -> None:
    ids = [ULID() for _ in range(3)]
    task = asyncio.create_task(_collect(bus, 3))
    await asyncio.sleep(0.01)
    for uid in ids:
        await bus.publish_cancellation(uid)
    result = await asyncio.wait_for(task, timeout=2.0)
    assert result == ids


async def test_invalid_ulid_skipped(bus: ZmqCancellationBus, zmq_bus: ZmqBus) -> None:
    """Invalid ULID bytes are logged and skipped; the generator continues."""
    expected = ULID()
    task = asyncio.create_task(_collect(bus, 1))
    await asyncio.sleep(0.01)
    pub = await zmq_bus._get_pub_sock()
    await pub.send_multipart([ZmqCancellationBus.TOPIC, b"not-a-valid-ulid"])
    await bus.publish_cancellation(expected)
    result = await asyncio.wait_for(task, timeout=2.0)
    assert result == [expected]


async def test_malformed_message_skipped(bus: ZmqCancellationBus, zmq_bus: ZmqBus) -> None:
    """A single-frame message (no data frame) is logged and skipped."""
    expected = ULID()
    task = asyncio.create_task(_collect(bus, 1))
    await asyncio.sleep(0.01)
    pub = await zmq_bus._get_pub_sock()
    await pub.send(ZmqCancellationBus.TOPIC + b"trailing")
    await bus.publish_cancellation(expected)
    result = await asyncio.wait_for(task, timeout=2.0)
    assert result == [expected]


async def test_topic_filter(bus: ZmqCancellationBus, zmq_bus: ZmqBus) -> None:
    """Messages with a non-matching topic are silently filtered by ZMQ."""
    expected = ULID()
    decoy = ULID()
    task = asyncio.create_task(_collect(bus, 1))
    await asyncio.sleep(0.01)
    pub = await zmq_bus._get_pub_sock()
    await pub.send_multipart([b"wrong_topic", str(decoy).encode()])
    await bus.publish_cancellation(expected)
    result = await asyncio.wait_for(task, timeout=2.0)
    assert result == [expected]


async def test_pub_sock_lazy_creation(ctx: zmq.asyncio.Context) -> None:
    """PUB socket is not created until first publish call."""
    addr = _make_addr()
    bus = ZmqBus(addr, addr, addr, addr, context=ctx)
    assert bus._pub_sock is None
    await bus._get_pub_sock()
    assert bus._pub_sock is not None
    bus.close()
