"""Tests for ZmqRoutingNotifications using inproc:// transport (no network needed)."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
import zmq
import zmq.asyncio

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

from jobbers.adapters.zmq import ZmqBus, ZmqRoutingNotifications

pytestmark = pytest.mark.asyncio


def _make_addr() -> str:
    return f"inproc://test-notif-{uuid.uuid4().hex}"


@pytest_asyncio.fixture
async def ctx() -> AsyncGenerator[zmq.asyncio.Context, None]:
    context: zmq.asyncio.Context = zmq.asyncio.Context()
    yield context
    context.term()


@pytest_asyncio.fixture
async def routing_notif(ctx: zmq.asyncio.Context) -> AsyncGenerator[ZmqRoutingNotifications, None]:
    """ZmqRoutingNotifications with PUB bound and ROUTER server started."""
    pub_addr = _make_addr()
    router_addr = _make_addr()
    bus = ZmqBus(pub_addr, pub_addr, router_addr, router_addr, context=ctx)
    await bus._get_pub_sock()  # bind PUB before any SUB connects
    notif = ZmqRoutingNotifications(bus)
    await notif.start_server()  # bind ROUTER socket in background task
    await asyncio.sleep(0.01)  # let ROUTER bind before REQ connects
    yield notif
    bus.close()


async def test_bump_and_get_routing_version(routing_notif: ZmqRoutingNotifications) -> None:
    await routing_notif.bump_routing_version()
    result = await routing_notif.get_routing_version()
    assert result is not None
    assert result == routing_notif._routing_version


async def test_get_routing_version_when_none(routing_notif: ZmqRoutingNotifications) -> None:
    result = await routing_notif.get_routing_version()
    assert result is None


async def test_notify_and_poll_refresh_signal(routing_notif: ZmqRoutingNotifications) -> None:
    await routing_notif.notify_refresh("default", "tag-abc")
    changed = await routing_notif.poll_refresh_signal("default")
    assert changed is True


async def test_poll_refresh_signal_unchanged(routing_notif: ZmqRoutingNotifications) -> None:
    await routing_notif.notify_refresh("default", "tag-xyz")
    await routing_notif.poll_refresh_signal("default")  # first poll: changed
    changed_again = await routing_notif.poll_refresh_signal("default")  # second: same tag
    assert changed_again is False


async def test_poll_refresh_different_roles(routing_notif: ZmqRoutingNotifications) -> None:
    await routing_notif.notify_refresh("role-a", "tag-1")
    changed_a = await routing_notif.poll_refresh_signal("role-a")
    changed_b = await routing_notif.poll_refresh_signal("role-b")
    assert changed_a is True
    assert changed_b is False  # role-b never notified; None == None → no change


async def test_router_unknown_command(routing_notif: ZmqRoutingNotifications) -> None:
    raw = await routing_notif._bus.request(b"UNKNOWN", b"whatever")
    assert raw == b"error"
