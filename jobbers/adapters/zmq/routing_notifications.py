"""
ZeroMQ routing notifications adapter.

- `ZmqRoutingNotifications` — RoutingNotificationProtocol backed by a shared ``ZmqBus``.

Manager side: ``bump_routing_version`` and ``notify_refresh`` update in-memory state and
broadcast on the PUB socket.  ``start_server`` registers a ROUTER handler so workers can
query the current state via REQ sockets.

Worker side: ``get_routing_version`` and ``poll_refresh_signal`` send REQ requests to the
Manager's ROUTER and return the response.  ``poll_refresh_signal`` caches the last-seen
refresh tag per role so it can detect changes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ulid import ULID

if TYPE_CHECKING:
    from jobbers.adapters.zmq.bus import ZmqBus

logger = logging.getLogger(__name__)


class ZmqRoutingNotifications:
    """
    RoutingNotificationProtocol backed by a shared ``ZmqBus``.

    Manager calls ``bump_routing_version`` / ``notify_refresh`` / ``start_server``.
    Workers call ``get_routing_version`` / ``poll_refresh_signal``.
    """

    TOPIC_VERSION = b"routing:version"
    TOPIC_REFRESH_PREFIX = b"refresh:"

    def __init__(self, bus: ZmqBus) -> None:
        self._bus = bus
        # Manager-side state
        self._routing_version: ULID | None = None
        self._refresh_tags: dict[str, str] = {}
        # Worker-side cache (detects changes via poll_refresh_signal)
        self._last_seen_refresh: dict[str, str | None] = {}

    # ── Manager-side ────────────────────────────────────────────────────────

    async def bump_routing_version(self) -> None:
        """Generate a new routing version, store it, and broadcast it."""
        self._routing_version = ULID()
        await self._bus.publish(self.TOPIC_VERSION, self._routing_version.bytes)

    async def notify_refresh(self, role: str, tag: str) -> None:
        """Store the current refresh tag for a role and broadcast it."""
        self._refresh_tags[role] = tag
        await self._bus.publish(self.TOPIC_REFRESH_PREFIX + role.encode(), tag.encode())

    async def start_server(self) -> None:
        """Start the ROUTER request-handler so workers can query current state."""
        await self._bus.start_router(self._handle_request)

    async def _handle_request(self, command: bytes, args: list[bytes]) -> bytes:
        if command == b"GET" and args:
            key = args[0]
            if key == b"routing:version":
                return self._routing_version.bytes if self._routing_version else b"null"
            if key.startswith(self.TOPIC_REFRESH_PREFIX):
                role = key[len(self.TOPIC_REFRESH_PREFIX) :].decode()
                tag = self._refresh_tags.get(role)
                return tag.encode() if tag else b"null"
        logger.warning("Unknown ZMQ request: command=%r args=%r", command, args)
        return b"error"

    # ── Worker-side ─────────────────────────────────────────────────────────

    async def get_routing_version(self) -> ULID | None:
        """Request the current routing version from the Manager."""
        raw = await self._bus.request(b"GET", b"routing:version")
        return ULID.from_bytes(raw) if raw != b"null" else None

    async def poll_refresh_signal(self, role: str) -> bool:
        """
        Return True if the Manager's refresh tag for this role has changed since last call.

        On first call for a role the cached value is None, so any existing tag is a change.
        """
        raw = await self._bus.request(b"GET", self.TOPIC_REFRESH_PREFIX + role.encode())
        tag: str | None = raw.decode() if raw not in (b"null", b"error") else None
        changed = tag != self._last_seen_refresh.get(role)
        self._last_seen_refresh[role] = tag
        return changed
