"""
Plain Redis routing notifications adapter.

- `RedisRoutingNotifications` — RoutingNotificationProtocol backed by Redis key + pub/sub.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ulid import ULID

if TYPE_CHECKING:
    from redis.asyncio.client import Redis


class RedisRoutingNotifications:
    """RoutingNotificationProtocol backed by a Redis key and pub/sub channels."""

    ROUTING_VERSION_KEY = "routing:version"
    REFRESH_CHANNEL = "queue-config-refresh:{role}".format

    def __init__(self, client: Redis) -> None:
        self._client = client
        self._pubsubs: dict[str, Any] = {}

    async def get_routing_version(self) -> ULID | None:
        raw: bytes | None = await self._client.get(self.ROUTING_VERSION_KEY)
        return ULID.from_bytes(raw) if raw else None

    async def bump_routing_version(self) -> None:
        await self._client.set(self.ROUTING_VERSION_KEY, ULID().bytes)

    async def notify_refresh(self, role: str, tag: str) -> None:
        await self._client.publish(self.REFRESH_CHANNEL(role=role), tag)

    async def poll_refresh_signal(self, role: str) -> bool:
        if role not in self._pubsubs:
            ps = self._client.pubsub()
            await ps.subscribe(self.REFRESH_CHANNEL(role=role))
            self._pubsubs[role] = ps
        return bool(await self._pubsubs[role].get_message(ignore_subscribe_messages=True, timeout=0.0))
