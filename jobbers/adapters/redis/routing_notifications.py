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
    """RoutingNotificationProtocol backed by Redis keys and pub/sub channels."""

    ROUTING_VERSION_KEY = "routing:version"
    REFRESH_CHANNEL = "queue-config-refresh:{role}".format
    ROLE_REFRESH_TAG_KEY = "config:role:{name}:refresh_tag".format

    def __init__(self, client: Redis) -> None:
        self._client = client
        self._pubsubs: dict[str, Any] = {}
        self._tag_cache: dict[str, ULID] = {}

    async def get_routing_version(self) -> ULID | None:
        raw: bytes | None = await self._client.get(self.ROUTING_VERSION_KEY)
        return ULID.from_bytes(raw) if raw else None

    async def bump_routing_version(self) -> None:
        await self._client.set(self.ROUTING_VERSION_KEY, ULID().bytes)

    async def get_refresh_tag(self, role: str) -> ULID:
        if role in self._tag_cache:
            return self._tag_cache[role]
        raw: bytes | None = await self._client.get(self.ROLE_REFRESH_TAG_KEY(name=role))
        if raw:
            tag = ULID.from_str(raw.decode())
        else:
            tag = ULID()
            await self._client.set(self.ROLE_REFRESH_TAG_KEY(name=role), str(tag), nx=True)
            raw = await self._client.get(self.ROLE_REFRESH_TAG_KEY(name=role))
            if raw:
                tag = ULID.from_str(raw.decode())
        self._tag_cache[role] = tag
        return tag

    async def bump_refresh_tag(self, role: str) -> str:
        new_tag = ULID()
        tag_str = str(new_tag)
        await self._client.set(self.ROLE_REFRESH_TAG_KEY(name=role), tag_str)
        self._tag_cache[role] = new_tag
        await self._client.publish(self.REFRESH_CHANNEL(role=role), tag_str)
        return tag_str

    async def poll_refresh_signal(self, role: str) -> ULID:
        if role not in self._pubsubs:
            ps = self._client.pubsub()
            await ps.subscribe(self.REFRESH_CHANNEL(role=role))
            self._pubsubs[role] = ps
            # subscribe() only sends the SUBSCRIBE command; the server always
            # sends back a confirmation frame, which is still unread on the
            # connection at this point. Block until it arrives and drain it
            # now, so later get_message() calls never see it interleaved with
            # real published messages (where a single filtered None would
            # otherwise be mistaken for "no more messages" and cut the drain
            # loop short).
            await self._pubsubs[role].get_message(ignore_subscribe_messages=True, timeout=None)
        while msg := await self._pubsubs[role].get_message(ignore_subscribe_messages=True, timeout=0.0):
            self._tag_cache[role] = ULID.from_str(msg["data"].decode())
        if role not in self._tag_cache:
            return await self.get_refresh_tag(role)
        return self._tag_cache[role]
