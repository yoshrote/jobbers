from __future__ import annotations

import json
from typing import TYPE_CHECKING

from ulid import ULID

from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task_routing import RoutingConfig, RoutingStrategy

if TYPE_CHECKING:
    from redis.asyncio.client import Redis


class RedisRoutingBackend:
    """
    RoutingBackendProtocol backed by Redis. No SQL required.

    Key scheme:
      config:queue:{name}                — JSON string (QueueConfig fields)
      config:queues                      — Set of all queue names
      config:role:{name}:queues          — Set of queue names for the role
      config:roles                       — Set of all role names
      config:role:{name}:refresh_tag     — String (ULID)
      config:routing:{task_name}:{ver}   — JSON string (RoutingConfig fields)
    """

    QUEUE_KEY = "config:queue:{name}".format
    QUEUES_INDEX = "config:queues"
    ROLE_QUEUES_KEY = "config:role:{name}:queues".format
    ROLES_INDEX = "config:roles"
    ROLE_REFRESH_TAG_KEY = "config:role:{name}:refresh_tag".format
    ROUTING_KEY = "config:routing:{task_name}:{task_version}".format

    def __init__(self, client: Redis) -> None:
        self._client = client

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        raw = await self._client.get(self.QUEUE_KEY(name=queue))
        if raw is None:
            return None
        data = json.loads(raw)
        return QueueConfig(
            name=data["name"],
            max_concurrent=data.get("max_concurrent"),
            rate_numerator=data.get("rate_numerator"),
            rate_denominator=data.get("rate_denominator"),
            rate_period=RatePeriod(data["rate_period"]) if data.get("rate_period") else None,
        )

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        payload = json.dumps(
            {
                "name": queue_config.name,
                "max_concurrent": queue_config.max_concurrent,
                "rate_numerator": queue_config.rate_numerator,
                "rate_denominator": queue_config.rate_denominator,
                "rate_period": queue_config.rate_period,
            }
        )
        pipe = self._client.pipeline(transaction=True)
        pipe.set(self.QUEUE_KEY(name=queue_config.name), payload)
        pipe.sadd(self.QUEUES_INDEX, queue_config.name)
        await pipe.execute()

    async def delete_queue(self, queue_name: str) -> None:
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.QUEUE_KEY(name=queue_name))
        pipe.srem(self.QUEUES_INDEX, queue_name)
        # Remove from every role that contains this queue and bump their tags.
        # We collect roles first, then operate in a second pipeline.
        await pipe.execute()
        raw_roles: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        role_names = [r.decode() for r in raw_roles]
        affected: list[str] = []
        for role in role_names:
            removed = await self._client.srem(self.ROLE_QUEUES_KEY(name=role), queue_name)  # type: ignore[misc]
            if removed:
                affected.append(role)
        if affected:
            new_tag = str(ULID())
            bump_pipe = self._client.pipeline(transaction=True)
            for role in affected:
                bump_pipe.set(self.ROLE_REFRESH_TAG_KEY(name=role), new_tag)
            await bump_pipe.execute()

    async def get_all_queues(self) -> list[str]:
        raw: set[bytes] = await self._client.smembers(self.QUEUES_INDEX)  # type: ignore[misc]
        return sorted(m.decode() for m in raw)

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        raw: set[bytes] = await self._client.smembers(self.ROLE_QUEUES_KEY(name=role))  # type: ignore[misc]
        return {m.decode() for m in raw}

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        new_tag = str(ULID())
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.ROLE_QUEUES_KEY(name=role))
        if queues_set:
            pipe.sadd(self.ROLE_QUEUES_KEY(name=role), *queues_set)
        pipe.sadd(self.ROLES_INDEX, role)
        pipe.set(self.ROLE_REFRESH_TAG_KEY(name=role), new_tag)
        await pipe.execute()
        return new_tag

    async def get_all_roles(self) -> list[str]:
        raw: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        return sorted(m.decode() for m in raw)

    async def delete_role(self, role: str) -> None:
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.ROLE_QUEUES_KEY(name=role))
        pipe.delete(self.ROLE_REFRESH_TAG_KEY(name=role))
        pipe.srem(self.ROLES_INDEX, role)
        await pipe.execute()

    # ── Refresh tags ──────────────────────────────────────────────────────────

    async def get_refresh_tag(self, role: str) -> ULID:
        raw: bytes | None = await self._client.get(self.ROLE_REFRESH_TAG_KEY(name=role))
        if raw:
            return ULID.from_str(raw.decode())
        # First access: create and store an initial tag.
        init_tag = ULID()
        # SET NX so two concurrent callers don't clobber each other.
        await self._client.set(self.ROLE_REFRESH_TAG_KEY(name=role), str(init_tag), nx=True)
        # Re-read the winner in case another process beat us.
        raw = await self._client.get(self.ROLE_REFRESH_TAG_KEY(name=role))
        return ULID.from_str(raw.decode()) if raw else init_tag

    async def bump_refresh_tag(self, role: str) -> str:
        new_tag = str(ULID())
        await self._client.set(self.ROLE_REFRESH_TAG_KEY(name=role), new_tag)
        return new_tag

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        raw_roles: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        role_names = [r.decode() for r in raw_roles]
        affected: list[str] = []
        for role in role_names:
            is_member: bool = await self._client.sismember(self.ROLE_QUEUES_KEY(name=role), queue_name)  # type: ignore[misc]
            if is_member:
                affected.append(role)
        if affected:
            new_tag = str(ULID())
            pipe = self._client.pipeline(transaction=True)
            for role in affected:
                pipe.set(self.ROLE_REFRESH_TAG_KEY(name=role), new_tag)
            await pipe.execute()
        return affected

    # ── Task routing config ───────────────────────────────────────────────────

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        raw = await self._client.get(self.ROUTING_KEY(task_name=task_name, task_version=task_version))
        if raw is None:
            return None
        data = json.loads(raw)
        return RoutingConfig(
            task_name=data["task_name"],
            task_version=data["task_version"],
            strategy=RoutingStrategy(data["strategy"]),
            queues=data["queues"],
            weights=data.get("weights"),
        )

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        payload = json.dumps(
            {
                "task_name": routing_config.task_name,
                "task_version": routing_config.task_version,
                "strategy": routing_config.strategy,
                "queues": routing_config.queues,
                "weights": routing_config.weights,
            }
        )
        await self._client.set(
            self.ROUTING_KEY(task_name=routing_config.task_name, task_version=routing_config.task_version),
            payload,
        )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        deleted: int = await self._client.delete(
            self.ROUTING_KEY(task_name=task_name, task_version=task_version)
        )
        return deleted > 0
