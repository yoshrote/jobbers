"""
Plain Redis routing sub-adapters and routing backend.

- `RedisQueueConfigAdapter` — QueueConfigProtocol backed by plain Redis keys/sets.
- `RedisTaskRoutingConfigAdapter` — TaskRoutingConfigProtocol backed by plain Redis keys.
- `RedisRoutingBackend` — RoutingBackendProtocol composing the two sub-adapters.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from jobbers.adapters.redis._helpers import _pack
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task_routing import RoutingConfig
from jobbers.utils.serialization import deserialize

if TYPE_CHECKING:
    from redis.asyncio.client import Redis


# ---------------------------------------------------------------------------
# RedisQueueConfigAdapter  (plain Redis: string values + sets)
# ---------------------------------------------------------------------------


class RedisQueueConfigAdapter:
    """
    QueueConfigProtocol backed by plain Redis keys and sets. No SQL required.

    Key scheme:
      config:queue:{name}            — msgpack bytes (QueueConfig fields)
      config:queues                  — Set of all queue names
      config:role:{name}:queues      — Set of queue names for the role
      config:roles                   — Set of all role names
    """

    QUEUE_KEY = "config:queue:{name}".format
    QUEUES_INDEX = "config:queues"
    ROLE_QUEUES_KEY = "config:role:{name}:queues".format
    ROLES_INDEX = "config:roles"

    def __init__(self, client: Redis) -> None:
        self._client = client

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        raw = await self._client.get(self.QUEUE_KEY(name=queue))
        if raw is None:
            return None
        return QueueConfig.model_validate(deserialize(raw))

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        payload = _pack(queue_config)
        pipe = self._client.pipeline(transaction=True)
        pipe.set(self.QUEUE_KEY(name=queue_config.name), payload)
        pipe.sadd(self.QUEUES_INDEX, queue_config.name)
        await pipe.execute()

    async def delete_queue(self, queue_name: str) -> list[str]:
        # Remove queue from all role sets first so that a crash after this point leaves
        # an orphaned-but-valid queue config rather than roles referencing a deleted queue.
        raw_roles: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        role_names = [r.decode() for r in raw_roles]
        affected: list[str] = []
        if role_names:
            srem_pipe = self._client.pipeline(transaction=False)
            for role in role_names:
                srem_pipe.srem(self.ROLE_QUEUES_KEY(name=role), queue_name)
            removed_counts: list[int] = await srem_pipe.execute()
            affected = [role for role, count in zip(role_names, removed_counts) if count]
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.QUEUE_KEY(name=queue_name))
        pipe.srem(self.QUEUES_INDEX, queue_name)
        await pipe.execute()
        return affected

    async def get_all_queues(self) -> list[str]:
        raw: set[bytes] = await self._client.smembers(self.QUEUES_INDEX)  # type: ignore[misc]
        return sorted(m.decode() for m in raw)

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        raw: set[bytes] = await self._client.smembers(self.ROLE_QUEUES_KEY(name=role))  # type: ignore[misc]
        return {m.decode() for m in raw}

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.ROLE_QUEUES_KEY(name=role))
        if queues_set:
            pipe.sadd(self.ROLE_QUEUES_KEY(name=role), *queues_set)
        pipe.sadd(self.ROLES_INDEX, role)
        await pipe.execute()

    async def get_all_roles(self) -> list[str]:
        raw: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        return sorted(m.decode() for m in raw)

    async def delete_role(self, role: str) -> None:
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.ROLE_QUEUES_KEY(name=role))
        pipe.srem(self.ROLES_INDEX, role)
        await pipe.execute()

    # ── Role discovery ────────────────────────────────────────────────────────

    async def get_roles_for_queue(self, queue_name: str) -> list[str]:
        """Return names of all roles that contain queue_name."""
        raw_roles: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        role_names = [r.decode() for r in raw_roles]
        if not role_names:
            return []
        check_pipe = self._client.pipeline(transaction=False)
        for role in role_names:
            check_pipe.sismember(self.ROLE_QUEUES_KEY(name=role), queue_name)
        is_member_list: list[bool] = await check_pipe.execute()
        return [role for role, is_member in zip(role_names, is_member_list) if is_member]

    # ── Queue limits (batch read) ─────────────────────────────────────────────

    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]:
        if not queues_set:
            return {}
        ordered = list(queues_set)
        pipe = self._client.pipeline(transaction=False)
        for name in ordered:
            pipe.get(self.QUEUE_KEY(name=name))
        raws: list[bytes | None] = await pipe.execute()
        result: dict[str, int | None] = {}
        for name, raw in zip(ordered, raws):
            if raw is None:
                result[name] = None
            else:
                cfg = QueueConfig.model_validate(deserialize(raw))
                result[name] = cfg.max_concurrent
        return result


# ---------------------------------------------------------------------------
# RedisTaskRoutingConfigAdapter  (plain Redis: string values)
# ---------------------------------------------------------------------------


class RedisTaskRoutingConfigAdapter:
    """
    TaskRoutingConfigProtocol backed by plain Redis keys. No SQL required.

    Key scheme:
      config:routing:{task_name}:{task_version}   — msgpack bytes (RoutingConfig fields)
    """

    ROUTING_KEY = "config:routing:{task_name}:{task_version}".format

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        raw = await self._client.get(self.ROUTING_KEY(task_name=task_name, task_version=task_version))
        if raw is None:
            return None
        return RoutingConfig.model_validate(deserialize(raw))

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        payload = _pack(routing_config)
        await self._client.set(
            self.ROUTING_KEY(task_name=routing_config.task_name, task_version=routing_config.task_version),
            payload,
        )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        deleted: int = await self._client.delete(
            self.ROUTING_KEY(task_name=task_name, task_version=task_version)
        )
        return deleted > 0


# ---------------------------------------------------------------------------
# RedisRoutingBackend  (composes the two sub-adapters above)
# ---------------------------------------------------------------------------


class RedisRoutingBackend:
    """RoutingBackendProtocol backed by Redis. Delegates to sub-adapters."""

    def __init__(self, client: Redis) -> None:
        self._qca = RedisQueueConfigAdapter(client)
        self._rca = RedisTaskRoutingConfigAdapter(client)

    async def drop_stale_indexes(self) -> list[str]:
        """No-op: plain-Redis routing backend uses simple key patterns, not a search index."""
        return []

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        return await self._qca.get_queue_config(queue)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        await self._qca.save_queue_config(queue_config)

    async def delete_queue(self, queue_name: str) -> list[str]:
        return await self._qca.delete_queue(queue_name)

    async def get_all_queues(self) -> list[str]:
        return await self._qca.get_all_queues()

    async def get_queues(self, role: str) -> set[str]:
        return await self._qca.get_queues(role)

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        await self._qca.save_role(role, queues_set)

    async def get_all_roles(self) -> list[str]:
        return await self._qca.get_all_roles()

    async def delete_role(self, role: str) -> None:
        await self._qca.delete_role(role)

    async def get_roles_for_queue(self, queue_name: str) -> list[str]:
        return await self._qca.get_roles_for_queue(queue_name)

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        return await self._rca.get_routing_config(task_name, task_version)

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        await self._rca.save_routing_config(routing_config)

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        return await self._rca.delete_routing_config(task_name, task_version)
