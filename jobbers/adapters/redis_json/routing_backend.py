"""
Redis Stack (RedisJSON + RediSearch) routing sub-adapters and routing backend.

- `RedisJSONQueueConfigAdapter` — QueueConfigProtocol backed by RedisJSON + RediSearch.
- `RedisJSONTaskRoutingConfigAdapter` — TaskRoutingConfigProtocol backed by RedisJSON.
- `RedisJSONRoutingBackend` — RoutingBackendProtocol composing the two sub-adapters.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query as SearchQuery
from redis.exceptions import ResponseError

from jobbers.adapters.redis_json._helpers import _escape_tag, _pack
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task_routing import RoutingConfig

if TYPE_CHECKING:
    from redis.asyncio.client import Redis


# ---------------------------------------------------------------------------
# RedisJSONQueueConfigAdapter  (Redis Stack: RedisJSON + RediSearch)
# ---------------------------------------------------------------------------


class RedisJSONQueueConfigAdapter:
    """
    QueueConfigProtocol backed by RedisJSON. Requires Redis Stack.

    Two RediSearch indexes replace the plain-Redis set indexes used by
    RedisQueueConfigAdapter:

      routing_queue_idx — on routing:queue:* docs; enables enumeration and fast
                          queue-membership queries without a separate index set.
      routing_role_idx  — on routing:role:* docs; enables bump_refresh_tags_for_queue
                          to find affected roles via an indexed query rather than
                          scanning all roles.

    Key scheme:
      routing:queue:{name}   — JSON doc (QueueConfig fields)
      routing:role:{name}    — JSON doc {queues: [...]}
    """

    QUEUE_IDX = "routing_queue_idx"
    ROLE_IDX = "routing_role_idx"
    QUEUE_KEY = "routing:queue:{name}".format
    ROLE_KEY = "routing:role:{name}".format

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def ensure_indexes(self) -> None:
        """Create RediSearch indexes for queues and roles if they do not already exist."""
        try:
            await self._client.ft(self.QUEUE_IDX).info()  # type: ignore[no-untyped-call]
        except ResponseError:
            await self._client.ft(self.QUEUE_IDX).create_index(
                fields=[TagField("$.name", as_name="name")],
                definition=IndexDefinition(prefix=["routing:queue:"], index_type=IndexType.JSON),  # type: ignore[no-untyped-call]
            )
        try:
            await self._client.ft(self.ROLE_IDX).info()  # type: ignore[no-untyped-call]
        except ResponseError:
            await self._client.ft(self.ROLE_IDX).create_index(
                fields=[TagField("$.queues[*]", as_name="queues")],
                definition=IndexDefinition(prefix=["routing:role:"], index_type=IndexType.JSON),  # type: ignore[no-untyped-call]
            )

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        raw: dict[str, Any] | None = await self._client.json().get(self.QUEUE_KEY(name=queue))  # type: ignore[misc]
        if raw is None:
            return None
        return QueueConfig.model_validate(raw)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        await self._client.json().set(self.QUEUE_KEY(name=queue_config.name), "$", _pack(queue_config))  # type: ignore[misc]

    async def delete_queue(self, queue_name: str) -> list[str]:
        # Remove queue from all role docs first so that a crash after this point leaves
        # an orphaned-but-valid queue config rather than roles referencing a deleted queue.
        results = await self._client.ft(self.ROLE_IDX).search(
            SearchQuery(f"@queues:{{{_escape_tag(queue_name)}}}").no_content()
        )
        affected = [doc.id.removeprefix("routing:role:") for doc in (results.docs or [])]
        if affected:
            get_pipe = self._client.pipeline(transaction=False)
            for role in affected:
                get_pipe.json().get(self.ROLE_KEY(name=role))
            role_docs: list[dict[str, Any] | None] = await get_pipe.execute()
            write_pipe = self._client.pipeline(transaction=True)
            for role, role_doc in zip(affected, role_docs):
                if role_doc is not None:
                    new_queues = [q for q in role_doc.get("queues", []) if q != queue_name]
                    write_pipe.json().set(self.ROLE_KEY(name=role), "$.queues", new_queues)
            await write_pipe.execute()
        await self._client.delete(self.QUEUE_KEY(name=queue_name))
        return affected

    async def get_all_queues(self) -> list[str]:
        results = await self._client.ft(self.QUEUE_IDX).search(SearchQuery("*").no_content().paging(0, 10000))
        return sorted(doc.id.removeprefix("routing:queue:") for doc in (results.docs or []))

    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]:
        if not queues_set:
            return {}
        ordered = list(queues_set)
        pipe = self._client.pipeline(transaction=False)
        for name in ordered:
            pipe.json().get(self.QUEUE_KEY(name=name))
        raws: list[dict[str, Any] | None] = await pipe.execute()
        result: dict[str, int | None] = {}
        for name, raw in zip(ordered, raws):
            if raw is None:
                result[name] = None
            else:
                result[name] = QueueConfig.model_validate(raw).max_concurrent
        return result

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        raw: dict[str, Any] | None = await self._client.json().get(self.ROLE_KEY(name=role))  # type: ignore[misc]
        if raw is None:
            return set()
        return set(raw.get("queues", []))

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        await self._client.json().set(self.ROLE_KEY(name=role), "$", {"queues": list(queues_set)})  # type: ignore[misc]

    async def get_all_roles(self) -> list[str]:
        results = await self._client.ft(self.ROLE_IDX).search(SearchQuery("*").no_content().paging(0, 10000))
        return sorted(doc.id.removeprefix("routing:role:") for doc in (results.docs or []))

    async def delete_role(self, role: str) -> None:
        await self._client.delete(self.ROLE_KEY(name=role))

    # ── Role discovery ────────────────────────────────────────────────────────

    async def get_roles_for_queue(self, queue_name: str) -> list[str]:
        """Return names of all roles containing queue_name via RediSearch."""
        results = await self._client.ft(self.ROLE_IDX).search(
            SearchQuery(f"@queues:{{{_escape_tag(queue_name)}}}").no_content()
        )
        return [doc.id.removeprefix("routing:role:") for doc in (results.docs or [])]


# ---------------------------------------------------------------------------
# RedisJSONTaskRoutingConfigAdapter  (Redis Stack: RedisJSON)
# ---------------------------------------------------------------------------


class RedisJSONTaskRoutingConfigAdapter:
    """TaskRoutingConfigProtocol backed by RedisJSON plain key-value docs."""

    ROUTING_KEY = "routing:config:{task_name}:{task_version}".format

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        raw: dict[str, Any] | None = await self._client.json().get(  # type: ignore[misc]
            self.ROUTING_KEY(task_name=task_name, task_version=task_version)
        )
        if raw is None:
            return None
        return RoutingConfig.model_validate(raw)

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        await self._client.json().set(  # type: ignore[misc]
            self.ROUTING_KEY(task_name=routing_config.task_name, task_version=routing_config.task_version),
            "$",
            _pack(routing_config),
        )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        deleted: int = await self._client.delete(
            self.ROUTING_KEY(task_name=task_name, task_version=task_version)
        )
        return deleted > 0


# ---------------------------------------------------------------------------
# RedisJSONRoutingBackend  (Redis Stack: RedisJSON + RediSearch)
# ---------------------------------------------------------------------------


class RedisJSONRoutingBackend:
    """RoutingBackendProtocol backed by RedisJSON. Delegates to sub-adapters."""

    def __init__(self, client: Redis) -> None:
        self._qca = RedisJSONQueueConfigAdapter(client)
        self._rca = RedisJSONTaskRoutingConfigAdapter(client)

    async def ensure_indexes(self) -> None:
        await self._qca.ensure_indexes()

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
