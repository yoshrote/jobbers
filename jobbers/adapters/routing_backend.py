from __future__ import annotations

from typing import TYPE_CHECKING, runtime_checkable

from typing_extensions import Protocol

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from ulid import ULID

    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task_routing import RoutingConfig


class RoutingBackendReadOnlyError(Exception):
    """Raised when a write operation is attempted on a read-only routing backend."""


@runtime_checkable
class RoutingBackendProtocol(Protocol):
    """Interface all routing backends must implement."""

    # Queue CRUD
    async def get_queue_config(self, queue: str) -> QueueConfig | None: ...

    async def save_queue_config(self, queue_config: QueueConfig) -> None: ...

    async def delete_queue(self, queue_name: str) -> None: ...

    async def get_all_queues(self) -> list[str]: ...

    # Role CRUD
    async def get_queues(self, role: str) -> set[str]: ...

    async def save_role(self, role: str, queues_set: set[str]) -> str: ...

    async def get_all_roles(self) -> list[str]: ...

    async def delete_role(self, role: str) -> None: ...

    # Change detection (ULID-stamped refresh tags drive pub/sub worker refresh)
    async def get_refresh_tag(self, role: str) -> ULID: ...

    async def bump_refresh_tag(self, role: str) -> str: ...

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]: ...

    # Task routing config CRUD
    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None: ...

    async def save_routing_config(self, routing_config: RoutingConfig) -> None: ...

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool: ...


class SQLRoutingBackend:
    """RoutingBackendProtocol backed by SQLAlchemy (the original SQL-based implementation)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        from jobbers.models.queue_config import QueueConfigAdapter
        from jobbers.models.task_routing import TaskRoutingConfigAdapter

        self._qca = QueueConfigAdapter(session_factory)
        self._rca = TaskRoutingConfigAdapter(session_factory)

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        return await self._qca.get_queue_config(queue)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        await self._qca.save_queue_config(queue_config)

    async def delete_queue(self, queue_name: str) -> None:
        await self._qca.delete_queue(queue_name)

    async def get_all_queues(self) -> list[str]:
        return await self._qca.get_all_queues()

    async def get_queues(self, role: str) -> set[str]:
        return await self._qca.get_queues(role)

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        return await self._qca.save_role(role, queues_set)

    async def get_all_roles(self) -> list[str]:
        return await self._qca.get_all_roles()

    async def delete_role(self, role: str) -> None:
        await self._qca.delete_role(role)

    async def get_refresh_tag(self, role: str) -> ULID:
        return await self._qca.get_refresh_tag(role)

    async def bump_refresh_tag(self, role: str) -> str:
        return await self._qca.bump_refresh_tag(role)

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        return await self._qca.bump_refresh_tags_for_queue(queue_name)

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        return await self._rca.get_routing_config(task_name, task_version)

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        await self._rca.save_routing_config(routing_config)

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        return await self._rca.delete_routing_config(task_name, task_version)
