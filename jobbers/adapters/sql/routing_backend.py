"""
SQLAlchemy routing sub-adapters and routing backend.

- `SQLQueueConfigAdapter` — QueueConfigProtocol backed by SQL tables.
- `SQLTaskRoutingConfigAdapter` — TaskRoutingConfigProtocol backed by SQL.
- `SQLRoutingBackend` — RoutingBackendProtocol composing the two sub-adapters.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy import delete, insert, select, update
from sqlalchemy.exc import IntegrityError

from jobbers.migrations.schema import (
    queues,
    role_queues,
    roles,
    task_routing_rules,
)
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task_routing import WILDCARD_LANE, RoutingConfig

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


# ---------------------------------------------------------------------------
# SQLQueueConfigAdapter
# ---------------------------------------------------------------------------


class SQLQueueConfigAdapter:
    """
    QueueConfigProtocol backed by SQLAlchemy async sessions.

    Tables:
      roles       — named roles.
      queues      — queue configurations (concurrency limits and rate limiting).
      role_queues — many-to-many mapping of roles to queues.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    queues.c.name,
                    queues.c.max_concurrent,
                    queues.c.rate_numerator,
                    queues.c.rate_denominator,
                    queues.c.rate_period,
                ).where(queues.c.name == queue)
            )
            row = result.fetchone()
        if row is None:
            return None
        return QueueConfig.from_row(row)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        async with self._session_factory.begin() as session:
            existing = await session.execute(select(queues.c.name).where(queues.c.name == queue_config.name))
            if existing.fetchone():
                await session.execute(
                    update(queues)
                    .where(queues.c.name == queue_config.name)
                    .values(
                        max_concurrent=queue_config.max_concurrent,
                        rate_numerator=queue_config.rate_numerator,
                        rate_denominator=queue_config.rate_denominator,
                        rate_period=queue_config.rate_period,
                    )
                )
            else:
                await session.execute(
                    insert(queues).values(
                        name=queue_config.name,
                        max_concurrent=queue_config.max_concurrent,
                        rate_numerator=queue_config.rate_numerator,
                        rate_denominator=queue_config.rate_denominator,
                        rate_period=queue_config.rate_period,
                    )
                )

    async def create_queue_config(self, queue_config: QueueConfig) -> bool:
        """Insert a new queue config. Returns False (no changes made) if the name already exists."""
        try:
            async with self._session_factory.begin() as session:
                await session.execute(
                    insert(queues).values(
                        name=queue_config.name,
                        max_concurrent=queue_config.max_concurrent,
                        rate_numerator=queue_config.rate_numerator,
                        rate_denominator=queue_config.rate_denominator,
                        rate_period=queue_config.rate_period,
                    )
                )
        except IntegrityError:
            return False
        return True

    async def delete_queue(self, queue_name: str) -> list[str]:
        """Delete a queue and cascade to role_queues. Returns affected role names."""
        async with self._session_factory.begin() as session:
            result = await session.execute(
                select(role_queues.c.role).where(role_queues.c.queue == queue_name).distinct()
            )
            affected_roles = [row[0] for row in result.fetchall()]
            await session.execute(delete(queues).where(queues.c.name == queue_name))
        return affected_roles

    async def get_all_queues(self) -> list[str]:
        async with self._session_factory() as session:
            result = await session.execute(select(queues.c.name).order_by(queues.c.name))
            return [row[0] for row in result.fetchall()]

    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]:
        """Return a map of queue name → max_concurrent for the requested queues."""
        if not queues_set:
            return {}
        async with self._session_factory() as session:
            result = await session.execute(
                select(queues.c.name, queues.c.max_concurrent).where(queues.c.name.in_(list(queues_set)))
            )
            found = {row[0]: row[1] for row in result.fetchall()}
        return {q: found.get(q) for q in queues_set}

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        async with self._session_factory() as session:
            result = await session.execute(select(role_queues.c.queue).where(role_queues.c.role == role))
            return {row[0] for row in result.fetchall()}

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        async with self._session_factory.begin() as session:
            existing = await session.execute(select(roles.c.name).where(roles.c.name == role))
            if not existing.fetchone():
                await session.execute(insert(roles).values(name=role))
            await session.execute(delete(role_queues).where(role_queues.c.role == role))
            if queues_set:
                await session.execute(
                    insert(role_queues),
                    [{"role": role, "queue": q} for q in queues_set],
                )

    async def create_role(self, role: str, queues_set: set[str]) -> bool:
        """Insert a new role with its queues. Returns False (no changes made) if the role already exists."""
        try:
            async with self._session_factory.begin() as session:
                await session.execute(insert(roles).values(name=role))
                if queues_set:
                    await session.execute(
                        insert(role_queues),
                        [{"role": role, "queue": q} for q in queues_set],
                    )
        except IntegrityError:
            return False
        return True

    async def get_all_roles(self) -> list[str]:
        async with self._session_factory() as session:
            result = await session.execute(select(roles.c.name).order_by(roles.c.name))
            return [row[0] for row in result.fetchall()]

    async def delete_role(self, role: str) -> None:
        """Delete a role (cascades to role_queues). Queue configs are preserved."""
        async with self._session_factory.begin() as session:
            await session.execute(delete(roles).where(roles.c.name == role))

    # ── Role discovery ────────────────────────────────────────────────────────

    async def get_roles_for_queue(self, queue_name: str) -> list[str]:
        """Return names of all roles that contain queue_name."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(role_queues.c.role).where(role_queues.c.queue == queue_name).distinct()
            )
            return [row[0] for row in result.fetchall()]


# ---------------------------------------------------------------------------
# SQLTaskRoutingConfigAdapter
# ---------------------------------------------------------------------------


class SQLTaskRoutingConfigAdapter:
    """
    TaskRoutingConfigProtocol backed by SQLAlchemy (``task_routing_rules`` table).

    One row per :class:`~jobbers.models.task_routing.RoutingRule`; a config's
    rules are read and written as a set, so ``save_routing_config`` replaces
    every rule for the task type rather than merging.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    task_routing_rules.c.from_lane,
                    task_routing_rules.c.strategy,
                    task_routing_rules.c.queues,
                    task_routing_rules.c.weights,
                ).where(
                    task_routing_rules.c.task_name == task_name,
                    task_routing_rules.c.task_version == task_version,
                )
            )
            rows = result.fetchall()
        if not rows:
            return None
        return RoutingConfig.from_rows(task_name, task_version, list(rows))

    async def save_routing_config(self, config: RoutingConfig) -> None:
        """Replace every rule for the task type with *config*'s rules."""
        async with self._session_factory.begin() as session:
            await session.execute(
                delete(task_routing_rules).where(
                    task_routing_rules.c.task_name == config.task_name,
                    task_routing_rules.c.task_version == config.task_version,
                )
            )
            await session.execute(
                insert(task_routing_rules),
                [
                    {
                        "task_name": config.task_name,
                        "task_version": config.task_version,
                        "from_lane": WILDCARD_LANE if rule.from_lane is None else rule.from_lane,
                        "strategy": rule.strategy,
                        "queues": json.dumps(rule.queues),
                        "weights": json.dumps(rule.weights) if rule.weights is not None else None,
                    }
                    for rule in config.rules
                ],
            )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        """Remove every rule for the task type. Returns False if none existed."""
        async with self._session_factory.begin() as session:
            existing = await session.execute(
                select(task_routing_rules.c.task_name).where(
                    task_routing_rules.c.task_name == task_name,
                    task_routing_rules.c.task_version == task_version,
                )
            )
            if existing.fetchone() is None:
                return False
            await session.execute(
                delete(task_routing_rules).where(
                    task_routing_rules.c.task_name == task_name,
                    task_routing_rules.c.task_version == task_version,
                )
            )
        return True


# ---------------------------------------------------------------------------
# SQLRoutingBackend  (composes the two sub-adapters above)
# ---------------------------------------------------------------------------


class SQLRoutingBackend:
    """RoutingBackendProtocol backed by SQLAlchemy. Delegates to sub-adapters."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._qca = SQLQueueConfigAdapter(session_factory)
        self._rca = SQLTaskRoutingConfigAdapter(session_factory)

    async def drop_stale_indexes(self) -> list[str]:
        """No-op: SQL indexes are managed by run_migrations, not versioned per-generation."""
        return []

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        return await self._qca.get_queue_config(queue)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        await self._qca.save_queue_config(queue_config)

    async def create_queue_config(self, queue_config: QueueConfig) -> bool:
        return await self._qca.create_queue_config(queue_config)

    async def delete_queue(self, queue_name: str) -> list[str]:
        return await self._qca.delete_queue(queue_name)

    async def get_all_queues(self) -> list[str]:
        return await self._qca.get_all_queues()

    async def get_queues(self, role: str) -> set[str]:
        return await self._qca.get_queues(role)

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        await self._qca.save_role(role, queues_set)

    async def create_role(self, role: str, queues_set: set[str]) -> bool:
        return await self._qca.create_role(role, queues_set)

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
