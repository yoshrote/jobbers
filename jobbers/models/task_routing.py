from __future__ import annotations

import json
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

from pydantic import BaseModel, model_validator
from sqlalchemy import delete, insert, select, update

from jobbers.migrations.schema import task_routing

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class RoutingStrategy(StrEnum):
    """Queue routing strategy for a task type."""

    SINGLE = "single"
    WEIGHTED = "weighted"


class RoutingConfig(BaseModel):
    """Queue routing configuration for a task type."""

    task_name: str = ""
    task_version: int = 0
    strategy: RoutingStrategy
    queues: list[str]
    weights: list[float] | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.strategy == RoutingStrategy.SINGLE:
            if len(self.queues) != 1:
                raise ValueError("SINGLE routing requires exactly one queue")
            if self.weights is not None:
                raise ValueError("SINGLE routing does not accept weights")
        elif self.strategy == RoutingStrategy.WEIGHTED:
            if len(self.queues) < 2:
                raise ValueError("WEIGHTED routing requires at least two queues")
            if self.weights is None or len(self.weights) != len(self.queues):
                raise ValueError("WEIGHTED routing requires weights with the same length as queues")
        return self

    @classmethod
    def from_row(cls, row: Any) -> Self:
        """Construct from a DB row (task_name, task_version, strategy, queues_json, weights_json)."""
        task_name, task_version, strategy, queues_json, weights_json = row
        return cls(
            task_name=task_name,
            task_version=task_version,
            strategy=RoutingStrategy(strategy),
            queues=json.loads(queues_json),
            weights=json.loads(weights_json) if weights_json is not None else None,
        )


class TaskRoutingConfigAdapter:
    """Manages task routing configuration in the SQL data store."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        """Return the routing config for a task type, or None if not configured."""
        async with self.session_factory() as session:
            result = await session.execute(
                select(
                    task_routing.c.task_name,
                    task_routing.c.task_version,
                    task_routing.c.strategy,
                    task_routing.c.queues,
                    task_routing.c.weights,
                ).where(
                    task_routing.c.task_name == task_name,
                    task_routing.c.task_version == task_version,
                )
            )
            row = result.fetchone()
        if row is None:
            return None
        return RoutingConfig.from_row(row)

    async def save_routing_config(self, config: RoutingConfig) -> None:
        """Create or replace the routing config for a task type."""
        queues_json = json.dumps(config.queues)
        weights_json = json.dumps(config.weights) if config.weights is not None else None
        async with self.session_factory.begin() as session:
            existing = await session.execute(
                select(task_routing.c.task_name).where(
                    task_routing.c.task_name == config.task_name,
                    task_routing.c.task_version == config.task_version,
                )
            )
            if existing.fetchone():
                await session.execute(
                    update(task_routing)
                    .where(
                        task_routing.c.task_name == config.task_name,
                        task_routing.c.task_version == config.task_version,
                    )
                    .values(strategy=config.strategy, queues=queues_json, weights=weights_json)
                )
            else:
                await session.execute(
                    insert(task_routing).values(
                        task_name=config.task_name,
                        task_version=config.task_version,
                        strategy=config.strategy,
                        queues=queues_json,
                        weights=weights_json,
                    )
                )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        """Remove the routing config for a task type. Returns False if it did not exist."""
        async with self.session_factory.begin() as session:
            existing = await session.execute(
                select(task_routing.c.task_name).where(
                    task_routing.c.task_name == task_name,
                    task_routing.c.task_version == task_version,
                )
            )
            if existing.fetchone() is None:
                return False
            await session.execute(
                delete(task_routing).where(
                    task_routing.c.task_name == task_name,
                    task_routing.c.task_version == task_version,
                )
            )
        return True
