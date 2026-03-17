from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

from pydantic import BaseModel
from sqlalchemy import delete, insert, select, update
from sqlalchemy.exc import IntegrityError
from ulid import ULID

from jobbers.migrations.schema import queues, role_queues, roles

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class RatePeriod(StrEnum):
    """Enumeration of rate limiting periods."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


class QueueConfig(BaseModel):
    """Configuration for a task queue."""

    name: str  # Name of the queue
    max_concurrent: int | None = (
        10  # Maximum number of concurrent tasks that can be processed from this queue
    )
    # Rate limiting is {task_number} tasks every {rate_number} {rate_period}
    #  e.g. 5 tasks every 2 minutes
    rate_numerator: int | None = None  # Number of tasks to process from this queue
    rate_denominator: int | None = None  # Number of tasks to rate limit
    rate_period: RatePeriod | None = None  # Period for rate limiting

    def period_in_seconds(self) -> int | None:
        """Convert the rate period to seconds."""
        if self.rate_period is None or self.rate_denominator is None:
            return None
        match self.rate_period:
            case RatePeriod.SECOND:
                return 1 * self.rate_denominator
            case RatePeriod.MINUTE:
                return 60 * self.rate_denominator
            case RatePeriod.HOUR:
                return 3600 * self.rate_denominator
            case RatePeriod.DAY:
                return 86400 * self.rate_denominator

    @classmethod
    def from_row(cls, row: Any) -> Self:
        """Construct from a row (name, max_concurrent, rate_numerator, rate_denominator, rate_period)."""
        name, max_concurrent, rate_numerator, rate_denominator, rate_period = row
        return cls(
            name=name,
            max_concurrent=max_concurrent,
            rate_numerator=rate_numerator,
            rate_denominator=rate_denominator,
            rate_period=RatePeriod(rate_period) if rate_period else None,
        )


class QueueConfigAdapter:
    """
    Manages queue configuration in a SQLite data store via SQLAlchemy async sessions.

    - `roles`: table of named roles, each with a refresh_tag for change detection.
    - `queues`: table of queue configurations (concurrency and rate limiting).
    - `role_queues`: many-to-many mapping of roles to queues.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def get_queues(self, role: str) -> set[str]:
        async with self.session_factory() as session:
            result = await session.execute(select(role_queues.c.queue).where(role_queues.c.role == role))
            return {row[0] for row in result.fetchall()}

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        new_tag = str(ULID())
        async with self.session_factory.begin() as session:
            existing = await session.execute(select(roles.c.name).where(roles.c.name == role))
            if existing.fetchone():
                await session.execute(update(roles).where(roles.c.name == role).values(refresh_tag=new_tag))
            else:
                await session.execute(insert(roles).values(name=role, refresh_tag=new_tag))
            await session.execute(delete(role_queues).where(role_queues.c.role == role))
            if queues_set:
                await session.execute(
                    insert(role_queues),
                    [{"role": role, "queue": q} for q in queues_set],
                )

    async def get_all_queues(self) -> list[str]:
        async with self.session_factory() as session:
            result = await session.execute(select(queues.c.name).order_by(queues.c.name))
            return [row[0] for row in result.fetchall()]

    async def get_all_roles(self) -> list[str]:
        async with self.session_factory() as session:
            result = await session.execute(select(roles.c.name).order_by(roles.c.name))
            return [row[0] for row in result.fetchall()]

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        async with self.session_factory() as session:
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
        async with self.session_factory.begin() as session:
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

    async def delete_queue(self, queue_name: str) -> None:
        """Delete a queue and cascade to role_queues; bump refresh_tag for affected roles."""
        new_tag = str(ULID())
        async with self.session_factory.begin() as session:
            result = await session.execute(
                select(role_queues.c.role).where(role_queues.c.queue == queue_name).distinct()
            )
            affected_roles = [row[0] for row in result.fetchall()]
            await session.execute(delete(queues).where(queues.c.name == queue_name))
            if affected_roles:
                await session.execute(
                    update(roles).where(roles.c.name.in_(affected_roles)).values(refresh_tag=new_tag)
                )

    async def delete_role(self, role: str) -> None:
        """Delete a role (cascades to role_queues). Queue configs are preserved."""
        async with self.session_factory.begin() as session:
            await session.execute(delete(roles).where(roles.c.name == role))

    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]:
        if not queues_set:
            return {}
        async with self.session_factory() as session:
            result = await session.execute(
                select(queues.c.name, queues.c.max_concurrent).where(queues.c.name.in_(list(queues_set)))
            )
            found = {row[0]: row[1] for row in result.fetchall()}
        return {q: found.get(q) for q in queues_set}

    async def get_refresh_tag(self, role: str) -> ULID:
        """Return the current refresh tag for a role, creating one if needed."""
        async with self.session_factory() as session:
            result = await session.execute(select(roles.c.refresh_tag).where(roles.c.name == role))
            row = result.fetchone()
        if row is not None:
            existing_tag: ULID = ULID.from_str(row[0])
            return existing_tag

        init_tag = ULID()
        try:
            async with self.session_factory.begin() as session:
                await session.execute(insert(roles).values(name=role, refresh_tag=str(init_tag)))
        except IntegrityError:
            pass  # Another process inserted first
        # Re-read in case another process won the race
        async with self.session_factory() as session:
            result = await session.execute(select(roles.c.refresh_tag).where(roles.c.name == role))
            row = result.fetchone()
        return ULID.from_str(row[0]) if row else init_tag
