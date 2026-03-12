from __future__ import annotations

from enum import StrEnum
from typing import Any, Self

import aiosqlite
from pydantic import BaseModel
from ulid import ULID


class RatePeriod(StrEnum):
    """Enumeration of rate limiting periods."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


class QueueConfig(BaseModel):
    """Configuration for a task queue."""

    name: str  # Name of the queue
    max_concurrent: int | None = 10  # Maximum number of concurrent tasks that can be processed from this queue
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
        """Construct from a sqlite3 row (name, max_concurrent, rate_numerator, rate_denominator, rate_period)."""
        name, max_concurrent, rate_numerator, rate_denominator, rate_period = row
        return cls(
            name=name,
            max_concurrent=max_concurrent,
            rate_numerator=rate_numerator,
            rate_denominator=rate_denominator,
            rate_period=RatePeriod(rate_period) if rate_period else None,
        )


async def create_schema(conn: aiosqlite.Connection) -> None:
    """Create tables and indexes for queue/role configuration."""
    await conn.executescript("""
        CREATE TABLE IF NOT EXISTS roles (
            name        TEXT PRIMARY KEY,
            refresh_tag TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS queues (
            name             TEXT PRIMARY KEY,
            max_concurrent   INTEGER,
            rate_numerator   INTEGER,
            rate_denominator INTEGER,
            rate_period      TEXT
        );

        CREATE TABLE IF NOT EXISTS role_queues (
            role  TEXT NOT NULL REFERENCES roles(name)  ON DELETE CASCADE,
            queue TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
            PRIMARY KEY (role, queue)
        );

        CREATE INDEX IF NOT EXISTS idx_roles_refresh_tag ON roles (refresh_tag);
        CREATE INDEX IF NOT EXISTS idx_role_queues_role_queue ON role_queues (role, queue);
    """)
    await conn.commit()


class QueueConfigAdapter:
    """
    Manages queue configuration in a SQLite data store.

    - `roles`: table of named roles, each with a refresh_tag for change detection.
    - `queues`: table of queue configurations (concurrency and rate limiting).
    - `role_queues`: many-to-many mapping of roles to queues.
    """

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self.conn = conn

    async def get_queues(self, role: str) -> set[str]:
        async with self.conn.execute(
            "SELECT queue FROM role_queues WHERE role = ?", (role,)
        ) as cursor:
            return {row[0] for row in await cursor.fetchall()}

    async def save_role(self, role: str, queues: set[str]) -> None:
        new_tag = str(ULID())
        async with self.conn.cursor() as cur:
            try:
                await cur.execute(
                    "INSERT INTO roles (name, refresh_tag) VALUES (?, ?)"
                    " ON CONFLICT(name) DO UPDATE SET refresh_tag = excluded.refresh_tag",
                    (role, new_tag),
                )
                await cur.execute("DELETE FROM role_queues WHERE role = ?", (role,))
                if queues:
                    await cur.executemany(
                        "INSERT INTO role_queues (role, queue) VALUES (?, ?)",
                        [(role, q) for q in queues],
                    )
            except aiosqlite.IntegrityError:
                await self.conn.rollback()
                raise
            else:
                await self.conn.commit()

    async def get_all_queues(self) -> list[str]:
        async with self.conn.execute("SELECT name FROM queues ORDER BY name") as cursor:
            return [row[0] for row in await cursor.fetchall()]

    async def get_all_roles(self) -> list[str]:
        async with self.conn.execute("SELECT name FROM roles ORDER BY name") as cursor:
            return [row[0] for row in await cursor.fetchall()]

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        async with self.conn.execute(
            "SELECT name, max_concurrent, rate_numerator, rate_denominator, rate_period"
            " FROM queues WHERE name = ?",
            (queue,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return QueueConfig.from_row(row)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        await self.conn.execute(
            "INSERT INTO queues (name, max_concurrent, rate_numerator, rate_denominator, rate_period)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(name) DO UPDATE SET"
            "   max_concurrent   = excluded.max_concurrent,"
            "   rate_numerator   = excluded.rate_numerator,"
            "   rate_denominator = excluded.rate_denominator,"
            "   rate_period      = excluded.rate_period",
            (
                queue_config.name,
                queue_config.max_concurrent,
                queue_config.rate_numerator,
                queue_config.rate_denominator,
                queue_config.rate_period,
            ),
        )
        await self.conn.commit()

    async def delete_queue(self, queue_name: str) -> None:
        """Delete a queue and cascade to role_queues; bump refresh_tag for affected roles."""
        new_tag = str(ULID())
        async with self.conn.cursor() as cur:
            # Collect roles that reference this queue before deleting
            await cur.execute("SELECT DISTINCT role FROM role_queues WHERE queue = ?", (queue_name,))
            affected_roles = [row[0] for row in await cur.fetchall()]
            await cur.execute("DELETE FROM queues WHERE name = ?", (queue_name,))
            if affected_roles:
                await cur.executemany(
                    "UPDATE roles SET refresh_tag = ? WHERE name = ?",
                    [(new_tag, r) for r in affected_roles],
                )
        await self.conn.commit()

    async def delete_role(self, role: str) -> None:
        """Delete a role (cascades to role_queues). Queue configs are preserved."""
        await self.conn.execute("DELETE FROM roles WHERE name = ?", (role,))
        await self.conn.commit()

    async def get_queue_limits(self, queues: set[str]) -> dict[str, int | None]:
        if not queues:
            return {}
        placeholders = ",".join("?" * len(queues))
        async with self.conn.execute(
            f"SELECT name, max_concurrent FROM queues WHERE name IN ({placeholders})",
            list(queues),
        ) as cursor:
            found = {row[0]: row[1] for row in await cursor.fetchall()}
        return {q: found.get(q) for q in queues}

    async def get_refresh_tag(self, role: str) -> ULID:
        """Return the current refresh tag for a role, creating one if needed."""
        async with self.conn.execute(
            "SELECT refresh_tag FROM roles WHERE name = ?", (role,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is not None:
            return ULID.from_str(row[0])

        init_tag = ULID()
        await self.conn.execute(
            "INSERT OR IGNORE INTO roles (name, refresh_tag) VALUES (?, ?)",
            (role, str(init_tag)),
        )
        await self.conn.commit()
        # Re-read in case another process won the race
        async with self.conn.execute(
            "SELECT refresh_tag FROM roles WHERE name = ?", (role,)
        ) as cursor:
            row = await cursor.fetchone()
        return ULID.from_str(row[0]) if row else init_tag
