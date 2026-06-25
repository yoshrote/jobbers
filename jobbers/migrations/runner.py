"""Migration runner: applies the Jobbers schema to the SQL database."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from jobbers.migrations.schema import TABLE_GROUPS, metadata

if TYPE_CHECKING:
    from sqlalchemy import Table


async def run_migrations(
    engine: AsyncEngine,
    features: set[str] | None = None,
) -> None:
    """
    Create tables and indexes (idempotent — safe to call on an existing database).

    ``features`` controls which table groups are created:

    - ``None`` (default) — create all tables (backward-compatible behaviour).
    - A set of feature names (``"routing"``, ``"task_state"``, ``"dead_letter"``,
      ``"task_schedule"``) — create only the tables for those features.
    """
    tables: list[Table] | None
    if features is None:
        tables = None
    else:
        tables = [t for f in features for t in TABLE_GROUPS.get(f, [])] or None
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: metadata.create_all(c, tables=tables))


async def ensure_redis_json_routing_indexes(redis_url: str) -> None:
    """Create the RedisJSON routing backend's RediSearch indexes (idempotent)."""
    from jobbers.adapters.redis_json import RedisJSONRoutingBackend

    client = redis.from_url(redis_url)
    try:
        await RedisJSONRoutingBackend(client).ensure_indexes()
    finally:
        await client.close()


async def run_cli() -> None:
    """CLI entry point: apply the schema to the configured database, plus any RedisJSON indexes."""
    db_path = os.environ.get("SQL_PATH", "sqlite+aiosqlite:///jobbers.db")
    engine = create_async_engine(db_path)
    try:
        await run_migrations(engine)
    finally:
        await engine.dispose()

    if os.environ.get("ROUTING_BACKEND", "sql") == "redis_json":
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        await ensure_redis_json_routing_indexes(redis_url)


def main() -> None:
    """Run the synchronous entry point used by the ``jobbers_migrate`` console script."""
    asyncio.run(run_cli())


if __name__ == "__main__":
    main()
