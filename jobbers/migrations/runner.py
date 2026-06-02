"""Migration runner: applies the Jobbers schema to the SQL database."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

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


async def run_cli() -> None:
    """CLI entry point: apply the schema to the configured database."""
    db_path = os.environ.get("SQL_PATH", "sqlite+aiosqlite:///jobbers.db")
    engine = create_async_engine(db_path)
    try:
        await run_migrations(engine)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_cli())
