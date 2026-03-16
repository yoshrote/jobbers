"""Migration runner: applies the Jobbers schema to the SQLite database."""
from __future__ import annotations

import asyncio
import os

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from jobbers.migrations.schema import metadata


async def run_migrations(engine: AsyncEngine) -> None:
    """Create all tables and indexes (idempotent — safe to call on an existing database)."""
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


async def run_cli() -> None:
    """CLI entry point: apply the schema to the configured database."""
    db_path = os.environ.get("SQLITE_PATH", "jobbers.db")
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        await run_migrations(engine)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_cli())
