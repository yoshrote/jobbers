"""
SqlPipeline — accumulates SQL DML operations for deferred execution.

Isolated in its own module so that neither ``task_adapter.py`` nor ``sql.py``
must import the other at module load time, avoiding circular imports while still
allowing a strict ``TaskPipeline = Pipeline | SqlPipeline`` union type.
"""

from __future__ import annotations

from typing import Any

import aiosqlite  # noqa: TC002


class SqlPipeline:
    """
    Accumulates SQL DML operations for deferred execution in a single transaction.

    Call ``queue_sql()`` to enqueue operations; call ``execute()`` to flush
    them all inside a single BEGIN…COMMIT block.  Mirrors the Redis
    ``Pipeline.execute()`` interface so callers can treat both uniformly.
    """

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn
        self._ops: list[tuple[str, list[Any]]] = []

    def queue_sql(self, sql: str, params: list[Any] | None = None) -> None:
        """Append a SQL statement and its bound parameters."""
        self._ops.append((sql, params or []))

    async def execute(self) -> list[Any]:
        """Run all queued statements inside a single transaction and clear the queue."""
        if not self._ops:
            return []
        try:
            for sql, params in self._ops:
                await self._conn.execute(sql, params)
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise
        finally:
            self._ops.clear()
        return []
