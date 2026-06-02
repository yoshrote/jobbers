"""
SQLAlchemy dead-letter queue adapter.

- `SQLDeadQueue` — DeadQueueProtocol + AtomicDeadQueueProtocol backed by SQLAlchemy.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, insert, select, update

from jobbers.migrations.schema import dead_letter_queue
from jobbers.models.task import Task
from jobbers.utils.sql_transaction import SQLTransactionBatch

if TYPE_CHECKING:
    import datetime as dt

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from jobbers.protocols import TransactionHandle


def _task_to_dlq_row(task: Task, failed_at: dt.datetime) -> dict[str, Any]:
    return {
        "id": str(task.id),
        "queue": task.queue,
        "name": task.name,
        "version": task.version,
        "failed_at": failed_at,
        "task_data": task.model_dump_json(),
    }


def _dlq_row_to_task(row: Any) -> Task:
    data: dict[str, Any] = json.loads(row.task_data)
    return Task.model_validate(data)


class SQLDeadQueue:
    """DeadQueueProtocol and AtomicDeadQueueProtocol backed by SQLAlchemy (``dead_letter_queue`` table)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], dsn: str = "") -> None:
        self._sf = session_factory
        self._dsn = dsn

    @property
    def backend_key(self) -> str:
        """Stable identifier shared by all SQL adapters pointing to the same database."""
        return self._dsn or str(id(self._sf))

    def pipeline(self, transaction: bool = True) -> SQLTransactionBatch:
        """Return a new SQLTransactionBatch for staged atomic writes."""
        return SQLTransactionBatch(self._sf)

    async def ensure_index(self) -> None:
        """No-op: indexes are created by run_migrations."""

    # ── AtomicDeadQueueProtocol: staged writes ─────────────────────────────

    def stage_add(self, pipe: TransactionHandle, task: Task, failed_at: dt.datetime) -> None:
        """Stage an INSERT into dead_letter_queue."""
        row = _task_to_dlq_row(task, failed_at)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _insert(s: AsyncSession) -> None:
            exists = await s.execute(select(dead_letter_queue).where(dead_letter_queue.c.id == row["id"]))
            if exists.first() is None:
                await s.execute(insert(dead_letter_queue).values(**row))
            else:
                non_pk = {k: v for k, v in row.items() if k != "id"}
                await s.execute(
                    update(dead_letter_queue).where(dead_letter_queue.c.id == row["id"]).values(**non_pk)
                )

        pipe.add_op(_insert)

    def stage_remove(self, pipe: TransactionHandle, task_id: Any, queue: str, name: str) -> None:
        """Stage a DELETE from dead_letter_queue."""
        task_id_str = str(task_id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _delete(s: AsyncSession) -> None:
            await s.execute(delete(dead_letter_queue).where(dead_letter_queue.c.id == task_id_str))

        pipe.add_op(_delete)

    # ── DeadQueueProtocol: direct writes ───────────────────────────────────

    async def add_to_dlq(self, task: Task, failed_at: dt.datetime) -> None:
        """Add a task to the DLQ directly (saga path)."""
        row = _task_to_dlq_row(task, failed_at)
        async with self._sf() as session:
            async with session.begin():
                exists = await session.execute(
                    select(dead_letter_queue).where(dead_letter_queue.c.id == row["id"])
                )
                if exists.first() is None:
                    await session.execute(insert(dead_letter_queue).values(**row))
                else:
                    non_pk = {k: v for k, v in row.items() if k != "id"}
                    await session.execute(
                        update(dead_letter_queue).where(dead_letter_queue.c.id == row["id"]).values(**non_pk)
                    )

    async def remove_from_dlq(self, task_id: Any, queue: str, name: str) -> None:
        """Remove a task from the DLQ directly (saga path)."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(dead_letter_queue).where(dead_letter_queue.c.id == str(task_id)))

    # ── DeadQueueProtocol: reads ───────────────────────────────────────────

    async def get_history(self, task_id: str) -> list[dict[str, Any]]:
        """Return the per-attempt error history from the stored task blob."""
        async with self._sf() as session:
            result = await session.execute(select(dead_letter_queue).where(dead_letter_queue.c.id == task_id))
            row = result.first()
            if row is None:
                return []
            task = _dlq_row_to_task(row)
            return [{"attempt": i, "error": e} for i, e in enumerate(task.errors)]

    async def get_by_ids(self, task_ids: list[str]) -> list[Task]:
        """Fetch DLQ entries by explicit task ID list."""
        if not task_ids:
            return []
        async with self._sf() as session:
            result = await session.execute(
                select(dead_letter_queue).where(dead_letter_queue.c.id.in_(task_ids))
            )
            return [_dlq_row_to_task(row) for row in result.all()]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list[Task]:
        """Fetch DLQ entries matching the given filter criteria."""
        stmt = select(dead_letter_queue).order_by(dead_letter_queue.c.failed_at.desc())
        if queue is not None:
            stmt = stmt.where(dead_letter_queue.c.queue == queue)
        if task_name is not None:
            stmt = stmt.where(dead_letter_queue.c.name == task_name)
        if task_version is not None:
            stmt = stmt.where(dead_letter_queue.c.version == task_version)
        stmt = stmt.limit(limit)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [_dlq_row_to_task(row) for row in result.all()]

    async def remove_many(self, task_ids: list[str]) -> None:
        """Remove multiple DLQ entries in a single transaction."""
        if not task_ids:
            return
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(dead_letter_queue).where(dead_letter_queue.c.id.in_(task_ids)))

    async def clean(self, earlier_than: dt.datetime) -> None:
        """Delete DLQ entries older than ``earlier_than``."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(dead_letter_queue).where(dead_letter_queue.c.failed_at < earlier_than)
                )
