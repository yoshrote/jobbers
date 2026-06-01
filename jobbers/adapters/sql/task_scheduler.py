"""
SQLAlchemy task scheduler adapter.

- `SQLTaskScheduler` — TaskSchedulerProtocol + AtomicTaskSchedulerProtocol backed by SQLAlchemy.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, insert, select, update

from jobbers.migrations.schema import task_schedule
from jobbers.models.task import Task
from jobbers.utils.sql_transaction import SQLTransactionBatch

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from jobbers.protocols import TransactionHandle


def _task_schedule_ensure_utc_nn(d: dt.datetime) -> dt.datetime:
    """SQLite returns naive datetimes; re-attach UTC (non-nullable variant)."""
    return d if d.tzinfo is not None else d.replace(tzinfo=dt.UTC)


def _task_to_schedule_row(task: Task, run_at: dt.datetime) -> dict[str, Any]:
    return {
        "task_id": str(task.id),
        "queue": task.queue,
        "task_data": task.model_dump_json(),
        "run_at": run_at,
    }


def _schedule_row_to_task(row: Any) -> Task:
    data: dict[str, Any] = json.loads(row.task_data)
    return Task.model_validate(data)


class SQLTaskScheduler:
    """
    Scheduled-task queue backed by SQLAlchemy (``task_schedule`` table).

    Implements ``AtomicTaskSchedulerProtocol`` using ``SQLTransactionBatch``.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        get_all_queues: Callable[[], Any],
        dsn: str = "",
    ) -> None:
        self._sf = session_factory
        self._get_all_queues = get_all_queues
        self._dsn = dsn

    @property
    def backend_key(self) -> str:
        """Stable identifier shared by all SQL adapters pointing to the same database."""
        return self._dsn or str(id(self._sf))

    def pipeline(self, transaction: bool = True) -> SQLTransactionBatch:
        """Return a new SQLTransactionBatch for staged atomic writes."""
        return SQLTransactionBatch(self._sf)

    # ── AtomicTaskSchedulerProtocol: staged writes ─────────────────────────

    def stage_add(self, pipe: TransactionHandle, task: Task, run_at: dt.datetime) -> None:
        """Stage an INSERT-or-UPDATE in task_schedule."""
        row = _task_to_schedule_row(task, run_at)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _upsert(s: AsyncSession) -> None:
            result = await s.execute(
                update(task_schedule)
                .where(task_schedule.c.task_id == row["task_id"])
                .values(queue=row["queue"], task_data=row["task_data"], run_at=row["run_at"])
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                await s.execute(insert(task_schedule).values(**row))

        pipe.add_op(_upsert)

    def stage_remove(self, pipe: TransactionHandle, task_id: Any, queue: str) -> None:
        """Stage a DELETE from task_schedule."""
        task_id_str = str(task_id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _delete(s: AsyncSession) -> None:
            await s.execute(delete(task_schedule).where(task_schedule.c.task_id == task_id_str))

        pipe.add_op(_delete)

    # ── TaskSchedulerProtocol: direct writes ───────────────────────────────

    async def add(self, task: Task, run_at: dt.datetime) -> None:
        """Add a task to the schedule directly (saga path)."""
        row = _task_to_schedule_row(task, run_at)
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    update(task_schedule)
                    .where(task_schedule.c.task_id == row["task_id"])
                    .values(queue=row["queue"], task_data=row["task_data"], run_at=row["run_at"])
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await session.execute(insert(task_schedule).values(**row))

    async def remove(self, task_id: Any, queue: str) -> None:
        """Remove a task from the schedule directly (saga path)."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(task_schedule).where(task_schedule.c.task_id == str(task_id)))

    # ── TaskSchedulerProtocol: reads ───────────────────────────────────────

    async def get_run_at(self, task_id: Any) -> dt.datetime | None:
        """Return the scheduled run_at for a task, or None if not scheduled."""
        async with self._sf() as session:
            result = await session.execute(
                select(task_schedule.c.run_at).where(task_schedule.c.task_id == str(task_id))
            )
            row = result.first()
            return _task_schedule_ensure_utc_nn(row.run_at) if row is not None else None

    async def next_due(self, queues: list[str] | None) -> Task | None:
        """Acquire and return the single earliest due task, or None."""
        results = await self.next_due_bulk(1, queues=queues)
        return results[0][0] if results else None

    async def next_due_bulk(self, n: int, queues: list[str] | None = None) -> list[tuple[Task, dt.datetime]]:
        """
        Atomically acquire and return up to n due tasks with their scheduled run_at.

        Tasks are removed from the schedule as they are acquired.
        """
        if queues is not None and not queues:
            return []
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            async with session.begin():
                stmt = select(task_schedule).where(task_schedule.c.run_at <= now)
                if queues is not None:
                    stmt = stmt.where(task_schedule.c.queue.in_(queues))
                elif queues is None:
                    all_q = await self._get_all_queues()
                    if all_q:
                        stmt = stmt.where(task_schedule.c.queue.in_(all_q))
                stmt = stmt.order_by(task_schedule.c.run_at).limit(n)
                result = await session.execute(stmt)
                rows = result.all()
                if not rows:
                    return []
                task_ids = [row.task_id for row in rows]
                await session.execute(delete(task_schedule).where(task_schedule.c.task_id.in_(task_ids)))
        return [(_schedule_row_to_task(row), _task_schedule_ensure_utc_nn(row.run_at)) for row in rows]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
        start_after: str | None = None,
    ) -> list[tuple[Task, dt.datetime]]:
        """Fetch scheduled entries matching the given filter criteria."""
        from ulid import ULID

        stmt = select(task_schedule).order_by(task_schedule.c.run_at)
        if queue is not None:
            stmt = stmt.where(task_schedule.c.queue == queue)
        if start_after is not None:
            cursor = str(ULID.from_str(start_after))
            stmt = stmt.where(task_schedule.c.task_id > cursor)
        stmt = stmt.limit(limit * 10)  # over-fetch to allow name/version filtering
        async with self._sf() as session:
            result = await session.execute(stmt)
            rows = result.all()
        results: list[tuple[Task, dt.datetime]] = []
        for row in rows:
            if len(results) >= limit:
                break
            task = _schedule_row_to_task(row)
            if task_name is not None and task.name != task_name:
                continue
            if task_version is not None and task.version != task_version:
                continue
            results.append((task, _task_schedule_ensure_utc_nn(row.run_at)))
        return results
