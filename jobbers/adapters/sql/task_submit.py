"""
SQLAlchemy task submit adapter.

- `SQLTaskSubmit` — TaskSubmitProtocol backed by SQLAlchemy (tasks + task_queue tables).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, insert, select, update

from jobbers.adapters.sql.task_state import _row_to_task, _task_to_row, _upsert_task
from jobbers.migrations.schema import dag_runs, task_queue, tasks

if TYPE_CHECKING:
    import datetime as dt

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task import Task


class SQLTaskSubmit:
    """
    TaskSubmitProtocol backed by SQLAlchemy.

    Tables: ``tasks`` and ``task_queue``.  Each submit/pop is a single transaction.
    Requires the same session factory as the paired ``SQLTaskState``.
    """

    # Key-helper stubs for structural compatibility with RedisTaskState.
    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], dsn: str = "") -> None:
        self._sf = session_factory
        self._dsn = dsn

    @property
    def _use_for_update(self) -> bool:
        return "sqlite" not in self._dsn

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """
        Atomically pop and return the oldest queued task from any of the given queues.

        Non-blocking: ``pop_timeout`` is ignored.  Removes the entry from ``task_queue``
        so subsequent calls will not return the same task.
        """
        if not queues:
            return None
        async with self._sf() as session:
            async with session.begin():
                stmt = (
                    select(task_queue.c.task_id)
                    .where(task_queue.c.queue.in_(queues))
                    .order_by(task_queue.c.submitted_at)
                    .limit(1)
                )
                if self._use_for_update:
                    stmt = stmt.with_for_update(skip_locked=True)
                result = await session.execute(stmt)
                row = result.first()
                if row is None:
                    return None
                task_id_str = row.task_id
                await session.execute(delete(task_queue).where(task_queue.c.task_id == task_id_str))
                task_result = await session.execute(select(tasks).where(tasks.c.id == task_id_str))
                task_row = task_result.first()
                return _row_to_task(task_row) if task_row is not None else None

    async def submit_task(self, task: Task) -> bool:
        """Submit a task directly (non-staged)."""
        assert task.submitted_at  # noqa: S101
        row = _task_to_row(task)
        task_id_str = str(task.id)
        async with self._sf() as session:
            async with session.begin():
                await _upsert_task(session, row)
                result = await session.execute(
                    update(task_queue)
                    .where(task_queue.c.task_id == task_id_str)
                    .values(queue=task.queue, submitted_at=task.submitted_at)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await session.execute(
                        insert(task_queue).values(
                            task_id=task_id_str, queue=task.queue, submitted_at=task.submitted_at
                        )
                    )
        if task.dag_run_id is not None:
            dag_run_id_str = str(task.dag_run_id)
            async with self._sf() as session:
                async with session.begin():
                    existing = await session.execute(
                        select(dag_runs).where(dag_runs.c.dag_run_id == dag_run_id_str)
                    )
                    if existing.first() is None:
                        await session.execute(
                            insert(dag_runs).values(dag_run_id=dag_run_id_str, submitted_at=task.submitted_at)
                        )
        return True

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """Rate-limited submit is not implemented for the SQL adapter."""
        raise NotImplementedError("SQLTaskSubmit does not support rate-limited submission")

    async def clean_rate_limiter(
        self, queues: set[bytes], now: dt.datetime, rate_limit_age: dt.timedelta
    ) -> None:
        pass
