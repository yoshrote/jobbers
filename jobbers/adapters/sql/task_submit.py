"""
SQLAlchemy task submit adapter.

- `SQLTaskSubmit` — TaskSubmitProtocol backed by SQLAlchemy (tasks + task_queue tables).
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError

from jobbers.adapters.sql.task_state import _row_to_task, _task_to_row, _upsert_task
from jobbers.migrations.schema import dag_runs, rate_limit_anchors, rate_limit_entries, task_queue, tasks

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task import Task


async def _register_dag_run(session_factory: async_sessionmaker[AsyncSession], task: Task) -> None:
    """Register the task's DAG run in ``dag_runs`` if it isn't already tracked."""
    if task.dag_run_id is None:
        return
    dag_run_id_str = str(task.dag_run_id)
    async with session_factory() as session:
        async with session.begin():
            existing = await session.execute(select(dag_runs).where(dag_runs.c.dag_run_id == dag_run_id_str))
            if existing.first() is None:
                await session.execute(
                    insert(dag_runs).values(dag_run_id=dag_run_id_str, submitted_at=task.submitted_at)
                )


class SQLTaskSubmit:
    """
    TaskSubmitProtocol backed by SQLAlchemy.

    Tables: ``tasks`` and ``task_queue``.  Each submit/pop is a single transaction.
    Requires the same session factory as the paired ``SQLTaskState``.
    """

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
        await _register_dag_run(self._sf, task)
        return True

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """
        Enqueue the task only if the queue's sliding rate-limit window has room.

        Mirrors the Redis sorted-set sliding window: ``rate_limit_entries`` holds one row
        per (queue, task_id) currently inside the window. Expired entries are pruned before
        the limit check, and re-submitting an already-tracked task_id is idempotent (refreshes
        the timestamp without consuming a new slot). A per-queue row in ``rate_limit_anchors``
        is locked via SELECT ... FOR UPDATE to serialize concurrent submitters to the same queue.
        """
        assert task.submitted_at  # noqa: S101
        period = queue_config.period_in_seconds()
        limit = queue_config.rate_numerator
        if period is None or limit is None:
            return await self.submit_task(task)

        now = task.submitted_at
        window_start = now - dt.timedelta(seconds=period)
        queue_name = task.queue
        task_id_str = str(task.id)

        async with self._sf() as session:
            async with session.begin():
                async with session.begin_nested() as sp:
                    try:
                        await session.execute(insert(rate_limit_anchors).values(queue=queue_name))
                    except IntegrityError:
                        await sp.rollback()

                anchor_stmt = select(rate_limit_anchors.c.queue).where(
                    rate_limit_anchors.c.queue == queue_name
                )
                if self._use_for_update:
                    anchor_stmt = anchor_stmt.with_for_update()
                await session.execute(anchor_stmt)

                await session.execute(
                    delete(rate_limit_entries).where(
                        rate_limit_entries.c.queue == queue_name,
                        rate_limit_entries.c.submitted_at < window_start,
                    )
                )

                existing = await session.execute(
                    select(rate_limit_entries.c.task_id).where(
                        rate_limit_entries.c.queue == queue_name,
                        rate_limit_entries.c.task_id == task_id_str,
                    )
                )
                is_resubmit = existing.first() is not None

                if not is_resubmit:
                    count_result = await session.execute(
                        select(func.count())
                        .select_from(rate_limit_entries)
                        .where(rate_limit_entries.c.queue == queue_name)
                    )
                    if count_result.scalar_one() >= limit:
                        return False

                result = await session.execute(
                    update(rate_limit_entries)
                    .where(
                        rate_limit_entries.c.queue == queue_name,
                        rate_limit_entries.c.task_id == task_id_str,
                    )
                    .values(submitted_at=now)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await session.execute(
                        insert(rate_limit_entries).values(
                            queue=queue_name, task_id=task_id_str, submitted_at=now
                        )
                    )

                await _upsert_task(session, _task_to_row(task))
                enqueue_result = await session.execute(
                    update(task_queue)
                    .where(task_queue.c.task_id == task_id_str)
                    .values(queue=queue_name, submitted_at=now)
                )
                if enqueue_result.rowcount == 0:  # type: ignore[attr-defined]
                    await session.execute(
                        insert(task_queue).values(task_id=task_id_str, queue=queue_name, submitted_at=now)
                    )

        await _register_dag_run(self._sf, task)
        return True

    async def clean_rate_limiter(
        self, queues: set[str], now: dt.datetime, rate_limit_age: dt.timedelta
    ) -> None:
        """Remove rate-limit window entries older than ``rate_limit_age`` for the given queues."""
        if not queues:
            return
        cutoff = now - rate_limit_age
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(rate_limit_entries).where(
                        rate_limit_entries.c.queue.in_(queues),
                        rate_limit_entries.c.submitted_at < cutoff,
                    )
                )
