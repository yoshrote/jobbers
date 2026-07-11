"""
SQLAlchemy task state adapter.

- `SQLTaskState` — TaskStateProtocol + AtomicTaskStateProtocol backed by SQLAlchemy.
  Uses SELECT FOR UPDATE (non-SQLite) in atomic_dispatch_scheduled instead of WATCH/MULTI.

Shared helpers used by sql/task_submit.py are also defined here and re-exported.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError
from ulid import ULID

from jobbers.migrations.schema import (
    dag_run_pending,
    dag_runs,
    fan_in_anchors,
    task_fan_in,
    task_queue,
    tasks,
)
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_status import TaskStatus
from jobbers.utils.sql_transaction import SQLTransactionBatch

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from jobbers.models.dag import DAGRunPagination
    from jobbers.protocols import TransactionHandle

logger = logging.getLogger(__name__)


def _task_to_row(task: Task) -> dict[str, Any]:
    """Serialize a Task to a dict of column values."""
    data = task.model_dump(mode="json")
    return {
        "id": str(task.id),
        "name": task.name,
        "queue": task.queue,
        "version": task.version,
        "status": task.status.value,
        "retry_attempt": task.retry_attempt,
        "submitted_at": task.submitted_at,
        "retried_at": task.retried_at,
        "started_at": task.started_at,
        "heartbeat_at": task.heartbeat_at,
        "completed_at": task.completed_at,
        "inject_parent_results": task.inject_parent_results,
        "cron_id": str(task.cron_id) if task.cron_id is not None else None,
        "dag_run_id": str(task.dag_run_id) if task.dag_run_id is not None else None,
        "parameters": json.dumps(data.get("parameters", {})),
        "results": json.dumps(data.get("results", {})),
        "errors": json.dumps(data.get("errors", [])),
        "parent_ids": json.dumps(data.get("parent_ids", [])),
        "dag_callbacks": json.dumps(data.get("dag_callbacks", [])),
    }


def _ensure_utc(d: dt.datetime | None) -> dt.datetime | None:
    """SQLite returns naive datetimes even for DateTime(timezone=True) columns; re-attach UTC."""
    if d is None or d.tzinfo is not None:
        return d
    return d.replace(tzinfo=dt.UTC)


def _ensure_utc_nn(d: dt.datetime) -> dt.datetime:
    """Non-nullable variant of _ensure_utc."""
    return d if d.tzinfo is not None else d.replace(tzinfo=dt.UTC)


def _row_to_task(row: Any) -> Task:
    """Deserialize a tasks-table row to a Task."""
    data: dict[str, Any] = {
        "id": row.id,
        "name": row.name,
        "queue": row.queue,
        "version": row.version,
        "status": row.status,
        "retry_attempt": row.retry_attempt,
        "submitted_at": _ensure_utc(row.submitted_at),
        "retried_at": _ensure_utc(row.retried_at),
        "started_at": _ensure_utc(row.started_at),
        "heartbeat_at": _ensure_utc(row.heartbeat_at),
        "completed_at": _ensure_utc(row.completed_at),
        "inject_parent_results": row.inject_parent_results,
        "cron_id": row.cron_id,
        "dag_run_id": row.dag_run_id,
        "parameters": json.loads(row.parameters) if row.parameters else {},
        "results": json.loads(row.results) if row.results else {},
        "errors": json.loads(row.errors) if row.errors else [],
        "parent_ids": json.loads(row.parent_ids) if row.parent_ids else [],
        "dag_callbacks": json.loads(row.dag_callbacks) if row.dag_callbacks else [],
    }
    return Task.model_validate(data)


async def _upsert_task(session: AsyncSession, row: dict[str, Any]) -> None:
    """INSERT or UPDATE a task row by id."""
    task_id = row["id"]
    non_pk = {k: v for k, v in row.items() if k != "id"}
    result = await session.execute(update(tasks).where(tasks.c.id == task_id).values(**non_pk))
    if result.rowcount == 0:  # type: ignore[attr-defined]
        await session.execute(insert(tasks).values(**row))


class SQLTaskState:
    """
    TaskStateProtocol + AtomicTaskStateProtocol backed by SQLAlchemy.

    Tables: ``tasks`` / ``task_fan_in`` / ``dag_runs``.  Uses ``SQLTransactionBatch``
    for staged writes.  ``atomic_dispatch_scheduled`` uses ``SELECT FOR UPDATE``
    (non-SQLite) instead of Redis WATCH/MULTI — no retry loop required.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], dsn: str = "") -> None:
        self._sf = session_factory
        self._dsn = dsn

    @property
    def backend_key(self) -> str:
        """Stable identifier shared by all SQL adapters pointing to the same database."""
        return self._dsn or str(id(self._sf))

    @property
    def _use_for_update(self) -> bool:
        return "sqlite" not in self._dsn

    def pipeline(self, transaction: bool = True) -> SQLTransactionBatch:
        """Return a new SQLTransactionBatch for staged atomic writes."""
        return SQLTransactionBatch(self._sf)

    # ── AtomicTaskStateProtocol: staged writes ─────────────────────────────

    def stage_save(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage an INSERT-or-UPDATE for this task."""
        row = _task_to_row(task)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _do_upsert(s: AsyncSession) -> None:
            await _upsert_task(s, row)

        pipe.add_op(_do_upsert)

    def _stage_enqueue(self, pipe: SQLTransactionBatch, task: Task) -> None:
        """Stage an INSERT-or-UPDATE in task_queue."""
        task_id_str = str(task.id)
        queue_name = task.queue
        submitted_at = task.submitted_at or dt.datetime.now(dt.UTC)

        async def _enqueue(s: AsyncSession) -> None:
            result = await s.execute(
                update(task_queue)
                .where(task_queue.c.task_id == task_id_str)
                .values(queue=queue_name, submitted_at=submitted_at)
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                await s.execute(
                    insert(task_queue).values(
                        task_id=task_id_str, queue=queue_name, submitted_at=submitted_at
                    )
                )

        pipe.add_op(_enqueue)

    def stage_requeue(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage a save + re-enqueue in task_queue."""
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101
        self.stage_save(pipe, task)
        self._stage_enqueue(pipe, task)

    def stage_submit_task(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage a save + enqueue in task_queue + DAG-run registration."""
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101
        self.stage_save(pipe, task)
        self._stage_enqueue(pipe, task)
        if task.dag_run_id is not None and task.submitted_at is not None:
            dag_run_id_str = str(task.dag_run_id)
            submitted_at = task.submitted_at
            task_id_str = str(task.id)

            async def _register_dag_run(s: AsyncSession) -> None:
                existing = await s.execute(select(dag_runs).where(dag_runs.c.dag_run_id == dag_run_id_str))
                if existing.first() is None:
                    await s.execute(
                        insert(dag_runs).values(dag_run_id=dag_run_id_str, submitted_at=submitted_at)
                    )
                async with s.begin_nested() as sp:
                    try:
                        await s.execute(
                            insert(dag_run_pending).values(dag_run_id=dag_run_id_str, task_id=task_id_str)
                        )
                    except IntegrityError:
                        await sp.rollback()

            pipe.add_op(_register_dag_run)

    def stage_remove_from_queue(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage a DELETE from task_queue; task record in tasks is preserved."""
        task_id_str = str(task.id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _remove(s: AsyncSession) -> None:
            await s.execute(delete(task_queue).where(task_queue.c.task_id == task_id_str))

        pipe.add_op(_remove)

    def stage_init_fan_in(
        self,
        pipe: TransactionHandle,
        dag_run_id: ULID,
        fan_in_key: str,
        predecessor_ids: set[ULID],
        ttl: int = 86400,
    ) -> None:
        """Stage INSERT of predecessor task-ids into task_fan_in (dag_run_id unused: fan_in_key is already unique)."""
        now = dt.datetime.now(dt.UTC)
        rows = [{"fan_in_key": fan_in_key, "task_id": str(pid), "created_at": now} for pid in predecessor_ids]
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _insert_fan_in(s: AsyncSession) -> None:
            async with s.begin_nested() as sp:
                try:
                    await s.execute(insert(fan_in_anchors).values(fan_in_key=fan_in_key))
                except IntegrityError:
                    await sp.rollback()
            for row in rows:
                async with s.begin_nested() as sp:
                    try:
                        await s.execute(insert(task_fan_in).values(**row))
                    except IntegrityError:
                        await sp.rollback()

        pipe.add_op(_insert_fan_in)

    async def read_for_watch(self, pipe: TransactionHandle, task_id: ULID) -> Task | None:
        """Read a task inside the batch transaction, locking the row on backends that support FOR UPDATE."""
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101
        session = await pipe._get_session()
        stmt = select(tasks).where(tasks.c.id == str(task_id))
        if self._use_for_update:
            stmt = stmt.with_for_update()
        try:
            result = await session.execute(stmt)
        except Exception:
            # This SELECT runs outside execute()'s own try/finally (the session must stay
            # open for later staged writes in the same transaction on success), so a failure
            # here must close the session itself or the connection leaks from the pool.
            await pipe.abort()
            raise
        row = result.first()
        return _row_to_task(row) if row is not None else None

    def stage_remove_heartbeat(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage an UPDATE that clears heartbeat_at for this task."""
        task_id_str = str(task.id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _clear(s: AsyncSession) -> None:
            await s.execute(update(tasks).where(tasks.c.id == task_id_str).values(heartbeat_at=None))

        pipe.add_op(_clear)

    async def atomic_dispatch_scheduled(
        self,
        task: Task,
        stage_extra: Callable[[TransactionHandle], None],
    ) -> bool:
        """
        Transition a SCHEDULED task to SUBMITTED within a single SQL transaction.

        Uses SELECT FOR UPDATE (non-SQLite) to lock the row for the duration of the
        transaction — no retry loop required, unlike the Redis WATCH/MULTI path.
        Returns True if dispatched, False if the task was missing or already CANCELLED.
        """
        pipe = self.pipeline(transaction=True)
        current = await self.read_for_watch(pipe, task.id)
        if current is None or current.status == TaskStatus.CANCELLED:
            await pipe.execute()
            return False
        task.set_status(TaskStatus.SUBMITTED)
        self.stage_requeue(pipe, task)
        stage_extra(pipe)
        await pipe.execute()
        return True

    # ── TaskStateProtocol: direct reads/writes ─────────────────────────────

    async def save_task(self, task: Task) -> None:
        """Persist a task directly (non-staged)."""
        async with self._sf() as session:
            async with session.begin():
                await _upsert_task(session, _task_to_row(task))

    async def get_task(self, task_id: ULID) -> Task | None:
        """Fetch a single task by id."""
        async with self._sf() as session:
            result = await session.execute(select(tasks).where(tasks.c.id == str(task_id)))
            row = result.first()
            return _row_to_task(row) if row is not None else None

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        """Fetch multiple tasks in a single query."""
        if not task_ids:
            return []
        id_strs = [str(t) for t in task_ids]
        async with self._sf() as session:
            result = await session.execute(select(tasks).where(tasks.c.id.in_(id_strs)))
            rows = {row.id: row for row in result.all()}
        return [_row_to_task(rows[s]) if s in rows else None for s in id_strs]

    async def task_exists(self, task_id: ULID) -> bool:
        async with self._sf() as session:
            result = await session.execute(
                select(func.count()).select_from(tasks).where(tasks.c.id == str(task_id))
            )
            return (result.scalar() or 0) > 0

    async def compare_and_set_status(self, task_id: ULID, expected: TaskStatus, new: TaskStatus) -> bool:
        """Atomically transition status only if it currently equals ``expected``."""
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    update(tasks)
                    .where(tasks.c.id == str(task_id), tasks.c.status == expected.value)
                    .values(status=new.value)
                )
                return bool(result.rowcount == 1)  # type: ignore[attr-defined]

    async def get_active_tasks(self, queues: set[str]) -> list[Task]:
        """Return tasks that have a non-null heartbeat_at for the given queues."""
        if not queues:
            return []
        async with self._sf() as session:
            result = await session.execute(
                select(tasks).where(
                    tasks.c.queue.in_(queues),
                    tasks.c.heartbeat_at.is_not(None),
                )
            )
            return [_row_to_task(row) for row in result.all()]

    async def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]:
        """Yield STARTED tasks whose heartbeat is older than ``stale_time``."""
        if not queues:
            return
        cutoff = dt.datetime.now(dt.UTC) - stale_time
        async with self._sf() as session:
            result = await session.execute(
                select(tasks).where(
                    tasks.c.queue.in_(queues),
                    tasks.c.status == TaskStatus.STARTED.value,
                    tasks.c.heartbeat_at < cutoff,
                )
            )
            for row in result.all():
                yield _row_to_task(row)

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Return a paginated list of tasks matching the filter criteria."""
        stmt = select(tasks).where(tasks.c.queue == pagination.queue)
        if pagination.task_name is not None:
            stmt = stmt.where(tasks.c.name == pagination.task_name)
        if pagination.task_version is not None:
            stmt = stmt.where(tasks.c.version == pagination.task_version)
        if pagination.status is not None:
            stmt = stmt.where(tasks.c.status == pagination.status.value)
        if pagination.start is not None:
            stmt = stmt.where(tasks.c.id > str(pagination.start))
        order_col = (
            tasks.c.submitted_at if pagination.order_by == PaginationOrder.SUBMITTED_AT else tasks.c.id
        )
        stmt = stmt.order_by(order_col).limit(pagination.limit).offset(pagination.offset)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [_row_to_task(row) for row in result.all()]

    async def update_task_heartbeat(self, task: Task) -> None:
        assert task.heartbeat_at  # noqa: S101
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    update(tasks).where(tasks.c.id == str(task.id)).values(heartbeat_at=task.heartbeat_at)
                )

    async def remove_task_heartbeat(self, task: Task) -> None:
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    update(tasks).where(tasks.c.id == str(task.id)).values(heartbeat_at=None)
                )

    # ── Fan-in ─────────────────────────────────────────────────────────────

    async def init_fan_in(
        self, dag_run_id: ULID, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None:
        """Persist fan-in predecessor set (TTL and dag_run_id are ignored for SQL)."""
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            async with session.begin():
                async with session.begin_nested() as sp:
                    try:
                        await session.execute(insert(fan_in_anchors).values(fan_in_key=fan_in_key))
                    except IntegrityError:
                        await sp.rollback()
                for pid in predecessor_ids:
                    async with session.begin_nested() as sp:
                        try:
                            await session.execute(
                                insert(task_fan_in).values(
                                    fan_in_key=fan_in_key, task_id=str(pid), created_at=now
                                )
                            )
                        except IntegrityError:
                            await sp.rollback()

    async def fan_in_complete(self, dag_run_id: ULID, fan_in_key: str, task_id: ULID) -> int:
        """
        Mark task_id complete in the fan-in set and return remaining (incomplete) count.

        Returns -1 if task_id was not a member or was already completed. dag_run_id is
        unused for SQL — fan_in_key is already unique.
        """
        async with self._sf() as session:
            async with session.begin():
                # Lock this fan-in group's anchor row as a mutex before reading/writing
                # task_fan_in, mirroring close_dag_run_task's dag_runs lock: without it,
                # two predecessors completing concurrently under Postgres READ COMMITTED
                # can each run their COUNT before the other's UPDATE commits, so both see
                # a stale remaining > 0 and the collector never fires. No-op on SQLite.
                lock_stmt = select(fan_in_anchors.c.fan_in_key).where(
                    fan_in_anchors.c.fan_in_key == fan_in_key
                )
                if self._use_for_update:
                    lock_stmt = lock_stmt.with_for_update()
                await session.execute(lock_stmt)

                result = await session.execute(
                    update(task_fan_in)
                    .where(
                        task_fan_in.c.fan_in_key == fan_in_key,
                        task_fan_in.c.task_id == str(task_id),
                        task_fan_in.c.completed == False,  # noqa: E712
                    )
                    .values(completed=True)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    return -1
                count_result = await session.execute(
                    select(func.count())
                    .select_from(task_fan_in)
                    .where(
                        task_fan_in.c.fan_in_key == fan_in_key,
                        task_fan_in.c.completed == False,  # noqa: E712
                    )
                )
                return count_result.scalar() or 0

    async def get_fan_in_members(self, dag_run_id: ULID, fan_in_key: str) -> list[ULID]:
        """Return all predecessor IDs for a fan-in key (dag_run_id unused for SQL)."""
        async with self._sf() as session:
            result = await session.execute(
                select(task_fan_in.c.task_id).where(task_fan_in.c.fan_in_key == fan_in_key)
            )
            return [ULID.from_str(row.task_id) for row in result.all()]

    async def delegate_fan_in(self, dag_run_id: ULID, fan_in_key: str, old_id: ULID, new_id: ULID) -> None:
        """Replace *old_id* with *new_id* in the fan-in set for *fan_in_key* (dag_run_id unused for SQL)."""
        old_str = str(old_id)
        new_str = str(new_id)
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            async with session.begin():
                # DELETE old row and INSERT new one — avoids mutating the composite PK in place.
                await session.execute(
                    delete(task_fan_in).where(
                        task_fan_in.c.fan_in_key == fan_in_key,
                        task_fan_in.c.task_id == old_str,
                    )
                )
                async with session.begin_nested() as sp:
                    try:
                        await session.execute(
                            insert(task_fan_in).values(fan_in_key=fan_in_key, task_id=new_str, created_at=now)
                        )
                    except IntegrityError:
                        await sp.rollback()  # new_id already present — idempotent

    # ── DAG run index ───────────────────────────────────────────────────────

    async def get_dag_runs(self, pagination: DAGRunPagination) -> tuple[list[tuple[ULID, dt.datetime]], int]:
        """Return a paginated list of DAG runs ordered by submission time."""
        async with self._sf() as session:
            total_result = await session.execute(select(func.count()).select_from(dag_runs))
            total: int = total_result.scalar() or 0
            result = await session.execute(
                select(dag_runs)
                .order_by(dag_runs.c.submitted_at)
                .limit(pagination.limit)
                .offset(pagination.offset)
            )
            entries = [
                (ULID.from_str(row.dag_run_id), _ensure_utc_nn(row.submitted_at)) for row in result.all()
            ]
        return entries, total

    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run, or None."""
        dag_run_id_str = str(dag_run_id)
        async with self._sf() as session:
            run_result = await session.execute(
                select(dag_runs).where(dag_runs.c.dag_run_id == dag_run_id_str)
            )
            run_row = run_result.first()
            if run_row is None:
                return None
            task_result = await session.execute(
                select(tasks.c.id).where(tasks.c.dag_run_id == dag_run_id_str)
            )
            # ULIDs sort lexicographically by creation time; sort here so task_ids
            # is chronologically ordered regardless of the row order SQL returns.
            task_ids = sorted(ULID.from_str(row.id) for row in task_result.all())
        return _ensure_utc_nn(run_row.submitted_at), task_ids

    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Delete DAG run entries (and their pending rows) older than ``max_age``."""
        cutoff = now - max_age
        async with self._sf() as session:
            async with session.begin():
                stale_result = await session.execute(
                    select(dag_runs.c.dag_run_id).where(dag_runs.c.submitted_at < cutoff)
                )
                stale_ids = [row.dag_run_id for row in stale_result.all()]
                await session.execute(delete(dag_runs).where(dag_runs.c.submitted_at < cutoff))
                if stale_ids:
                    await session.execute(
                        delete(dag_run_pending).where(dag_run_pending.c.dag_run_id.in_(stale_ids))
                    )

    async def close_dag_run_task(self, dag_run_id: ULID, task_id: ULID) -> int:
        """
        Mark task_id resolved in the DAG run's pending set and return the remaining count.

        Returns -1 if task_id was not pending (already closed, or the run's pending
        rows were swept by clean_dag_runs).
        """
        dag_run_id_str = str(dag_run_id)
        async with self._sf() as session:
            async with session.begin():
                # Lock the run's single dag_runs row as a mutex before reading/writing
                # dag_run_pending. Without this, two siblings closing concurrently under
                # READ COMMITTED (e.g. Postgres) can each run their COUNT before the
                # other's UPDATE commits, so both compute a stale remaining > 0 and the
                # fast-path sweep never fires. Locking this one anchor row serializes
                # closers for the run without taking a lock proportional to the run's
                # size (locking every dag_run_pending row would itself serialize what
                # should be independent, concurrent completions). This is a no-op on
                # SQLite, which has no concurrent-writer MVCC race to guard against here.
                lock_stmt = select(dag_runs.c.dag_run_id).where(dag_runs.c.dag_run_id == dag_run_id_str)
                if self._use_for_update:
                    lock_stmt = lock_stmt.with_for_update()
                await session.execute(lock_stmt)

                result = await session.execute(
                    update(dag_run_pending)
                    .where(
                        dag_run_pending.c.dag_run_id == dag_run_id_str,
                        dag_run_pending.c.task_id == str(task_id),
                        dag_run_pending.c.closed == False,  # noqa: E712
                    )
                    .values(closed=True)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    return -1
                count_result = await session.execute(
                    select(func.count())
                    .select_from(dag_run_pending)
                    .where(
                        dag_run_pending.c.dag_run_id == dag_run_id_str,
                        dag_run_pending.c.closed == False,  # noqa: E712
                    )
                )
                return count_result.scalar() or 0

    # ── Lifecycle ───────────────────────────────────────────────────────────

    async def ensure_index(self) -> None:
        """No-op: indexes are created by run_migrations."""

    async def drop_stale_indexes(self) -> list[str]:
        """No-op: SQL indexes are managed by run_migrations, not versioned per-generation."""
        return []

    async def delete_task(self, task: Task) -> None:
        """Delete a single task record."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(tasks).where(tasks.c.id == str(task.id)))

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Delete terminal tasks older than ``max_age``."""
        cutoff = now - max_age
        statuses = [s.value for s in TaskStatus.terminal_statuses()]
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(tasks).where(
                        tasks.c.status.in_(statuses),
                        tasks.c.completed_at < cutoff,
                    )
                )

    async def clean(
        self,
        queues: set[str],
        now: dt.datetime,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
    ) -> None:
        """Remove queue entries within a time range."""
        if not (min_queue_age or max_queue_age):
            return
        earliest = min_queue_age or dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
        latest = max_queue_age or now
        queue_names = list(queues)
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(tasks).where(
                        tasks.c.queue.in_(queue_names),
                        tasks.c.submitted_at >= earliest,
                        tasks.c.submitted_at <= latest,
                    )
                )
