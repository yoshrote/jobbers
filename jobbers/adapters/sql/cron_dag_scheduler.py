"""
SQLAlchemy cron DAG scheduler adapter.

- `SQLCronDAGScheduler` — CronDAGSchedulerProtocol + AtomicCronDAGSchedulerProtocol backed by SQLAlchemy.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, insert, select, update
from sqlalchemy.exc import IntegrityError
from ulid import ULID

from jobbers.migrations.schema import cron_dag_active_runs, cron_dag_entries
from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGTaskSpec
from jobbers.utils.sql_transaction import SQLTransactionBatch

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from jobbers.protocols import TransactionHandle


def _ensure_utc_cron(d: dt.datetime) -> dt.datetime:
    """SQLite returns naive datetimes; re-attach UTC (non-nullable variant)."""
    return d if d.tzinfo is not None else d.replace(tzinfo=dt.UTC)


def _ensure_utc_cron_opt(d: dt.datetime | None) -> dt.datetime | None:
    if d is None:
        return None
    return d if d.tzinfo is not None else d.replace(tzinfo=dt.UTC)


def _cron_row_to_entry(row: Any) -> CronDAGEntry:
    return CronDAGEntry(
        id=ULID.from_str(row.id),
        name=row.name,
        cron_expr=row.cron_expr,
        dag_spec=DAGTaskSpec.model_validate(json.loads(row.dag_spec)),
        enabled=bool(row.enabled),
        concurrency_policy=ConcurrencyPolicy(row.concurrency_policy),
        created_at=_ensure_utc_cron(row.created_at),
    )


def _cron_entry_values(entry: CronDAGEntry, next_run_at: dt.datetime) -> dict[str, Any]:
    return {
        "id": str(entry.id),
        "name": entry.name,
        "cron_expr": entry.cron_expr,
        "dag_spec": entry.dag_spec.model_dump_json(),
        "enabled": entry.enabled,
        "concurrency_policy": entry.concurrency_policy.value,
        "created_at": entry.created_at,
        "next_run_at": next_run_at,
    }


class SQLCronDAGScheduler:
    """
    Cron DAG scheduler backed by SQLAlchemy.

    Implements ``AtomicCronDAGSchedulerProtocol`` using ``SQLTransactionBatch``.
    When the cron scheduler and task-state adapter share the same ``session_factory``
    (same ``dsn``), ``StateManager`` can fold cron and task-state ops into one transaction.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        dsn: str = "",
    ) -> None:
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

    # ── AtomicCronDAGSchedulerProtocol: staged writes ─────────────────────────

    def stage_reschedule(self, pipe: TransactionHandle, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Stage an UPDATE of next_run_at onto the batch."""
        cron_id_str = str(cron_id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _update(s: AsyncSession) -> None:
            await s.execute(
                update(cron_dag_entries)
                .where(cron_dag_entries.c.id == cron_id_str)
                .values(next_run_at=next_run_at)
            )

        pipe.add_op(_update)

    def stage_set_active_run(
        self,
        pipe: TransactionHandle,
        cron_id: ULID,
        task_id: ULID,
        ttl: int = 86400,
        nx: bool = False,
    ) -> None:
        """
        Stage an INSERT-or-UPSERT into cron_dag_active_runs onto the batch.

        When ``nx=True``, uses INSERT with IntegrityError suppression — the NX guard
        against duplicate dispatches.  The result is not checked after execute();
        callers read ``get_active_run()`` before building the pipeline to determine
        whether to proceed, mirroring the Redis NX pattern.
        """
        cron_id_str = str(cron_id)
        task_id_str = str(task_id)
        expires_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=ttl)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        if nx:

            async def _insert_nx(s: AsyncSession) -> None:
                async with s.begin_nested() as sp:
                    try:
                        await s.execute(
                            insert(cron_dag_active_runs).values(
                                cron_id=cron_id_str, task_id=task_id_str, expires_at=expires_at
                            )
                        )
                    except IntegrityError:
                        await sp.rollback()

            pipe.add_op(_insert_nx)
        else:

            async def _upsert(s: AsyncSession) -> None:
                result = await s.execute(
                    update(cron_dag_active_runs)
                    .where(cron_dag_active_runs.c.cron_id == cron_id_str)
                    .values(task_id=task_id_str, expires_at=expires_at)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await s.execute(
                        insert(cron_dag_active_runs).values(
                            cron_id=cron_id_str, task_id=task_id_str, expires_at=expires_at
                        )
                    )

            pipe.add_op(_upsert)

    def stage_clear_active_run(self, pipe: TransactionHandle, cron_id: ULID) -> None:
        """Stage a DELETE from cron_dag_active_runs onto the batch."""
        cron_id_str = str(cron_id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _delete(s: AsyncSession) -> None:
            await s.execute(delete(cron_dag_active_runs).where(cron_dag_active_runs.c.cron_id == cron_id_str))

        pipe.add_op(_delete)

    # ── CronDAGSchedulerProtocol: direct writes ────────────────────────────────

    async def add(self, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        """Upsert a cron entry and set its next_run_at."""
        row = _cron_entry_values(entry, next_run_at)
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    update(cron_dag_entries)
                    .where(cron_dag_entries.c.id == row["id"])
                    .values({k: v for k, v in row.items() if k != "id"})
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await session.execute(insert(cron_dag_entries).values(**row))

    async def remove(self, cron_id: ULID) -> None:
        """Delete a cron entry (cascades to active runs)."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(cron_dag_entries).where(cron_dag_entries.c.id == str(cron_id)))

    # ── CronDAGSchedulerProtocol: reads ───────────────────────────────────────

    async def get(self, cron_id: ULID) -> CronDAGEntry | None:
        """Fetch a single CronDAGEntry by id, or None if missing."""
        async with self._sf() as session:
            result = await session.execute(
                select(cron_dag_entries).where(cron_dag_entries.c.id == str(cron_id))
            )
            row = result.first()
            return _cron_row_to_entry(row) if row is not None else None

    async def next_due_bulk(self, n: int) -> list[tuple[CronDAGEntry, dt.datetime]]:
        """
        Atomically acquire and return up to n due cron entries.

        Marks acquired entries by setting ``next_run_at = NULL``; caller is responsible
        for rescheduling via ``reschedule()`` or ``stage_reschedule()`` after dispatching.
        """
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            async with session.begin():
                stmt = (
                    select(cron_dag_entries)
                    .where(
                        cron_dag_entries.c.next_run_at.is_not(None),
                        cron_dag_entries.c.next_run_at <= now,
                    )
                    .order_by(cron_dag_entries.c.next_run_at)
                    .limit(n)
                )
                if self._use_for_update:
                    stmt = stmt.with_for_update(skip_locked=True)
                result = await session.execute(stmt)
                rows = result.all()
                if not rows:
                    return []
                ids = [row.id for row in rows]
                await session.execute(
                    update(cron_dag_entries).where(cron_dag_entries.c.id.in_(ids)).values(next_run_at=None)
                )
        return [(_cron_row_to_entry(row), _ensure_utc_cron(row.next_run_at)) for row in rows]

    async def reschedule(self, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Update next_run_at for a cron entry (direct, non-staged version)."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    update(cron_dag_entries)
                    .where(cron_dag_entries.c.id == str(cron_id))
                    .values(next_run_at=next_run_at)
                )

    async def get_active_run(self, cron_id: ULID) -> str | None:
        """Return the active root task ID string, or None if no active (non-expired) run."""
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            result = await session.execute(
                select(cron_dag_active_runs.c.task_id).where(
                    cron_dag_active_runs.c.cron_id == str(cron_id),
                    cron_dag_active_runs.c.expires_at > now,
                )
            )
            row = result.first()
            return row.task_id if row is not None else None

    async def set_active_run(self, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False) -> bool:
        """
        Set the active-run marker for a cron entry.

        When ``nx=True``, inserts only if no row exists (returns False on conflict).
        When ``nx=False``, upserts unconditionally (always returns True).
        """
        cron_id_str = str(cron_id)
        task_id_str = str(task_id)
        expires_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=ttl)
        async with self._sf() as session:
            async with session.begin():
                if nx:
                    async with session.begin_nested() as sp:
                        try:
                            await session.execute(
                                insert(cron_dag_active_runs).values(
                                    cron_id=cron_id_str,
                                    task_id=task_id_str,
                                    expires_at=expires_at,
                                )
                            )
                            return True
                        except IntegrityError:
                            await sp.rollback()
                            return False
                else:
                    result = await session.execute(
                        update(cron_dag_active_runs)
                        .where(cron_dag_active_runs.c.cron_id == cron_id_str)
                        .values(task_id=task_id_str, expires_at=expires_at)
                    )
                    if result.rowcount == 0:  # type: ignore[attr-defined]
                        await session.execute(
                            insert(cron_dag_active_runs).values(
                                cron_id=cron_id_str,
                                task_id=task_id_str,
                                expires_at=expires_at,
                            )
                        )
                    return True

    async def clear_active_run(self, cron_id: ULID) -> None:
        """Delete the active-run marker for a cron entry."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(cron_dag_active_runs).where(cron_dag_active_runs.c.cron_id == str(cron_id))
                )

    async def get_next_run_at(self, cron_id: ULID) -> dt.datetime | None:
        """Return the next scheduled run time, or None if not scheduled (acquired or missing)."""
        async with self._sf() as session:
            result = await session.execute(
                select(cron_dag_entries.c.next_run_at).where(cron_dag_entries.c.id == str(cron_id))
            )
            row = result.first()
            if row is None:
                return None
            return _ensure_utc_cron_opt(row.next_run_at)

    async def list(
        self, offset: int = 0, limit: int = 50
    ) -> tuple[list[tuple[CronDAGEntry, dt.datetime]], int]:
        """
        Return a page of scheduled cron entries ordered by next_run_at ascending.

        Excludes acquired entries (next_run_at IS NULL).  Returns (entries, total_scheduled).
        """
        async with self._sf() as session:
            count_result = await session.execute(
                select(cron_dag_entries).where(cron_dag_entries.c.next_run_at.is_not(None))
            )
            total = len(count_result.all())

            result = await session.execute(
                select(cron_dag_entries)
                .where(cron_dag_entries.c.next_run_at.is_not(None))
                .order_by(cron_dag_entries.c.next_run_at)
                .offset(offset)
                .limit(limit)
            )
            rows = result.all()

        return [(_cron_row_to_entry(row), _ensure_utc_cron(row.next_run_at)) for row in rows], total
