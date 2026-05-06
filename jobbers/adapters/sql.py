"""
SQLAlchemy-backed implementations of TaskAdapterProtocol, DeadQueueProtocol,
and a SQL-native TaskScheduler.

``SqlTaskAdapter``   – stores tasks in the ``tasks`` + ``task_queue`` tables.
``SqlDeadQueue``     – stores dead-letter entries in the ``dead_letter_queue`` table.
``SqlTaskScheduler`` – stores scheduled tasks in the ``scheduled_tasks`` table.

All three accept a ``SqlPipeline`` (from ``jobbers.adapters.pipeline``) in their
``stage_*`` methods.  Operations queued onto the pipeline are committed in a single
transaction when ``pipeline.execute()`` is called.

Dequeue is implemented via an atomic ``DELETE … WHERE task_id = (SELECT … LIMIT 1)
RETURNING task_id`` so that two concurrent workers can never claim the same task.
A short polling loop with ``asyncio.sleep`` simulates the blocking behaviour of
Redis's ``BZPOPMIN``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, text
from ulid import ULID

from jobbers.adapters.pipeline import SqlPipeline
from jobbers.constants import TIME_ZERO
from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_status import TaskStatus

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from jobbers.adapters.task_adapter import TaskAdapterProtocol
    from jobbers.models.queue_config import QueueConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TERMINAL_STATUSES = (
    TaskStatus.COMPLETED,
    TaskStatus.FAILED,
    TaskStatus.CANCELLED,
    TaskStatus.STALLED,
    TaskStatus.DROPPED,
)

_POLL_INTERVAL = 0.05  # seconds between dequeue attempts


def _ts(v: dt.datetime | None) -> str | None:
    return v.isoformat() if v is not None else None


def _pack(task: Task) -> dict[str, Any]:
    """Flatten a Task into a row dict suitable for INSERT / UPDATE."""
    return {
        "id": str(task.id),
        "name": task.name,
        "queue": task.queue,
        "version": task.version,
        "parameters": json.dumps(task.parameters or {}),
        "results": json.dumps(task.results or {}),
        "errors": json.dumps(task.errors),
        "retry_attempt": task.retry_attempt,
        "status": str(task.status),
        "submitted_at": _ts(task.submitted_at),
        "retried_at": _ts(task.retried_at),
        "started_at": _ts(task.started_at),
        "heartbeat_at": _ts(task.heartbeat_at),
        "completed_at": _ts(task.completed_at),
        "dag_callbacks": json.dumps([cb.model_dump(mode="json") for cb in task.dag_callbacks]),
        "parent_ids": json.dumps([str(p) for p in task.parent_ids]),
        "inject_parent_results": task.inject_parent_results,
        "cron_id": str(task.cron_id) if task.cron_id is not None else None,
        "dag_run_id": str(task.dag_run_id) if task.dag_run_id is not None else None,
    }


def _unpack(row: Any) -> Task:
    """Reconstruct a Task from a SQLAlchemy row mapping."""
    task_id = ULID.from_str(row["id"])
    return Task.from_dict(
        task_id,
        {
            "name": row["name"],
            "queue": row["queue"],
            "version": row["version"],
            "parameters": json.loads(row["parameters"]),
            "results": json.loads(row["results"]),
            "errors": json.loads(row["errors"]),
            "retry_attempt": row["retry_attempt"],
            "status": row["status"],
            "submitted_at": row["submitted_at"],
            "retried_at": row["retried_at"],
            "started_at": row["started_at"],
            "heartbeat_at": row["heartbeat_at"],
            "completed_at": row["completed_at"],
            "dag_callbacks": json.loads(row["dag_callbacks"] or "[]"),
            "parent_ids": json.loads(row["parent_ids"] or "[]"),
            "inject_parent_results": bool(row["inject_parent_results"]),
            "cron_id": row["cron_id"],
            "dag_run_id": row["dag_run_id"],
        },
    )


_UPSERT_TASK = text("""
    INSERT INTO tasks
        (id, name, queue, version, parameters, results, errors,
         retry_attempt, status, submitted_at, retried_at,
         started_at, heartbeat_at, completed_at)
    VALUES
        (:id, :name, :queue, :version, :parameters, :results, :errors,
         :retry_attempt, :status, :submitted_at, :retried_at,
         :started_at, :heartbeat_at, :completed_at)
    ON CONFLICT(id) DO UPDATE SET
        name=excluded.name,
        queue=excluded.queue,
        version=excluded.version,
        parameters=excluded.parameters,
        results=excluded.results,
        errors=excluded.errors,
        retry_attempt=excluded.retry_attempt,
        status=excluded.status,
        submitted_at=excluded.submitted_at,
        retried_at=excluded.retried_at,
        started_at=excluded.started_at,
        heartbeat_at=excluded.heartbeat_at,
        completed_at=excluded.completed_at
""")

_ENQUEUE = text("""
    INSERT OR REPLACE INTO task_queue (queue, task_id, submitted_at)
    VALUES (:queue, :task_id, :submitted_at)
""")

_DEQUEUE = text("""
    DELETE FROM task_queue
    WHERE (queue, task_id) = (
        SELECT queue, task_id
        FROM   task_queue
        WHERE  queue = :queue
        ORDER  BY submitted_at ASC
        LIMIT  1
    )
    RETURNING task_id
""")


# ---------------------------------------------------------------------------
# SqlTaskAdapter
# ---------------------------------------------------------------------------


class SqlTaskAdapter:
    """
    SQLAlchemy-backed task adapter.

    Key design:
    - ``tasks`` table is the single source of truth for task state.
    - ``task_queue`` is a lightweight inbox table; a row exists only while a
      task is SUBMITTED.  Dequeue atomically removes the row with
      ``DELETE … RETURNING`` so two workers cannot claim the same task.
    - Heartbeat is stored as a column (``heartbeat_at``) rather than a
      separate sorted-set.
    - ``stage_*`` methods append async callables to a ``SqlPipeline`` so the
      caller can commit multiple operations in a single transaction.
    """

    # Key-name helpers kept for API compatibility with the Redis adapters.
    # StateManager / TaskScheduler use these to construct key strings that are
    # only meaningful for Redis; SQL adapters ignore the values but the
    # attributes must exist to satisfy the protocol.
    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    HEARTBEAT_SCORES = "task-heartbeats:{queue}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    DLQ_MISSING_DATA = "dlq-missing-data"

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    # -- pipeline factory ----------------------------------------------------

    def pipeline(self, transaction: bool = True) -> SqlPipeline:  # noqa: ARG002
        return SqlPipeline(self._engine)

    # -- staged write helpers ------------------------------------------------

    def stage_save(self, pipe: SqlPipeline, task: Task) -> None:
        """Queue an UPSERT of the task row onto pipe (no execute)."""
        row = _pack(task)

        async def _op(conn: AsyncConnection) -> None:
            await conn.execute(_UPSERT_TASK, row)

        pipe.add(_op)

    def stage_requeue(self, pipe: SqlPipeline, task: Task) -> None:
        """Queue an UPSERT + task_queue INSERT onto pipe (no execute)."""
        assert task.submitted_at  # noqa: S101
        self.stage_save(pipe, task)
        tid = str(task.id)
        queue = task.queue
        sat = task.submitted_at.isoformat()

        async def _op(conn: AsyncConnection) -> None:
            await conn.execute(_ENQUEUE, {"queue": queue, "task_id": tid, "submitted_at": sat})

        pipe.add(_op)

    def stage_remove_from_queue(self, pipe: SqlPipeline, task: Task) -> None:
        """Queue removal of the task from task_queue onto pipe (no execute)."""
        queue = task.queue
        tid = str(task.id)

        async def _op(conn: AsyncConnection) -> None:
            await conn.execute(
                text("DELETE FROM task_queue WHERE queue = :q AND task_id = :tid"),
                {"q": queue, "tid": tid},
            )

        pipe.add(_op)

    def stage_remove_heartbeat(self, pipe: SqlPipeline, task: Task) -> None:
        """Queue clearing the heartbeat_at column for the task (no execute)."""
        tid = str(task.id)

        async def _op(conn: AsyncConnection) -> None:
            await conn.execute(
                text("UPDATE tasks SET heartbeat_at = NULL WHERE id = :tid"),
                {"tid": tid},
            )

        pipe.add(_op)

    # -- immediate writes (not staged) ---------------------------------------

    async def submit_task(self, task: Task) -> bool:
        """Atomically enqueue a new task with no rate limiting."""
        assert task.submitted_at  # noqa: S101
        pipe = self.pipeline()
        self.stage_requeue(pipe, task)
        await pipe.execute()
        return True

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """Check the rate window then enqueue; returns False if rate-limited."""
        assert task.submitted_at  # noqa: S101
        now = dt.datetime.now(dt.UTC)
        period = queue_config.period_in_seconds() or 0
        earliest = (now - dt.timedelta(seconds=period)).isoformat()
        numerator = queue_config.rate_numerator or 0
        tid = str(task.id)

        async with self._engine.begin() as conn:
            # Prune expired rate-limiter window
            await conn.execute(
                text("DELETE FROM rate_limiter WHERE queue = :q AND submitted_at < :e"),
                {"q": task.queue, "e": earliest},
            )
            # Idempotency: if the task already exists, just update it
            exists_row = await conn.execute(
                text("SELECT 1 FROM tasks WHERE id = :tid"), {"tid": tid}
            )
            if exists_row.first() is not None:
                await conn.execute(_UPSERT_TASK, _pack(task))
                return True
            # Check rate window
            if numerator != 0:
                count_row = await conn.execute(
                    text("SELECT COUNT(*) FROM rate_limiter WHERE queue = :q"),
                    {"q": task.queue},
                )
                count = count_row.scalar() or 0
                if count >= numerator:
                    return False
            assert task.submitted_at  # noqa: S101
            await conn.execute(
                text(
                    "INSERT OR REPLACE INTO rate_limiter (queue, task_id, submitted_at)"
                    " VALUES (:q, :tid, :sat)"
                ),
                {"q": task.queue, "tid": tid, "sat": task.submitted_at.isoformat()},
            )
            await conn.execute(_UPSERT_TASK, _pack(task))
            await conn.execute(
                _ENQUEUE,
                {"queue": task.queue, "task_id": tid, "submitted_at": task.submitted_at.isoformat()},
            )
        return True

    # -- read path -----------------------------------------------------------

    async def get_task(self, task_id: ULID) -> Task | None:
        async with self._engine.connect() as conn:
            return await self._load_task(conn, task_id)

    async def _load_task(self, conn: AsyncConnection, task_id: ULID) -> Task | None:
        row = await conn.execute(
            text("SELECT * FROM tasks WHERE id = :tid"), {"tid": str(task_id)}
        )
        result = row.mappings().first()
        return _unpack(result) if result is not None else None

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        if not task_ids:
            return []
        id_strs = [str(t) for t in task_ids]
        async with self._engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "SELECT * FROM tasks WHERE id IN :ids"
                ).bindparams(bindparam("ids", expanding=True)),
                {"ids": id_strs},
            )
            by_id = {r["id"]: _unpack(r) for r in rows.mappings()}
        return [by_id.get(str(tid)) for tid in task_ids]

    async def read_for_watch(self, pipe: SqlPipeline, task_id: ULID) -> Task | None:  # noqa: ARG002
        """SQL transactions provide isolation — just read directly."""
        return await self.get_task(task_id)

    async def task_exists(self, task_id: ULID) -> bool:
        async with self._engine.connect() as conn:
            row = await conn.execute(
                text("SELECT 1 FROM tasks WHERE id = :tid"), {"tid": str(task_id)}
            )
            return row.first() is not None

    async def get_active_tasks(self, queues: set[str]) -> list[Task]:
        """Return tasks that currently have a heartbeat (i.e. are STARTED/HEARTBEAT)."""
        if not queues:
            return []
        async with self._engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "SELECT id FROM tasks WHERE queue IN :queues AND heartbeat_at IS NOT NULL"
                ).bindparams(bindparam("queues", expanding=True)),
                {"queues": list(queues)},
            )
            ids = [ULID.from_str(r[0]) for r in rows]
        return await self.get_tasks_bulk(ids)

    async def get_stale_tasks(
        self, queues: set[str], stale_time: dt.timedelta
    ) -> AsyncGenerator[Task, None]:
        """Yield tasks whose heartbeat is older than stale_time."""
        cutoff = (dt.datetime.now(dt.UTC) - stale_time).isoformat()
        if not queues:
            return
        async with self._engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "SELECT id FROM tasks"
                    " WHERE queue IN :queues"
                    "   AND heartbeat_at IS NOT NULL"
                    "   AND heartbeat_at < :cutoff"
                ).bindparams(bindparam("queues", expanding=True)),
                {"queues": list(queues), "cutoff": cutoff},
            )
            ids = [ULID.from_str(r[0]) for r in rows]
        for task in await self.get_tasks_bulk(ids):
            if task is not None:
                yield task

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        from jobbers.models.task import PaginationOrder

        conditions = ["queue = :queue"]
        params: dict[str, Any] = {
            "queue": pagination.queue,
            "limit": pagination.limit,
            "offset": pagination.offset,
        }
        if pagination.task_name is not None:
            conditions.append("name = :name")
            params["name"] = pagination.task_name
        if pagination.task_version is not None:
            conditions.append("version = :version")
            params["version"] = pagination.task_version
        if pagination.status is not None:
            conditions.append("status = :status")
            params["status"] = str(pagination.status)

        order = (
            "submitted_at ASC" if pagination.order_by == PaginationOrder.SUBMITTED_AT else "id ASC"
        )
        where = " AND ".join(conditions)
        async with self._engine.connect() as conn:
            rows = await conn.execute(
                text(
                    f"SELECT * FROM tasks WHERE {where}"
                    f" ORDER BY {order} LIMIT :limit OFFSET :offset"
                ),
                params,
            )
            return [_unpack(r) for r in rows.mappings()]

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """
        Poll task_queue for the next claimable task.

        Blocks for up to *pop_timeout* seconds (like Redis ``BZPOPMIN``).
        Each attempt uses an atomic ``DELETE … RETURNING`` so concurrent
        workers cannot claim the same task.
        """
        loop = asyncio.get_event_loop()
        deadline = loop.time() + pop_timeout if pop_timeout > 0 else None
        # Sort queues for a deterministic (FIFO-across-queues) scan order
        ordered_queues = sorted(queues)
        while True:
            for queue in ordered_queues:
                task = await self._try_dequeue(queue)
                if task is not None:
                    return task
            if deadline is None or loop.time() >= deadline:
                logger.info("task query timed out")
                return None
            await asyncio.sleep(_POLL_INTERVAL)

    async def _try_dequeue(self, queue: str) -> Task | None:
        """Atomically pop the oldest task from task_queue for *queue*."""
        async with self._engine.begin() as conn:
            result = await conn.execute(_DEQUEUE, {"queue": queue})
            row = result.first()
            if row is None:
                return None
            task_id_str: str = row[0]
            task = await self._load_task(conn, ULID.from_str(task_id_str))
            if task is not None:
                return task
            # Data missing — record in dlq_missing_data and move on
            logger.error(
                "Task %s dequeued from SQL task_queue but row missing from tasks table",
                task_id_str,
            )
            await conn.execute(
                text(
                    "INSERT OR REPLACE INTO dlq_missing_data (task_id, recorded_at)"
                    " VALUES (:tid, :ts)"
                ),
                {"tid": task_id_str, "ts": dt.datetime.now(dt.UTC).isoformat()},
            )
        return None

    # -- heartbeat -----------------------------------------------------------

    async def update_task_heartbeat(self, task: Task) -> None:
        assert task.heartbeat_at  # noqa: S101
        async with self._engine.begin() as conn:
            await conn.execute(
                text("UPDATE tasks SET heartbeat_at = :hb WHERE id = :tid"),
                {"hb": task.heartbeat_at.isoformat(), "tid": str(task.id)},
            )

    async def remove_task_heartbeat(self, task: Task) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text("UPDATE tasks SET heartbeat_at = NULL WHERE id = :tid"),
                {"tid": str(task.id)},
            )

    # -- lifecycle -----------------------------------------------------------

    async def ensure_index(self) -> None:
        """No-op: schema is created by the migration runner."""

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        cutoff = (now - max_age).isoformat()
        statuses = [str(s) for s in _TERMINAL_STATUSES]
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "DELETE FROM tasks"
                    " WHERE status IN :statuses"
                    "   AND completed_at IS NOT NULL"
                    "   AND completed_at < :cutoff"
                ).bindparams(bindparam("statuses", expanding=True)),
                {"statuses": statuses, "cutoff": cutoff},
            )

    async def clean_rate_limiter(
        self,
        queues: set[bytes],
        now: dt.datetime,
        rate_limit_age: dt.timedelta,
    ) -> None:
        earliest = (now - rate_limit_age).isoformat()
        queue_strs = [q.decode() if isinstance(q, bytes) else q for q in queues]
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "DELETE FROM rate_limiter WHERE queue IN :queues AND submitted_at < :e"
                ).bindparams(bindparam("queues", expanding=True)),
                {"queues": queue_strs, "e": earliest},
            )

    async def clean(
        self,
        queues: set[bytes],
        now: dt.datetime,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
    ) -> None:
        """Remove task_queue entries within the given submitted_at time range."""
        if not (min_queue_age or max_queue_age):
            return
        earliest = (min_queue_age or TIME_ZERO).isoformat()
        latest = (max_queue_age or now).isoformat()
        queue_strs = [q.decode() if isinstance(q, bytes) else q for q in queues]
        async with self._engine.begin() as conn:
            if earliest <= latest:
                await conn.execute(
                    text(
                        "DELETE FROM task_queue"
                        " WHERE queue IN :queues"
                        "   AND submitted_at BETWEEN :lo AND :hi"
                    ).bindparams(bindparam("queues", expanding=True)),
                    {"queues": queue_strs, "lo": earliest, "hi": latest},
                )
            else:
                await conn.execute(
                    text(
                        "DELETE FROM task_queue"
                        " WHERE queue IN :queues"
                        "   AND (submitted_at <= :lo OR submitted_at >= :hi)"
                    ).bindparams(bindparam("queues", expanding=True)),
                    {"queues": queue_strs, "lo": earliest, "hi": latest},
                )


# ---------------------------------------------------------------------------
# SqlDeadQueue
# ---------------------------------------------------------------------------


class SqlDeadQueue:
    """
    Dead-letter queue backed by the ``dead_letter_queue`` table.

    Full task data still lives in the ``tasks`` table; this table stores only
    the lightweight index (task_id, queue, name, version, failed_at) required
    for filtering and cleanup.
    """

    def __init__(self, engine: AsyncEngine, task_adapter: TaskAdapterProtocol) -> None:
        self._engine = engine
        self.ta = task_adapter

    async def ensure_index(self) -> None:
        """No-op: schema created by the migration runner."""

    def stage_add(self, pipe: SqlPipeline, task: Task, failed_at: dt.datetime) -> None:
        """Queue an INSERT OR REPLACE into dead_letter_queue onto pipe (no execute)."""
        row = {
            "task_id": str(task.id),
            "queue": task.queue,
            "name": task.name,
            "version": task.version,
            "failed_at": failed_at.isoformat(),
        }

        async def _op(conn: AsyncConnection) -> None:
            await conn.execute(
                text(
                    "INSERT OR REPLACE INTO dead_letter_queue"
                    " (task_id, queue, name, version, failed_at)"
                    " VALUES (:task_id, :queue, :name, :version, :failed_at)"
                ),
                row,
            )

        pipe.add(_op)

    def stage_remove(self, pipe: SqlPipeline, task_id: ULID, queue: str, name: str) -> None:  # noqa: ARG002
        """Queue a DELETE from dead_letter_queue onto pipe (no execute)."""
        tid = str(task_id)

        async def _op(conn: AsyncConnection) -> None:
            await conn.execute(
                text("DELETE FROM dead_letter_queue WHERE task_id = :tid"), {"tid": tid}
            )

        pipe.add(_op)

    async def get_history(self, task_id: str) -> list[dict[str, Any]]:
        task = await self.ta.get_task(ULID.from_str(task_id))
        if task is None:
            return []
        return [{"attempt": i, "error": e} for i, e in enumerate(task.errors)]

    async def get_by_ids(self, task_ids: list[str]) -> list[Task]:
        if not task_ids:
            return []
        async with self._engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "SELECT task_id FROM dead_letter_queue WHERE task_id IN :ids"
                ).bindparams(bindparam("ids", expanding=True)),
                {"ids": task_ids},
            )
            valid = {r[0] for r in rows}
        ulids = [ULID.from_str(t) for t in task_ids if t in valid]
        tasks = await self.ta.get_tasks_bulk(ulids)
        return [t for t in tasks if t is not None]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list[Task]:
        conditions: list[str] = []
        params: dict[str, Any] = {"limit": limit}
        if queue is not None:
            conditions.append("queue = :queue")
            params["queue"] = queue
        if task_name is not None:
            conditions.append("name = :name")
            params["name"] = task_name
        if task_version is not None:
            conditions.append("version = :version")
            params["version"] = task_version
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        async with self._engine.connect() as conn:
            rows = await conn.execute(
                text(
                    f"SELECT task_id FROM dead_letter_queue {where}"
                    " ORDER BY failed_at DESC LIMIT :limit"
                ),
                params,
            )
            ulids = [ULID.from_str(r[0]) for r in rows]
        tasks = await self.ta.get_tasks_bulk(ulids)
        return [t for t in tasks if t is not None]

    async def remove_many(self, task_ids: list[str]) -> None:
        if not task_ids:
            return
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "DELETE FROM dead_letter_queue WHERE task_id IN :ids"
                ).bindparams(bindparam("ids", expanding=True)),
                {"ids": task_ids},
            )

    async def clean(self, earlier_than: dt.datetime) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM dead_letter_queue WHERE failed_at < :cutoff"),
                {"cutoff": earlier_than.isoformat()},
            )


# ---------------------------------------------------------------------------
# SqlTaskScheduler
# ---------------------------------------------------------------------------


class SqlTaskScheduler:
    """
    SQL-native scheduler that replaces the Redis-backed ``TaskScheduler``.

    Uses the ``scheduled_tasks`` table instead of per-queue sorted sets.
    ``next_due_bulk`` uses an atomic ``DELETE … RETURNING`` to acquire tasks
    so concurrent scheduler processes cannot double-dispatch.
    """

    def __init__(self, engine: AsyncEngine, task_adapter: TaskAdapterProtocol) -> None:
        self._engine = engine
        self.ta = task_adapter

    # stage_* mirror the TaskScheduler interface so StateManager works
    # without modification regardless of which scheduler is in use.

    def stage_add(self, pipe: SqlPipeline, task: Task, run_at: dt.datetime) -> None:
        """Queue an INSERT OR REPLACE into scheduled_tasks onto pipe (no execute)."""
        row = {
            "task_id": str(task.id),
            "queue": task.queue,
            "run_at": run_at.isoformat(),
        }

        async def _op(conn: AsyncConnection) -> None:
            await conn.execute(
                text(
                    "INSERT OR REPLACE INTO scheduled_tasks (task_id, queue, run_at)"
                    " VALUES (:task_id, :queue, :run_at)"
                ),
                row,
            )

        pipe.add(_op)

    def stage_remove(self, pipe: SqlPipeline, task_id: ULID, queue: str) -> None:  # noqa: ARG002
        """Queue a DELETE from scheduled_tasks onto pipe (no execute)."""
        tid = str(task_id)

        async def _op(conn: AsyncConnection) -> None:
            await conn.execute(
                text("DELETE FROM scheduled_tasks WHERE task_id = :tid"), {"tid": tid}
            )

        pipe.add(_op)

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
        start_after: str | None = None,
    ) -> list[Task]:
        conditions: list[str] = []
        params: dict[str, Any] = {"limit": limit}
        if queue is not None:
            conditions.append("s.queue = :queue")
            params["queue"] = queue
        if start_after is not None:
            conditions.append("s.task_id > :start_after")
            params["start_after"] = start_after
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        async with self._engine.connect() as conn:
            rows = await conn.execute(
                text(
                    f"SELECT s.task_id FROM scheduled_tasks s {where}"
                    " ORDER BY s.task_id ASC LIMIT :limit"
                ),
                params,
            )
            ulids = [ULID.from_str(r[0]) for r in rows]
        fetched = await self.ta.get_tasks_bulk(ulids)
        results: list[Task] = []
        for task in fetched:
            if task is None:
                continue
            if task_name is not None and task.name != task_name:
                continue
            if task_version is not None and task.version != task_version:
                continue
            results.append(task)
            if len(results) >= limit:
                break
        return results

    async def next_due(self, queues: list[str] | None = None) -> Task | None:
        results = await self.next_due_bulk(1, queues=queues)
        return results[0][0] if results else None

    async def next_due_bulk(
        self, n: int, queues: list[str] | None = None
    ) -> list[tuple[Task, dt.datetime]]:
        """
        Atomically acquire up to *n* due tasks.

        Uses ``DELETE … RETURNING`` so concurrent scheduler processes cannot
        double-dispatch the same task.
        """
        if queues is not None and not queues:
            return []

        now = dt.datetime.now(dt.UTC).isoformat()
        params: dict[str, Any] = {"now": now, "limit": n}

        if queues is None:
            acquire = text(
                "DELETE FROM scheduled_tasks"
                " WHERE task_id IN ("
                "   SELECT task_id FROM scheduled_tasks"
                "   WHERE run_at <= :now ORDER BY run_at ASC LIMIT :limit"
                ")"
                " RETURNING task_id, run_at"
            )
        else:
            acquire = text(
                "DELETE FROM scheduled_tasks"
                " WHERE task_id IN ("
                "   SELECT task_id FROM scheduled_tasks"
                "   WHERE queue IN :queues AND run_at <= :now"
                "   ORDER BY run_at ASC LIMIT :limit"
                ")"
                " RETURNING task_id, run_at"
            ).bindparams(bindparam("queues", expanding=True))
            params["queues"] = queues

        async with self._engine.begin() as conn:
            rows = await conn.execute(acquire, params)
            acquired = [(ULID.from_str(r[0]), r[1]) for r in rows]

        if not acquired:
            return []

        task_ids = [tid for tid, _ in acquired]
        run_ats_by_id = {str(tid): run_at for tid, run_at in acquired}
        tasks = await self.ta.get_tasks_bulk(task_ids)

        return [
            (task, dt.datetime.fromisoformat(run_ats_by_id[str(task.id)]))
            for task in tasks
            if task is not None
        ]
