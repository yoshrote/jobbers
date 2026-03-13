"""
SQLite-backed task adapter and dead letter queue.

``SqlTaskAdapter``  – implements ``TaskAdapterProtocol`` using aiosqlite.
``SqlDeadQueue``    – implements ``DeadQueueProtocol`` using aiosqlite.

Concurrent task dequeuing is safe because ``get_next_task`` runs inside a
``BEGIN IMMEDIATE`` transaction, which acquires SQLite's write lock before
reading — only one worker can pop at a time.

Table layout (created by ``ensure_index()``):

  jobber_tasks             — task data (JSON blob) + indexed metadata columns
  jobber_task_queue        — queue entries: task_id, queue name, score (submitted_at ts)
  jobber_task_heartbeats   — heartbeat timestamps per task
  jobber_rate_limiter      — per-queue rate-limit timestamps
  jobber_dlq               — dead letter queue entries
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import TYPE_CHECKING, Any, cast

import aiosqlite  # noqa: TC002
from ulid import ULID

from jobbers.adapters.pipeline import SqlPipeline
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_status import TaskStatus

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from jobbers.adapters.task_adapter import TaskPipeline
    from jobbers.models.queue_config import QueueConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobber_tasks (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    queue        TEXT NOT NULL,
    version      INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL,
    data         TEXT NOT NULL,
    submitted_at REAL,
    completed_at REAL
);
CREATE TABLE IF NOT EXISTS jobber_task_queue (
    task_id  TEXT NOT NULL PRIMARY KEY,
    queue    TEXT NOT NULL,
    score    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS jobber_task_heartbeats (
    task_id  TEXT NOT NULL PRIMARY KEY,
    queue    TEXT NOT NULL,
    score    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS jobber_rate_limiter (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    queue     TEXT NOT NULL,
    timestamp REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS jobber_dlq (
    task_id   TEXT NOT NULL PRIMARY KEY,
    name      TEXT NOT NULL,
    queue     TEXT NOT NULL,
    version   INTEGER NOT NULL DEFAULT 0,
    failed_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobber_task_queue_score
    ON jobber_task_queue (queue, score);
CREATE INDEX IF NOT EXISTS idx_jobber_tasks_status
    ON jobber_tasks (status);
CREATE INDEX IF NOT EXISTS idx_jobber_tasks_name
    ON jobber_tasks (name);
CREATE INDEX IF NOT EXISTS idx_jobber_rate_limiter_queue_ts
    ON jobber_rate_limiter (queue, timestamp);
CREATE INDEX IF NOT EXISTS idx_jobber_dlq_queue  ON jobber_dlq (queue);
CREATE INDEX IF NOT EXISTS idx_jobber_dlq_name   ON jobber_dlq (name);
CREATE INDEX IF NOT EXISTS idx_jobber_dlq_failed_at ON jobber_dlq (failed_at);
"""


# ---------------------------------------------------------------------------
# SqlTaskAdapter
# ---------------------------------------------------------------------------


class SqlTaskAdapter:
    """
    TaskAdapterProtocol implementation backed by SQLite via aiosqlite.

    Does **not** extend ``_BaseTaskAdapter``; that class is Redis-coupled.
    All state — task data, queue entries, heartbeats, rate-limit counters —
    lives in SQLite tables created by ``ensure_index()``.
    """

    # Protocol-required format-callable constants.  Values follow the same
    # naming convention as the Redis adapters for cross-adapter compatibility
    # (e.g. state_manager references self.ta.HEARTBEAT_SCORES(queue=...)).
    # In the SQL adapter these return logical identifiers used only as query
    # parameters, not as Redis key names.
    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    HEARTBEAT_SCORES = "task-heartbeats:{queue}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    DLQ_MISSING_DATA = "dlq-missing-data"

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self.data_store: aiosqlite.Connection = conn

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def ensure_index(self) -> None:
        """Create all jobber_* tables and indexes (idempotent)."""
        await self.data_store.executescript(_SCHEMA)
        await self.data_store.commit()

    # ------------------------------------------------------------------
    # Pipeline factory
    # ------------------------------------------------------------------

    def pipeline(self) -> SqlPipeline:
        """Return a new SqlPipeline for use with stage_* methods."""
        return SqlPipeline(self.data_store)

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    async def submit_task(self, task: Task) -> bool:
        """
        Atomically enqueue a new task.

        Returns True if the task was newly inserted, False if it already existed.
        Uses BEGIN IMMEDIATE so that the existence check and insert are atomic.
        """
        assert task.submitted_at  # noqa: S101
        task_id_str = str(task.id)
        await self.data_store.execute("BEGIN IMMEDIATE")
        try:
            row = await (
                await self.data_store.execute(
                    "SELECT 1 FROM jobber_tasks WHERE id = ?", [task_id_str]
                )
            ).fetchone()
            is_new = row is None
            await self.data_store.execute(
                """INSERT INTO jobber_tasks
                       (id, name, queue, version, status, data, submitted_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       status = excluded.status,
                       data   = excluded.data""",
                [
                    task_id_str,
                    task.name,
                    task.queue,
                    task.version,
                    str(task.status),
                    task.pack(),
                    task.submitted_at.timestamp(),
                ],
            )
            if is_new:
                await self.data_store.execute(
                    "INSERT INTO jobber_task_queue (task_id, queue, score) VALUES (?, ?, ?)",
                    [task_id_str, task.queue, task.submitted_at.timestamp()],
                )
            await self.data_store.commit()
        except Exception:
            await self.data_store.rollback()
            raise
        return is_new

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """
        Check the per-queue rate limit and enqueue if not exceeded.

        Returns True if the task was accepted, False if it was rate-limited out.
        Re-submissions (task already exists) bypass the rate limit and always succeed.
        """
        assert task.submitted_at  # noqa: S101
        now = dt.datetime.now(dt.UTC)
        period_secs = queue_config.period_in_seconds() or 0
        earliest_ts = (now - dt.timedelta(seconds=period_secs)).timestamp()
        task_id_str = str(task.id)

        await self.data_store.execute("BEGIN IMMEDIATE")
        try:
            # Prune stale rate-limiter entries first.
            await self.data_store.execute(
                "DELETE FROM jobber_rate_limiter WHERE queue = ? AND timestamp < ?",
                [task.queue, earliest_ts],
            )
            existing = await (
                await self.data_store.execute(
                    "SELECT 1 FROM jobber_tasks WHERE id = ?", [task_id_str]
                )
            ).fetchone()

            if existing is not None:
                # Re-submission path: just update the task data.
                await self.data_store.execute(
                    "UPDATE jobber_tasks SET status = ?, data = ? WHERE id = ?",
                    [str(task.status), task.pack(), task_id_str],
                )
                await self.data_store.commit()
                return True

            numerator = queue_config.rate_numerator or 0
            if numerator == 0:
                # No rate limit configured — enqueue unconditionally.
                enqueued = True
            else:
                count_row = await (
                    await self.data_store.execute(
                        "SELECT COUNT(*) FROM jobber_rate_limiter WHERE queue = ?",
                        [task.queue],
                    )
                ).fetchone()
                count = count_row[0] if count_row else 0
                enqueued = count < numerator

            if enqueued:
                if numerator > 0:
                    await self.data_store.execute(
                        "INSERT INTO jobber_rate_limiter (queue, timestamp) VALUES (?, ?)",
                        [task.queue, now.timestamp()],
                    )
                await self.data_store.execute(
                    """INSERT INTO jobber_tasks
                           (id, name, queue, version, status, data, submitted_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [
                        task_id_str,
                        task.name,
                        task.queue,
                        task.version,
                        str(task.status),
                        task.pack(),
                        task.submitted_at.timestamp(),
                    ],
                )
                await self.data_store.execute(
                    "INSERT INTO jobber_task_queue (task_id, queue, score) VALUES (?, ?, ?)",
                    [task_id_str, task.queue, task.submitted_at.timestamp()],
                )

            await self.data_store.commit()
        except Exception:
            await self.data_store.rollback()
            raise
        return enqueued

    def stage_save(self, pipe: TaskPipeline, task: Task) -> None:
        """Queue an UPSERT of this task onto the SQL pipeline."""
        sql_pipe = cast("SqlPipeline", pipe)
        task_id_str = str(task.id)
        sql_pipe.queue_sql(
            """INSERT INTO jobber_tasks
                   (id, name, queue, version, status, data, submitted_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   status       = excluded.status,
                   data         = excluded.data,
                   completed_at = excluded.completed_at""",
            [
                task_id_str,
                task.name,
                task.queue,
                task.version,
                str(task.status),
                task.pack(),
                task.submitted_at.timestamp() if task.submitted_at else None,
                task.completed_at.timestamp() if task.completed_at else None,
            ],
        )

    def stage_requeue(self, pipe: TaskPipeline, task: Task) -> None:
        """Queue a save + queue-entry insert onto the SQL pipeline."""
        assert task.submitted_at  # noqa: S101
        sql_pipe = cast("SqlPipeline", pipe)
        self.stage_save(sql_pipe, task)
        sql_pipe.queue_sql(
            """INSERT INTO jobber_task_queue (task_id, queue, score) VALUES (?, ?, ?)
               ON CONFLICT(task_id) DO UPDATE SET
                   queue = excluded.queue,
                   score = excluded.score""",
            [str(task.id), task.queue, task.submitted_at.timestamp()],
        )

    def stage_remove_from_queue(self, pipe: TaskPipeline, task: Task) -> None:
        """Queue a DELETE from the task queue onto the SQL pipeline."""
        sql_pipe = cast("SqlPipeline", pipe)
        sql_pipe.queue_sql(
            "DELETE FROM jobber_task_queue WHERE task_id = ?",
            [str(task.id)],
        )

    async def save_task(self, task: Task) -> None:
        """Save a task outside of a pipeline."""
        pipe = self.pipeline()
        self.stage_save(pipe, task)
        await pipe.execute()

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    async def get_task(self, task_id: ULID) -> Task | None:
        """Fetch a single task by ID, hydrating heartbeat_at if present."""
        task_id_str = str(task_id)
        row = await (
            await self.data_store.execute(
                "SELECT data FROM jobber_tasks WHERE id = ?", [task_id_str]
            )
        ).fetchone()
        if row is None:
            return None
        task = Task.unpack(task_id, row[0])
        hb_row = await (
            await self.data_store.execute(
                "SELECT score FROM jobber_task_heartbeats WHERE task_id = ?",
                [task_id_str],
            )
        ).fetchone()
        if hb_row is not None:
            task.heartbeat_at = dt.datetime.fromtimestamp(hb_row[0], dt.UTC)
        return task

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        """Fetch multiple tasks in 2 IN-clause queries instead of N individual calls."""
        if not task_ids:
            return []
        id_strs = [str(tid) for tid in task_ids]
        ph = ",".join("?" * len(id_strs))

        rows = await (
            await self.data_store.execute(
                f"SELECT id, data FROM jobber_tasks WHERE id IN ({ph})", id_strs
            )
        ).fetchall()
        data_map: dict[str, str] = {r[0]: r[1] for r in rows}

        hb_rows = await (
            await self.data_store.execute(
                f"SELECT task_id, score FROM jobber_task_heartbeats WHERE task_id IN ({ph})",
                id_strs,
            )
        ).fetchall()
        hb_map: dict[str, float] = {r[0]: r[1] for r in hb_rows}

        results: list[Task | None] = []
        for tid, tid_str in zip(task_ids, id_strs):
            if tid_str not in data_map:
                results.append(None)
                continue
            task = Task.unpack(tid, data_map[tid_str])
            if tid_str in hb_map:
                task.heartbeat_at = dt.datetime.fromtimestamp(hb_map[tid_str], dt.UTC)
            results.append(task)
        return results

    async def read_for_watch(self, pipe: TaskPipeline, task_id: ULID) -> Task | None:
        """
        Read a task for use in an optimistic-lock dispatch loop.

        SQLite uses ``BEGIN IMMEDIATE`` instead of Redis WATCH/MULTI/EXEC,
        so no watch is needed — just read the task directly.
        The ``pipe`` parameter is accepted for Protocol compatibility but ignored.
        """
        return await self.get_task(task_id)

    async def task_exists(self, task_id: ULID) -> bool:
        row = await (
            await self.data_store.execute(
                "SELECT 1 FROM jobber_tasks WHERE id = ?", [str(task_id)]
            )
        ).fetchone()
        return row is not None

    async def get_active_tasks(self, queues: set[str]) -> list[Task]:
        """Return all tasks that have a heartbeat entry in any of the given queues."""
        if not queues:
            return []
        ph = ",".join("?" * len(queues))
        rows = await (
            await self.data_store.execute(
                f"SELECT task_id FROM jobber_task_heartbeats WHERE queue IN ({ph})",
                sorted(queues),
            )
        ).fetchall()
        if not rows:
            return []
        fetched = await self.get_tasks_bulk([ULID.from_str(r[0]) for r in rows])
        return [t for t in fetched if t is not None]

    async def get_stale_tasks(
        self, queues: set[str], stale_time: dt.timedelta
    ) -> AsyncGenerator[Task, None]:
        """Async generator yielding tasks whose heartbeat is older than stale_time."""
        if not queues:
            return
        now = dt.datetime.now(dt.UTC)
        cutoff = (now - stale_time).timestamp()
        ph = ",".join("?" * len(queues))
        rows = await (
            await self.data_store.execute(
                f"""SELECT task_id FROM jobber_task_heartbeats
                    WHERE score < ? AND queue IN ({ph})""",
                [cutoff, *sorted(queues)],
            )
        ).fetchall()
        for row in rows:
            task = await self.get_task(ULID.from_str(row[0]))
            if task is not None:
                yield task

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Return a filtered, paginated list of tasks."""
        conditions: list[str] = ["queue = ?"]
        params: list[Any] = [pagination.queue]

        if pagination.task_name is not None:
            conditions.append("name = ?")
            params.append(pagination.task_name)
        if pagination.task_version is not None:
            conditions.append("version = ?")
            params.append(pagination.task_version)
        if pagination.status is not None:
            conditions.append("status = ?")
            params.append(str(pagination.status))
        if pagination.start is not None:
            conditions.append("id > ?")
            params.append(str(pagination.start))

        order = (
            "submitted_at ASC"
            if pagination.order_by == PaginationOrder.SUBMITTED_AT
            else "id ASC"
        )
        where = " AND ".join(conditions)
        params.extend([pagination.limit, pagination.offset])

        rows = await (
            await self.data_store.execute(
                f"SELECT id, data FROM jobber_tasks WHERE {where}"
                f" ORDER BY {order} LIMIT ? OFFSET ?",
                params,
            )
        ).fetchall()

        return [Task.unpack(ULID.from_str(r[0]), r[1]) for r in rows]

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------

    async def _try_pop_task(self, queues: set[str]) -> Task | None:
        """
        Attempt a single non-blocking pop from the named queues.

        Uses ``BEGIN IMMEDIATE`` to hold the SQLite write lock for the duration
        of the SELECT + DELETE, preventing concurrent workers from popping the
        same task.
        """
        ph = ",".join("?" * len(queues))
        queue_list = sorted(queues)  # deterministic ordering avoids deadlocks

        await self.data_store.execute("BEGIN IMMEDIATE")
        try:
            row = await (
                await self.data_store.execute(
                    f"""SELECT task_id FROM jobber_task_queue
                        WHERE queue IN ({ph})
                        ORDER BY score ASC
                        LIMIT 1""",
                    queue_list,
                )
            ).fetchone()
            if row is None:
                await self.data_store.rollback()
                return None
            task_id_str: str = row[0]
            await self.data_store.execute(
                "DELETE FROM jobber_task_queue WHERE task_id = ?", [task_id_str]
            )
            await self.data_store.commit()
        except Exception:
            await self.data_store.rollback()
            raise

        task = await self.get_task(ULID.from_str(task_id_str))
        if task is None:
            logger.error(
                "Task %s was popped from the queue but its data is missing.", task_id_str
            )
        return task

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """
        Pop and return the next available task from the given queues (FIFO by score).

        If ``pop_timeout == 0`` returns immediately (None if empty).
        If ``pop_timeout > 0`` polls every 0.5 s until the deadline.
        """
        task = await self._try_pop_task(queues)
        if task is not None or pop_timeout == 0:
            return task

        loop = asyncio.get_event_loop()
        deadline = loop.time() + pop_timeout
        while loop.time() < deadline:
            await asyncio.sleep(min(0.5, deadline - loop.time()))
            task = await self._try_pop_task(queues)
            if task is not None:
                return task

        logger.info("task query timed out")
        return None

    async def update_task_heartbeat(self, task: Task) -> None:
        """Upsert the heartbeat timestamp for a task."""
        assert task.heartbeat_at  # noqa: S101
        await self.data_store.execute(
            """INSERT INTO jobber_task_heartbeats (task_id, queue, score)
               VALUES (?, ?, ?)
               ON CONFLICT(task_id) DO UPDATE SET score = excluded.score""",
            [str(task.id), task.queue, task.heartbeat_at.timestamp()],
        )
        await self.data_store.commit()

    async def remove_task_heartbeat(self, task: Task) -> None:
        """Remove the heartbeat entry for a task."""
        await self.data_store.execute(
            "DELETE FROM jobber_task_heartbeats WHERE task_id = ?", [str(task.id)]
        )
        await self.data_store.commit()

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Delete tasks in terminal states that completed more than max_age ago."""
        cutoff = (now - max_age).timestamp()
        terminal = [
            str(s)
            for s in (
                TaskStatus.COMPLETED,
                TaskStatus.FAILED,
                TaskStatus.CANCELLED,
                TaskStatus.STALLED,
                TaskStatus.DROPPED,
            )
        ]
        ph = ",".join("?" * len(terminal))
        rows = await (
            await self.data_store.execute(
                f"""SELECT id FROM jobber_tasks
                    WHERE status IN ({ph})
                      AND completed_at IS NOT NULL
                      AND completed_at < ?""",
                [*terminal, cutoff],
            )
        ).fetchall()
        if not rows:
            return
        ids = [r[0] for r in rows]
        id_ph = ",".join("?" * len(ids))
        await self.data_store.execute("BEGIN")
        try:
            await self.data_store.execute(
                f"DELETE FROM jobber_tasks WHERE id IN ({id_ph})", ids
            )
            await self.data_store.execute(
                f"DELETE FROM jobber_task_heartbeats WHERE task_id IN ({id_ph})", ids
            )
            await self.data_store.commit()
        except Exception:
            await self.data_store.rollback()
            raise

    async def clean(
        self,
        queues: set[bytes],
        now: dt.datetime,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
    ) -> None:
        """Remove queue entries whose score falls within the given time range."""
        if not (min_queue_age or max_queue_age):
            return
        from jobbers.constants import TIME_ZERO

        earliest = min_queue_age or TIME_ZERO
        latest = max_queue_age or now
        queue_strs = [q.decode() for q in queues]
        if not queue_strs:
            return

        ph = ",".join("?" * len(queue_strs))
        await self.data_store.execute("BEGIN")
        try:
            if earliest <= latest:
                await self.data_store.execute(
                    f"""DELETE FROM jobber_task_queue
                        WHERE queue IN ({ph})
                          AND score >= ? AND score <= ?""",
                    [*queue_strs, earliest.timestamp(), latest.timestamp()],
                )
            else:
                await self.data_store.execute(
                    f"""DELETE FROM jobber_task_queue
                        WHERE queue IN ({ph})
                          AND (score <= ? OR score >= ?)""",
                    [*queue_strs, latest.timestamp(), earliest.timestamp()],
                )
            await self.data_store.commit()
        except Exception:
            await self.data_store.rollback()
            raise


# ---------------------------------------------------------------------------
# SqlDeadQueue
# ---------------------------------------------------------------------------


class SqlDeadQueue:
    """
    DeadQueueProtocol implementation backed by SQLite.

    Stores DLQ entries in the ``jobber_dlq`` table.  Full task data is
    retrieved from the paired ``SqlTaskAdapter``.
    """

    def __init__(self, conn: aiosqlite.Connection, ta: SqlTaskAdapter) -> None:
        self.data_store: aiosqlite.Connection = conn
        self.ta = ta

    async def ensure_index(self) -> None:
        """Tables are created by SqlTaskAdapter.ensure_index(); this is a no-op."""

    def pipeline(self) -> SqlPipeline:
        return self.ta.pipeline()

    def stage_add(self, pipe: TaskPipeline, task: Task, failed_at: dt.datetime) -> None:
        """Queue a DLQ entry insert onto the SQL pipeline."""
        sql_pipe = cast("SqlPipeline", pipe)
        sql_pipe.queue_sql(
            """INSERT INTO jobber_dlq (task_id, name, queue, version, failed_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(task_id) DO UPDATE SET failed_at = excluded.failed_at""",
            [str(task.id), task.name, task.queue, task.version, failed_at.timestamp()],
        )

    def stage_remove(
        self, pipe: TaskPipeline, task_id: ULID, queue: str, name: str
    ) -> None:
        """Queue a DLQ entry delete onto the SQL pipeline."""
        sql_pipe = cast("SqlPipeline", pipe)
        sql_pipe.queue_sql(
            "DELETE FROM jobber_dlq WHERE task_id = ?", [str(task_id)]
        )

    async def get_history(self, task_id: str) -> list[dict[str, Any]]:
        """Return the per-attempt error history for a DLQ task from its stored task blob."""
        task = await self.ta.get_task(ULID.from_str(task_id))
        if task is None:
            return []
        return [{"attempt": i, "error": e} for i, e in enumerate(task.errors)]

    async def get_by_ids(self, task_ids: list[str]) -> list[Task]:
        """Fetch full task objects for the given DLQ entry IDs."""
        if not task_ids:
            return []
        ph = ",".join("?" * len(task_ids))
        rows = await (
            await self.data_store.execute(
                f"SELECT task_id FROM jobber_dlq WHERE task_id IN ({ph})", task_ids
            )
        ).fetchall()
        valid_ids = [ULID.from_str(r[0]) for r in rows]
        if not valid_ids:
            return []
        fetched = await self.ta.get_tasks_bulk(valid_ids)
        return [t for t in fetched if t is not None]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list[Task]:
        """Return DLQ entries matching the filter criteria, newest first."""
        conditions: list[str] = []
        params: list[Any] = []
        if queue is not None:
            conditions.append("queue = ?")
            params.append(queue)
        if task_name is not None:
            conditions.append("name = ?")
            params.append(task_name)

        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        # Over-fetch when filtering by version (applied in Python).
        fetch_limit = limit * 5 if task_version is not None else limit
        params.append(fetch_limit)

        rows = await (
            await self.data_store.execute(
                f"SELECT task_id FROM jobber_dlq{where}"
                " ORDER BY failed_at DESC LIMIT ?",
                params,
            )
        ).fetchall()
        if not rows:
            return []

        ulids = [ULID.from_str(r[0]) for r in rows]
        fetched = await self.ta.get_tasks_bulk(ulids)
        results: list[Task] = []
        for task in fetched:
            if len(results) >= limit:
                break
            if task is None:
                continue
            if task_version is not None and task.version != task_version:
                continue
            results.append(task)
        return results

    async def remove_many(self, task_ids: list[str]) -> None:
        """Delete the given DLQ entries."""
        if not task_ids:
            return
        ph = ",".join("?" * len(task_ids))
        await self.data_store.execute(
            f"DELETE FROM jobber_dlq WHERE task_id IN ({ph})", task_ids
        )
        await self.data_store.commit()

    async def clean(self, earlier_than: dt.datetime) -> None:
        """Delete DLQ entries older than the given timestamp."""
        await self.data_store.execute(
            "DELETE FROM jobber_dlq WHERE failed_at < ?", [earlier_than.timestamp()]
        )
        await self.data_store.commit()
