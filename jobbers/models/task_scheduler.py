import datetime as dt
import sqlite3
from pathlib import Path
from typing import Any

from ulid import ULID

from jobbers.models.task import Task


class TaskScheduler:
    """Manages scheduled tasks persisted in a SQLite database."""

    _CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS schedule (
            task_id  TEXT    PRIMARY KEY,
            task_name  TEXT    NOT NULL,
            task_version INTEGER    NOT NULL,
            task     TEXT    NOT NULL,
            queue    TEXT    NOT NULL,
            run_at   TEXT    NOT NULL,
            acquired INTEGER NOT NULL DEFAULT 0
        )
    """
    # Partial index: only unacquired rows are ever queried by next_due.
    _CREATE_INDEX = """
        CREATE INDEX IF NOT EXISTS schedule_run_at_queue
        ON schedule (run_at, queue)
        WHERE acquired = 0
    """

    # Support efficient aggregation and querying of scheduled tasks by queue and task type.
    _CREATE_INDEX = """
        CREATE INDEX IF NOT EXISTS schedule_queue_categories
        ON schedule (queue, task_name, task_version)
    """

    def __init__(self, db_path: str | Path) -> None:
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.execute(self._CREATE_TABLE)
            self._conn.execute(self._CREATE_INDEX)

    def add(self, task: Task, run_at: dt.datetime) -> None:
        """Insert or replace a task in the schedule."""
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO schedule (task_id, task_name, task_version, task, queue, run_at) VALUES (?, ?, ?, ?, ?, ?)",
                (str(task.id), task.name, task.version, task.model_dump_json(), task.queue, run_at.isoformat()),
            )

    def remove(self, task_id: ULID) -> None:
        """Remove a scheduled task by ID."""
        with self._conn:
            self._conn.execute(
                "DELETE FROM schedule WHERE task_id = ?",
                (str(task_id),),
            )

    def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
        start_after: str | None = None,
    ) -> list[Task]:
        """
        Fetch schedule entries matching the given filter criteria.

        ``start_after`` is an exclusive ULID cursor: only tasks whose
        ``task_id`` sorts after this value are returned, enabling
        page-by-page iteration over large result sets.
        """
        conditions: list[str] = []
        params: list[Any] = []
        if queue is not None:
            conditions.append("queue = ?")
            params.append(queue)
        if task_name is not None:
            conditions.append("task_name = ?")
            params.append(task_name)
        if task_version is not None:
            conditions.append("task_version = ?")
            params.append(task_version)
        if start_after is not None:
            conditions.append("task_id > ?")
            params.append(start_after)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        rows = self._conn.execute(
            f"SELECT task FROM schedule {where} ORDER BY task_id LIMIT ?",
            params,
        ).fetchall()
        return [Task.model_validate_json(row["task"]) for row in rows]

    def next_due_bulk(self, n: int, queues: list[str] | None = None) -> list[tuple[Task, dt.datetime]]:
        """
        Atomically acquire and return up to n due tasks paired with their scheduled run_at time.

        Each element is a ``(task, run_at)`` tuple where ``run_at`` is the UTC datetime
        the task was originally scheduled for.  Callers can use ``run_at`` to measure
        dispatch latency (``now - run_at``).

        Sets acquired = 1 on the selected rows so no other TaskScheduler instance
        can return the same tasks.

        - ``queues=None`` — match any queue
        - ``queues=[]`` — return [] immediately
        - ``queues=[...]`` — only match tasks in the given queues
        """
        if queues is not None and not queues:
            return []
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        if queues is None:
            queue_filter = ""
            params: tuple[object, ...] = (now, n)
        else:
            placeholders = ",".join("?" * len(queues))
            queue_filter = f"AND queue IN ({placeholders})"
            params = (now, *queues, n)
        rows = self._conn.execute(
            f"""
            UPDATE schedule SET acquired = 1
            WHERE task_id IN (
                SELECT task_id FROM schedule
                WHERE run_at <= ? {queue_filter} AND acquired = 0
                ORDER BY run_at, task_id LIMIT ?
            )
            RETURNING task, run_at
            """,
            params,
        ).fetchall()
        return [
            (Task.model_validate_json(row["task"]), dt.datetime.fromisoformat(row["run_at"]))
            for row in rows
        ]

    def next_due(self, queues: list[str] | None = None) -> Task | None:
        """
        Atomically acquire and return the earliest due task, or None.

        Sets acquired = 1 on the selected row so no other TaskScheduler instance
        can return the same task.

        - ``queues=None`` — match any queue (used by the scheduler runner)
        - ``queues=[]`` — return None immediately (no queues to match)
        - ``queues=[...]`` — only match tasks in the given queues
        """
        if queues is not None and not queues:
            return None
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        if queues is None:
            queue_filter = ""
            params: tuple[object, ...] = (now,)
        else:
            placeholders = ",".join("?" * len(queues))
            queue_filter = f"AND queue IN ({placeholders})"
            params = (now, *queues)
        row = self._conn.execute(
            f"""
            UPDATE schedule SET acquired = 1
            WHERE task_id = (
                SELECT task_id FROM schedule
                WHERE run_at <= ? {queue_filter} AND acquired = 0
                ORDER BY run_at, task_id LIMIT 1
            )
            RETURNING task
            """,
            params,
        ).fetchone()
        if row is None:
            return None
        return Task.model_validate_json(row["task"])

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> "TaskScheduler":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
