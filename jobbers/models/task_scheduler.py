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
                "INSERT OR REPLACE INTO schedule (task_id, task, queue, run_at) VALUES (?, ?, ?, ?)",
                (str(task.id), task.model_dump_json(), task.queue, run_at.isoformat()),
            )

    def remove(self, task_id: ULID) -> None:
        """Remove a scheduled task by ID."""
        with self._conn:
            self._conn.execute(
                "DELETE FROM schedule WHERE task_id = ?",
                (str(task_id),),
            )

    def next_due(self, queues: list[str]) -> Task | None:
        """
        Atomically acquire and return the earliest due task in the given queues, or None.

        Sets acquired = 1 on the selected row so no other TaskScheduler instance
        can return the same task.
        """
        if not queues:
            return None
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        placeholders = ",".join("?" * len(queues))
        row = self._conn.execute(
            f"""
            UPDATE schedule SET acquired = 1
            WHERE task_id = (
                SELECT task_id FROM schedule
                WHERE run_at <= ? AND queue IN ({placeholders}) AND acquired = 0
                ORDER BY run_at LIMIT 1
            )
            RETURNING task
            """,
            (now, *queues),
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
