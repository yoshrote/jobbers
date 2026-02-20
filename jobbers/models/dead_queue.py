import datetime as dt
import sqlite3
from pathlib import Path
from typing import Any

from jobbers.models.task import Task


class DeadQueue:
    """Maintains record of failed tasks in a SQLite database."""

    _CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS dead_queue (
            task_id  TEXT    PRIMARY KEY,
            task     TEXT    NOT NULL,
            queue    TEXT    NOT NULL,
            task_name  TEXT    NOT NULL,
            task_version INTEGER    NOT NULL,
            failed_at   DATETIME    NOT NULL,
            error_message TEXT
        )
    """
    # Support efficient aggregation and querying of failed tasks by queue and task type.
    _CREATE_INDEX = """
        CREATE INDEX IF NOT EXISTS dead_queue_categories
        ON dead_queue (queue, task_name, task_version)
    """

    # Support efficient cleanup based on age of failed tasks.
    _CREATE_INDEX_AGE = """
        CREATE INDEX IF NOT EXISTS dead_queue_failed_at
        ON dead_queue (failed_at)
    """
    def __init__(self, db_path: str | Path) -> None:
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.execute(self._CREATE_TABLE)
            self._conn.execute(self._CREATE_INDEX)

    def add(self, task: Task, failed_at: dt.datetime) -> None:
        """Insert or replace a task in the schedule."""
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO dead_queue (task_id, task, queue, task_name, task_version, failed_at, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (str(task.id), task.model_dump_json(), task.queue, task.name, task.version, failed_at.isoformat(), task.error),
            )

    def clean(self, earlier_than: dt.datetime) -> None:
        """Remove failed tasks older than the specified datetime."""
        with self._conn:
            self._conn.execute(
                "DELETE FROM dead_queue WHERE failed_at < ?",
                (earlier_than.isoformat(),),
            )

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> "DeadQueue":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
