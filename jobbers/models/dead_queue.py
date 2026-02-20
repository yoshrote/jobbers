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
        """Insert or replace the current DLQ entry for a task (latest failure wins)."""
        last_error = task.errors[-1] if task.errors else None
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO dead_queue (task_id, task, queue, task_name, task_version, failed_at, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (str(task.id), task.model_dump_json(), task.queue, task.name, task.version, failed_at.isoformat(), last_error),
            )

    def get_history(self, task_id: str) -> list[dict[str, Any]]:
        """Return the per-attempt error history for a DLQ task from its stored task blob."""
        row = self._conn.execute(
            "SELECT task FROM dead_queue WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            return []
        task = Task.model_validate_json(row["task"])
        return [{"attempt": i, "error": e} for i, e in enumerate(task.errors)]

    def get_by_ids(self, task_ids: list[str]) -> list[Task]:
        """Fetch DLQ entries by explicit task ID list."""
        placeholders = ",".join("?" * len(task_ids))
        rows = self._conn.execute(
            f"SELECT task FROM dead_queue WHERE task_id IN ({placeholders})",
            task_ids,
        ).fetchall()
        return [Task.model_validate_json(row["task"]) for row in rows]

    def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list[Task]:
        """Fetch DLQ entries matching the given filter criteria."""
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
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        rows = self._conn.execute(
            f"SELECT task FROM dead_queue {where} LIMIT ?",
            params,
        ).fetchall()
        return [Task.model_validate_json(row["task"]) for row in rows]

    def remove(self, task_id: str) -> None:
        """Remove a single entry from the dead letter queue."""
        with self._conn:
            self._conn.execute("DELETE FROM dead_queue WHERE task_id = ?", (task_id,))

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
