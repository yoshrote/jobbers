import datetime as dt
from typing import Any, Optional

from pydantic import BaseModel, Field
from ulid import ULID

from jobbers.serialization import (
    EMPTY_DICT,
    NONE,
    deserialize,
    serialize,
)

from . import TaskConfig, TaskStatus


class Task(BaseModel):
    """A task to be executed."""

    id: ULID
    # task mapping fields
    name: str
    queue: str = "default"
    version: int = 0
    parameters: dict = {}
    results: dict = {}
    error: Optional[str] = None
    # status fields
    retry_attempt: int = 0  # Number of times this task has been retried
    status: TaskStatus = Field(default=TaskStatus.UNSUBMITTED)
    submitted_at: Optional[dt.datetime] = None
    retried_at: Optional[dt.datetime] = None
    started_at: Optional[dt.datetime] = None
    heartbeat_at: Optional[dt.datetime] = None
    completed_at: Optional[dt.datetime] = None

    def should_retry(self, task_config: TaskConfig) -> bool:
        return self.retry_attempt < task_config.max_retries

    def has_callbacks(self) -> bool:
        return False

    def generate_callbacks(self) -> list["Task"]:
        return []

    def summarized(self) -> dict[str, Any]:
        summary = self.model_dump(include={"id", "name", "parameters", "status", "retry_attempt", "submitted_at"})
        summary["id"] = str(self.id)
        return summary

    @classmethod
    def from_redis(cls, task_id: ULID, raw_task_data: bytes) -> "Task":
        # Try to set good defaults for missing fields so when new fields are added to the task model, we don't break
        unpacked_data = deserialize(raw_task_data)
        print(unpacked_data)
        return cls(
            id=task_id,
            name=unpacked_data.get(b"name", ""),
            version=unpacked_data.get(b"version", 0),
            parameters=unpacked_data.get(b"parameters") or {},
            results=unpacked_data.get(b"results") or {},
            error=unpacked_data.get(b"error") or None,
            status=TaskStatus.from_bytes(unpacked_data.get(b"status")),
            submitted_at=unpacked_data.get(b"submitted_at") or None,
            started_at=unpacked_data.get(b"started_at") or None,
            heartbeat_at=unpacked_data.get(b"heartbeat_at") or None,
            completed_at=unpacked_data.get(b"completed_at") or None,
        )

    def to_redis(self):
        return serialize({
            b"name": self.name,
            b"version": self.version,
            b"parameters": self.parameters or {},
            b"results": self.results or {},
            b"error": self.error,
            b"status": self.status.to_bytes(),
            b"submitted_at": self.submitted_at,
            b"started_at": self.started_at,
            b"heartbeat_at": self.heartbeat_at,
            b"completed_at": self.completed_at,
        })
