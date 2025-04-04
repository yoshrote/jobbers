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

    # expected_exceptions = tuple([]) #[Exception]

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
    def from_redis(cls, task_id: ULID, raw_task_data: dict) -> "Task":
        # Try to set good defaults for missing fields so when new fields are added to the task model, we don't break
        print(repr(raw_task_data.get(b"submitted_at")))
        return cls(
            id=task_id,
            name=raw_task_data.get(b"name", b"").decode(),
            version=int(raw_task_data.get(b"version", b"0")),
            parameters=deserialize(raw_task_data.get(b"parameters") or EMPTY_DICT),
            results=deserialize(raw_task_data.get(b"results") or EMPTY_DICT),
            error=deserialize(raw_task_data.get(b"error") or NONE),
            status=TaskStatus.from_bytes(raw_task_data.get(b"status")),
            submitted_at=deserialize(raw_task_data.get(b"submitted_at") or NONE),
            started_at=deserialize(raw_task_data.get(b"started_at") or NONE),
            heartbeat_at=deserialize(raw_task_data.get(b"heartbeat_at") or NONE),
            completed_at=deserialize(raw_task_data.get(b"completed_at") or NONE),
        )

    def to_redis(self):
        return {
            b"name": self.name.encode(),
            b"version": self.version,
            b"parameters": serialize(self.parameters or {}),
            b"results": serialize(self.results or {}),
            b"error": serialize(self.error),
            b"status": self.status.to_bytes(),
            b"submitted_at": serialize(self.submitted_at),
            b"started_at": serialize(self.started_at),
            b"heartbeat_at": serialize(self.heartbeat_at),
            b"completed_at": serialize(self.completed_at),
        }
