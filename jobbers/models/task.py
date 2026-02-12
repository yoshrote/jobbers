import datetime as dt
import inspect
from typing import Any, Self

from pydantic import BaseModel, Field
from ulid import ULID

from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.state_manager import StateManager
from jobbers.utils.serialization import (
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
    parameters: dict[Any, Any] = {}
    results: dict[Any, Any] = {}
    error: str | None = None
    # status fields
    retry_attempt: int = 0  # Number of times this task has been retried
    status: TaskStatus = Field(default=TaskStatus.UNSUBMITTED)
    submitted_at: dt.datetime | None = None
    retried_at: dt.datetime | None = None
    started_at: dt.datetime | None = None
    heartbeat_at: dt.datetime | None = None
    completed_at: dt.datetime | None = None

    task_config: TaskConfig | None = Field(default=None, exclude=True)

    def valid_task_params(self) -> bool:
        if not self.task_config:
            # Safer to fail here than chance something funky downstream
            return False
        signature = inspect.get_annotations(self.task_config.function)
        for param, psig in signature.items():
            # Skip the return type annotation
            if param != "return" and not isinstance(self.parameters[param], psig):
                return False
        return True

    def shutdown(self) -> None:
        if self.task_config is None:
            return
        match self.task_config.on_shutdown:
            case TaskShutdownPolicy.CONTINUE:
                # NOOP: The execution of the task function needs to be wrapped
                # in `shield()` already.
                # TODO: maybe warn or panic since this should be unreachable
                pass
            case TaskShutdownPolicy.STOP:
                self.status = TaskStatus.STALLED
                self.completed_at = dt.datetime.now(dt.timezone.utc)
            case TaskShutdownPolicy.RESUBMIT:
                self.status = TaskStatus.UNSUBMITTED

    def should_retry(self) -> bool:
        if not self.task_config:
            # safer to fail here than chance something funky downstream
            return False
        return self.retry_attempt < self.task_config.max_retries

    def has_callbacks(self) -> bool:
        return False

    def generate_callbacks(self) -> list[Self]:
        return []

    def summarized(self) -> dict[str, Any]:
        summary = self.model_dump(include={"id", "name", "parameters", "status", "retry_attempt", "submitted_at"})
        summary["id"] = str(self.id)
        return summary

    @property
    def _sm(self) -> StateManager:
        from jobbers.db import get_state_manager

        return get_state_manager()

    @classmethod
    def from_redis(cls, task_id: ULID, raw_task_data: dict[bytes, bytes]) -> Self:
        # Try to set good defaults for missing fields so when new fields are added to the task model, we don't break
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

    def to_redis(self) -> dict[bytes, bytes | int]:
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

    def set_status(self, status: TaskStatus) -> None:
        match status:
            case TaskStatus.STARTED:
                if not self.started_at:
                    self.started_at = dt.datetime.now(dt.timezone.utc)
                else:
                    self.retried_at = dt.datetime.now(dt.timezone.utc)
            case TaskStatus.SUBMITTED:
                self.submitted_at = dt.datetime.now(dt.timezone.utc)
            case TaskStatus.COMPLETED | TaskStatus.FAILED | \
                 TaskStatus.CANCELLED | TaskStatus.STALLED | TaskStatus.DROPPED:
                self.completed_at = dt.datetime.now(dt.timezone.utc)
        self.status = status

    def set_to_retry(self) -> None:
        self.status = TaskStatus.UNSUBMITTED
        self.retry_attempt += 1
