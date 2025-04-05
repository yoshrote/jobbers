from enum import StrEnum
from typing import Optional


class TaskStatus(StrEnum):
    """Enumeration of task statuses."""

    UNSUBMITTED = "unsubmitted"
    SUBMITTED = "submitted"
    STARTED = "started"
    HEARTBEAT = "heartbeat"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    DROPPED = "dropped"

    @classmethod
    def from_bytes(cls, raw_status: Optional[bytes]) -> "TaskStatus":
        if not raw_status:
            return cls.UNSUBMITTED
        return cls(raw_status.decode("utf-8"))

    def to_bytes(self) -> bytes:
        return self.value.encode("utf-8")
