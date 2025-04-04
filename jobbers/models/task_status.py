from enum import StrEnum


class TaskStatus(StrEnum):
    """Enumeration of task statuses."""

    UNSUBMITTED = "unsubmitted"
    SUBMITTED = "submitted"
    STARTED = "started"
    HEARTBEAT = "heartbeat"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"

    @classmethod
    def from_bytes(cls, raw_status: bytes) -> "TaskStatus":
        if not raw_status:
            return cls.UNSUBMITTED
        return cls(raw_status.decode("utf-8"))

    def to_bytes(self) -> bytes:
        return self.value.encode("utf-8")
