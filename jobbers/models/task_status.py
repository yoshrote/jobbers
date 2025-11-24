from enum import StrEnum


class TaskStatus(StrEnum):
    """Enumeration of task statuses."""

    UNSUBMITTED = "unsubmitted"
    SUBMITTED = "submitted"
    STARTED = "started"
    HEARTBEAT = "heartbeat"
    COMPLETED = "completed"
    CANCELLED = "cancelled" # cancelled by the user
    STALLED = "stalled" # cancelled by the system
    FAILED = "failed"
    DROPPED = "dropped"

    @classmethod
    def from_bytes(cls, raw_status: bytes | None) -> "TaskStatus":
        if not raw_status:
            return cls.UNSUBMITTED
        return cls(raw_status.decode())

    def to_bytes(self) -> bytes:
        return self.value.encode()
