from enum import StrEnum


class TaskStatus(StrEnum):
    """Enumeration of task statuses."""

    UNSUBMITTED = "unsubmitted"
    SUBMITTED = "submitted"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELLED = "cancelled"  # cancelled by the user
    STALLED = "stalled"  # cancelled by the system
    FAILED = "failed"
    DROPPED = "dropped"
    SCHEDULED = "scheduled"  # waiting in the delay queue for a future retry

    @classmethod
    def from_bytes(cls, raw_status: bytes | None) -> "TaskStatus":
        if not raw_status:
            return cls.UNSUBMITTED
        return cls(raw_status.decode())

    def to_bytes(self) -> bytes:
        return self.value.encode()

    @classmethod
    def active_statuses(cls) -> set["TaskStatus"]:
        return {cls.SUBMITTED, cls.STARTED, cls.SCHEDULED}

    @classmethod
    def terminal_statuses(cls) -> set["TaskStatus"]:
        return {cls.COMPLETED, cls.FAILED, cls.CANCELLED, cls.STALLED, cls.DROPPED}

    @classmethod
    def stuck_statuses(cls) -> set["TaskStatus"]:
        """
        Terminal statuses that indicate a DAG run may need operator intervention to resume.

        A task in one of these statuses never calls ``generate_callbacks()``, so any
        ``FanInCallback``/``DynamicFanOutCallback`` it carries never fires — the DAG's
        collector is permanently blocked until the task is retried. Used to gate the
        automatic cleanup-on-completion sweep: a run containing a stuck task is left
        untouched (fan-in tracking, sibling task records) so it can be manually resumed,
        instead of being swept away as if it had completed normally.
        """
        return {cls.FAILED, cls.STALLED}
