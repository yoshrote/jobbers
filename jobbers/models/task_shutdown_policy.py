from enum import StrEnum
from typing import Self


class TaskShutdownPolicy(StrEnum):
    """Possible ways to handle tasks when the worker is being shut down."""

    CONTINUE = "continue"
    STOP = "stop"
    RESUBMIT = "resubmit"

    @classmethod
    def from_bytes(cls, raw_status: bytes | None) -> Self:
        if not raw_status:
            return cls.STOP
        return cls(raw_status.decode())

    def to_bytes(self) -> bytes:
        return self.value.encode()
