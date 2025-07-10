from enum import StrEnum
from typing import Self

from pydantic import BaseModel

from jobbers.utils.serialization import NONE, deserialize


class RatePeriod(StrEnum):
    """Enumeration of rate limiting periods."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"

    @classmethod
    def from_bytes(cls, value: bytes | None) -> Self | None:
        if value is None:
            return None
        period_str = value.decode()
        try:
            return cls(period_str)
        except ValueError:
            # TODO: Swallowing bad data may not be the best approach
            return None

    def to_bytes(self) -> bytes:
        return self.value.encode()

class QueueConfig(BaseModel):
    """Configuration for a task queue."""

    name: str  # Name of the queue
    max_concurrent: int | None = 10  # Maximum number of concurrent tasks that can be processed from this queue
    # max_tasks_per_worker: int | None = None  # Maximum number of tasks a worker can process before shutting down
    # task_timeout: int | None = None  # Timeout for tasks in this queue in seconds, if applicable
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying failed tasks in this queue
    # backoff_strategy: str = "exponential"  # Backoff strategy for retries (e.g., "exponential", "linear")
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying the task
    # Rate limiting is {task_number} tasks every {rate_number} {rate_period}
    #  e.g. 5 tasks every 2 minutea
    rate_numerator: int | None = None  # Number of tasks to process from this queue
    rate_denominator: int | None = None  # Number of tasks to rate limit
    rate_period: RatePeriod | None = None  # Period for rate limiting in seconds

    def period_in_seconds(self) -> int | None:
        """Convert the rate period to seconds."""
        if self.rate_period is None or self.rate_denominator is None:
            return None
        match self.rate_period:
            case RatePeriod.SECOND:
                return 1 * self.rate_denominator
            case RatePeriod.MINUTE:
                return 60 * self.rate_denominator
            case RatePeriod.HOUR:
                return 3600 * self.rate_denominator
            case RatePeriod.DAY:
                return 86400 * self.rate_denominator

    @classmethod
    def from_redis(cls, name: str, raw_task_data: dict[bytes, bytes]) -> Self:
        return cls(
            name=name,
            max_concurrent=int(raw_task_data.get(b"max_concurrent") or b"10"),
            rate_numerator=deserialize(raw_task_data.get(b"rate_numerator") or NONE),
            rate_denominator=deserialize(raw_task_data.get(b"rate_denominator") or NONE),
            rate_period=RatePeriod.from_bytes(raw_task_data.get(b"rate_period")),
            # max_tasks_per_worker=int(raw_task_data.get(b"max_tasks_per_worker", b"0")),
            # task_timeout=int(raw_task_data.get(b"task_timeout", b"0")),
            # retry_delay=dt.timedelta(seconds=int(raw_task_data.get(b"retry_delay", b"5"))),
            # backoff_strategy=raw_task_data.get(b"backoff_strategy", b"exponential").decode(),
        )
