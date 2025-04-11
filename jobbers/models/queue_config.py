from enum import StrEnum
from typing import Optional

from pydantic import BaseModel

from jobbers.serialization import NONE, deserialize


class RatePeriod(StrEnum):
    """Enumeration of rate limiting periods."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"

    @classmethod
    def from_bytes(cls, value: bytes) -> Optional["RatePeriod"]:
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
    max_concurrent: Optional[int] = 10  # Maximum number of concurrent tasks that can be processed from this queue
    # max_tasks_per_worker: Optional[int] = None  # Maximum number of tasks a worker can process before shutting down
    # task_timeout: Optional[int] = None  # Timeout for tasks in this queue in seconds, if applicable
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying failed tasks in this queue
    # backoff_strategy: str = "exponential"  # Backoff strategy for retries (e.g., "exponential", "linear")
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying the task
    # Rate limiting is {task_number} tasks every {rate_number} {rate_period}
    #  e.g. 5 tasks every 2 minutea
    rate_numerator: Optional[int] = None  # Number of tasks to process from this queue
    rate_denominator: Optional[int] = None  # Number of tasks to rate limit
    rate_period: Optional[RatePeriod] = None  # Period for rate limiting in seconds

    @classmethod
    def from_redis(cls, name: str, raw_task_data: dict) -> "QueueConfig":
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
