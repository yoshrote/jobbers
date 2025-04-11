from typing import Optional

from pydantic import BaseModel


class QueueConfig(BaseModel):
    """Configuration for a task queue."""

    name: str  # Name of the queue
    max_concurrent: Optional[int] = 10  # Maximum number of concurrent tasks that can be processed from this queue
    # max_tasks_per_worker: Optional[int] = None  # Maximum number of tasks a worker can process before shutting down
    # task_timeout: Optional[int] = None  # Timeout for tasks in this queue in seconds, if applicable
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying failed tasks in this queue
    # backoff_strategy: str = "exponential"  # Backoff strategy for retries (e.g., "exponential", "linear")
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying the task

    @classmethod
    def from_redis(cls, name: str, raw_task_data: dict) -> "QueueConfig":
        return cls(
            name=name,
            max_concurrent=int(raw_task_data.get(b"max_concurrent", b"10")),
            # max_tasks_per_worker=int(raw_task_data.get(b"max_tasks_per_worker", b"0")),
            # task_timeout=int(raw_task_data.get(b"task_timeout", b"0")),
            # retry_delay=dt.timedelta(seconds=int(raw_task_data.get(b"retry_delay", b"5"))),
            # backoff_strategy=raw_task_data.get(b"backoff_strategy", b"exponential").decode(),
        )
