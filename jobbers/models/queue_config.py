from pydantic import BaseModel


class QueueConfig(BaseModel):
    """Configuration for a task queue."""

    name: str  # Name of the queue
    # max_concurrent: Optional[int] = 10  # Maximum number of concurrent tasks that can be processed from this queue
    # max_tasks_per_worker: Optional[int] = None  # Maximum number of tasks a worker can process before shutting down
    # task_timeout: Optional[int] = None  # Timeout for tasks in this queue in seconds, if applicable
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying failed tasks in this queue
    # backoff_strategy: str = "exponential"  # Backoff strategy for retries (e.g., "exponential", "linear")
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying the task

