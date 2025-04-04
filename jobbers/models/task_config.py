import datetime as dt
from typing import Callable, Optional

from pydantic import BaseModel


class TaskConfig(BaseModel):
    """Configuration for a task."""

    name: str  # Name of the task
    version: int = 0  # Version of the task, used for versioning task definitions
    max_concurrent: Optional[int] = 1  # Maximum number of concurrent executions of this task
    timeout: Optional[int] = None  # Timeout for the task in seconds, if applicable
    max_retries: int = 3  # Maximum number of retries for the task
    retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying the task

    @property
    def task_function(self) -> Callable:
        """Get the task function name."""
        from jobbers.registry import get_task_function
        return get_task_function(self.name, self.version)
