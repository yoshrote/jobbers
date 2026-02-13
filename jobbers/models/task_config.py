import datetime as dt
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, Field

from jobbers.models.task_shutdown_policy import TaskShutdownPolicy


class TaskConfig(BaseModel):
    """Configuration for a task."""

    name: str  # Name of the task
    version: int = 0  # Version of the task, used for versioning task definitions
    max_concurrent: int | None = Field(default=1)  # Maximum number of concurrent executions of this task
    timeout: int | None = Field(default=None)  # Timeout for the task in seconds, if applicable
    max_retries: int = Field(default=3)  # Maximum number of retries for the task
    retry_delay: int | None = Field(default=None)  # Delay before retrying the task in seconds
    on_shutdown: TaskShutdownPolicy = Field(default=TaskShutdownPolicy.STOP)
    max_heartbeat_interval: dt.timedelta | None = Field(default=None)  # Interval for sending heartbeats in seconds, if applicable

    # The actual function to execute for this task, used internally by the worker
    function: Callable[..., Awaitable[Any]] = Field(exclude=True)

    # The tuple of expected exceptions that can be handled by the task processor
    expected_exceptions: tuple[type[Exception]] | None = Field(default=None)
