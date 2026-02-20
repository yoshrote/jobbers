import datetime as dt
import random
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from jobbers.models.task_shutdown_policy import TaskShutdownPolicy


class BackoffStrategy(StrEnum):
    """Backoff strategy to use when retrying a task."""

    CONSTANT = "constant"             # Fixed delay each retry
    LINEAR = "linear"                 # delay * attempt
    EXPONENTIAL = "exponential"       # delay * 2^attempt
    EXPONENTIAL_JITTER = "exponential_jitter"  # uniform(0, delay * 2^attempt)


class DeadLetterPolicy(StrEnum):
    """Dead letter policy to use when a task fails."""

    NONE = "none"                     # Do not save failed tasks
    SAVE = "save"                     # Save failed tasks for later inspection


class TaskConfig(BaseModel):
    """Configuration for a task."""

    name: str  # Name of the task
    version: int = 0  # Version of the task, used for versioning task definitions
    max_concurrent: int | None = Field(default=1)  # Maximum number of concurrent executions of this task
    timeout: int | None = Field(default=None)  # Timeout for the task in seconds, if applicable
    max_retries: int = Field(default=3)  # Maximum number of retries for the task
    retry_delay: int | None = Field(default=None)  # Base delay before retrying the task in seconds
    backoff_strategy: BackoffStrategy = Field(default=BackoffStrategy.EXPONENTIAL)
    max_retry_delay: int = Field(default=3600)  # Upper bound on computed retry delay in seconds
    on_shutdown: TaskShutdownPolicy = Field(default=TaskShutdownPolicy.STOP)
    dead_letter_policy: DeadLetterPolicy = Field(default=DeadLetterPolicy.NONE)
    max_heartbeat_interval: dt.timedelta | None = Field(default=None)  # Interval for sending heartbeats in seconds, if applicable

    # The actual function to execute for this task, used internally by the worker
    function: Callable[..., Awaitable[Any]] = Field(exclude=True)

    # The tuple of expected exceptions that can be handled by the task processor
    expected_exceptions: tuple[type[Exception]] | None = Field(default=None)

    def compute_retry_at(self, attempt: int) -> dt.datetime:
        """Return the datetime at which the given retry attempt should run."""
        base = float(self.retry_delay or 0)
        match self.backoff_strategy:
            case BackoffStrategy.CONSTANT:
                delay = base
            case BackoffStrategy.LINEAR:
                delay = base * attempt
            case BackoffStrategy.EXPONENTIAL:
                delay = base * (2 ** attempt)
            case BackoffStrategy.EXPONENTIAL_JITTER:
                delay = random.uniform(0, base * (2 ** attempt))
        return dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=min(delay, self.max_retry_delay))
