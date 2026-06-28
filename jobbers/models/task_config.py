import datetime as dt
import random
from collections.abc import Callable
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, Field, PlainSerializer, WithJsonSchema, field_validator

from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.models.task_status import TaskStatus

SerializableCallable = Annotated[
    Callable[..., Any],
    WithJsonSchema({"type": "string", "readOnly": True}),
    PlainSerializer(
        lambda f: f"{f.__module__}.{f.__qualname__}",
        return_type=str,
        when_used="json",
    ),
]


class BackoffStrategy(StrEnum):
    """Backoff strategy to use when retrying a task."""

    CONSTANT = "constant"  # Fixed delay each retry
    LINEAR = "linear"  # delay * attempt
    EXPONENTIAL = "exponential"  # delay * 2^attempt
    EXPONENTIAL_JITTER = "exponential_jitter"  # uniform(0, delay * 2^attempt)


class DeadLetterPolicy(StrEnum):
    """Dead letter policy to use when a task fails."""

    NONE = "none"  # Do not save failed tasks
    SAVE = "save"  # Save failed tasks for later inspection


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
    # True for tasks registered with a non-async function — executed in a dedicated subprocess
    # (see jobbers/sync_runner.py) rather than awaited in-process.
    is_sync: bool = Field(default=False)
    cleanup_on: frozenset[TaskStatus] | None = Field(default=None)
    max_heartbeat_interval: dt.timedelta | None = Field(
        default=None
    )  # Interval for sending heartbeats in seconds, if applicable

    # The actual function to execute for this task, used internally by the worker
    function: SerializableCallable

    # The tuple of expected exceptions that can be handled by the task processor
    expected_exceptions: tuple[type[Exception]] | None = Field(default=None)

    # Pre-computed dependency graph for DI — populated by @register_task at decoration time.
    # Stored as list[Any] at runtime (list[DependencyNode]); serialised to qualified names for JSON.
    dependency_graph: Annotated[
        list[Any],
        WithJsonSchema({"type": "array", "readOnly": True}),
        PlainSerializer(
            lambda nodes: [f"{n.provider.__module__}.{n.provider.__qualname__}" for n in nodes],
            return_type=list[str],
            when_used="json",
        ),
    ] = Field(default_factory=list)

    @field_validator("cleanup_on")
    @classmethod
    def _validate_cleanup_on(cls, v: frozenset[TaskStatus] | None) -> frozenset[TaskStatus] | None:
        if v is None:
            return v
        _non_terminal = TaskStatus.active_statuses() | {TaskStatus.UNSUBMITTED}
        invalid = v & _non_terminal
        if invalid:
            raise ValueError(f"cleanup_on may not contain non-terminal statuses: {invalid}")
        return v

    def compute_retry_at(self, attempt: int) -> dt.datetime:
        """Return the datetime at which the given retry attempt should run."""
        base = float(self.retry_delay or 0)
        match self.backoff_strategy:
            case BackoffStrategy.CONSTANT:
                delay = base
            case BackoffStrategy.LINEAR:
                delay = base * attempt
            case BackoffStrategy.EXPONENTIAL:
                delay = base * (2**attempt)
            case BackoffStrategy.EXPONENTIAL_JITTER:
                delay = random.uniform(0, base * (2**attempt))
        return dt.datetime.now(dt.UTC) + dt.timedelta(seconds=min(delay, self.max_retry_delay))
