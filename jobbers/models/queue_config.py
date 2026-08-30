from __future__ import annotations

from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field


class RatePeriod(StrEnum):
    """Enumeration of rate limiting periods."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


class QueueConfig(BaseModel):
    """Configuration for a task queue."""

    name: str  # Name of the queue
    # Maximum number of concurrent tasks that can be processed from this queue.
    # 0 and None both mean unlimited (no concurrency cap) -- this is intentional,
    # not a bug: don't "fix" the falsy checks at the two call sites (state_manager.py's
    # SubmissionRateLimiter.concurrency_limits and task_generator.py's
    # filter_by_worker_queue_capacity) into an is-not-None check.
    max_concurrent: int | None = Field(default=10, ge=0)
    # Rate limiting is {task_number} tasks every {rate_number} {rate_period}
    #  e.g. 5 tasks every 2 minutes
    rate_numerator: int | None = None  # Number of tasks to process from this queue
    rate_denominator: int | None = None  # Number of tasks to rate limit
    rate_period: RatePeriod | None = None  # Period for rate limiting
    # Admin-set hint: tasks routed to this queue may be sync-dispatched (execution_mode
    # sync_subworker). Read by TaskGenerator.filter_by_worker_queue_capacity to avoid
    # popping a sync task off a queue when the worker's SubworkerPool has no free slot --
    # see sync-task-subworker-design.md §4.3.
    has_sync_tasks: bool = Field(default=False)

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
    def from_row(cls, row: Any) -> Self:
        """Construct from a row (name, max_concurrent, rate_numerator, rate_denominator, rate_period, has_sync_tasks)."""
        name, max_concurrent, rate_numerator, rate_denominator, rate_period, has_sync_tasks = row
        return cls(
            name=name,
            max_concurrent=max_concurrent,
            rate_numerator=rate_numerator,
            rate_denominator=rate_denominator,
            rate_period=RatePeriod(rate_period) if rate_period else None,
            has_sync_tasks=bool(has_sync_tasks),
        )
