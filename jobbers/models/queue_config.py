from __future__ import annotations

import re
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field, field_validator

# A queue name must be addressable from a mermaid edge label, so it shares the
# identifier rule used for task and router names in jobbers/utils/mermaid_dag.py.
# Notably this excludes hyphens: use `priority_shard_a`, not `priority-shard-a`.
# The pattern is also applied to the `queue_name` path parameter of PUT /queues/{queue_name},
# which assigns to `QueueConfig.name` directly and so bypasses this field validator.
QUEUE_NAME_PATTERN = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
QUEUE_NAME_RE = re.compile(QUEUE_NAME_PATTERN)


class RatePeriod(StrEnum):
    """Enumeration of rate limiting periods."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


class QueueConfig(BaseModel):
    """Configuration for a task queue."""

    # json_schema_extra rather than Field(pattern=...): the pattern belongs in openapi.json
    # for the frontend, but a real pattern constraint would run first and shadow
    # _validate_name's message, which is the part that teaches the hyphen fix.
    name: str = Field(json_schema_extra={"pattern": QUEUE_NAME_PATTERN})  # Name of the queue
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

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        if not QUEUE_NAME_RE.match(v):
            raise ValueError(
                f"Invalid queue name {v!r}: must match [a-zA-Z_][a-zA-Z0-9_]* so the queue "
                "can be named by a mermaid edge label. Hyphens are not allowed -- use underscores."
            )
        return v

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
        """Construct from a row (name, max_concurrent, rate_numerator, rate_denominator, rate_period)."""
        name, max_concurrent, rate_numerator, rate_denominator, rate_period = row
        return cls(
            name=name,
            max_concurrent=max_concurrent,
            rate_numerator=rate_numerator,
            rate_denominator=rate_denominator,
            rate_period=RatePeriod(rate_period) if rate_period else None,
        )
