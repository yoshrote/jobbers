import asyncio
from typing import Any

from jobbers.models.dag import TaskResult
from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy
from jobbers.registry import register_task


class CustomException(Exception):
    """Custom exception for testing expected exceptions in task processing."""

    pass


@register_task(
    name="echo_task",
    version=1,
    max_concurrent=None,
    max_retries=0,
)
async def echo_task(value: str = "default") -> TaskResult:
    return TaskResult(results={"value": value})


@register_task(
    name="always_fail_task",
    version=1,
    max_concurrent=None,
    max_retries=2,
    retry_delay=None,
    expected_exceptions=(CustomException,),
    dead_letter_policy=DeadLetterPolicy.SAVE,
)
async def always_fail_task() -> dict[Any, Any]:
    raise CustomException("always fails")


@register_task(
    name="scheduled_fail_task",
    version=1,
    max_concurrent=None,
    max_retries=2,
    retry_delay=0,
    backoff_strategy=BackoffStrategy.CONSTANT,
    expected_exceptions=(CustomException,),
    dead_letter_policy=DeadLetterPolicy.SAVE,
)
async def scheduled_fail_task() -> dict[Any, Any]:
    raise CustomException("scheduled fail")


@register_task(
    name="slow_task",
    version=1,
    max_concurrent=None,
    max_retries=0,
)
async def slow_task() -> dict[Any, Any]:
    await asyncio.sleep(30)
    return {}
