from asyncio import sleep
from typing import Any

from jobbers.models.task_config import DeadLetterPolicy
from jobbers.registry import register_task


@register_task(
    name="fast_task",
    version=1,
    max_concurrent=None,
    timeout=None,
    max_retries=3,
    retry_delay=None,
    expected_exceptions=None
)
async def fast_task() -> dict[Any, Any]:
    return {}

@register_task(
    name="medium_task",
    version=1,
    max_concurrent=None,
    timeout=None,
    max_retries=3,
    retry_delay=None,
    expected_exceptions=None
)
async def medium_task() -> dict[Any, Any]:
    await sleep(30)
    return {}

@register_task(
    name="slow_task",
    version=1,
    max_concurrent=None,
    timeout=None,
    max_retries=3,
    retry_delay=None,
    expected_exceptions=None
)
async def slow_task() -> dict[Any, Any]:
    await sleep(60)
    return {}

@register_task(
    name="fail_task",
    version=1,
    max_concurrent=None,
    timeout=None,
    max_retries=3,
    retry_delay=10,
    expected_exceptions=None,
    dead_letter_policy=DeadLetterPolicy.SAVE
)
async def fail_task() -> dict[Any, Any]:
    await sleep(30)
    raise Exception("Task failed")