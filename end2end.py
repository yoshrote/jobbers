from asyncio import sleep
from typing import Any

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
    await sleep(1)
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
    await sleep(10)
    return {}
