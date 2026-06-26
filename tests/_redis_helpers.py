"""Shared real-Redis connection helper for test fixtures."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError

from jobbers.db import DEFAULT_REDIS_URL, REDIS_PROTOCOL_VERSION


@asynccontextmanager
async def real_redis_connection() -> AsyncIterator[aioredis.Redis]:
    """
    Real (non-fake) Redis connection for tests that need actual Redis/Redis-Stack behavior.

    Skips the current test via ``pytest.skip`` if Redis is unreachable. Flushes db 0
    on entry and exit, and always closes the connection on exit.
    """
    r = aioredis.from_url(DEFAULT_REDIS_URL, db=0, protocol=REDIS_PROTOCOL_VERSION)
    try:
        await r.flushdb()
    except RedisConnectionError as exc:  # pragma: no cover
        await r.aclose()
        pytest.skip(f"Redis not available: {exc}")
    try:
        yield r
    finally:
        await r.flushdb()
        await r.aclose()
