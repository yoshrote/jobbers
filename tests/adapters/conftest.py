import aiosqlite
import fakeredis
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from jobbers.adapters.json_redis import JsonDeadQueue, JsonTaskAdapter
from jobbers.adapters.raw_redis import DeadQueue, MsgpackTaskAdapter
from jobbers.adapters.sql import SqlDeadQueue, SqlTaskAdapter
from jobbers.db import DEFAULT_REDIS_URL


@pytest.fixture
def msgpack_adapter(redis) -> MsgpackTaskAdapter:
    """Fixture providing a MsgpackTaskAdapter instance."""
    return MsgpackTaskAdapter(redis)


@pytest_asyncio.fixture(params=["raw", "json", "sql"], ids=["raw", "json", "sql"])
async def dead_queue(request, dummy_task_adapter):
    """
    Parameterized fixture yielding (dq, task_adapter) for each DeadQueueProtocol implementation.

    - ``"raw"``: DeadQueue backed by FakeAsyncRedis + DummyTaskAdapter
    - ``"json"``: JsonDeadQueue backed by real Redis Stack; skips if unavailable
    - ``"sql"``: SqlDeadQueue backed by in-memory SQLite
    """
    if request.param == "raw":
        r = fakeredis.FakeAsyncRedis()
        dq = DeadQueue(r, dummy_task_adapter)
        yield dq, dummy_task_adapter
        await r.aclose()
    elif request.param == "sql":
        async with aiosqlite.connect(":memory:") as c:
            c.row_factory = aiosqlite.Row
            adapter = SqlTaskAdapter(c)
            await adapter.ensure_index()
            dq = SqlDeadQueue(c, adapter)
            yield dq, adapter
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
        try:
            await r.flushdb()
        except RedisConnectionError as exc: # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        adapter = JsonTaskAdapter(r)
        jdq = JsonDeadQueue(r, adapter)
        try:
            await adapter.ensure_index()
            await jdq.ensure_index()
        except ResponseError as exc: # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield jdq, adapter
        await r.flushdb()
        await r.aclose()
