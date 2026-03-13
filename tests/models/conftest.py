import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from jobbers.adapters.json_redis import JsonTaskAdapter
from jobbers.adapters.raw_redis import MsgpackTaskAdapter
from jobbers.db import DEFAULT_REDIS_URL


@pytest.fixture
def task_adapter_dt_module(task_adapter) -> str:
    """Return the dotted module path for patching 'dt' in the active task adapter."""
    return f"{type(task_adapter).__module__}.dt"


@pytest_asyncio.fixture(params=[JsonTaskAdapter, MsgpackTaskAdapter], ids=["json", "msgpack"])
async def real_task_adapter(request):
    """Both adapter implementations on real Redis; skips if Redis/RediSearch unavailable."""
    client = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
    try:
        await client.flushdb()
    except RedisConnectionError as exc:  # pragma: no cover
        await client.aclose()
        pytest.skip(f"Redis not available: {exc}")
    adapter = request.param(client)
    try:
        await adapter.ensure_index()
    except ResponseError as exc:  # pragma: no cover
        await client.aclose()
        pytest.skip(f"RediSearch not available: {exc}")
    yield adapter
    await client.flushdb()
    await client.aclose()
