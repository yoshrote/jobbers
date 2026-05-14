import fakeredis
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from jobbers.adapters.redis import (
    DeadQueue,
    MsgpackTaskAdapter,
    RedisQueueConfigAdapter,
    RedisTaskRoutingConfigAdapter,
)
from jobbers.adapters.redis_json import (
    JsonDeadQueue,
    JsonTaskAdapter,
    RedisJSONQueueConfigAdapter,
    RedisJSONTaskRoutingConfigAdapter,
)
from jobbers.adapters.sql import SQLQueueConfigAdapter, SQLTaskRoutingConfigAdapter
from jobbers.db import DEFAULT_REDIS_URL


@pytest.fixture
def task_adapter_dt_module(task_adapter: object) -> str:
    """Return the dotted module path for patching 'dt' in SharedTaskAdapterMixin."""
    return "jobbers.adapters._shared.dt"


@pytest.fixture
def msgpack_adapter(redis) -> MsgpackTaskAdapter:
    """Fixture providing a MsgpackTaskAdapter instance."""
    return MsgpackTaskAdapter(redis)


@pytest.fixture
def msgpack_dead_queue(redis, dummy_task_adapter) -> DeadQueue:
    """DeadQueue backed by FakeAsyncRedis + DummyTaskAdapter; for raw-adapter-specific edge cases."""
    return DeadQueue(redis, dummy_task_adapter)


@pytest_asyncio.fixture(params=["raw", "json"], ids=["raw", "json"])
async def task_adapter(request, redis):
    """
    Parameterized fixture yielding a TaskAdapterProtocol implementation for each backend.

    - ``"raw"``: MsgpackTaskAdapter backed by FakeAsyncRedis
    - ``"json"``: JsonTaskAdapter backed by real Redis Stack; skips if unavailable

    Each variant manages its own connection inline. request.getfixturevalue cannot
    lazily resolve async fixtures (json_adapter, real_redis) in pytest-asyncio strict
    mode — it tries to create a new Runner inside an already-running event loop.
    """
    if request.param == "raw":
        yield MsgpackTaskAdapter(redis)
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        adapter = JsonTaskAdapter(r)
        try:
            await adapter.ensure_index()
        except ResponseError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield adapter
        await r.flushdb()
        await r.aclose()


@pytest_asyncio.fixture
async def json_adapter():
    """
    JsonTaskAdapter backed by real Redis Stack; skips if unavailable.

    Use this for json-specific edge case tests that cannot be expressed as
    protocol contract tests (e.g., null JSON blob, missing RediSearch doc).
    """
    r = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
    try:
        await r.flushdb()
    except RedisConnectionError as exc:  # pragma: no cover
        await r.aclose()
        pytest.skip(f"Redis not available: {exc}")
    adapter = JsonTaskAdapter(r)
    try:
        await adapter.ensure_index()
    except ResponseError as exc:  # pragma: no cover
        await r.aclose()
        pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
    yield adapter
    await r.flushdb()
    await r.aclose()


@pytest_asyncio.fixture
async def json_dead_queue(json_adapter):
    """
    JsonDeadQueue backed by real Redis Stack; skips if unavailable (via json_adapter).

    Use this for JsonDeadQueue-specific edge case tests.
    """
    dq = JsonDeadQueue(json_adapter.data_store, json_adapter)
    await dq.ensure_index()
    yield dq


@pytest_asyncio.fixture(params=["sql", "redis", "redis_json"], ids=["sql", "redis", "redis_json"])
async def queue_config_adapter(request, session_factory, redis):
    """
    Parameterized fixture yielding a QueueConfigProtocol implementation for each backend.

    - ``"sql"``: SQLQueueConfigAdapter backed by in-memory SQLite
    - ``"redis"``: RedisQueueConfigAdapter backed by FakeAsyncRedis
    - ``"redis_json"``: RedisJSONQueueConfigAdapter backed by real Redis Stack; skips if unavailable
    """
    if request.param == "sql":
        yield SQLQueueConfigAdapter(session_factory)
    elif request.param == "redis":
        yield RedisQueueConfigAdapter(redis)
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        adapter = RedisJSONQueueConfigAdapter(r)
        try:
            await adapter.ensure_indexes()
        except ResponseError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield adapter
        await r.flushdb()
        await r.aclose()


@pytest_asyncio.fixture(params=["sql", "redis", "redis_json"], ids=["sql", "redis", "redis_json"])
async def task_routing_config_adapter(request, session_factory, redis):
    """
    Parameterized fixture yielding a TaskRoutingConfigProtocol implementation for each backend.

    - ``"sql"``: SQLTaskRoutingConfigAdapter backed by in-memory SQLite
    - ``"redis"``: RedisTaskRoutingConfigAdapter backed by FakeAsyncRedis
    - ``"redis_json"``: RedisJSONTaskRoutingConfigAdapter backed by real Redis Stack; skips if unavailable
    """
    if request.param == "sql":
        yield SQLTaskRoutingConfigAdapter(session_factory)
    elif request.param == "redis":
        yield RedisTaskRoutingConfigAdapter(redis)
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        adapter = RedisJSONTaskRoutingConfigAdapter(r)
        yield adapter
        await r.flushdb()
        await r.aclose()


@pytest_asyncio.fixture(params=["raw", "json"], ids=["raw", "json"])
async def dead_queue(request, dummy_task_adapter):
    """
    Parameterized fixture yielding (dq, task_adapter) for each DeadQueueProtocol implementation.

    - ``"raw"``: DeadQueue backed by FakeAsyncRedis + DummyTaskAdapter
    - ``"json"``: JsonDeadQueue backed by real Redis Stack; skips if unavailable

    Each variant manages its own connection inline for the same reason as task_adapter.
    """
    if request.param == "raw":
        r = fakeredis.FakeAsyncRedis()
        dq = DeadQueue(r, dummy_task_adapter)
        yield dq, dummy_task_adapter
        await r.aclose()
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        adapter = JsonTaskAdapter(r)
        jdq = JsonDeadQueue(r, adapter)
        try:
            await adapter.ensure_index()
            await jdq.ensure_index()
        except ResponseError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield jdq, adapter
        await r.flushdb()
        await r.aclose()
