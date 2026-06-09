import fakeredis
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from jobbers.adapters.redis import (
    RedisDeadQueue,
    RedisQueueConfigAdapter,
    RedisTaskRoutingConfigAdapter,
    RedisTaskState,
    RedisTaskSubmit,
)
from jobbers.adapters.redis_json import (
    RedisJSONDeadQueue,
    RedisJSONQueueConfigAdapter,
    RedisJSONTaskRoutingConfigAdapter,
    RedisJSONTaskState,
    RedisJSONTaskSubmit,
)
from jobbers.adapters.sql import (
    SQLDeadQueue,
    SQLQueueConfigAdapter,
    SQLTaskRoutingConfigAdapter,
    SQLTaskState,
    SQLTaskSubmit,
)
from jobbers.db import DEFAULT_REDIS_URL


@pytest.fixture
def task_adapter_dt_module(task_adapter: object) -> str:
    """Return the dotted module path for patching 'dt' in SharedTaskAdapterMixin."""
    return "jobbers.adapters._shared.dt"


@pytest.fixture
def redis_task_adapter_dt_module(redis_task_adapter: object) -> str:
    """Return the dt patch target for use with redis_task_adapter tests."""
    return "jobbers.adapters._shared.dt"


@pytest.fixture
def msgpack_adapter(redis) -> tuple[RedisTaskState, RedisTaskSubmit]:
    """Fixture providing a (RedisTaskState, RedisTaskSubmit) pair."""
    state = RedisTaskState(redis)
    return state, RedisTaskSubmit(redis, state)


@pytest.fixture
def msgpack_dead_queue(redis, dummy_task_adapter) -> RedisDeadQueue:
    """RedisDeadQueue backed by FakeAsyncRedis + DummyTaskAdapter; for raw-adapter-specific edge cases."""
    return RedisDeadQueue(redis, dummy_task_adapter)


@pytest_asyncio.fixture(params=["redis", "redis_json", "sql"], ids=["redis", "redis_json", "sql"])
async def task_adapter(request, redis, session_factory):
    """
    Parameterized fixture yielding a (state, submit) pair for each backend.

    - ``"redis"``: (RedisTaskState, RedisTaskSubmit) backed by FakeAsyncRedis
    - ``"redis_json"``: (RedisJSONTaskState, RedisJSONTaskSubmit) backed by real Redis Stack; skips if unavailable
    - ``"sql"``: (SQLTaskState, SQLTaskSubmit) backed by in-memory SQLite

    Each variant manages its own connection inline. request.getfixturevalue cannot
    lazily resolve async fixtures (json_adapter, real_redis) in pytest-asyncio strict
    mode — it tries to create a new Runner inside an already-running event loop.
    """
    if request.param == "redis":
        state = RedisTaskState(redis)
        yield state, RedisTaskSubmit(redis, state)
    elif request.param == "sql":
        yield SQLTaskState(session_factory), SQLTaskSubmit(session_factory)
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0, protocol=2)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        state = RedisJSONTaskState(r)
        try:
            await state.ensure_index()
        except ResponseError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield state, RedisJSONTaskSubmit(r, state)
        await r.flushdb()
        await r.aclose()


@pytest_asyncio.fixture(params=["redis", "redis_json"], ids=["redis", "redis_json"])
async def redis_task_adapter(request, redis):
    """
    Parameterized fixture yielding a (state, submit) pair for each Redis backend.

    - ``"redis"``: (RedisTaskState, RedisTaskSubmit) backed by FakeAsyncRedis
    - ``"redis_json"``: (RedisJSONTaskState, RedisJSONTaskSubmit) backed by real Redis Stack; skips if unavailable

    Use this for Redis-atomic-pipeline tests that inspect data_store internals directly.
    """
    if request.param == "redis":
        state = RedisTaskState(redis)
        yield state, RedisTaskSubmit(redis, state)
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0, protocol=2)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        state = RedisJSONTaskState(r)
        try:
            await state.ensure_index()
        except ResponseError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield state, RedisJSONTaskSubmit(r, state)
        await r.flushdb()
        await r.aclose()


@pytest_asyncio.fixture
async def json_adapter():
    """
    (RedisJSONTaskState, RedisJSONTaskSubmit) backed by real Redis Stack; skips if unavailable.

    Use this for json-specific edge case tests that cannot be expressed as
    protocol contract tests (e.g., null JSON blob, missing RediSearch doc).
    """
    r = aioredis.from_url(DEFAULT_REDIS_URL, db=0, protocol=2)
    try:
        await r.flushdb()
    except RedisConnectionError as exc:  # pragma: no cover
        await r.aclose()
        pytest.skip(f"Redis not available: {exc}")
    state = RedisJSONTaskState(r)
    try:
        await state.ensure_index()
    except ResponseError as exc:  # pragma: no cover
        await r.aclose()
        pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
    yield state, RedisJSONTaskSubmit(r, state)
    await r.flushdb()
    await r.aclose()


@pytest_asyncio.fixture
async def json_dead_queue(json_adapter):
    """
    RedisJSONDeadQueue backed by real Redis Stack; skips if unavailable (via json_adapter).

    Use this for RedisJSONDeadQueue-specific edge case tests.
    """
    state, _ = json_adapter
    dq = RedisJSONDeadQueue(state.data_store, state)
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
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0, protocol=2)
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
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0, protocol=2)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        adapter = RedisJSONTaskRoutingConfigAdapter(r)
        yield adapter
        await r.flushdb()
        await r.aclose()


@pytest_asyncio.fixture(params=["redis", "redis_json", "sql"], ids=["redis", "redis_json", "sql"])
async def dead_queue(request, dummy_task_adapter, session_factory):
    """
    Parameterized fixture yielding (dq, task_adapter) for each DeadQueueProtocol implementation.

    - ``"redis"``: RedisDeadQueue backed by FakeAsyncRedis + DummyTaskAdapter
    - ``"redis_json"``: RedisJSONDeadQueue backed by real Redis Stack; skips if unavailable
    - ``"sql"``: SQLDeadQueue backed by in-memory SQLite + SQLTaskState

    SQLDeadQueue stores full task data in the dead_letter_queue table, so the
    task_adapter returned for ``"sql"`` is a SQLTaskState — ``save_task`` calls
    write to the tasks table but are irrelevant to the DLQ's own storage.

    Each variant manages its own connection inline for the same reason as task_adapter.
    """
    if request.param == "redis":
        r = fakeredis.FakeAsyncRedis()
        dq = RedisDeadQueue(r, dummy_task_adapter)
        yield dq, dummy_task_adapter
        await r.aclose()
    elif request.param == "sql":
        yield SQLDeadQueue(session_factory), SQLTaskState(session_factory)
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0, protocol=2)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        state = RedisJSONTaskState(r)
        jdq = RedisJSONDeadQueue(r, state)
        try:
            await state.ensure_index()
            await jdq.ensure_index()
        except ResponseError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield jdq, state
        await r.flushdb()
        await r.aclose()
