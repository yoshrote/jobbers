import pytest
import pytest_asyncio

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
    """RedisDeadQueue backed by the shared real Redis connection + DummyTaskAdapter; for raw-adapter-specific edge cases."""
    return RedisDeadQueue(redis, dummy_task_adapter)


@pytest_asyncio.fixture(params=["redis", "redis_json", "sql"], ids=["redis", "redis_json", "sql"])
async def task_adapter(request, redis, session_factory):
    """
    Parameterized fixture yielding a (state, submit) pair for each backend.

    - ``"redis"``: (RedisTaskState, RedisTaskSubmit) backed by the shared real Redis connection
    - ``"redis_json"``: (RedisJSONTaskState, RedisJSONTaskSubmit) backed by the shared real Redis connection;
      the autouse ``redis`` fixture already skips the test if Redis Stack is unavailable
    - ``"sql"``: (SQLTaskState, SQLTaskSubmit) backed by in-memory SQLite
    """
    if request.param == "redis":
        state = RedisTaskState(redis)
        yield state, RedisTaskSubmit(redis, state)
    elif request.param == "sql":
        yield SQLTaskState(session_factory), SQLTaskSubmit(session_factory)
    else:
        state = RedisJSONTaskState(redis)
        yield state, RedisJSONTaskSubmit(redis, state)


@pytest_asyncio.fixture(params=["redis", "redis_json"], ids=["redis", "redis_json"])
async def redis_task_adapter(request, redis):
    """
    Parameterized fixture yielding a (state, submit) pair for each Redis backend.

    - ``"redis"``: (RedisTaskState, RedisTaskSubmit) backed by the shared real Redis connection
    - ``"redis_json"``: (RedisJSONTaskState, RedisJSONTaskSubmit) backed by the shared real Redis connection;
      the autouse ``redis`` fixture already skips the test if Redis Stack is unavailable

    Use this for Redis-atomic-pipeline tests that inspect data_store internals directly.
    """
    if request.param == "redis":
        state = RedisTaskState(redis)
        yield state, RedisTaskSubmit(redis, state)
    else:
        state = RedisJSONTaskState(redis)
        yield state, RedisJSONTaskSubmit(redis, state)


@pytest_asyncio.fixture
async def json_adapter(redis):
    """
    (RedisJSONTaskState, RedisJSONTaskSubmit) backed by the shared real Redis connection.

    The autouse ``redis`` fixture already skips the test if Redis Stack is unavailable.
    Use this for json-specific edge case tests that cannot be expressed as
    protocol contract tests (e.g., null JSON blob, missing RediSearch doc).
    """
    state = RedisJSONTaskState(redis)
    yield state, RedisJSONTaskSubmit(redis, state)


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
    - ``"redis"``: RedisQueueConfigAdapter backed by the shared real Redis connection
    - ``"redis_json"``: RedisJSONQueueConfigAdapter backed by the shared real Redis connection
    """
    if request.param == "sql":
        yield SQLQueueConfigAdapter(session_factory)
    elif request.param == "redis":
        yield RedisQueueConfigAdapter(redis)
    else:
        adapter = RedisJSONQueueConfigAdapter(redis)
        await adapter.ensure_indexes()
        yield adapter


@pytest_asyncio.fixture(params=["sql", "redis", "redis_json"], ids=["sql", "redis", "redis_json"])
async def task_routing_config_adapter(request, session_factory, redis):
    """
    Parameterized fixture yielding a TaskRoutingConfigProtocol implementation for each backend.

    - ``"sql"``: SQLTaskRoutingConfigAdapter backed by in-memory SQLite
    - ``"redis"``: RedisTaskRoutingConfigAdapter backed by the shared real Redis connection
    - ``"redis_json"``: RedisJSONTaskRoutingConfigAdapter backed by the shared real Redis connection
    """
    if request.param == "sql":
        yield SQLTaskRoutingConfigAdapter(session_factory)
    elif request.param == "redis":
        yield RedisTaskRoutingConfigAdapter(redis)
    else:
        yield RedisJSONTaskRoutingConfigAdapter(redis)


@pytest_asyncio.fixture(params=["redis", "redis_json", "sql"], ids=["redis", "redis_json", "sql"])
async def dead_queue(request, redis, dummy_task_adapter, session_factory):
    """
    Parameterized fixture yielding (dq, task_adapter) for each DeadQueueProtocol implementation.

    - ``"redis"``: RedisDeadQueue backed by the shared real Redis connection + DummyTaskAdapter
    - ``"redis_json"``: RedisJSONDeadQueue backed by the shared real Redis connection
    - ``"sql"``: SQLDeadQueue backed by in-memory SQLite + SQLTaskState

    SQLDeadQueue stores full task data in the dead_letter_queue table, so the
    task_adapter returned for ``"sql"`` is a SQLTaskState — ``save_task`` calls
    write to the tasks table but are irrelevant to the DLQ's own storage.
    """
    if request.param == "redis":
        dq = RedisDeadQueue(redis, dummy_task_adapter)
        yield dq, dummy_task_adapter
    elif request.param == "sql":
        yield SQLDeadQueue(session_factory), SQLTaskState(session_factory)
    else:
        state = RedisJSONTaskState(redis)
        jdq = RedisJSONDeadQueue(redis, state)
        await jdq.ensure_index()
        yield jdq, state
