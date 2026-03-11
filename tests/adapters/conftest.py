import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.adapters.json_redis import JsonTaskAdapter
from jobbers.adapters.raw_redis import MsgpackTaskAdapter
from jobbers.adapters.task_adapter import _BaseTaskAdapter
from jobbers.models.task import Task


@pytest_asyncio.fixture(autouse=True)
async def redis():
    """Fixture to reset the tasks in the mocked Redis before each test."""
    fake_store = fakeredis.FakeRedis()
    yield fake_store
    await fake_store.close()


@pytest.fixture(params=[JsonTaskAdapter, MsgpackTaskAdapter], ids=["json", "msgpack"])
def task_adapter(redis, request):
    """Fixture providing a task adapter instance parametrized over all implementations."""
    return request.param(redis)


class DummyTaskAdapter:
    """
    Minimal in-memory TaskAdapterProtocol for use as a test dependency.

    Only ``save_task``, ``get_task``, and ``get_tasks_bulk`` are implemented.
    Any other protocol method raises ``NotImplementedError`` immediately,
    so accidental use is caught at the call site.
    """

    TASKS_BY_QUEUE = _BaseTaskAdapter.TASKS_BY_QUEUE
    TASK_DETAILS = _BaseTaskAdapter.TASK_DETAILS
    HEARTBEAT_SCORES = _BaseTaskAdapter.HEARTBEAT_SCORES
    TASK_BY_TYPE_IDX = _BaseTaskAdapter.TASK_BY_TYPE_IDX
    QUEUE_RATE_LIMITER = _BaseTaskAdapter.QUEUE_RATE_LIMITER
    DLQ_MISSING_DATA = _BaseTaskAdapter.DLQ_MISSING_DATA

    def __init__(self) -> None:
        self._store: dict[ULID, Task] = {}

    async def save_task(self, task: Task) -> None:
        self._store[task.id] = task

    async def get_task(self, task_id: ULID) -> Task | None:
        return self._store.get(task_id)

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        return [self._store.get(task_id) for task_id in task_ids]

    def __getattr__(self, name: str) -> object:
        raise NotImplementedError(f"DummyTaskAdapter.{name} is not implemented")


@pytest.fixture
def dummy_task_adapter() -> DummyTaskAdapter:
    """Fixture providing a fresh DummyTaskAdapter for each test."""
    return DummyTaskAdapter()
