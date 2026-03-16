import aiosqlite
import fakeredis
import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.adapters.json_redis import JsonTaskAdapter
from jobbers.adapters.raw_redis import MsgpackTaskAdapter
from jobbers.adapters.task_adapter import _BaseTaskAdapter
from jobbers.models.queue_config import create_schema
from jobbers.models.task import Task
from jobbers.state_manager import StateManager


@pytest_asyncio.fixture(autouse=True)
async def redis():
    """Fixture to reset the tasks in the mocked Redis before each test."""
    fake_store = fakeredis.FakeAsyncRedis()
    yield fake_store
    await fake_store.close()


@pytest.fixture(params=[JsonTaskAdapter, MsgpackTaskAdapter], ids=["json", "msgpack"])
def task_adapter(redis, request):
    """Fixture providing a task adapter instance parametrized over all implementations."""
    return request.param(redis)


@pytest_asyncio.fixture
async def state_manager(redis, sqlite_conn, dummy_task_adapter):
    """StateManager backed by DummyTaskAdapter: fast, single-run, for tests that don't exercise adapter internals."""
    return StateManager(redis, sqlite_conn, task_adapter=dummy_task_adapter)


@pytest_asyncio.fixture
async def state_manager_real_ta(redis, sqlite_conn):
    """StateManager backed by MsgpackTaskAdapter for tests that exercise the full adapter call path."""
    return StateManager(redis, sqlite_conn, task_adapter=MsgpackTaskAdapter(redis))


class DummyTaskAdapter:
    """
    In-memory TaskAdapterProtocol stub for use as a test dependency.

    Implements the subset of the protocol needed for StateManager tests that
    exercise orchestration logic rather than adapter internals.  Pipeline-staged
    methods (stage_requeue, stage_remove_from_queue) add real Redis commands to
    whatever pipeline they receive from the caller, so tests that verify Redis
    state via the ``redis`` fixture still work as expected.

    ``save_task`` is a test-convenience wrapper (not part of the protocol).
    Any unimplemented method raises ``NotImplementedError`` immediately so
    accidental use is caught at the call site.
    """

    TASKS_BY_QUEUE = _BaseTaskAdapter.TASKS_BY_QUEUE
    TASK_DETAILS = _BaseTaskAdapter.TASK_DETAILS
    HEARTBEAT_SCORES = _BaseTaskAdapter.HEARTBEAT_SCORES
    TASK_BY_TYPE_IDX = _BaseTaskAdapter.TASK_BY_TYPE_IDX
    QUEUE_RATE_LIMITER = _BaseTaskAdapter.QUEUE_RATE_LIMITER
    DLQ_MISSING_DATA = _BaseTaskAdapter.DLQ_MISSING_DATA

    def __init__(self) -> None:
        self._store: dict[ULID, Task] = {}

    # ── read ──────────────────────────────────────────────────────────────────

    async def get_task(self, task_id: ULID) -> Task | None:
        return self._store.get(task_id)

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        return [self._store.get(task_id) for task_id in task_ids]

    async def read_for_watch(self, pipe: object, task_id: ULID) -> Task | None:
        """Read from in-memory store; the pipe is in WATCH mode but is not used here."""
        return self._store.get(task_id)

    # ── write ─────────────────────────────────────────────────────────────────

    async def submit_task(self, task: Task) -> bool:
        self._store[task.id] = task
        return True

    def stage_save(self, pipe: object, task: Task) -> None:
        self._store[task.id] = task

    def stage_requeue(self, pipe: object, task: Task) -> None:
        """Eagerly store the task and add a ZADD to the caller's Redis pipeline."""
        assert task.submitted_at  # noqa: S101
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})  # type: ignore[union-attr]
        self._store[task.id] = task

    def stage_remove_from_queue(self, pipe: object, task: Task) -> None:
        """Add ZREM + SREM commands to the caller's Redis pipeline."""
        pipe.zrem(self.TASKS_BY_QUEUE(queue=task.queue), bytes(task.id))  # type: ignore[union-attr]
        pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))  # type: ignore[union-attr]

    # ── convenience (not part of protocol) ───────────────────────────────────

    async def save_task(self, task: Task) -> None:
        self.stage_save(None, task)

    def __getattr__(self, name: str) -> object:
        raise NotImplementedError(f"DummyTaskAdapter.{name} is not implemented")


@pytest.fixture
def dummy_task_adapter() -> DummyTaskAdapter:
    """Fixture providing a fresh DummyTaskAdapter for each test."""
    return DummyTaskAdapter()


@pytest_asyncio.fixture
async def sqlite_conn():
    """In-memory SQLite connection with schema applied."""
    async with aiosqlite.connect(":memory:") as conn:
        await conn.execute("PRAGMA foreign_keys = ON")
        await create_schema(conn)
        yield conn
