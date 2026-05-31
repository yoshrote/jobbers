import fakeredis
import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from ulid import ULID

from jobbers.adapters._shared import SharedTaskAdapterMixin
from jobbers.adapters.redis import (
    RedisCancellationBus,
    RedisCronDAGScheduler,
    RedisDeadQueue,
    RedisRoutingNotifications,
    RedisTaskScheduler,
    RedisTaskState,
    RedisTaskSubmit,
)
from jobbers.adapters.redis_json import RedisJSONTaskState, RedisJSONTaskSubmit
from jobbers.adapters.sql import SQLRoutingBackend
from jobbers.migrations.runner import run_migrations
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task
from jobbers.models.task_routing import RoutingConfig
from jobbers.models.task_status import TaskStatus
from jobbers.state_manager import StateManager


@pytest_asyncio.fixture(autouse=True)
async def redis():
    """Fixture to reset the tasks in the mocked Redis before each test."""
    fake_store = fakeredis.FakeAsyncRedis()
    yield fake_store
    await fake_store.close()


@pytest.fixture(params=["redis_json", "redis"], ids=["redis_json", "redis"])
def task_adapter(redis, request):
    """Fixture providing a (state, submit) pair parametrized over both Redis implementations."""
    if request.param == "redis":
        state = RedisTaskState(redis)
        return state, RedisTaskSubmit(redis, state)
    state_json = RedisJSONTaskState(redis)
    return state_json, RedisJSONTaskSubmit(redis, state_json)


@pytest_asyncio.fixture
async def routing_backend(session_factory):
    """SQLRoutingBackend backed by the in-memory SQLite session factory."""
    return SQLRoutingBackend(session_factory)


@pytest_asyncio.fixture
async def state_manager(redis, dummy_routing_backend, dummy_task_adapter):
    """StateManager backed by DummyTaskAdapter + DummyRoutingBackend: fully in-memory, no SQL."""
    sm = StateManager(
        redis,
        dummy_routing_backend,
        task_state=dummy_task_adapter,
        task_submit=dummy_task_adapter,
        dead_queue=RedisDeadQueue(redis, dummy_task_adapter),
        task_scheduler=RedisTaskScheduler(redis, dummy_task_adapter, dummy_routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )
    sm.get_queue_config = sm.routing.get_queue_config
    sm.get_routing_config = sm.routing.get_routing_config
    sm.get_queues = sm.routing.get_queues
    sm.get_all_queues = sm.routing.get_all_queues
    return sm


@pytest_asyncio.fixture
async def state_manager_real_ta(redis, dummy_routing_backend):
    """StateManager backed by RedisTaskState/RedisTaskSubmit + DummyRoutingBackend for tests that exercise the full adapter call path."""
    task_state = RedisTaskState(redis)
    task_submit = RedisTaskSubmit(redis, task_state)
    sm = StateManager(
        redis,
        dummy_routing_backend,
        task_state=task_state,
        task_submit=task_submit,
        dead_queue=RedisDeadQueue(redis, task_state),
        task_scheduler=RedisTaskScheduler(redis, task_state, dummy_routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )
    sm.get_queue_config = sm.routing.get_queue_config
    sm.get_routing_config = sm.routing.get_routing_config
    sm.get_queues = sm.routing.get_queues
    sm.get_all_queues = sm.routing.get_all_queues
    return sm


class DummyTaskAdapter:
    """
    In-memory AtomicTaskStateProtocol + TaskSubmitProtocol stub for StateManager orchestration tests.

    Implements AtomicTaskStateProtocol so StateManager detects it as atomic-capable
    and uses the MULTI/EXEC pipeline path (same behaviour as in production with a real
    Redis-backed adapter).  Pipeline-staged methods add real Redis commands to whatever
    pipeline they receive from the caller, so tests that verify Redis state via the
    ``redis`` fixture still work as expected.

    Also satisfies TaskSubmitProtocol: submit_task stores eagerly in the in-memory dict;
    submit_rate_limited_task and get_next_task raise NotImplementedError so accidental
    use is caught at the call site.
    """

    TASKS_BY_QUEUE = SharedTaskAdapterMixin.TASKS_BY_QUEUE
    TASK_DETAILS = SharedTaskAdapterMixin.TASK_DETAILS
    HEARTBEAT_SCORES = SharedTaskAdapterMixin.HEARTBEAT_SCORES
    TASK_BY_TYPE_IDX = SharedTaskAdapterMixin.TASK_BY_TYPE_IDX
    QUEUE_RATE_LIMITER = SharedTaskAdapterMixin.QUEUE_RATE_LIMITER
    DLQ_MISSING_DATA = SharedTaskAdapterMixin.DLQ_MISSING_DATA

    def __init__(self, data_store: object = None) -> None:
        self._store: dict[ULID, Task] = {}
        self._data_store = data_store

    # ── AtomicTaskStateProtocol: backend identity + pipeline ──────────────────

    @property
    def backend_key(self) -> str:
        return str(id(self._data_store))

    def pipeline(self, transaction: bool = True) -> object:
        return self._data_store.pipeline(transaction=transaction)  # type: ignore[union-attr]

    # ── TaskStateProtocol: read ───────────────────────────────────────────────

    async def get_task(self, task_id: ULID) -> Task | None:
        return self._store.get(task_id)

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        return [self._store.get(task_id) for task_id in task_ids]

    async def task_exists(self, task_id: ULID) -> bool:
        return task_id in self._store

    async def get_active_tasks(self, queues: object) -> list[Task]:
        raise NotImplementedError("DummyTaskAdapter.get_active_tasks")

    def get_stale_tasks(self, queues: object, stale_time: object) -> object:
        raise NotImplementedError("DummyTaskAdapter.get_stale_tasks")

    async def get_all_tasks(self, pagination: object) -> list[Task]:
        raise NotImplementedError("DummyTaskAdapter.get_all_tasks")

    async def compare_and_set_status(self, task_id: ULID, expected: object, new: object) -> bool:
        task = self._store.get(task_id)
        if task is None or task.status != expected:
            return False
        task.set_status(new)  # type: ignore[arg-type]
        return True

    async def update_task_heartbeat(self, task: Task) -> None:
        raise NotImplementedError("DummyTaskAdapter.update_task_heartbeat")

    async def remove_task_heartbeat(self, task: Task) -> None:
        raise NotImplementedError("DummyTaskAdapter.remove_task_heartbeat")

    async def init_fan_in(self, fan_in_key: str, predecessor_ids: object, ttl: int = 86400) -> None:
        pass  # no-op: fan-in sets not needed for in-memory orchestration tests

    async def fan_in_complete(self, fan_in_key: str, task_id: ULID) -> int:
        raise NotImplementedError("DummyTaskAdapter.fan_in_complete")

    async def get_fan_in_members(self, fan_in_key: str) -> list[ULID]:
        raise NotImplementedError("DummyTaskAdapter.get_fan_in_members")

    async def get_dag_runs(self, pagination: object) -> object:
        raise NotImplementedError("DummyTaskAdapter.get_dag_runs")

    async def get_dag_run(self, dag_run_id: ULID) -> object:
        raise NotImplementedError("DummyTaskAdapter.get_dag_run")

    async def clean_dag_runs(self, now: object, max_age: object) -> None:
        raise NotImplementedError("DummyTaskAdapter.clean_dag_runs")

    async def ensure_index(self) -> None:
        raise NotImplementedError("DummyTaskAdapter.ensure_index")

    async def clean_terminal_tasks(self, now: object, max_age: object) -> None:
        raise NotImplementedError("DummyTaskAdapter.clean_terminal_tasks")

    async def clean(self, queues: object, now: object, min_queue_age: object, max_queue_age: object) -> None:
        raise NotImplementedError("DummyTaskAdapter.clean")

    # ── AtomicTaskStateProtocol: pipeline-staged writes ───────────────────────

    async def read_for_watch(self, pipe: object, task_id: ULID) -> Task | None:
        """Read from in-memory store; the pipe is in WATCH mode but is not used here."""
        return self._store.get(task_id)

    def stage_save(self, pipe: object, task: Task) -> None:
        self._store[task.id] = task

    def stage_requeue(self, pipe: object, task: Task) -> None:
        """Eagerly store the task and add a ZADD to the caller's Redis pipeline."""
        assert task.submitted_at
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})  # type: ignore[union-attr]
        self._store[task.id] = task

    def stage_submit_task(self, pipe: object, task: Task) -> None:
        """Eagerly store the task and add a ZADD to the caller's Redis pipeline."""
        assert task.submitted_at
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})  # type: ignore[union-attr]
        self._store[task.id] = task

    def stage_remove_from_queue(self, pipe: object, task: Task) -> None:
        """Add ZREM + SREM commands to the caller's Redis pipeline."""
        pipe.zrem(self.TASKS_BY_QUEUE(queue=task.queue), bytes(task.id))  # type: ignore[union-attr]
        pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))  # type: ignore[union-attr]

    def stage_remove_heartbeat(self, pipe: object, task: Task) -> None:
        """Add ZREM heartbeat command to the caller's Redis pipeline."""
        pipe.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))  # type: ignore[union-attr]

    async def optimistic_dispatch_scheduled(self, task: Task, stage_extra: object) -> bool:
        """CAS dispatch: check in-memory store, set SUBMITTED, enqueue via real Redis pipeline."""
        watched_task = self._store.get(task.id)
        if watched_task is None or watched_task.status == TaskStatus.CANCELLED:
            return False
        task.set_status(TaskStatus.SUBMITTED)
        pipe = self._data_store.pipeline(transaction=True)  # type: ignore[union-attr]
        self.stage_requeue(pipe, task)
        stage_extra(pipe)  # type: ignore[operator]
        await pipe.execute()
        return True

    def stage_init_fan_in(
        self, pipe: object, fan_in_key: str, predecessor_ids: object, ttl: int = 86400
    ) -> None:
        """No-op stub: fan-in sets are not needed for in-memory orchestration tests."""

    # ── TaskSubmitProtocol ────────────────────────────────────────────────────

    async def submit_task(self, task: Task) -> bool:
        self._store[task.id] = task
        return True

    async def submit_rate_limited_task(self, task: Task, queue_config: object) -> bool:
        raise NotImplementedError("DummyTaskAdapter.submit_rate_limited_task")

    async def clean_rate_limiter(self, queues: object, now: object, rate_limit_age: object) -> None:
        pass

    async def get_next_task(self, queues: object, pop_timeout: int = 0) -> Task | None:
        raise NotImplementedError("DummyTaskAdapter.get_next_task")

    # ── convenience (not part of any protocol) ────────────────────────────────

    async def save_task(self, task: Task) -> None:
        self.stage_save(None, task)

    def __getattr__(self, name: str) -> object:
        raise NotImplementedError(f"DummyTaskAdapter.{name} is not implemented")


@pytest.fixture
def dummy_task_adapter(redis: object) -> DummyTaskAdapter:
    """Fixture providing a fresh DummyTaskAdapter backed by the test Redis for each test."""
    return DummyTaskAdapter(redis)


class DummyRoutingBackend:
    """
    In-memory RoutingBackendProtocol stub for orchestration tests.

    All protocol methods are implemented with plain dicts. Intended for
    StateManager / task-route tests that need a routing backend without
    depending on a real database or Redis.
    """

    def __init__(self) -> None:
        self._queues: dict[str, QueueConfig] = {}
        self._roles: dict[str, set[str]] = {}
        self._routing: dict[tuple[str, int], RoutingConfig] = {}
        self._tags: dict[str, ULID] = {}

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        return self._queues.get(queue)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        self._queues[queue_config.name] = queue_config

    async def delete_queue(self, queue_name: str) -> None:
        self._queues.pop(queue_name, None)
        new_tag = ULID()
        for role, queues in self._roles.items():
            if queue_name in queues:
                queues.discard(queue_name)
                self._tags[role] = new_tag

    async def get_all_queues(self) -> list[str]:
        return sorted(self._queues)

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        return set(self._roles.get(role, set()))

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        self._roles[role] = set(queues_set)
        new_tag = ULID()
        self._tags[role] = new_tag
        return str(new_tag)

    async def get_all_roles(self) -> list[str]:
        return sorted(self._roles)

    async def delete_role(self, role: str) -> None:
        self._roles.pop(role, None)
        self._tags.pop(role, None)

    # ── Refresh tags ──────────────────────────────────────────────────────────

    async def get_refresh_tag(self, role: str) -> ULID:
        if role not in self._tags:
            self._tags[role] = ULID()
        return self._tags[role]

    async def bump_refresh_tag(self, role: str) -> str:
        new_tag = ULID()
        self._tags[role] = new_tag
        return str(new_tag)

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        affected = [role for role, queues in self._roles.items() if queue_name in queues]
        if affected:
            new_tag = ULID()
            for role in affected:
                self._tags[role] = new_tag
        return affected

    # ── Task routing config ───────────────────────────────────────────────────

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        return self._routing.get((task_name, task_version))

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        self._routing[(routing_config.task_name, routing_config.task_version)] = routing_config

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        return self._routing.pop((task_name, task_version), None) is not None


@pytest.fixture
def dummy_routing_backend() -> DummyRoutingBackend:
    """Fixture providing a fresh DummyRoutingBackend for each test."""
    return DummyRoutingBackend()


@pytest_asyncio.fixture
async def session_factory():
    """In-memory SQLite async_sessionmaker with schema applied."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn: object, connection_record: object) -> None:
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    await run_migrations(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()
