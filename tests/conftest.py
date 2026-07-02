import datetime as dt

import pytest
import pytest_asyncio
from redis.exceptions import ResponseError
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
from jobbers.models.cron_dag import CronDAGEntry
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task
from jobbers.models.task_routing import RoutingConfig
from jobbers.models.task_status import TaskStatus
from jobbers.state_manager import StateManager
from tests._redis_helpers import real_redis_connection


@pytest_asyncio.fixture(autouse=True)
async def redis():
    """Fixture to reset the tasks in the real Redis before each test."""
    async with real_redis_connection() as r:
        state = RedisJSONTaskState(r)
        try:
            await state.ensure_index()
        except ResponseError as exc:  # pragma: no cover
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield r


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


class DummyTaskState:
    """
    Non-atomic TaskStateProtocol stub for StateManager orchestration tests.

    Does NOT implement AtomicTaskStateProtocol, so StateManager sets _atomic_state=None
    and uses saga mode for all state-save paths.  Heartbeat tracking is in-memory so
    that get_stale_tasks() can return results without a real Redis sorted set.
    """

    TASKS_BY_QUEUE = SharedTaskAdapterMixin.TASKS_BY_QUEUE
    TASK_DETAILS = SharedTaskAdapterMixin.TASK_DETAILS
    HEARTBEAT_SCORES = SharedTaskAdapterMixin.HEARTBEAT_SCORES
    TASK_BY_TYPE_IDX = SharedTaskAdapterMixin.TASK_BY_TYPE_IDX
    QUEUE_RATE_LIMITER = SharedTaskAdapterMixin.QUEUE_RATE_LIMITER
    DLQ_MISSING_DATA = SharedTaskAdapterMixin.DLQ_MISSING_DATA

    def __init__(self) -> None:
        self._store: dict[ULID, Task] = {}
        self._heartbeats: dict[ULID, float] = {}

    # ── TaskStateProtocol: reads ──────────────────────────────────────────────

    async def get_task(self, task_id: ULID) -> Task | None:
        return self._store.get(task_id)

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        return [self._store.get(tid) for tid in task_ids]

    async def task_exists(self, task_id: ULID) -> bool:
        return task_id in self._store

    async def compare_and_set_status(self, task_id: ULID, expected: object, new: object) -> bool:
        task = self._store.get(task_id)
        if task is None or task.status != expected:
            return False
        task.set_status(new)  # type: ignore[arg-type]
        return True

    async def get_active_tasks(self, queues: object) -> list[Task]:
        return [t for tid, t in self._store.items() if tid in self._heartbeats]

    async def get_stale_tasks(self, queues: object, stale_time: dt.timedelta):  # type: ignore[return]
        cutoff = (dt.datetime.now(dt.UTC) - stale_time).timestamp()
        for task_id, score in list(self._heartbeats.items()):
            if score < cutoff:
                task = self._store.get(task_id)
                if task is not None:
                    yield task

    async def get_all_tasks(self, pagination: object) -> list[Task]:
        raise NotImplementedError("DummyTaskState.get_all_tasks")

    async def update_task_heartbeat(self, task: Task) -> None:
        assert task.heartbeat_at is not None
        self._heartbeats[task.id] = task.heartbeat_at.timestamp()

    async def remove_task_heartbeat(self, task: Task) -> None:
        self._heartbeats.pop(task.id, None)

    async def init_fan_in(self, fan_in_key: str, predecessor_ids: object, ttl: int = 86400) -> None:
        pass

    async def fan_in_complete(self, fan_in_key: str, task_id: ULID) -> int:
        raise NotImplementedError("DummyTaskState.fan_in_complete")

    async def get_fan_in_members(self, fan_in_key: str) -> list[ULID]:
        raise NotImplementedError("DummyTaskState.get_fan_in_members")

    async def get_dag_runs(self, pagination: object) -> object:
        raise NotImplementedError("DummyTaskState.get_dag_runs")

    async def get_dag_run(self, dag_run_id: ULID) -> object:
        raise NotImplementedError("DummyTaskState.get_dag_run")

    async def clean_dag_runs(self, now: object, max_age: object) -> None:
        raise NotImplementedError("DummyTaskState.clean_dag_runs")

    async def ensure_index(self) -> None:
        raise NotImplementedError("DummyTaskState.ensure_index")

    async def drop_stale_indexes(self) -> list[str]:
        raise NotImplementedError("DummyTaskState.drop_stale_indexes")

    async def clean_terminal_tasks(self, now: object, max_age: object) -> None:
        raise NotImplementedError("DummyTaskState.clean_terminal_tasks")

    async def clean(self, queues: object, now: object, min_queue_age: object, max_queue_age: object) -> None:
        raise NotImplementedError("DummyTaskState.clean")

    # ── TaskStateProtocol: write ──────────────────────────────────────────────

    async def save_task(self, task: Task) -> Task:
        self._store[task.id] = task
        return task

    async def delete_task(self, task: Task) -> None:
        self._store.pop(task.id, None)
        self._heartbeats.pop(task.id, None)

    def __getattr__(self, name: str) -> object:
        raise NotImplementedError(f"DummyTaskState.{name} is not implemented")


class AtomicDummyTaskState(DummyTaskState):
    """
    AtomicTaskStateProtocol stub — adds pipeline staging on top of DummyTaskState.

    StateManager detects this as atomic-capable and uses MULTI/EXEC pipelines.
    Pipeline-staged writes also update the in-memory _store so assertions against
    _store still work even when operations went through a pipeline.
    """

    def __init__(self, data_store: object) -> None:
        super().__init__()
        self._data_store = data_store

    # ── AtomicTaskStateProtocol: backend identity + pipeline ──────────────────

    @property
    def backend_key(self) -> str:
        return str(id(self._data_store))

    def pipeline(self, transaction: bool = True) -> object:
        return self._data_store.pipeline(transaction=transaction)  # type: ignore[union-attr]

    # ── AtomicTaskStateProtocol: pipeline-staged writes ──────────────────────

    async def read_for_watch(self, pipe: object, task_id: ULID) -> Task | None:
        return self._store.get(task_id)

    def stage_save(self, pipe: object, task: Task) -> None:
        self._store[task.id] = task

    def stage_requeue(self, pipe: object, task: Task) -> None:
        assert task.submitted_at
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})  # type: ignore[union-attr]
        self._store[task.id] = task

    def stage_submit_task(self, pipe: object, task: Task) -> None:
        assert task.submitted_at
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})  # type: ignore[union-attr]
        self._store[task.id] = task

    def stage_remove_from_queue(self, pipe: object, task: Task) -> None:
        pipe.zrem(self.TASKS_BY_QUEUE(queue=task.queue), bytes(task.id))  # type: ignore[union-attr]
        pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))  # type: ignore[union-attr]

    def stage_remove_heartbeat(self, pipe: object, task: Task) -> None:
        pipe.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))  # type: ignore[union-attr]

    async def atomic_dispatch_scheduled(self, task: Task, stage_extra: object) -> bool:
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
        pass

    async def delegate_fan_in(self, fan_in_key: str, old_id: object, new_id: object) -> None:
        pass  # no in-memory fan-in tracking; tests that exercise this go through real adapters


class DummyTaskSubmit:
    """
    TaskSubmitProtocol stub that shares the _store dict with a DummyTaskState.

    submit_task writes into the shared store so that get_task() on the paired
    DummyTaskState immediately reflects submitted tasks.
    """

    def __init__(self, store: dict) -> None:
        self._store = store

    async def submit_task(self, task: Task) -> bool:
        self._store[task.id] = task
        return True

    async def submit_rate_limited_task(self, task: Task, queue_config: object) -> bool:
        raise NotImplementedError("DummyTaskSubmit.submit_rate_limited_task")

    async def clean_rate_limiter(self, queues: object, now: object, rate_limit_age: object) -> None:
        pass

    async def get_next_task(self, queues: object, pop_timeout: int = 0) -> Task | None:
        raise NotImplementedError("DummyTaskSubmit.get_next_task")

    def __getattr__(self, name: str) -> object:
        raise NotImplementedError(f"DummyTaskSubmit.{name} is not implemented")


class DummyDeadQueue:
    """
    Non-atomic DeadQueueProtocol stub for saga-mode StateManager tests.

    Does NOT implement AtomicDeadQueueProtocol, so StateManager sets _atomic_dlq=None
    when this is the dead queue.  All operations are purely in-memory.
    """

    def __init__(self) -> None:
        self._entries: dict[str, Task] = {}

    async def add_to_dlq(self, task: Task, failed_at: dt.datetime) -> None:
        self._entries[str(task.id)] = task

    async def remove_from_dlq(self, task_id: object, queue: str, name: str) -> None:
        self._entries.pop(str(task_id), None)

    async def get_by_ids(self, task_ids: list[str]) -> list[Task]:
        return [self._entries[tid] for tid in task_ids if tid in self._entries]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list[Task]:
        results = list(self._entries.values())
        if queue is not None:
            results = [t for t in results if t.queue == queue]
        if task_name is not None:
            results = [t for t in results if t.name == task_name]
        if task_version is not None:
            results = [t for t in results if t.version == task_version]
        return results[:limit]

    async def get_history(self, task_id: str) -> list[dict]:
        task = self._entries.get(task_id)
        if task is None:
            return []
        return [{"attempt": i, "error": e} for i, e in enumerate(task.errors or [])]

    async def remove_many(self, task_ids: list[str]) -> None:
        for tid in task_ids:
            self._entries.pop(tid, None)

    async def clean(self, earlier_than: dt.datetime) -> None:
        pass

    async def ensure_index(self) -> None:
        pass

    async def drop_stale_indexes(self) -> list[str]:
        return []


class DummyTaskScheduler:
    """
    Non-atomic TaskSchedulerProtocol stub for saga-mode StateManager tests.

    Does NOT implement AtomicTaskSchedulerProtocol, so StateManager sets
    _atomic_scheduler=None when this is the task scheduler.  All operations
    are purely in-memory.
    """

    def __init__(self) -> None:
        self._entries: dict[ULID, tuple[Task, dt.datetime]] = {}

    async def add(self, task: Task, run_at: dt.datetime) -> None:
        self._entries[task.id] = (task, run_at)

    async def remove(self, task_id: ULID, queue: str) -> None:
        self._entries.pop(task_id, None)

    async def get_run_at(self, task_id: ULID) -> dt.datetime | None:
        entry = self._entries.get(task_id)
        return entry[1] if entry else None

    async def next_due(self, queues: object, now: dt.datetime | None = None) -> Task | None:
        now = now or dt.datetime.now(dt.UTC)
        for task_id, (task, run_at) in list(self._entries.items()):
            if run_at <= now:
                del self._entries[task_id]
                return task
        return None

    async def next_due_bulk(
        self, queues: object, limit: int = 10, now: dt.datetime | None = None
    ) -> list[tuple[Task, dt.datetime]]:
        now = now or dt.datetime.now(dt.UTC)
        results = []
        for task_id, (task, run_at) in list(self._entries.items()):
            if run_at <= now:
                del self._entries[task_id]
                results.append((task, run_at))
                if len(results) >= limit:
                    break
        return results

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 10,
        start_after: str | None = None,
    ) -> list[tuple[Task, dt.datetime]]:
        results = [
            (t, run_at)
            for t, run_at in self._entries.values()
            if (queue is None or t.queue == queue)
            and (task_name is None or t.name == task_name)
            and (task_version is None or t.version == task_version)
        ]
        return results[:limit]


class DummyCronDAGScheduler:
    """
    Non-atomic CronDAGSchedulerProtocol stub for saga-mode StateManager tests.

    Does NOT implement AtomicCronDAGSchedulerProtocol, so StateManager sets
    _atomic_cron=None when this is the cron scheduler.  All operations are
    purely in-memory.
    """

    def __init__(self) -> None:
        self._entries: dict[ULID, tuple[CronDAGEntry, dt.datetime | None]] = {}
        self._active_runs: dict[ULID, str] = {}

    async def add(self, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        self._entries[entry.id] = (entry, next_run_at)

    async def remove(self, entry_id: ULID) -> None:
        self._entries.pop(entry_id, None)

    async def get(self, entry_id: ULID) -> CronDAGEntry | None:
        pair = self._entries.get(entry_id)
        return pair[0] if pair else None

    async def get_next_run_at(self, entry_id: ULID) -> dt.datetime | None:
        pair = self._entries.get(entry_id)
        return pair[1] if pair else None

    async def next_due_bulk(
        self, limit: int = 10, now: dt.datetime | None = None
    ) -> list[tuple[CronDAGEntry, dt.datetime]]:
        now = now or dt.datetime.now(dt.UTC)
        results = []
        for entry_id, (entry, run_at) in list(self._entries.items()):
            if run_at is not None and run_at <= now:
                self._entries[entry_id] = (entry, None)
                results.append((entry, run_at))
                if len(results) >= limit:
                    break
        return results

    async def reschedule(self, entry_id: ULID, next_run_at: dt.datetime) -> None:
        pair = self._entries.get(entry_id)
        if pair:
            self._entries[entry_id] = (pair[0], next_run_at)

    async def list(
        self, offset: int = 0, limit: int = 50
    ) -> tuple[list[tuple[CronDAGEntry, dt.datetime | None]], int]:
        items = [(e, r) for e, r in self._entries.values() if r is not None]
        total = len(items)
        return items[offset : offset + limit], total

    async def set_active_run(self, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False) -> bool:
        if nx and cron_id in self._active_runs:
            return False
        self._active_runs[cron_id] = str(task_id)
        return True

    async def get_active_run(self, cron_id: ULID) -> str | None:
        return self._active_runs.get(cron_id)

    async def clear_active_run(self, cron_id: ULID) -> None:
        self._active_runs.pop(cron_id, None)


def _make_state_manager(
    redis: object,
    routing: object,
    task_state: object,
    task_submit: object,
    dead_queue: object,
    task_scheduler: object,
    cron_dag_scheduler: object,
) -> StateManager:
    sm = StateManager(
        routing,
        task_state=task_state,
        task_submit=task_submit,
        dead_queue=dead_queue,
        task_scheduler=task_scheduler,
        cron_dag_scheduler=cron_dag_scheduler,
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )
    sm.get_queue_config = sm.routing.get_queue_config
    sm.get_routing_config = sm.routing.get_routing_config
    sm.get_queues = sm.routing.get_queues
    sm.get_all_queues = sm.routing.get_all_queues
    return sm


@pytest.fixture
def dummy_task_adapter(redis: object) -> AtomicDummyTaskState:
    """Fixture providing a fresh AtomicDummyTaskState backed by the test Redis for each test."""
    return AtomicDummyTaskState(redis)


@pytest_asyncio.fixture
async def state_manager(redis, dummy_routing_backend):
    """StateManager backed by AtomicDummyTaskState + DummyTaskSubmit + DummyRoutingBackend."""
    task_state = AtomicDummyTaskState(redis)
    task_submit = DummyTaskSubmit(task_state._store)
    return _make_state_manager(
        redis,
        dummy_routing_backend,
        task_state=task_state,
        task_submit=task_submit,
        dead_queue=RedisDeadQueue(redis, task_state),
        task_scheduler=RedisTaskScheduler(redis, task_state, dummy_routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
    )


@pytest_asyncio.fixture
async def state_manager_real_ta(redis, dummy_routing_backend):
    """StateManager backed by RedisTaskState/RedisTaskSubmit + DummyRoutingBackend for tests that exercise the full adapter call path."""
    task_state = RedisTaskState(redis)
    task_submit = RedisTaskSubmit(redis, task_state)
    sm = StateManager(
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


@pytest_asyncio.fixture
async def saga_state_manager(redis, dummy_routing_backend):
    """
    StateManager with DummyTaskState (non-atomic) — _atomic_state is None.

    This causes all checks of the form ``if self._atomic_state is not None`` to
    evaluate False, exercising every saga-mode save, schedule, cancel, and DLQ path.
    The DLQ, scheduler, and cron adapters are still backed by real Redis so that
    add/remove helpers in tests work as expected.
    """
    task_state = DummyTaskState()
    task_submit = DummyTaskSubmit(task_state._store)
    return _make_state_manager(
        redis,
        dummy_routing_backend,
        task_state=task_state,
        task_submit=task_submit,
        dead_queue=RedisDeadQueue(redis, task_state),
        task_scheduler=RedisTaskScheduler(redis, task_state, dummy_routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
    )


@pytest_asyncio.fixture
async def cron_saga_state_manager(redis, dummy_routing_backend):
    """
    StateManager with AtomicDummyTaskState + DummyCronDAGScheduler.

    _atomic_cron is None (DummyCronDAGScheduler is non-atomic) while
    _atomic_state is not None.  This exercises the non-pipeline cron dispatch
    and complete_cron_task paths (the ``elif self._atomic_state is not None``
    branch in complete_cron_task and the ``else`` branch in dispatch_cron_dag).
    """
    task_state = AtomicDummyTaskState(redis)
    task_submit = DummyTaskSubmit(task_state._store)
    cron = DummyCronDAGScheduler()
    return _make_state_manager(
        redis,
        dummy_routing_backend,
        task_state=task_state,
        task_submit=task_submit,
        dead_queue=RedisDeadQueue(redis, task_state),
        task_scheduler=RedisTaskScheduler(redis, task_state, dummy_routing_backend.get_all_queues),
        cron_dag_scheduler=cron,
    )


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

    async def drop_stale_indexes(self) -> list[str]:
        return []

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        return self._queues.get(queue)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        self._queues[queue_config.name] = queue_config

    async def delete_queue(self, queue_name: str) -> list[str]:
        self._queues.pop(queue_name, None)
        affected = []
        for role, queues in self._roles.items():
            if queue_name in queues:
                queues.discard(queue_name)
                affected.append(role)
        return affected

    async def get_all_queues(self) -> list[str]:
        return sorted(self._queues)

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        return set(self._roles.get(role, set()))

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        self._roles[role] = set(queues_set)

    async def get_all_roles(self) -> list[str]:
        return sorted(self._roles)

    async def delete_role(self, role: str) -> None:
        self._roles.pop(role, None)

    # ── Role discovery ────────────────────────────────────────────────────────

    async def get_roles_for_queue(self, queue_name: str) -> list[str]:
        return [role for role, queues in self._roles.items() if queue_name in queues]

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
