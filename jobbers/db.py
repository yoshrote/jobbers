from __future__ import annotations

import os
from typing import TYPE_CHECKING

import redis.asyncio as redis
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from jobbers.adapters import (
    RedisDeadQueue,
    RedisJSONDeadQueue,
    RedisJSONTaskState,
    RedisJSONTaskSubmit,
    RedisTaskState,
    RedisTaskSubmit,
)
from jobbers.adapters.redis import (
    RedisCancellationBus,
    RedisCronDAGScheduler,
    RedisRoutingBackend,
    RedisRoutingNotifications,
    RedisTaskScheduler,
)
from jobbers.adapters.redis_json import RedisJSONRoutingBackend
from jobbers.adapters.sql import SQLRoutingBackend
from jobbers.adapters.static import StaticCronDAGScheduler, StaticRoutingBackend
from jobbers.migrations.runner import run_migrations

if TYPE_CHECKING:
    from jobbers.adapters.zmq import ZmqBus
    from jobbers.protocols import (
        CancellationBusProtocol,
        DeadQueueProtocol,
        RoutingBackendProtocol,
        RoutingNotificationProtocol,
        TaskStateProtocol,
        TaskSubmitProtocol,
    )
    from jobbers.state_manager import StateManager

DEFAULT_REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
REDIS_PROTOCOL_VERSION = 3

# Backend feature flags.
TASK_BACKEND = os.environ.get("TASK_BACKEND", "redis_json")
DLQ_BACKEND = os.environ.get("DLQ_BACKEND", "redis")
TASK_SCHEDULER_BACKEND = os.environ.get("TASK_SCHEDULER_BACKEND", "redis")
CRON_DAG_SCHEDULER_BACKEND = os.environ.get("CRON_DAG_SCHEDULER_BACKEND", "redis")
CANCELLATION_BUS_BACKEND = os.environ.get("CANCELLATION_BUS_BACKEND", "redis")
ROUTING_NOTIFICATIONS_BACKEND = os.environ.get("ROUTING_NOTIFICATIONS_BACKEND", "redis")
FORCE_SAGA_MODE = os.environ.get("FORCE_SAGA_MODE", "false").lower() == "true"

_client: redis.Redis | None = None
_state_manager: StateManager | None = None
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_task_adapter: TaskStateProtocol | None = None
_pre_registered_routing_backend: RoutingBackendProtocol | None = None
_zmq_bus: ZmqBus | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(DEFAULT_REDIS_URL, protocol=REDIS_PROTOCOL_VERSION, legacy_responses=False)
    return _client


def set_client(new_client: redis.Redis) -> redis.Redis:
    global _client
    if _client is not None:
        _client.close()

    _client = new_client
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        raise RuntimeError("Session factory not initialized. Call init_state_manager() first.")
    return _session_factory


async def close_sqlite_conn() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


def create_redis_task_state(client: redis.Redis) -> RedisTaskState | RedisJSONTaskState:
    """Create the Redis task state adapter for the configured backend."""
    if TASK_BACKEND == "redis":
        return RedisTaskState(client)
    return RedisJSONTaskState(client)


def create_redis_task_submit(
    client: redis.Redis, state: RedisTaskState | RedisJSONTaskState
) -> RedisTaskSubmit | RedisJSONTaskSubmit:
    """Create the Redis task submit adapter paired with the given state adapter."""
    if isinstance(state, RedisTaskState):
        return RedisTaskSubmit(client, state)
    return RedisJSONTaskSubmit(client, state)


def create_dead_queue(client: redis.Redis, task_adapter: TaskStateProtocol) -> DeadQueueProtocol:
    """Create a dead-queue adapter matched to the task adapter backend."""
    if TASK_BACKEND == "redis":
        return RedisDeadQueue(client, task_adapter)
    return RedisJSONDeadQueue(client, task_adapter)


def get_task_adapter() -> TaskStateProtocol:
    """Return the singleton task adapter (must call init_state_manager first)."""
    if _task_adapter is None:
        raise RuntimeError("Task adapter not initialized. Call init_state_manager() first.")
    return _task_adapter


def register_routing_backend(backend: RoutingBackendProtocol) -> None:
    """
    Pre-register a routing backend for library/programmatic use.

    Call this before init_state_manager(). When set, it takes priority over
    the ROUTING_BACKEND environment variable.
    """
    global _pre_registered_routing_backend
    _pre_registered_routing_backend = backend


async def _get_or_create_sql(features: set[str]) -> async_sessionmaker[AsyncSession]:
    """Initialize the shared SQLAlchemy engine and session factory (idempotent)."""
    global _engine, _session_factory
    if _session_factory is not None:
        return _session_factory

    db_path = os.environ.get("SQL_PATH", "sqlite+aiosqlite:///jobbers.db")
    _engine = create_async_engine(db_path)

    @event.listens_for(_engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn: object, connection_record: object) -> None:
        if db_path.startswith("sqlite"):
            cursor = dbapi_conn.cursor()  # type: ignore[attr-defined]
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    await run_migrations(_engine, features=features)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _session_factory


async def _create_routing_backend(client: redis.Redis) -> RoutingBackendProtocol:
    """Create the routing backend, initializing SQL only when needed."""
    if _pre_registered_routing_backend is not None:
        return _pre_registered_routing_backend

    backend_type = os.environ.get("ROUTING_BACKEND", "sql")

    if backend_type == "redis":
        return RedisRoutingBackend(client)

    if backend_type == "redis_json":
        backend = RedisJSONRoutingBackend(client)
        await backend.ensure_indexes()
        return backend

    if backend_type == "static" and os.environ.get("STATIC_CONFIG_FILE") is not None:
        return StaticRoutingBackend.from_file(os.environ["STATIC_CONFIG_FILE"])

    # sql (default) — initialize SQLAlchemy
    sf = await _get_or_create_sql({"routing"})
    return SQLRoutingBackend(sf)


def _get_or_create_zmq_bus() -> ZmqBus:
    """Return the process-singleton ZmqBus, creating it on first call."""
    global _zmq_bus
    if _zmq_bus is None:
        from jobbers.adapters.zmq import ZmqBus

        # Fall back to old ZMQ_CANCELLATION_* names for backward compatibility.
        pub = os.environ.get("ZMQ_PUB_ADDRESS", os.environ.get("ZMQ_CANCELLATION_PUB_ADDRESS", "tcp://127.0.0.1:5555"))
        sub = os.environ.get("ZMQ_SUB_ADDRESS", os.environ.get("ZMQ_CANCELLATION_SUB_ADDRESS", "tcp://127.0.0.1:5555"))
        router = os.environ.get("ZMQ_ROUTER_ADDRESS", "tcp://127.0.0.1:5556")
        req = os.environ.get("ZMQ_DEALER_ADDRESS", "tcp://127.0.0.1:5556")
        _zmq_bus = ZmqBus(pub, sub, router, req)
    return _zmq_bus


def _create_cancellation_bus(client: redis.Redis) -> CancellationBusProtocol:
    """Create the cancellation bus adapter for the configured backend."""
    if CANCELLATION_BUS_BACKEND == "zmq":
        from jobbers.adapters.zmq import ZmqCancellationBus

        return ZmqCancellationBus(_get_or_create_zmq_bus())
    return RedisCancellationBus(client)


def _create_routing_notifications(client: redis.Redis) -> RoutingNotificationProtocol:
    """Create the routing notifications adapter for the configured backend."""
    if ROUTING_NOTIFICATIONS_BACKEND == "zmq":
        from jobbers.adapters.zmq import ZmqRoutingNotifications

        return ZmqRoutingNotifications(_get_or_create_zmq_bus())
    return RedisRoutingNotifications(client)


async def init_state_manager() -> StateManager:
    global _state_manager, _task_adapter
    from jobbers.state_manager import StateManager

    client = get_client()

    # Determine which SQL features are needed so we run only the required migrations.
    needed_sql_features: set[str] = set()
    routing_backend_type = os.environ.get("ROUTING_BACKEND", "sql")
    if routing_backend_type == "sql":
        needed_sql_features.add("routing")
    if TASK_BACKEND == "sql":
        needed_sql_features.add("task_state")
    if DLQ_BACKEND == "sql":
        needed_sql_features.add("dead_letter")
    if TASK_SCHEDULER_BACKEND == "sql":
        needed_sql_features.add("task_schedule")
    if CRON_DAG_SCHEDULER_BACKEND == "sql":
        needed_sql_features.add("cron_dag")

    routing_backend = await _create_routing_backend(client)

    # Task state adapter (blob persistence) + submit adapter (enqueue/pop)
    task_submit: TaskSubmitProtocol
    if TASK_BACKEND == "sql":
        from jobbers.adapters.sql import SQLTaskState, SQLTaskSubmit

        sf = await _get_or_create_sql(needed_sql_features)
        dsn = os.environ.get("SQL_PATH", "sqlite+aiosqlite:///jobbers.db")
        _task_adapter = SQLTaskState(sf, dsn=dsn)
        task_submit = SQLTaskSubmit(sf, dsn=dsn)
    else:
        _task_adapter = create_redis_task_state(client)
        task_submit = create_redis_task_submit(client, _task_adapter)

    # Dead-letter queue
    if DLQ_BACKEND == "sql":
        from jobbers.adapters.sql import SQLDeadQueue

        sf = await _get_or_create_sql(needed_sql_features)
        dsn = os.environ.get("SQL_PATH", "sqlite+aiosqlite:///jobbers.db")
        dead_queue: DeadQueueProtocol = SQLDeadQueue(sf, dsn=dsn)
    else:
        dead_queue = create_dead_queue(client, _task_adapter)

    # Task scheduler
    if TASK_SCHEDULER_BACKEND == "sql":
        from jobbers.adapters.sql import SQLTaskScheduler

        sf = await _get_or_create_sql(needed_sql_features)
        dsn = os.environ.get("SQL_PATH", "sqlite+aiosqlite:///jobbers.db")
        task_scheduler: RedisTaskScheduler | SQLTaskScheduler = SQLTaskScheduler(
            sf, routing_backend.get_all_queues, dsn=dsn
        )
    else:
        task_scheduler = RedisTaskScheduler(client, _task_adapter, routing_backend.get_all_queues)

    if CRON_DAG_SCHEDULER_BACKEND == "sql":
        from jobbers.adapters.sql import SQLCronDAGScheduler

        sf = await _get_or_create_sql(needed_sql_features)
        dsn = os.environ.get("SQL_PATH", "sqlite+aiosqlite:///jobbers.db")
        cron_dag_scheduler: RedisCronDAGScheduler | SQLCronDAGScheduler | StaticCronDAGScheduler = (
            SQLCronDAGScheduler(sf, dsn=dsn)
        )
    elif CRON_DAG_SCHEDULER_BACKEND == "static":
        cron_dag_scheduler = StaticCronDAGScheduler()
    else:
        cron_dag_scheduler = RedisCronDAGScheduler(client)

    _state_manager = StateManager(
        routing_backend,
        task_state=_task_adapter,
        task_submit=task_submit,
        dead_queue=dead_queue,
        task_scheduler=task_scheduler,
        cron_dag_scheduler=cron_dag_scheduler,
        cancellation_bus=_create_cancellation_bus(client),
        routing_notifications=_create_routing_notifications(client),
        force_saga=FORCE_SAGA_MODE,
    )
    await _task_adapter.ensure_index()
    await _state_manager.dead_queue.ensure_index()

    if ROUTING_NOTIFICATIONS_BACKEND == "zmq" and os.environ.get("ZMQ_IS_ROUTER", "").lower() == "true":
        from jobbers.adapters.zmq import ZmqRoutingNotifications

        if not isinstance(_state_manager.routing_notifications, ZmqRoutingNotifications):
            raise RuntimeError("ZMQ_IS_ROUTER=true but routing_notifications is not ZmqRoutingNotifications")
        await _state_manager.routing_notifications.start_server()

    return _state_manager


def get_state_manager() -> StateManager:
    if _state_manager is None:
        raise RuntimeError("State manager not initialized. Call init_state_manager() first.")
    return _state_manager
