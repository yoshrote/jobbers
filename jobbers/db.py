from __future__ import annotations

import os
from typing import TYPE_CHECKING

import redis.asyncio as redis
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from jobbers.adapters import JsonTaskAdapter, MsgpackTaskAdapter
from jobbers.adapters.redis import RedisRoutingBackend, RedisTaskScheduler
from jobbers.adapters.redis_json import RedisJSONRoutingBackend
from jobbers.adapters.sql import SQLCronDAGScheduler, SQLRoutingBackend
from jobbers.adapters.static import StaticCronDAGScheduler, StaticRoutingBackend
from jobbers.migrations.runner import run_migrations
from jobbers.schedulers.cron_dag_scheduler import RedisCronDAGScheduler

if TYPE_CHECKING:
    from jobbers.protocols import CronDAGSchedulerProtocol, RoutingBackendProtocol, TaskAdapterProtocol
    from jobbers.state_manager import StateManager

TASK_ADAPTER_BACKEND = os.environ.get("TASK_ADAPTER", "json")
DEFAULT_REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

_client: redis.Redis | None = None
_state_manager: StateManager | None = None
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_task_adapter: TaskAdapterProtocol | None = None
_pre_registered_routing_backend: RoutingBackendProtocol | None = None
_pre_registered_cron_scheduler: CronDAGSchedulerProtocol | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(DEFAULT_REDIS_URL)
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


def create_task_adapter(client: redis.Redis) -> TaskAdapterProtocol:
    """Create a TaskAdapter based on the TASK_ADAPTER_BACKEND variable."""
    if TASK_ADAPTER_BACKEND == "msgpack":
        return MsgpackTaskAdapter(client)
    return JsonTaskAdapter(client)


def get_task_adapter() -> TaskAdapterProtocol:
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


def register_cron_scheduler(scheduler: CronDAGSchedulerProtocol) -> None:
    """
    Pre-register a cron DAG scheduler for library/programmatic use.

    Call this before init_state_manager(). When set, it takes priority over
    the CRON_BACKEND environment variable.
    """
    global _pre_registered_cron_scheduler
    _pre_registered_cron_scheduler = scheduler


async def _create_routing_backend(client: redis.Redis) -> RoutingBackendProtocol:
    """Create the routing backend, initializing SQL only when needed."""
    global _engine, _session_factory

    if _pre_registered_routing_backend is not None:
        return _pre_registered_routing_backend

    backend_type = os.environ.get("ROUTING_BACKEND", "sql")

    if backend_type == "redis":
        return RedisRoutingBackend(client)

    if backend_type == "redis_json":
        backend = RedisJSONRoutingBackend(client)
        await backend.ensure_indexes()
        return backend

    if backend_type == "static":
        return StaticRoutingBackend.from_env()

    # sql (default) — initialize SQLAlchemy
    db_path = os.environ.get("SQL_PATH", "sqlite+aiosqlite:///jobbers.db")
    _engine = create_async_engine(db_path)

    @event.listens_for(_engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn: object, connection_record: object) -> None:
        if db_path.startswith("sqlite"):
            cursor = dbapi_conn.cursor()  # type: ignore[attr-defined]
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    await run_migrations(_engine)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return SQLRoutingBackend(_session_factory)


async def _create_cron_scheduler(client: redis.Redis) -> CronDAGSchedulerProtocol:
    """
    Create the cron DAG scheduler.

    Defaults to ROUTING_BACKEND if CRON_BACKEND is unset, so users only need one variable.
    Call _create_routing_backend first when SQL is used, so _session_factory is initialized.
    """
    if _pre_registered_cron_scheduler is not None:
        return _pre_registered_cron_scheduler

    cron_backend = os.environ.get("CRON_BACKEND", os.environ.get("ROUTING_BACKEND", "sql"))

    if cron_backend in ("redis", "redis_json"):
        return RedisCronDAGScheduler(client)

    if cron_backend == "static":
        return StaticCronDAGScheduler.from_env()

    # sql (default) — reuse session factory initialized by _create_routing_backend
    if _session_factory is None:
        raise RuntimeError(
            "SQL session factory not initialized. "
            "Ensure _create_routing_backend() runs before _create_cron_scheduler() "
            "when CRON_BACKEND=sql."
        )
    return SQLCronDAGScheduler(_session_factory)


async def init_state_manager() -> StateManager:
    global _state_manager, _task_adapter
    from jobbers.state_manager import StateManager

    client = get_client()
    _task_adapter = create_task_adapter(client)
    routing_backend = await _create_routing_backend(client)
    cron_scheduler = await _create_cron_scheduler(client)
    task_scheduler = RedisTaskScheduler(client, _task_adapter, routing_backend.get_all_queues)
    _state_manager = StateManager(
        client,
        routing_backend,
        task_scheduler=task_scheduler,
        task_adapter=_task_adapter,
        cron_dag_scheduler=cron_scheduler,
    )
    await _task_adapter.ensure_index()
    await _state_manager.dead_queue.ensure_index()
    return _state_manager


def get_state_manager() -> StateManager:
    if _state_manager is None:
        raise RuntimeError("State manager not initialized. Call init_state_manager() first.")
    return _state_manager
