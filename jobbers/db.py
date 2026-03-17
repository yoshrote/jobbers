from __future__ import annotations

import os
from typing import TYPE_CHECKING

import redis.asyncio as redis
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from jobbers.migrations.runner import run_migrations

if TYPE_CHECKING:
    from jobbers.adapters.task_adapter import TaskAdapterProtocol
    from jobbers.state_manager import StateManager

TASK_ADAPTER_BACKEND = "json"
DEFAULT_REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

_client: redis.Redis | None = None
_state_manager: StateManager | None = None
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_task_adapter: TaskAdapterProtocol | None = None


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
        raise RuntimeError("SQLite not initialized. Call init_state_manager() first.")
    return _session_factory


async def close_sqlite_conn() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


def create_task_adapter(client: redis.Redis) -> TaskAdapterProtocol:
    """Create a TaskAdapter based on the TASK_ADAPTER_BACKEND variable."""
    from jobbers.adapters import JsonTaskAdapter, MsgpackTaskAdapter

    if TASK_ADAPTER_BACKEND == "msgpack":
        return MsgpackTaskAdapter(client)
    return JsonTaskAdapter(client)


def get_task_adapter() -> TaskAdapterProtocol:
    """Return the singleton task adapter (must call init_state_manager first)."""
    if _task_adapter is None:
        raise RuntimeError("Task adapter not initialized. Call init_state_manager() first.")
    return _task_adapter


async def init_state_manager() -> StateManager:
    global _state_manager, _engine, _session_factory, _task_adapter
    from jobbers.state_manager import StateManager

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
    client = get_client()
    _task_adapter = create_task_adapter(client)
    _state_manager = StateManager(client, _session_factory, task_adapter=_task_adapter)
    await _task_adapter.ensure_index()
    await _state_manager.dead_queue.ensure_index()
    return _state_manager


def get_state_manager() -> StateManager:
    if _state_manager is None:
        raise RuntimeError("State manager not initialized. Call init_state_manager() first.")
    return _state_manager
