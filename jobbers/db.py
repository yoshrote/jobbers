from __future__ import annotations

import os
from typing import TYPE_CHECKING

import aiosqlite
import redis.asyncio as redis

from jobbers.models.queue_config import create_schema

if TYPE_CHECKING:
    from jobbers.adapters.task_adapter import TaskAdapterProtocol
    from jobbers.state_manager import StateManager

TASK_ADAPTER_BACKEND = "json"
DEFAULT_REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

_client: redis.Redis | None = None
_state_manager: StateManager | None = None
_sqlite_conn: aiosqlite.Connection | None = None
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


def get_sqlite_conn() -> aiosqlite.Connection:
    global _sqlite_conn
    if _sqlite_conn is None:
        raise RuntimeError("SQLite not initialized. Call init_state_manager() first.")
    return _sqlite_conn


async def close_sqlite_conn() -> None:
    global _sqlite_conn
    if _sqlite_conn is not None:
        await _sqlite_conn.close()
        _sqlite_conn = None


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
    global _state_manager, _sqlite_conn, _task_adapter
    from jobbers.state_manager import StateManager

    db_path = os.environ.get("SQLITE_PATH", "jobbers.db")
    _sqlite_conn = await aiosqlite.connect(db_path)
    await _sqlite_conn.execute("PRAGMA foreign_keys = ON")
    await create_schema(_sqlite_conn)
    client = get_client()
    _task_adapter = create_task_adapter(client)
    await _task_adapter.ensure_index()
    _state_manager = StateManager(client, _sqlite_conn, task_adapter=_task_adapter)
    return _state_manager


def get_state_manager() -> StateManager:
    if _state_manager is None:
        raise RuntimeError("State manager not initialized. Call init_state_manager() first.")
    return _state_manager
