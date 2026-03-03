from __future__ import annotations

import os
from typing import TYPE_CHECKING

import aiosqlite
import redis.asyncio as redis

# from jobbers.models.queue_config import create_schema

if TYPE_CHECKING:
    from jobbers.state_manager import StateManager

_client: redis.Redis | None = None
_state_manager: StateManager | None = None
_sqlite_conn: aiosqlite.Connection | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))
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


async def init_state_manager() -> StateManager:
    global _state_manager, _sqlite_conn
    from jobbers.state_manager import StateManager
    db_path = os.environ.get("SQLITE_PATH", "jobbers.db")
    _sqlite_conn = await aiosqlite.connect(db_path)
    await _sqlite_conn.execute("PRAGMA foreign_keys = ON")
    # await create_schema(_sqlite_conn)
    _state_manager = StateManager(get_client(), _sqlite_conn)
    return _state_manager


def get_state_manager() -> StateManager:
    if _state_manager is None:
        raise RuntimeError("State manager not initialized. Call init_state_manager() first.")
    return _state_manager
