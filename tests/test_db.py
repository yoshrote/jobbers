from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.engine import make_url

import jobbers.db as db
from jobbers.db import (
    DEFAULT_REDIS_URL,
    REDIS_PROTOCOL_VERSION,
    close_client,
    get_client,
    needed_sql_features,
    set_client,
)


@pytest.fixture
def mock_redis():
    """Fixture to reset the mocked Redis before each test."""
    with patch("jobbers.db._client", None):
        with patch("jobbers.db.redis.from_url", return_value=AsyncMock()) as mock_redis:
            yield mock_redis


@pytest.mark.asyncio
async def test_get_client_creates_new_client(mock_redis):
    """Test that get_client creates a new Redis client if none exists."""
    client = get_client()
    assert client is not None
    mock_redis.assert_called_once_with(
        DEFAULT_REDIS_URL, protocol=REDIS_PROTOCOL_VERSION, legacy_responses=False, socket_timeout=None
    )


@pytest.mark.asyncio
async def test_get_client_uses_existing_client(mock_redis):
    """Test that get_client reuses an existing Redis client."""
    # First call to create the client
    client1 = get_client()
    # Second call should reuse the same client
    client2 = get_client()

    assert client1 is client2
    mock_redis.assert_called_once_with(
        DEFAULT_REDIS_URL, protocol=REDIS_PROTOCOL_VERSION, legacy_responses=False, socket_timeout=None
    )


@pytest.mark.asyncio
async def test_set_client_replaces_existing_client():
    """Test that set_client replaces an existing Redis client."""
    old_client = AsyncMock()
    new_client = AsyncMock()

    with patch("jobbers.db._client", old_client):
        await set_client(new_client)

        # Ensure the old client is closed
        old_client.close.assert_called_once()
        # Ensure the new client is set
        assert get_client() is new_client


@pytest.mark.asyncio
async def test_close_client_closes_existing_client():
    """Test that close_client closes the existing Redis client."""
    client = AsyncMock()

    with patch("jobbers.db._client", client):
        await close_client()

        # Ensure the client is closed
        client.close.assert_called_once()
        # Ensure the global client is set to None
        assert get_client() is not client


@pytest.mark.asyncio
async def test_close_client_no_existing_client():
    """Test that close_client does nothing if no client exists."""
    with patch("jobbers.db._client", None):
        # Ensure no exception is raised
        await close_client()


def test_needed_sql_features_all_redis_is_empty(monkeypatch):
    """No backend set to sql -- jobbers_migrate should create nothing."""
    monkeypatch.setattr(db, "TASK_BACKEND", "redis")
    monkeypatch.setattr(db, "DLQ_BACKEND", "redis")
    monkeypatch.setattr(db, "TASK_SCHEDULER_BACKEND", "redis")
    monkeypatch.setattr(db, "CRON_DAG_SCHEDULER_BACKEND", "redis")
    monkeypatch.setenv("ROUTING_BACKEND", "redis")

    assert needed_sql_features() == set()


def test_needed_sql_features_only_configured_backends(monkeypatch):
    """Only the backends actually set to sql contribute a feature."""
    monkeypatch.setattr(db, "TASK_BACKEND", "sql")
    monkeypatch.setattr(db, "DLQ_BACKEND", "redis")
    monkeypatch.setattr(db, "TASK_SCHEDULER_BACKEND", "sql")
    monkeypatch.setattr(db, "CRON_DAG_SCHEDULER_BACKEND", "redis")
    monkeypatch.setenv("ROUTING_BACKEND", "redis_json")

    assert needed_sql_features() == {"task_state", "task_schedule"}


@pytest.mark.asyncio
async def test_get_or_create_sql_pops_pool_params_from_url_for_non_sqlite(monkeypatch):
    """?pool_size=...&max_overflow=...&pool_timeout=... on SQL_PATH are popped off the URL and
    forwarded as engine kwargs -- SQLAlchemy would otherwise forward them straight to the
    DBAPI driver's connect(), which rejects unrecognized kwargs."""
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(db, "_session_factory", None)
    monkeypatch.setenv(
        "SQL_PATH",
        "postgresql+asyncpg://user:pass@host/db?pool_size=20&max_overflow=5&pool_timeout=10",
    )

    mock_engine = MagicMock()
    mock_engine.sync_engine = MagicMock()

    with (
        patch("jobbers.db.create_async_engine", return_value=mock_engine) as mock_create,
        patch("jobbers.db.event.listens_for", return_value=lambda fn: fn),
        patch("jobbers.db.run_migrations", new=AsyncMock()),
        patch("jobbers.db.async_sessionmaker", return_value=AsyncMock()),
    ):
        await db._get_or_create_sql(set())

    mock_create.assert_called_once_with(
        make_url("postgresql+asyncpg://user:pass@host/db"),
        pool_size=20,
        max_overflow=5,
        pool_timeout=10.0,
    )


@pytest.mark.asyncio
async def test_get_or_create_sql_ignores_pool_params_for_sqlite(monkeypatch):
    """Pool params are never popped/forwarded for SQLite DSNs -- its pool class (StaticPool)
    rejects them outright."""
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(db, "_session_factory", None)
    sql_path = "sqlite+aiosqlite:///:memory:?pool_size=20&max_overflow=5"
    monkeypatch.setenv("SQL_PATH", sql_path)

    mock_engine = MagicMock()
    mock_engine.sync_engine = MagicMock()

    with (
        patch("jobbers.db.create_async_engine", return_value=mock_engine) as mock_create,
        patch("jobbers.db.event.listens_for", return_value=lambda fn: fn),
        patch("jobbers.db.run_migrations", new=AsyncMock()),
        patch("jobbers.db.async_sessionmaker", return_value=AsyncMock()),
    ):
        await db._get_or_create_sql(set())

    mock_create.assert_called_once_with(make_url(sql_path))


def test_needed_sql_features_default_routing_is_sql(monkeypatch):
    """ROUTING_BACKEND defaults to sql, so routing is included even with no env vars set."""
    monkeypatch.setattr(db, "TASK_BACKEND", "redis_json")
    monkeypatch.setattr(db, "DLQ_BACKEND", "redis")
    monkeypatch.setattr(db, "TASK_SCHEDULER_BACKEND", "redis")
    monkeypatch.setattr(db, "CRON_DAG_SCHEDULER_BACKEND", "redis")
    monkeypatch.delenv("ROUTING_BACKEND", raising=False)

    assert needed_sql_features() == {"routing"}
