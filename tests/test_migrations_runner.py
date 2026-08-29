from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine

from jobbers.migrations import runner


@pytest.fixture(autouse=True)
def _reset_sql_env(monkeypatch):
    monkeypatch.setenv("SQL_PATH", "sqlite+aiosqlite:///:memory:")


@pytest.mark.asyncio
async def test_run_migrations_empty_features_creates_no_tables():
    """An explicitly empty (non-None) features set must create nothing, not fall back to 'all tables'."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        await runner.run_migrations(engine, features=set())
        async with engine.connect() as conn:
            table_names = await conn.run_sync(lambda c: inspect(c).get_table_names())
        assert table_names == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_run_migrations_features_creates_only_requested_tables():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        await runner.run_migrations(engine, features={"dead_letter"})
        async with engine.connect() as conn:
            table_names = await conn.run_sync(lambda c: inspect(c).get_table_names())
        assert table_names == ["dead_letter_queue"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_run_cli_only_migrates_backends_set_to_sql(monkeypatch):
    """jobbers_migrate should only create tables for backends actually configured as sql."""
    monkeypatch.setattr("jobbers.db.TASK_BACKEND", "sql")
    monkeypatch.setattr("jobbers.db.DLQ_BACKEND", "redis")
    monkeypatch.setattr("jobbers.db.TASK_SCHEDULER_BACKEND", "redis")
    monkeypatch.setattr("jobbers.db.CRON_DAG_SCHEDULER_BACKEND", "redis")
    monkeypatch.setenv("ROUTING_BACKEND", "redis")

    captured = {}

    async def fake_run_migrations(engine, features=None):
        captured["features"] = features

    monkeypatch.setattr(runner, "run_migrations", fake_run_migrations)

    await runner.run_cli()

    assert captured["features"] == {"task_state"}


@pytest.mark.asyncio
async def test_run_cli_skips_redis_json_indexes_by_default(monkeypatch):
    monkeypatch.delenv("ROUTING_BACKEND", raising=False)
    called = False

    async def fake_ensure(redis_url):
        nonlocal called
        called = True

    monkeypatch.setattr(runner, "ensure_redis_json_routing_indexes", fake_ensure)

    await runner.run_cli()

    assert called is False


@pytest.mark.asyncio
async def test_run_cli_creates_redis_json_indexes_when_configured(monkeypatch):
    monkeypatch.setenv("ROUTING_BACKEND", "redis_json")
    seen_url = None

    async def fake_ensure(redis_url):
        nonlocal seen_url
        seen_url = redis_url

    monkeypatch.setattr(runner, "ensure_redis_json_routing_indexes", fake_ensure)

    await runner.run_cli()

    assert seen_url == "redis://localhost:6379"


@pytest.mark.asyncio
async def test_ensure_redis_json_routing_indexes_creates_indexes_and_closes_client(monkeypatch):
    from jobbers.adapters.redis_json import RedisJSONRoutingBackend

    fake_client = MagicMock()
    fake_client.close = AsyncMock()
    ensure_indexes_called = False

    async def fake_ensure_indexes(self):
        nonlocal ensure_indexes_called
        ensure_indexes_called = True

    monkeypatch.setattr(runner.redis, "from_url", lambda url, **kwargs: fake_client)
    monkeypatch.setattr(RedisJSONRoutingBackend, "ensure_indexes", fake_ensure_indexes)

    await runner.ensure_redis_json_routing_indexes("redis://example")

    assert ensure_indexes_called is True
    fake_client.close.assert_awaited_once()
