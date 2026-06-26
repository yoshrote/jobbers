from unittest.mock import AsyncMock, MagicMock

import pytest

from jobbers.migrations import runner


@pytest.fixture(autouse=True)
def _reset_sql_env(monkeypatch):
    monkeypatch.setenv("SQL_PATH", "sqlite+aiosqlite:///:memory:")


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
