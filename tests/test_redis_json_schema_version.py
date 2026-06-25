"""
Tests for the RedisJSON schema-version gate.

Uses a mocked `.ft()` so they run against fakeredis without requiring a real Redis Stack instance.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis
import pytest

from jobbers.adapters.redis_json import RedisJSONDeadQueue, RedisJSONTaskState
from jobbers.adapters.redis_json._helpers import _stale_index_names
from jobbers.adapters.redis_json.routing_backend import RedisJSONQueueConfigAdapter


def _mock_ft() -> MagicMock:
    mock_search = MagicMock()
    mock_search.info = AsyncMock(return_value={"attributes": []})
    mock_search.create_index = AsyncMock()
    mock_search.alter_schema_add = AsyncMock()
    return mock_search


@pytest.mark.asyncio
async def test_task_state_ensure_index_skips_when_already_at_current_version():
    client = fakeredis.FakeAsyncRedis()
    state = RedisJSONTaskState(client)
    await client.set(state._VERSION_KEY, state.SCHEMA_VERSION)

    with patch.object(client, "ft", return_value=_mock_ft()) as ft:
        await state.ensure_index()

    ft.assert_not_called()


@pytest.mark.asyncio
async def test_task_state_ensure_index_runs_and_stamps_version_when_stale():
    client = fakeredis.FakeAsyncRedis()
    state = RedisJSONTaskState(client)

    with patch.object(client, "ft", return_value=_mock_ft()) as ft:
        await state.ensure_index()

    ft.assert_called()
    assert await client.get(state._VERSION_KEY) == str(state.SCHEMA_VERSION).encode()


@pytest.mark.asyncio
async def test_dead_queue_ensure_index_skips_when_already_at_current_version():
    client = fakeredis.FakeAsyncRedis()
    dq = RedisJSONDeadQueue(client, task_adapter=MagicMock())
    await client.set(dq._VERSION_KEY, dq.SCHEMA_VERSION)

    with patch.object(client, "ft", return_value=_mock_ft()) as ft:
        await dq.ensure_index()

    ft.assert_not_called()


@pytest.mark.asyncio
async def test_routing_queue_config_ensure_indexes_skips_when_already_at_current_version():
    client = fakeredis.FakeAsyncRedis()
    qca = RedisJSONQueueConfigAdapter(client)
    await client.set(qca._VERSION_KEY, qca.SCHEMA_VERSION)

    with patch.object(client, "ft", return_value=_mock_ft()) as ft:
        await qca.ensure_indexes()

    ft.assert_not_called()


# ── _stale_index_names ──────────────────────────────────────────────────────


def test_stale_index_names_matches_older_generations_only():
    existing = ["task-idx-v1", "task-idx-v2", "task-idx-v3", "unrelated-idx"]

    stale = _stale_index_names(existing, current_name="task-idx-v3", current_version=3)

    assert stale == ["task-idx-v1", "task-idx-v2"]


def test_stale_index_names_returns_empty_when_only_current_exists():
    stale = _stale_index_names(["task-idx-v1"], current_name="task-idx-v1", current_version=1)

    assert stale == []


# ── drop_stale_indexes ───────────────────────────────────────────────────────


def _mock_client_with_indexes(*names: str) -> MagicMock:
    client = MagicMock()
    client.execute_command = AsyncMock(return_value=list(names))
    client.ft = MagicMock(return_value=MagicMock(dropindex=AsyncMock()))
    return client


@pytest.mark.asyncio
async def test_task_state_drop_stale_indexes_drops_only_older_generations():
    client = _mock_client_with_indexes("task-idx-v1", "task-idx-v2")
    state = RedisJSONTaskState(client)
    state.SCHEMA_VERSION = 2
    state.INDEX_NAME = "task-idx-v2"

    dropped = await state.drop_stale_indexes()

    assert dropped == ["task-idx-v1"]
    client.ft.assert_called_once_with("task-idx-v1")


@pytest.mark.asyncio
async def test_dead_queue_drop_stale_indexes_drops_only_older_generations():
    client = _mock_client_with_indexes("dlq-json-idx-v1", "dlq-json-idx-v2")
    dq = RedisJSONDeadQueue(client, task_adapter=MagicMock())
    dq.SCHEMA_VERSION = 2
    dq.INDEX_NAME = "dlq-json-idx-v2"

    dropped = await dq.drop_stale_indexes()

    assert dropped == ["dlq-json-idx-v1"]
    client.ft.assert_called_once_with("dlq-json-idx-v1")


@pytest.mark.asyncio
async def test_routing_queue_config_drop_stale_indexes_drops_both_index_generations():
    client = _mock_client_with_indexes(
        "routing_queue_idx_v1", "routing_queue_idx_v2", "routing_role_idx_v1", "routing_role_idx_v2"
    )
    qca = RedisJSONQueueConfigAdapter(client)
    qca.SCHEMA_VERSION = 2
    qca.QUEUE_IDX = "routing_queue_idx_v2"
    qca.ROLE_IDX = "routing_role_idx_v2"

    dropped = await qca.drop_stale_indexes()

    assert sorted(dropped) == ["routing_queue_idx_v1", "routing_role_idx_v1"]
