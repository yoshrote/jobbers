"""
Protocol contract tests for TaskRoutingConfigProtocol.

Parametrized over SQLTaskRoutingConfigAdapter (in-memory SQLite) and
RedisTaskRoutingConfigAdapter (FakeAsyncRedis). Any new implementation added to
the task_routing_config_adapter fixture in conftest.py will automatically
inherit all tests here.
"""

from __future__ import annotations

import pytest

from jobbers.models.task_routing import RoutingConfig, RoutingStrategy


@pytest.mark.asyncio
async def test_get_routing_config_returns_none_when_absent(task_routing_config_adapter):
    result = await task_routing_config_adapter.get_routing_config("unknown_task", 1)
    assert result is None


@pytest.mark.asyncio
async def test_save_and_get_single(task_routing_config_adapter):
    config = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    await task_routing_config_adapter.save_routing_config(config)
    result = await task_routing_config_adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.strategy == RoutingStrategy.SINGLE
    assert result.queues == ["fast"]
    assert result.weights is None


@pytest.mark.asyncio
async def test_save_and_get_weighted(task_routing_config_adapter):
    config = RoutingConfig(
        task_name="echo",
        task_version=1,
        strategy=RoutingStrategy.WEIGHTED,
        queues=["fast", "slow"],
        weights=[2.0, 1.0],
    )
    await task_routing_config_adapter.save_routing_config(config)
    result = await task_routing_config_adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.strategy == RoutingStrategy.WEIGHTED
    assert result.queues == ["fast", "slow"]
    assert result.weights == [2.0, 1.0]


@pytest.mark.asyncio
async def test_save_upserts_existing(task_routing_config_adapter):
    config1 = RoutingConfig(
        task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"]
    )
    await task_routing_config_adapter.save_routing_config(config1)

    config2 = RoutingConfig(
        task_name="echo",
        task_version=1,
        strategy=RoutingStrategy.WEIGHTED,
        queues=["fast", "slow"],
        weights=[1.0, 1.0],
    )
    await task_routing_config_adapter.save_routing_config(config2)

    result = await task_routing_config_adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.strategy == RoutingStrategy.WEIGHTED


@pytest.mark.asyncio
async def test_configs_are_isolated_by_version(task_routing_config_adapter):
    v1 = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    v2 = RoutingConfig(
        task_name="echo",
        task_version=2,
        strategy=RoutingStrategy.WEIGHTED,
        queues=["fast", "slow"],
        weights=[1.0, 1.0],
    )
    await task_routing_config_adapter.save_routing_config(v1)
    await task_routing_config_adapter.save_routing_config(v2)

    r1 = await task_routing_config_adapter.get_routing_config("echo", 1)
    r2 = await task_routing_config_adapter.get_routing_config("echo", 2)
    assert r1 is not None
    assert r1.strategy == RoutingStrategy.SINGLE
    assert r2 is not None
    assert r2.strategy == RoutingStrategy.WEIGHTED


@pytest.mark.asyncio
async def test_delete_returns_false_when_absent(task_routing_config_adapter):
    deleted = await task_routing_config_adapter.delete_routing_config("ghost", 99)
    assert deleted is False


@pytest.mark.asyncio
async def test_delete_removes_config(task_routing_config_adapter):
    config = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    await task_routing_config_adapter.save_routing_config(config)
    deleted = await task_routing_config_adapter.delete_routing_config("echo", 1)
    assert deleted is True
    assert await task_routing_config_adapter.get_routing_config("echo", 1) is None


@pytest.mark.asyncio
async def test_delete_second_call_returns_false(task_routing_config_adapter):
    config = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    await task_routing_config_adapter.save_routing_config(config)
    await task_routing_config_adapter.delete_routing_config("echo", 1)
    assert await task_routing_config_adapter.delete_routing_config("echo", 1) is False
