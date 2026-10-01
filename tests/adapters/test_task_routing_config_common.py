"""
Protocol contract tests for TaskRoutingConfigProtocol.

Parametrized over SQLTaskRoutingConfigAdapter (in-memory SQLite) and
RedisTaskRoutingConfigAdapter (the shared real Redis connection). Any new implementation
added to the task_routing_config_adapter fixture in conftest.py will automatically
inherit all tests here.
"""

from __future__ import annotations

import pytest

from jobbers.models.task_routing import RoutingConfig, RoutingRule, RoutingStrategy


@pytest.mark.asyncio
async def test_get_routing_config_returns_none_when_absent(task_routing_config_adapter):
    result = await task_routing_config_adapter.get_routing_config("unknown_task", 1)
    assert result is None


@pytest.mark.asyncio
async def test_save_and_get_single(task_routing_config_adapter):
    config = RoutingConfig(
        task_name="echo",
        task_version=1,
        rules=[RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["fast"])],
    )
    await task_routing_config_adapter.save_routing_config(config)
    result = await task_routing_config_adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.rules[0].strategy == RoutingStrategy.SINGLE
    assert result.rules[0].queues == ["fast"]
    assert result.rules[0].weights is None


@pytest.mark.asyncio
async def test_save_and_get_weighted(task_routing_config_adapter):
    config = RoutingConfig(
        task_name="echo",
        task_version=1,
        rules=[RoutingRule(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[2.0, 1.0])],
    )
    await task_routing_config_adapter.save_routing_config(config)
    result = await task_routing_config_adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.rules[0].strategy == RoutingStrategy.WEIGHTED
    assert result.rules[0].queues == ["fast", "slow"]
    assert result.rules[0].weights == [2.0, 1.0]


@pytest.mark.asyncio
async def test_save_upserts_existing(task_routing_config_adapter):
    config1 = RoutingConfig(
        task_name="echo",
        task_version=1,
        rules=[RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["fast"])],
    )
    await task_routing_config_adapter.save_routing_config(config1)

    config2 = RoutingConfig(
        task_name="echo",
        task_version=1,
        rules=[RoutingRule(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[1.0, 1.0])],
    )
    await task_routing_config_adapter.save_routing_config(config2)

    result = await task_routing_config_adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.rules[0].strategy == RoutingStrategy.WEIGHTED


@pytest.mark.asyncio
async def test_configs_are_isolated_by_version(task_routing_config_adapter):
    v1 = RoutingConfig(
        task_name="echo",
        task_version=1,
        rules=[RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["fast"])],
    )
    v2 = RoutingConfig(
        task_name="echo",
        task_version=2,
        rules=[RoutingRule(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[1.0, 1.0])],
    )
    await task_routing_config_adapter.save_routing_config(v1)
    await task_routing_config_adapter.save_routing_config(v2)

    r1 = await task_routing_config_adapter.get_routing_config("echo", 1)
    r2 = await task_routing_config_adapter.get_routing_config("echo", 2)
    assert r1 is not None
    assert r1.rules[0].strategy == RoutingStrategy.SINGLE
    assert r2 is not None
    assert r2.rules[0].strategy == RoutingStrategy.WEIGHTED


@pytest.mark.asyncio
async def test_delete_returns_false_when_absent(task_routing_config_adapter):
    deleted = await task_routing_config_adapter.delete_routing_config("ghost", 99)
    assert deleted is False


@pytest.mark.asyncio
async def test_delete_removes_config(task_routing_config_adapter):
    config = RoutingConfig(
        task_name="echo",
        task_version=1,
        rules=[RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["fast"])],
    )
    await task_routing_config_adapter.save_routing_config(config)
    deleted = await task_routing_config_adapter.delete_routing_config("echo", 1)
    assert deleted is True
    assert await task_routing_config_adapter.get_routing_config("echo", 1) is None


@pytest.mark.asyncio
async def test_delete_second_call_returns_false(task_routing_config_adapter):
    config = RoutingConfig(
        task_name="echo",
        task_version=1,
        rules=[RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["fast"])],
    )
    await task_routing_config_adapter.save_routing_config(config)
    await task_routing_config_adapter.delete_routing_config("echo", 1)
    assert await task_routing_config_adapter.delete_routing_config("echo", 1) is False


# ── lane-scoped rules ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_and_get_multiple_lane_scoped_rules(task_routing_config_adapter):
    """Every rule in a config survives a save/get cycle, lane scoping included."""
    config = RoutingConfig(
        task_name="fulfil_order",
        task_version=1,
        rules=[
            RoutingRule(from_lane="priority", strategy=RoutingStrategy.SINGLE, queues=["fast"]),
            RoutingRule(
                from_lane="standard",
                strategy=RoutingStrategy.WEIGHTED,
                queues=["bulk_a", "bulk_b"],
                weights=[2.0, 1.0],
            ),
            RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["catch_all"]),
        ],
    )
    await task_routing_config_adapter.save_routing_config(config)
    result = await task_routing_config_adapter.get_routing_config("fulfil_order", 1)

    assert result is not None
    assert result.rule_for("priority").queues == ["fast"]
    assert result.rule_for("standard").queues == ["bulk_a", "bulk_b"]
    assert result.rule_for("standard").weights == [2.0, 1.0]
    # Any unlisted lane falls back to the wildcard rule.
    assert result.rule_for("anything_else").queues == ["catch_all"]


@pytest.mark.asyncio
async def test_lane_without_matching_rule_has_no_rule(task_routing_config_adapter):
    """With no wildcard rule stored, an unlisted lane resolves to no rule at all."""
    config = RoutingConfig(
        task_name="fulfil_order",
        task_version=1,
        rules=[RoutingRule(from_lane="priority", strategy=RoutingStrategy.SINGLE, queues=["fast"])],
    )
    await task_routing_config_adapter.save_routing_config(config)
    result = await task_routing_config_adapter.get_routing_config("fulfil_order", 1)

    assert result is not None
    assert result.rule_for("standard") is None


@pytest.mark.asyncio
async def test_save_replaces_rules_rather_than_merging(task_routing_config_adapter):
    """Saving a config drops rules the new config does not carry."""
    await task_routing_config_adapter.save_routing_config(
        RoutingConfig(
            task_name="echo",
            task_version=1,
            rules=[
                RoutingRule(from_lane="a", strategy=RoutingStrategy.SINGLE, queues=["qa"]),
                RoutingRule(from_lane="b", strategy=RoutingStrategy.SINGLE, queues=["qb"]),
            ],
        )
    )
    await task_routing_config_adapter.save_routing_config(
        RoutingConfig(
            task_name="echo",
            task_version=1,
            rules=[RoutingRule(from_lane="a", strategy=RoutingStrategy.SINGLE, queues=["qa2"])],
        )
    )
    result = await task_routing_config_adapter.get_routing_config("echo", 1)

    assert result is not None
    assert len(result.rules) == 1
    assert result.rule_for("a").queues == ["qa2"]
    assert result.rule_for("b") is None
