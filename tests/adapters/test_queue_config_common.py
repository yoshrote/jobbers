"""
Protocol contract tests for QueueConfigProtocol.

Parametrized over SQLQueueConfigAdapter (in-memory SQLite) and RedisQueueConfigAdapter
(FakeAsyncRedis). Any new implementation added to the queue_config_adapter fixture
in conftest.py will automatically inherit all tests here.
"""

from __future__ import annotations

import pytest

from jobbers.models.queue_config import QueueConfig, RatePeriod

# ── get_queues / save_role ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_queues(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_role("role1", {"queue1", "queue2"})
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == {"queue1", "queue2"}


@pytest.mark.asyncio
async def test_get_queues_empty(queue_config_adapter):
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == set()


@pytest.mark.asyncio
async def test_save_role(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_role("role1", {"queue1", "queue2"})
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == {"queue1", "queue2"}


@pytest.mark.asyncio
async def test_save_role_replaces_existing(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue3"))
    await queue_config_adapter.save_role("role1", {"queue1", "queue2"})
    await queue_config_adapter.save_role("role1", {"queue3"})
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == {"queue3"}


# ── get_all_queues ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_queues(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue3"))
    queues = await queue_config_adapter.get_all_queues()
    assert set(queues) == {"queue1", "queue2", "queue3"}


@pytest.mark.asyncio
async def test_get_all_queues_empty(queue_config_adapter):
    queues = await queue_config_adapter.get_all_queues()
    assert queues == []


# ── get_all_roles ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_all_roles(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_role("role1", {"queue1"})
    await queue_config_adapter.save_role("role2", {"queue2"})
    roles = await queue_config_adapter.get_all_roles()
    assert set(roles) == {"role1", "role2"}


@pytest.mark.asyncio
async def test_get_all_roles_empty(queue_config_adapter):
    roles = await queue_config_adapter.get_all_roles()
    assert roles == []


# ── get_queue_limits ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_queue_limits_empty_set(queue_config_adapter):
    result = await queue_config_adapter.get_queue_limits(set())
    assert result == {}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_with_limit(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="test_queue", max_concurrent=5))
    result = await queue_config_adapter.get_queue_limits({"test_queue"})
    assert result == {"test_queue": 5}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_no_limit(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="unlimited_queue", max_concurrent=None))
    result = await queue_config_adapter.get_queue_limits({"unlimited_queue"})
    assert result == {"unlimited_queue": None}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_zero_limit(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="zero_limit_queue", max_concurrent=0))
    result = await queue_config_adapter.get_queue_limits({"zero_limit_queue"})
    assert result == {"zero_limit_queue": 0}


@pytest.mark.asyncio
async def test_get_queue_limits_multiple_queues(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1", max_concurrent=3))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2", max_concurrent=10))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue3", max_concurrent=1))
    result = await queue_config_adapter.get_queue_limits({"queue1", "queue2", "queue3"})
    assert result == {"queue1": 3, "queue2": 10, "queue3": 1}


@pytest.mark.asyncio
async def test_get_queue_limits_with_nonexistent_queue(queue_config_adapter):
    result = await queue_config_adapter.get_queue_limits({"nonexistent_queue"})
    assert result == {"nonexistent_queue": None}


@pytest.mark.asyncio
async def test_get_queue_limits_mixed_existing_and_nonexistent(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="existing_queue", max_concurrent=7))
    result = await queue_config_adapter.get_queue_limits({"existing_queue", "nonexistent_queue"})
    assert result == {"existing_queue": 7, "nonexistent_queue": None}


@pytest.mark.asyncio
async def test_get_queue_limits_with_rate_limiting_config(queue_config_adapter):
    await queue_config_adapter.save_queue_config(
        QueueConfig(
            name="rate_limited_queue",
            max_concurrent=15,
            rate_numerator=5,
            rate_denominator=2,
            rate_period=RatePeriod.MINUTE,
        )
    )
    result = await queue_config_adapter.get_queue_limits({"rate_limited_queue"})
    assert result == {"rate_limited_queue": 15}


@pytest.mark.asyncio
async def test_get_queue_limits_large_number_of_queues(queue_config_adapter):
    queue_names = [f"queue_{i}" for i in range(20)]
    for i, name in enumerate(queue_names):
        await queue_config_adapter.save_queue_config(QueueConfig(name=name, max_concurrent=i + 1))
    result = await queue_config_adapter.get_queue_limits(set(queue_names))
    assert len(result) == 20
    for i, name in enumerate(queue_names):
        assert result[name] == i + 1


# ── delete_queue ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_queue_removes_queue(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.delete_queue("q1")
    assert await queue_config_adapter.get_queue_config("q1") is None


@pytest.mark.asyncio
async def test_delete_queue_removes_role_queue_association(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.save_role("role1", {"q1"})
    await queue_config_adapter.delete_queue("q1")
    assert await queue_config_adapter.get_queues("role1") == set()


@pytest.mark.asyncio
async def test_delete_queue_bumps_refresh_tag_for_affected_roles(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.save_role("role1", {"q1"})
    tag_before = await queue_config_adapter.get_refresh_tag("role1")
    await queue_config_adapter.delete_queue("q1")
    tag_after = await queue_config_adapter.get_refresh_tag("role1")
    assert tag_after != tag_before


@pytest.mark.asyncio
async def test_delete_queue_does_not_bump_unaffected_roles(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="q2"))
    await queue_config_adapter.save_role("role1", {"q1"})
    await queue_config_adapter.save_role("role2", {"q2"})
    tag_before = await queue_config_adapter.get_refresh_tag("role2")
    await queue_config_adapter.delete_queue("q1")
    tag_after = await queue_config_adapter.get_refresh_tag("role2")
    assert tag_after == tag_before


@pytest.mark.asyncio
async def test_delete_queue_leaves_other_queues_intact(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="q2"))
    await queue_config_adapter.delete_queue("q1")
    assert await queue_config_adapter.get_queue_config("q2") is not None


@pytest.mark.asyncio
async def test_delete_queue_nonexistent_is_silent(queue_config_adapter):
    await queue_config_adapter.delete_queue("does_not_exist")


# ── delete_role ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_role_removes_role(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.save_role("role1", {"q1"})
    await queue_config_adapter.delete_role("role1")
    assert "role1" not in await queue_config_adapter.get_all_roles()


@pytest.mark.asyncio
async def test_delete_role_removes_role_queue_associations(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.save_role("role1", {"q1"})
    await queue_config_adapter.delete_role("role1")
    assert await queue_config_adapter.get_queues("role1") == set()


@pytest.mark.asyncio
async def test_delete_role_preserves_queue_configs(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.save_role("role1", {"q1"})
    await queue_config_adapter.delete_role("role1")
    assert await queue_config_adapter.get_queue_config("q1") is not None


@pytest.mark.asyncio
async def test_delete_role_leaves_other_roles_intact(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="q2"))
    await queue_config_adapter.save_role("role1", {"q1"})
    await queue_config_adapter.save_role("role2", {"q2"})
    await queue_config_adapter.delete_role("role1")
    assert "role2" in await queue_config_adapter.get_all_roles()
    assert await queue_config_adapter.get_queues("role2") == {"q2"}


@pytest.mark.asyncio
async def test_delete_role_nonexistent_is_silent(queue_config_adapter):
    await queue_config_adapter.delete_role("does_not_exist")


# ── get_queue_config / save_queue_config ──────────────────────────────────────


@pytest.mark.asyncio
async def test_save_and_get_queue_config(queue_config_adapter):
    cfg = QueueConfig(name="myqueue", max_concurrent=3)
    await queue_config_adapter.save_queue_config(cfg)
    result = await queue_config_adapter.get_queue_config("myqueue")
    assert result is not None
    assert result.name == "myqueue"
    assert result.max_concurrent == 3


@pytest.mark.asyncio
async def test_get_queue_config_returns_none_for_missing(queue_config_adapter):
    result = await queue_config_adapter.get_queue_config("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_save_queue_config_with_rate_limiting(queue_config_adapter):
    cfg = QueueConfig(
        name="limited",
        max_concurrent=5,
        rate_numerator=10,
        rate_denominator=2,
        rate_period=RatePeriod.MINUTE,
    )
    await queue_config_adapter.save_queue_config(cfg)
    result = await queue_config_adapter.get_queue_config("limited")
    assert result is not None
    assert result.rate_numerator == 10
    assert result.rate_denominator == 2
    assert result.rate_period == RatePeriod.MINUTE


@pytest.mark.asyncio
async def test_save_queue_config_overwrites_existing(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="q", max_concurrent=1))
    await queue_config_adapter.save_queue_config(QueueConfig(name="q", max_concurrent=9))
    result = await queue_config_adapter.get_queue_config("q")
    assert result is not None
    assert result.max_concurrent == 9


# ── refresh tags ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_refresh_tag_creates_and_returns_consistent(queue_config_adapter):
    tag1 = await queue_config_adapter.get_refresh_tag("role_a")
    assert tag1 is not None
    tag2 = await queue_config_adapter.get_refresh_tag("role_a")
    assert tag1 == tag2


@pytest.mark.asyncio
async def test_bump_refresh_tag_returns_new_value(queue_config_adapter):
    tag_before = await queue_config_adapter.get_refresh_tag("role_b")
    new_tag_str = await queue_config_adapter.bump_refresh_tag("role_b")
    tag_after = await queue_config_adapter.get_refresh_tag("role_b")
    assert str(tag_after) == new_tag_str
    assert tag_after != tag_before


@pytest.mark.asyncio
async def test_bump_refresh_tags_for_queue_affects_containing_roles(queue_config_adapter):
    for q in ("shared_q", "other_q", "unrelated_q"):
        await queue_config_adapter.save_queue_config(QueueConfig(name=q))
    await queue_config_adapter.save_role("role_x", {"shared_q", "other_q"})
    await queue_config_adapter.save_role("role_y", {"shared_q"})
    await queue_config_adapter.save_role("role_z", {"unrelated_q"})

    tag_x_before = await queue_config_adapter.get_refresh_tag("role_x")
    tag_z_before = await queue_config_adapter.get_refresh_tag("role_z")

    affected = await queue_config_adapter.bump_refresh_tags_for_queue("shared_q")
    assert set(affected) == {"role_x", "role_y"}

    tag_x_after = await queue_config_adapter.get_refresh_tag("role_x")
    tag_z_after = await queue_config_adapter.get_refresh_tag("role_z")
    assert tag_x_after != tag_x_before
    assert tag_z_after == tag_z_before


@pytest.mark.asyncio
async def test_bump_refresh_tags_for_queue_no_roles_returns_empty(queue_config_adapter):
    affected = await queue_config_adapter.bump_refresh_tags_for_queue("any_queue")
    assert affected == []


@pytest.mark.asyncio
async def test_bump_refresh_tags_for_queue_no_matching_role_returns_empty(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="other_q"))
    await queue_config_adapter.save_role("r", {"other_q"})
    tag_before = await queue_config_adapter.get_refresh_tag("r")

    affected = await queue_config_adapter.bump_refresh_tags_for_queue("missing_q")

    assert affected == []
    assert await queue_config_adapter.get_refresh_tag("r") == tag_before
