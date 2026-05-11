"""
Protocol contract tests for RoutingBackendProtocol.

Parametrized over SQLRoutingBackend (in-memory SQLite) and RedisRoutingBackend
(FakeAsyncRedis). Any new backend that should satisfy the protocol can be added
to the routing_backend fixture here and all tests run automatically.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from jobbers.adapters.redis import RedisRoutingBackend
from jobbers.adapters.redis_json import RedisJSONRoutingBackend
from jobbers.adapters.sql import SQLRoutingBackend
from jobbers.db import DEFAULT_REDIS_URL
from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task_routing import RoutingConfig, RoutingStrategy


@pytest_asyncio.fixture(params=["sql", "redis", "redis_json"], ids=["sql", "redis", "redis_json"])
async def routing_backend(request, session_factory, redis):
    if request.param == "sql":
        yield SQLRoutingBackend(session_factory)
    elif request.param == "redis":
        yield RedisRoutingBackend(redis)
    else:
        r = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
        try:
            await r.flushdb()
        except RedisConnectionError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis not available: {exc}")
        backend = RedisJSONRoutingBackend(r)
        try:
            await backend.ensure_indexes()
        except ResponseError as exc:  # pragma: no cover
            await r.aclose()
            pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
        yield backend
        await r.flushdb()
        await r.aclose()


# ── queue CRUD ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_and_get_queue_config(routing_backend):
    cfg = QueueConfig(name="myqueue", max_concurrent=3)
    await routing_backend.save_queue_config(cfg)
    result = await routing_backend.get_queue_config("myqueue")
    assert result is not None
    assert result.name == "myqueue"
    assert result.max_concurrent == 3


@pytest.mark.asyncio
async def test_get_queue_config_returns_none_for_missing(routing_backend):
    result = await routing_backend.get_queue_config("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_save_queue_config_with_rate_limiting(routing_backend):
    cfg = QueueConfig(
        name="limited",
        max_concurrent=5,
        rate_numerator=10,
        rate_denominator=2,
        rate_period=RatePeriod.MINUTE,
    )
    await routing_backend.save_queue_config(cfg)
    result = await routing_backend.get_queue_config("limited")
    assert result is not None
    assert result.rate_numerator == 10
    assert result.rate_denominator == 2
    assert result.rate_period == RatePeriod.MINUTE


@pytest.mark.asyncio
async def test_save_queue_config_overwrites_existing(routing_backend):
    await routing_backend.save_queue_config(QueueConfig(name="q", max_concurrent=1))
    await routing_backend.save_queue_config(QueueConfig(name="q", max_concurrent=9))
    result = await routing_backend.get_queue_config("q")
    assert result is not None
    assert result.max_concurrent == 9


@pytest.mark.asyncio
async def test_get_all_queues_returns_sorted(routing_backend):
    await routing_backend.save_queue_config(QueueConfig(name="zebra"))
    await routing_backend.save_queue_config(QueueConfig(name="alpha"))
    await routing_backend.save_queue_config(QueueConfig(name="mango"))
    queues = await routing_backend.get_all_queues()
    assert queues == ["alpha", "mango", "zebra"]


@pytest.mark.asyncio
async def test_delete_queue(routing_backend):
    await routing_backend.save_queue_config(QueueConfig(name="todelete"))
    await routing_backend.delete_queue("todelete")
    result = await routing_backend.get_queue_config("todelete")
    assert result is None
    all_queues = await routing_backend.get_all_queues()
    assert "todelete" not in all_queues


# ── role CRUD ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_and_get_role(routing_backend):
    await routing_backend.save_queue_config(QueueConfig(name="q1"))
    await routing_backend.save_queue_config(QueueConfig(name="q2"))
    await routing_backend.save_role("workers", {"q1", "q2"})
    queues = await routing_backend.get_queues("workers")
    assert queues == {"q1", "q2"}


@pytest.mark.asyncio
async def test_get_queues_returns_empty_for_unknown_role(routing_backend):
    queues = await routing_backend.get_queues("unknown_role")
    assert queues == set()


@pytest.mark.asyncio
async def test_save_role_overwrites_queues(routing_backend):
    await routing_backend.save_queue_config(QueueConfig(name="old"))
    await routing_backend.save_queue_config(QueueConfig(name="new1"))
    await routing_backend.save_queue_config(QueueConfig(name="new2"))
    await routing_backend.save_role("r", {"old"})
    await routing_backend.save_role("r", {"new1", "new2"})
    queues = await routing_backend.get_queues("r")
    assert queues == {"new1", "new2"}


@pytest.mark.asyncio
async def test_get_all_roles_returns_sorted(routing_backend):
    await routing_backend.save_role("zebra", set())
    await routing_backend.save_role("alpha", set())
    roles = await routing_backend.get_all_roles()
    assert roles == ["alpha", "zebra"]


@pytest.mark.asyncio
async def test_delete_role(routing_backend):
    await routing_backend.save_queue_config(QueueConfig(name="q"))
    await routing_backend.save_role("todelete", {"q"})
    await routing_backend.delete_role("todelete")
    queues = await routing_backend.get_queues("todelete")
    assert queues == set()
    all_roles = await routing_backend.get_all_roles()
    assert "todelete" not in all_roles


# ── refresh tags ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_refresh_tag_creates_and_returns_consistent(routing_backend):
    tag1 = await routing_backend.get_refresh_tag("role_a")
    assert tag1 is not None
    tag2 = await routing_backend.get_refresh_tag("role_a")
    assert tag1 == tag2


@pytest.mark.asyncio
async def test_bump_refresh_tag_returns_new_value(routing_backend):
    tag_before = await routing_backend.get_refresh_tag("role_b")
    new_tag_str = await routing_backend.bump_refresh_tag("role_b")
    tag_after = await routing_backend.get_refresh_tag("role_b")
    assert str(tag_after) == new_tag_str
    assert tag_after != tag_before


@pytest.mark.asyncio
async def test_bump_refresh_tags_for_queue_affects_containing_roles(routing_backend):
    for q in ("shared_q", "other_q", "unrelated_q"):
        await routing_backend.save_queue_config(QueueConfig(name=q))
    await routing_backend.save_role("role_x", {"shared_q", "other_q"})
    await routing_backend.save_role("role_y", {"shared_q"})
    await routing_backend.save_role("role_z", {"unrelated_q"})

    tag_x_before = await routing_backend.get_refresh_tag("role_x")
    tag_z_before = await routing_backend.get_refresh_tag("role_z")

    affected = await routing_backend.bump_refresh_tags_for_queue("shared_q")
    assert set(affected) == {"role_x", "role_y"}

    tag_x_after = await routing_backend.get_refresh_tag("role_x")
    tag_z_after = await routing_backend.get_refresh_tag("role_z")
    assert tag_x_after != tag_x_before
    assert tag_z_after == tag_z_before


# ── task routing config ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_and_get_routing_config_single(routing_backend):
    cfg = RoutingConfig(
        task_name="my_task",
        task_version=1,
        strategy=RoutingStrategy.SINGLE,
        queues=["fast"],
    )
    await routing_backend.save_routing_config(cfg)
    result = await routing_backend.get_routing_config("my_task", 1)
    assert result is not None
    assert result.task_name == "my_task"
    assert result.task_version == 1
    assert result.strategy == RoutingStrategy.SINGLE
    assert result.queues == ["fast"]
    assert result.weights is None


@pytest.mark.asyncio
async def test_save_and_get_routing_config_weighted(routing_backend):
    cfg = RoutingConfig(
        task_name="t",
        task_version=2,
        strategy=RoutingStrategy.WEIGHTED,
        queues=["a", "b"],
        weights=[0.7, 0.3],
    )
    await routing_backend.save_routing_config(cfg)
    result = await routing_backend.get_routing_config("t", 2)
    assert result is not None
    assert result.weights == [0.7, 0.3]


@pytest.mark.asyncio
async def test_get_routing_config_returns_none_for_missing(routing_backend):
    result = await routing_backend.get_routing_config("no_task", 99)
    assert result is None


@pytest.mark.asyncio
async def test_save_routing_config_overwrites_existing(routing_backend):
    cfg1 = RoutingConfig(task_name="t", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["old"])
    cfg2 = RoutingConfig(task_name="t", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["new"])
    await routing_backend.save_routing_config(cfg1)
    await routing_backend.save_routing_config(cfg2)
    result = await routing_backend.get_routing_config("t", 1)
    assert result is not None
    assert result.queues == ["new"]


@pytest.mark.asyncio
async def test_delete_routing_config_returns_true_when_existed(routing_backend):
    cfg = RoutingConfig(task_name="t", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["q"])
    await routing_backend.save_routing_config(cfg)
    deleted = await routing_backend.delete_routing_config("t", 1)
    assert deleted is True
    assert await routing_backend.get_routing_config("t", 1) is None


@pytest.mark.asyncio
async def test_delete_routing_config_returns_false_when_missing(routing_backend):
    deleted = await routing_backend.delete_routing_config("nonexistent", 1)
    assert deleted is False


# ── delete_queue cascade ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_queue_also_removes_from_roles_and_bumps_refresh_tag(routing_backend):
    """delete_queue removes the queue from roles that contained it and bumps their refresh tag."""
    await routing_backend.save_queue_config(QueueConfig(name="q"))
    await routing_backend.save_queue_config(QueueConfig(name="other_q"))
    await routing_backend.save_role("r", {"q"})
    await routing_backend.save_role("r2", {"other_q"})
    tag_r_before = await routing_backend.get_refresh_tag("r")
    tag_r2_before = await routing_backend.get_refresh_tag("r2")

    await routing_backend.delete_queue("q")

    assert await routing_backend.get_queue_config("q") is None
    assert "q" not in await routing_backend.get_queues("r")
    assert tag_r_before != await routing_backend.get_refresh_tag("r")
    assert tag_r2_before == await routing_backend.get_refresh_tag("r2")


@pytest.mark.asyncio
async def test_delete_queue_with_roles_but_none_containing_it_skips_tag_bump(routing_backend):
    """delete_queue does not bump any refresh tag when roles exist but none contain the deleted queue."""
    await routing_backend.save_queue_config(QueueConfig(name="q"))
    await routing_backend.save_queue_config(QueueConfig(name="other_q"))
    await routing_backend.save_role("r", {"other_q"})
    tag_before = await routing_backend.get_refresh_tag("r")

    await routing_backend.delete_queue("q")

    assert await routing_backend.get_queue_config("q") is None
    assert await routing_backend.get_refresh_tag("r") == tag_before


@pytest.mark.asyncio
async def test_bump_refresh_tags_for_queue_no_roles_returns_empty(routing_backend):
    """bump_refresh_tags_for_queue returns [] when no roles exist."""
    affected = await routing_backend.bump_refresh_tags_for_queue("any_queue")
    assert affected == []


@pytest.mark.asyncio
async def test_bump_refresh_tags_for_queue_no_matching_role_returns_empty(routing_backend):
    """bump_refresh_tags_for_queue returns [] when roles exist but none contain the queue."""
    await routing_backend.save_queue_config(QueueConfig(name="other_q"))
    await routing_backend.save_role("r", {"other_q"})
    tag_before = await routing_backend.get_refresh_tag("r")

    affected = await routing_backend.bump_refresh_tags_for_queue("missing_q")

    assert affected == []
    assert await routing_backend.get_refresh_tag("r") == tag_before
