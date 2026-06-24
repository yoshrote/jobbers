"""
Tests for RedisRoutingNotifications.

Covers get_routing_version(), bump_routing_version(), get_refresh_tag(),
bump_refresh_tag(), and poll_refresh_signal() including the lazy subscription cache.
"""

import asyncio

import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.adapters.redis import RedisRoutingNotifications


@pytest_asyncio.fixture
async def notifications(redis):
    return RedisRoutingNotifications(redis)


@pytest.mark.asyncio
async def test_get_routing_version_returns_none_when_not_set(notifications):
    """get_routing_version returns None before any version has been written."""
    result = await notifications.get_routing_version()
    assert result is None


@pytest.mark.asyncio
async def test_get_routing_version_returns_ulid_after_bump(notifications):
    """get_routing_version returns a ULID after bump_routing_version is called."""
    await notifications.bump_routing_version()
    result = await notifications.get_routing_version()
    assert result is not None


@pytest.mark.asyncio
async def test_bump_routing_version_changes_value(notifications):
    """Calling bump_routing_version twice produces two different ULIDs."""
    await notifications.bump_routing_version()
    first = await notifications.get_routing_version()
    await notifications.bump_routing_version()
    second = await notifications.get_routing_version()
    assert first is not None
    assert second is not None
    assert first != second


# ── get_refresh_tag ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_refresh_tag_creates_on_first_call(notifications, redis):
    """get_refresh_tag creates and persists a ULID for a role that has no entry."""
    tag = await notifications.get_refresh_tag("myrole")
    assert isinstance(tag, ULID)
    raw = await redis.get(notifications.ROLE_REFRESH_TAG_KEY(name="myrole"))
    assert raw is not None
    assert ULID.from_str(raw.decode()) == tag


@pytest.mark.asyncio
async def test_get_refresh_tag_cached_after_first_call(notifications, redis):
    """Second call returns the cached value without hitting Redis."""
    first = await notifications.get_refresh_tag("myrole")
    # Overwrite in Redis — should not affect cached result
    await redis.set(notifications.ROLE_REFRESH_TAG_KEY(name="myrole"), str(ULID()))
    second = await notifications.get_refresh_tag("myrole")
    assert first == second


@pytest.mark.asyncio
async def test_get_refresh_tag_reads_existing_key(notifications, redis):
    """get_refresh_tag returns an existing persisted tag on first call."""
    expected = ULID()
    await redis.set(notifications.ROLE_REFRESH_TAG_KEY(name="myrole"), str(expected))
    tag = await notifications.get_refresh_tag("myrole")
    assert tag == expected


@pytest.mark.asyncio
async def test_get_refresh_tag_independent_per_role(notifications):
    """Different roles get independent tags."""
    tag_a = await notifications.get_refresh_tag("role_a")
    tag_b = await notifications.get_refresh_tag("role_b")
    assert tag_a != tag_b


# ── bump_refresh_tag ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_bump_refresh_tag_writes_redis_and_updates_cache(notifications, redis):
    """bump_refresh_tag persists the new tag to Redis and updates the in-process cache."""
    tag_str = await notifications.bump_refresh_tag("myrole")
    raw = await redis.get(notifications.ROLE_REFRESH_TAG_KEY(name="myrole"))
    assert raw is not None
    assert raw.decode() == tag_str
    # Cache reflects the new value immediately (no Redis hit needed)
    assert notifications._tag_cache["myrole"] == ULID.from_str(tag_str)


@pytest.mark.asyncio
async def test_bump_refresh_tag_publishes_to_pubsub(redis):
    """bump_refresh_tag publishes the new tag to the role's refresh channel via a second subscriber."""
    sender = RedisRoutingNotifications(redis)
    receiver = RedisRoutingNotifications(redis)

    # Subscribe via receiver before bumping
    await receiver.poll_refresh_signal("myrole")
    await asyncio.sleep(0.05)

    tag_str = await sender.bump_refresh_tag("myrole")
    await asyncio.sleep(0.05)

    # The receiver should pick up the published tag
    received_tag = await receiver.poll_refresh_signal("myrole")
    assert str(received_tag) == tag_str


@pytest.mark.asyncio
async def test_bump_refresh_tag_increments_on_each_call(notifications):
    """Each bump_refresh_tag call produces a strictly later ULID."""
    first_str = await notifications.bump_refresh_tag("myrole")
    second_str = await notifications.bump_refresh_tag("myrole")
    assert ULID.from_str(first_str) < ULID.from_str(second_str)


# ── poll_refresh_signal ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_poll_refresh_signal_subscribes_on_first_call(redis):
    """poll_refresh_signal subscribes to the role's channel the first time it is called."""
    notif = RedisRoutingNotifications(redis)
    assert "myrole" not in notif._pubsubs

    await notif.poll_refresh_signal("myrole")

    assert "myrole" in notif._pubsubs


@pytest.mark.asyncio
async def test_poll_refresh_signal_reuses_subscription_on_repeat_calls(redis):
    """A second call for the same role reuses the cached pubsub object."""
    notif = RedisRoutingNotifications(redis)
    await notif.poll_refresh_signal("myrole")
    first_ps = notif._pubsubs["myrole"]

    await notif.poll_refresh_signal("myrole")
    second_ps = notif._pubsubs["myrole"]

    assert first_ps is second_ps


@pytest.mark.asyncio
async def test_poll_refresh_signal_returns_tag_on_first_call(redis):
    """poll_refresh_signal always returns a ULID — fetches from Redis on first call."""
    notif = RedisRoutingNotifications(redis)
    result = await notif.poll_refresh_signal("myrole")
    assert isinstance(result, ULID)


@pytest.mark.asyncio
async def test_poll_refresh_signal_returns_cached_tag_when_no_new_messages(redis):
    """After initialization, repeated calls return the same cached tag with no pub/sub messages."""
    notif = RedisRoutingNotifications(redis)
    first = await notif.poll_refresh_signal("myrole")
    second = await notif.poll_refresh_signal("myrole")
    assert first == second


@pytest.mark.asyncio
async def test_poll_refresh_signal_returns_ulid_when_message_available(redis):
    """poll_refresh_signal returns the ULID from the published message."""
    notif = RedisRoutingNotifications(redis)
    await notif.poll_refresh_signal("myrole")  # subscribe
    tag_str = await notif.bump_refresh_tag("myrole")
    await asyncio.sleep(0.05)

    result = await notif.poll_refresh_signal("myrole")
    assert result == ULID.from_str(tag_str)


@pytest.mark.asyncio
async def test_poll_refresh_signal_drains_all_messages_returns_last(redis):
    """When multiple messages are queued, drains all and returns the last ULID."""
    notif = RedisRoutingNotifications(redis)
    await notif.poll_refresh_signal("myrole")  # subscribe
    tags = [await notif.bump_refresh_tag("myrole") for _ in range(3)]
    await asyncio.sleep(0.05)

    result = await notif.poll_refresh_signal("myrole")
    assert result == ULID.from_str(tags[-1])
    # All messages drained — next call returns cached last tag (not None)
    assert await notif.poll_refresh_signal("myrole") == ULID.from_str(tags[-1])


@pytest.mark.asyncio
async def test_poll_refresh_signal_creates_separate_subscriptions_per_role(redis):
    """Different roles get independent pubsub subscriptions."""
    notif = RedisRoutingNotifications(redis)
    await notif.poll_refresh_signal("role_a")
    await notif.poll_refresh_signal("role_b")
    assert notif._pubsubs["role_a"] is not notif._pubsubs["role_b"]
