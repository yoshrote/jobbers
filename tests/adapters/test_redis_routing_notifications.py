"""
Tests for RedisRoutingNotifications.

Covers get_routing_version(), bump_routing_version(), notify_refresh(), and
poll_refresh_signal() including the lazy subscription cache.
"""

import pytest
import pytest_asyncio

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
async def test_poll_refresh_signal_returns_true_when_message_available(redis):
    """poll_refresh_signal returns True when a message has been published to the channel."""
    notif = RedisRoutingNotifications(redis)
    # Subscribe first so messages are buffered
    await notif.poll_refresh_signal("myrole")
    # Publish a message
    await notif.notify_refresh("myrole", "some-tag")
    # Give the pubsub a moment to receive it
    import asyncio

    await asyncio.sleep(0.05)

    result = await notif.poll_refresh_signal("myrole")
    assert result is True


@pytest.mark.asyncio
async def test_poll_refresh_signal_creates_separate_subscriptions_per_role(redis):
    """Different roles get independent pubsub subscriptions."""
    notif = RedisRoutingNotifications(redis)
    await notif.poll_refresh_signal("role_a")
    await notif.poll_refresh_signal("role_b")
    assert notif._pubsubs["role_a"] is not notif._pubsubs["role_b"]
