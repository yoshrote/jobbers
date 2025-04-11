import datetime as dt

import pytest

from jobbers.worker_proc import LocalTTL


@pytest.mark.asyncio
async def test_local_ttl_initial_refresh():
    """Test that LocalTTL refreshes on the first use."""
    ttl = LocalTTL(config_ttl=60)

    async with ttl as needs_refresh:
        assert needs_refresh is True, "LocalTTL should refresh on the first use"
        assert ttl.last_refreshed is None, "last_refreshed should not be set during the first use"

    assert ttl.last_refreshed is not None, "last_refreshed should be updated after the first use"

@pytest.mark.asyncio
async def test_local_ttl_no_refresh_within_ttl():
    """Test that LocalTTL does not refresh within the TTL duration."""
    ttl = LocalTTL(config_ttl=60)
    ttl.last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=30)

    async with ttl as needs_refresh:
        assert needs_refresh is False, "LocalTTL should not refresh within the TTL duration"

    assert ttl.last_refreshed is not None, "last_refreshed should remain unchanged"

@pytest.mark.asyncio
async def test_local_ttl_refresh_after_ttl():
    """Test that LocalTTL refreshes after the TTL duration has expired."""
    ttl = LocalTTL(config_ttl=60)
    ttl.last_refreshed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=61)

    async with ttl as needs_refresh:
        assert needs_refresh is True, "LocalTTL should refresh after the TTL duration has expired"

    assert ttl.last_refreshed is not None, "last_refreshed should be updated after the refresh"
