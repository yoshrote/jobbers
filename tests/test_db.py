import os
from unittest.mock import AsyncMock, patch

import pytest

from jobbers.db import close_client, get_client, set_client


@pytest.fixture
def mock_redis():
    """Fixture to reset the mocked Redis before each test."""
    with patch("jobbers.db._client", None):
        with patch("jobbers.db.redis.from_url", return_value=AsyncMock()) as mock_redis:
            yield mock_redis


@pytest.mark.asyncio
async def test_get_client_creates_new_client(mock_redis):
    """Test that get_client creates a new Redis client if none exists."""
    client = get_client()
    assert client is not None
    mock_redis.assert_called_once_with("redis://localhost:6379")

@pytest.mark.asyncio
async def test_get_client_creates_new_client_based_on_os_env(mock_redis):
    """Test that get_client creates a new Redis client if none exists."""
    with patch.dict(os.environ, {"REDIS_URL": "redis://override:6379"}, clear=True):
        client = get_client()
        assert client is not None
        mock_redis.assert_called_once_with("redis://override:6379")

@pytest.mark.asyncio
async def test_get_client_uses_existing_client(mock_redis):
    """Test that get_client reuses an existing Redis client."""
    # First call to create the client
    client1 = get_client()
    # Second call should reuse the same client
    client2 = get_client()

    assert client1 is client2
    mock_redis.assert_called_once_with("redis://localhost:6379")


@pytest.mark.asyncio
async def test_set_client_replaces_existing_client():
    """Test that set_client replaces an existing Redis client."""
    old_client = AsyncMock()
    new_client = AsyncMock()

    with patch("jobbers.db._client", old_client):
        set_client(new_client)

        # Ensure the old client is closed
        old_client.close.assert_called_once()
        # Ensure the new client is set
        assert get_client() is new_client


@pytest.mark.asyncio
async def test_close_client_closes_existing_client():
    """Test that close_client closes the existing Redis client."""
    client = AsyncMock()

    with patch("jobbers.db._client", client):
        await close_client()

        # Ensure the client is closed
        client.close.assert_called_once()
        # Ensure the global client is set to None
        assert get_client() is not client


@pytest.mark.asyncio
async def test_close_client_no_existing_client():
    """Test that close_client does nothing if no client exists."""
    with patch("jobbers.db._client", None):
        # Ensure no exception is raised
        await close_client()
