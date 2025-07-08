import os

import redis.asyncio as redis

_client: redis.Redis | None = None

def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))  # type: ignore
    return _client

def set_client(new_client: redis.Redis) -> redis.Redis:
    global _client
    if _client is not None:
        _client.close()

    _client = new_client
    return _client

async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None
