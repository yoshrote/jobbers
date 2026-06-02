"""
Plain Redis cancellation bus adapter.

- `RedisCancellationBus` — CancellationBusProtocol backed by Redis pub/sub.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from ulid import ULID

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from redis.asyncio.client import Redis

logger = logging.getLogger(__name__)


class RedisCancellationBus:
    """CancellationBusProtocol backed by a Redis pub/sub channel."""

    CHANNEL = "task_cancellations"

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def publish_cancellation(self, task_id: ULID) -> None:
        await self._client.publish(self.CHANNEL, str(task_id))

    def listen_cancellations(self) -> AsyncGenerator[ULID, None]:
        return self._listen_gen()

    async def _listen_gen(self) -> AsyncGenerator[ULID, None]:
        async with self._client.pubsub() as pubsub:
            await pubsub.subscribe(self.CHANNEL)
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    raw = message["data"]
                    task_id_str = raw.decode() if isinstance(raw, bytes) else raw
                    try:
                        yield ULID.from_str(task_id_str)
                    except Exception:
                        logger.warning("Invalid task_id in cancellations channel: %r", task_id_str)
                await asyncio.sleep(0.01)
