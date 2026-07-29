"""
Plain Redis cancellation bus adapter.

- `RedisCancellationBus` — CancellationBusProtocol backed by Redis pub/sub.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from ulid import ULID

from jobbers.protocols import CancellationMessage

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
        await self._client.publish(self.CHANNEL, f"task:{task_id}")

    async def publish_dag_cancellation(self, dag_run_id: ULID) -> None:
        await self._client.publish(self.CHANNEL, f"dag:{dag_run_id}")

    def listen_cancellations(self) -> AsyncGenerator[CancellationMessage, None]:
        return self._listen_gen()

    async def _listen_gen(self) -> AsyncGenerator[CancellationMessage, None]:
        async with self._client.pubsub() as pubsub:
            await pubsub.subscribe(self.CHANNEL)
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    raw = message["data"]
                    payload = raw.decode() if isinstance(raw, bytes) else raw
                    kind, sep, raw_id = payload.partition(":")
                    if sep and kind in ("task", "dag"):
                        try:
                            yield CancellationMessage(kind, ULID.from_str(raw_id))  # type: ignore[arg-type]
                        except Exception:
                            logger.warning("Invalid id in cancellations channel: %r", payload)
                    else:
                        logger.warning("Malformed cancellations channel message: %r", payload)
                await asyncio.sleep(0.01)
