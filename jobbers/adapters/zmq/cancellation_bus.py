"""
ZeroMQ cancellation bus adapter.

- `ZmqCancellationBus` — CancellationBusProtocol backed by a shared ``ZmqBus``.

The Manager publishes cancellation signals via the bus PUB socket; Workers listen
by creating a SUB socket through the same bus.  All ZMQ socket lifecycle is managed
by ``ZmqBus``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ulid import ULID

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from jobbers.adapters.zmq.bus import ZmqBus

logger = logging.getLogger(__name__)


class ZmqCancellationBus:
    """
    CancellationBusProtocol backed by a shared ``ZmqBus`` PUB/SUB socket pair.

    Topic ``b"cancel"`` is used so that cancellation messages are distinguishable
    from routing messages published on the same PUB socket.
    """

    TOPIC = b"cancel"

    def __init__(self, bus: ZmqBus) -> None:
        self._bus = bus

    async def publish_cancellation(self, task_id: ULID) -> None:
        """Publish a cancellation signal to all connected subscribers."""
        await self._bus.publish(self.TOPIC, str(task_id).encode())

    def listen_cancellations(self) -> AsyncGenerator[ULID, None]:
        """Return an async generator that yields task IDs as cancellations arrive."""
        return self._listen_gen()

    async def _listen_gen(self) -> AsyncGenerator[ULID, None]:
        sock = self._bus.new_sub_socket(self.TOPIC)
        try:
            while True:
                parts = await sock.recv_multipart()
                if len(parts) < 2:
                    logger.warning("Malformed ZMQ message on cancellation bus: %r", parts)
                    continue
                try:
                    yield ULID.from_str(parts[1].decode())
                except Exception:
                    logger.warning("Invalid task_id in cancellations channel: %r", parts[1])
        finally:
            sock.close(linger=0)
