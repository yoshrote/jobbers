"""
Shared ZeroMQ transport bus.

- `ZmqBus` — owns the PUB and ROUTER sockets for the Manager process, and the
  REQ socket for Worker processes.  Both ``ZmqCancellationBus`` and
  ``ZmqRoutingNotifications`` hold a reference to one ``ZmqBus`` instance so that
  all ZMQ communication uses a single PUB socket and a single ROUTER/REQ socket pair.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import zmq
import zmq.asyncio

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


class ZmqBus:
    """
    Shared ZMQ transport: one PUB socket for broadcasts, one ROUTER/REQ pair for request-response.

    Manager process: calls ``publish()`` and ``start_router(handler)``.
    Worker process: calls ``new_sub_socket()`` and ``request()``.
    Sockets are created lazily so only the ones actually used by a given process
    are ever opened.
    """

    def __init__(
        self,
        pub_bind_address: str,
        sub_connect_address: str,
        router_bind_address: str,
        req_connect_address: str,
        *,
        context: zmq.asyncio.Context | None = None,
    ) -> None:
        self._pub_bind_address = pub_bind_address
        self._sub_connect_address = sub_connect_address
        self._router_bind_address = router_bind_address
        self._req_connect_address = req_connect_address
        self._ctx = context
        self._owns_context = context is None
        self._pub_sock: zmq.asyncio.Socket | None = None
        self._req_sock: zmq.asyncio.Socket | None = None
        self._req_lock: asyncio.Lock = asyncio.Lock()
        self._router_task: asyncio.Task[None] | None = None

    def _get_context(self) -> zmq.asyncio.Context:
        if self._ctx is None:
            self._ctx = zmq.asyncio.Context()
        return self._ctx

    async def _get_pub_sock(self) -> zmq.asyncio.Socket:
        if self._pub_sock is None:
            sock: zmq.asyncio.Socket = self._get_context().socket(zmq.PUB)
            sock.setsockopt(zmq.LINGER, 0)
            sock.bind(self._pub_bind_address)
            self._pub_sock = sock
        return self._pub_sock

    async def _get_req_sock(self) -> zmq.asyncio.Socket:
        if self._req_sock is None:
            sock: zmq.asyncio.Socket = self._get_context().socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(self._req_connect_address)
            self._req_sock = sock
        return self._req_sock

    async def publish(self, topic: bytes, *data: bytes) -> None:
        """Send a multipart message on the shared PUB socket."""
        sock = await self._get_pub_sock()
        await sock.send_multipart([topic, *data])

    def new_sub_socket(self, *topics: bytes) -> zmq.asyncio.Socket:
        """Create a connected SUB socket subscribed to the given topics. Caller owns lifecycle."""
        sock: zmq.asyncio.Socket = self._get_context().socket(zmq.SUB)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(self._sub_connect_address)
        for topic in topics:
            sock.setsockopt(zmq.SUBSCRIBE, topic)
        return sock

    async def request(self, *parts: bytes) -> bytes:
        """
        Send a REQ and return the single-frame response from the Manager ROUTER.

        Serialized by an asyncio.Lock — only one request may be in flight at a time.
        Raises ``asyncio.TimeoutError`` after 5 seconds if no response arrives.
        Wrap the call with ``asyncio.timeout(n)`` to use a custom deadline.
        """
        async with self._req_lock:
            sock = await self._get_req_sock()
            await sock.send_multipart(list(parts))
            # REQ socket prepends an empty delimiter on send; ROUTER echoes it back.
            # recv_multipart() returns [b"", response_frame].
            async with asyncio.timeout(5.0):
                frames = await sock.recv_multipart()
            return frames[-1]

    async def start_router(
        self,
        handler: Callable[[bytes, list[bytes]], Awaitable[bytes]],
    ) -> None:
        """Bind the ROUTER socket and start the request-handling background task (idempotent)."""
        if self._router_task is not None and not self._router_task.done():
            return
        self._router_task = asyncio.create_task(self._router_loop(handler))

    async def _router_loop(
        self,
        handler: Callable[[bytes, list[bytes]], Awaitable[bytes]],
    ) -> None:
        sock: zmq.asyncio.Socket = self._get_context().socket(zmq.ROUTER)
        sock.setsockopt(zmq.LINGER, 0)
        sock.bind(self._router_bind_address)
        try:
            while True:
                frames = await sock.recv_multipart()
                # Expected: [identity, b"", command, *args]
                if len(frames) < 3:
                    logger.warning("Malformed ROUTER frame (too short), skipping: %r", frames)
                    continue
                identity = frames[0]
                command = frames[2]
                args = list(frames[3:])
                try:
                    response = await handler(command, args)
                except Exception:
                    logger.exception("ZmqBus ROUTER handler raised")
                    response = b"error"
                await sock.send_multipart([identity, b"", response])
        finally:
            sock.close(linger=0)

    def close(self) -> None:
        """Close all sockets, cancel the ROUTER task, and terminate the context if owned."""
        if self._router_task is not None and not self._router_task.done():
            self._router_task.cancel()
            self._router_task = None
        if self._pub_sock is not None:
            self._pub_sock.close(linger=0)
            self._pub_sock = None
        if self._req_sock is not None:
            self._req_sock.close(linger=0)
            self._req_sock = None
        if self._owns_context and self._ctx is not None:
            self._ctx.term()
            self._ctx = None
