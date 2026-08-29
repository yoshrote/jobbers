"""
The transport-agnostic pool sketched in ``subworker-handle-protocol-design.md`` §5.

Written once against ``SubworkerHandleProtocol``; a pool of ``MultiprocessingSubworkerHandle``
and a pool of ``StdioSubworkerHandle`` differ only in the ``handle_factory`` passed to
``__init__`` — everything here (occupancy tracking, TTL recycling, crash-triggered respawn,
cancel/shutdown escalation, bridging ``dispatch()`` to an awaitable result) is shared.

Deliberately self-contained: no dependency on ``TaskConfig``, ``QueueConfig``, the task
registry, or ``TaskProcessor``. Wiring a pool into the worker's task-execution path is a
separate integration, out of scope here (see that design doc's §0 scope note and the
sibling ``sync-task-subworker-design.md``).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from ulid import ULID

from jobbers.subworker.errors import SubworkerCrashedError, SubworkerTaskFailure
from jobbers.subworker.protocols import HeartbeatMsg, ResultMsg, SubworkerExited, SubworkerTaskError

if TYPE_CHECKING:
    from collections.abc import Callable

    from jobbers.subworker.protocols import SubworkerHandleProtocol

logger = logging.getLogger(__name__)


class SubworkerPool:
    """
    A fixed-size pool of long-lived subworker handles, one dedicated handle per in-flight task.

    ``ttl`` is the number of dispatches a handle serves before being retired and replaced
    (mirrors ``WORKER_SYNC_SUBWORKER_TTL`` in the design docs); ``0`` disables recycling.
    """

    def __init__(
        self,
        handle_factory: Callable[[], SubworkerHandleProtocol],
        size: int,
        ttl: int = 0,
        *,
        on_heartbeat: Callable[[str], None] | None = None,
    ) -> None:
        if size < 1:
            raise ValueError("size must be at least 1")
        self._handle_factory = handle_factory
        self._size = size
        self._ttl = ttl
        self._on_heartbeat_cb = on_heartbeat

        self._handles: dict[int, SubworkerHandleProtocol] = {}
        self._occupancy: dict[int, str | None] = {}
        self._task_counts: dict[int, int] = {}
        self._reader_tasks: dict[int, asyncio.Task[None]] = {}
        self._futures: dict[str, asyncio.Future[Any]] = {}
        self._request_slot: dict[str, int] = {}
        self._heartbeat_callbacks: dict[str, Callable[[], None]] = {}
        self._free_slots: asyncio.Queue[int] = asyncio.Queue()

        self._started = False
        self._closed = False

    @property
    def size(self) -> int:
        return self._size

    @property
    def free_slots(self) -> int:
        """Slots currently idle and available for a dispatch — for external capacity gating."""
        return sum(1 for occupant in self._occupancy.values() if occupant is None)

    async def start(self) -> None:
        """Spawn all ``size`` subworker handles and start their reader loops. Call once."""
        if self._started:
            raise RuntimeError("start() already called")
        self._started = True
        for slot_id in range(self._size):
            await self._spawn_slot(slot_id)

    async def dispatch(
        self,
        task_name: str,
        task_version: int,
        kwargs: dict[str, Any],
        *,
        request_id: str | None = None,
        on_heartbeat: Callable[[], None] | None = None,
        cancel_grace_period: float = 5.0,
    ) -> Any:
        """
        Run one task on a free handle and return its result.

        Blocks until a slot is free, then blocks until the task completes. Raises
        ``SubworkerTaskFailure`` if the task itself failed, or ``SubworkerCrashedError``
        if the subworker exited (crash, or a forced kill escalation) before replying.
        This is the bridge described in the design doc: the caller awaits this coroutine
        the same way ``TaskProcessor.process()`` awaits an async task function.

        Cancelling the awaiting coroutine (directly, or via an enclosing
        ``asyncio.timeout()``) does not just abandon the subworker to keep running
        unobserved: ``dispatch()`` catches its own cancellation, calls ``self.cancel()``
        (cooperative cancel, escalating to a hard kill after ``cancel_grace_period``
        seconds) on the caller's behalf, and only then re-raises ``CancelledError`` --
        cancelling a sync task actually stops the process behind it.

        ``request_id`` defaults to a fresh ULID; a caller that wants a stable identifier
        for logging/correlation (e.g. the originating task's own id) can supply its own —
        unique per in-flight dispatch is the only requirement. ``on_heartbeat``, if given,
        is called (synchronously, with no arguments) every time this specific request's
        subworker reports a heartbeat; it runs in addition to the pool-level
        ``on_heartbeat`` passed to ``__init__``, not instead of it.
        """
        if not self._started:
            raise RuntimeError("start() must be called before dispatch()")
        if self._closed:
            raise RuntimeError("pool is shut down")

        slot_id = await self._free_slots.get()
        if self._closed:
            self._free_slots.put_nowait(slot_id)
            raise RuntimeError("pool is shut down")

        handle = self._handles[slot_id]
        request_id = request_id or str(ULID())
        self._occupancy[slot_id] = request_id
        self._request_slot[request_id] = slot_id
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._futures[request_id] = future
        if on_heartbeat is not None:
            self._heartbeat_callbacks[request_id] = on_heartbeat
        await handle.dispatch(request_id, task_name, task_version, kwargs)
        try:
            # Shielded: an external cancellation of *this* await must not cancel the
            # future itself, or the except clause below would find its own bookkeeping
            # already gone (future popped, nothing left to hand to self.cancel()).
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            await self.cancel(request_id, cancel_grace_period)
            if future.done() and not future.cancelled():
                # We're discarding the outcome in favor of re-raising CancelledError;
                # retrieve it so a resolved-but-unretrieved exception doesn't get logged
                # as "Future exception was never retrieved" when the future is GC'd.
                with contextlib.suppress(BaseException):
                    future.exception()
            raise
        finally:
            self._futures.pop(request_id, None)
            self._request_slot.pop(request_id, None)
            self._heartbeat_callbacks.pop(request_id, None)

    async def cancel(self, request_id: str, grace_period: float) -> None:
        """
        Best-effort cooperative cancel of an in-flight request, escalating to a hard kill.

        A no-op if the request is unknown (already completed, or never existed). Does not
        itself raise on behalf of the cancelled ``dispatch()`` call — that coroutine
        observes the cancellation as a ``SubworkerTaskFailure``/``SubworkerCrashedError``
        through its own return path once the reader loop processes the outcome.

        A signal like ``SIGUSR2`` has no acknowledgment channel of its own, so if the
        grace period elapses with no ``ResultMsg``, ``handle.current_request_id()`` (the
        out-of-band status file — see ``jobbers.subworker.status_file``) is checked as a
        tie-breaker before killing: if it shows the subworker already moved off this
        request, the message is just delayed, not stuck, and the kill is skipped.
        """
        slot_id = self._request_slot.get(request_id)
        if slot_id is None:
            return
        handle = self._handles.get(slot_id)
        future = self._futures.get(request_id)
        if handle is None or future is None:
            return
        await handle.cancel(request_id)
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout=grace_period)
        except TimeoutError:
            with contextlib.suppress(Exception):
                if await handle.current_request_id() != request_id:
                    return
            await handle.kill(grace_period=0)
        except Exception:
            pass

    async def shutdown(self, grace_period: float) -> None:
        """
        Stop accepting new dispatches; retire idle handles, cancel busy ones, then hard-kill stragglers.

        ``grace_period`` bounds the *total* time spent waiting for graceful exits, not a
        per-handle budget — handles share one deadline.
        """
        if self._closed:
            return
        self._closed = True
        loop = asyncio.get_running_loop()

        for slot_id, handle in list(self._handles.items()):
            occupant = self._occupancy.get(slot_id)
            with contextlib.suppress(Exception):
                if occupant is None:
                    await handle.retire()
                else:
                    await handle.cancel(occupant)

        deadline = loop.time() + grace_period
        for handle in list(self._handles.values()):
            remaining = max(0.0, deadline - loop.time())
            with contextlib.suppress(Exception):
                await handle.kill(remaining)

        reader_tasks = [t for t in self._reader_tasks.values() if not t.done()]
        if reader_tasks:
            _, pending = await asyncio.wait(reader_tasks, timeout=max(1.0, grace_period))
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        # Any request the reader loops didn't get a chance to resolve (should be rare).
        for request_id, future in list(self._futures.items()):
            if not future.done():
                future.set_exception(SubworkerCrashedError(request_id, None))
        self._futures.clear()

    async def _spawn_slot(self, slot_id: int) -> None:
        handle = self._handle_factory()
        await handle.start()
        self._handles[slot_id] = handle
        self._occupancy[slot_id] = None
        self._task_counts[slot_id] = 0
        self._reader_tasks[slot_id] = asyncio.create_task(self._reader_loop(slot_id))
        self._free_slots.put_nowait(slot_id)

    async def _reader_loop(self, slot_id: int) -> None:
        handle = self._handles[slot_id]
        while True:
            msg = await handle.recv()

            if isinstance(msg, HeartbeatMsg):
                if self._on_heartbeat_cb is not None:
                    with contextlib.suppress(Exception):
                        self._on_heartbeat_cb(msg.request_id)
                per_request_cb = self._heartbeat_callbacks.get(msg.request_id)
                if per_request_cb is not None:
                    with contextlib.suppress(Exception):
                        per_request_cb()
                continue

            if isinstance(msg, ResultMsg):
                self._resolve(msg.request_id, msg)
                self._task_counts[slot_id] += 1
                self._occupancy[slot_id] = None
                if self._closed:
                    return
                if self._ttl and self._task_counts[slot_id] >= self._ttl:
                    with contextlib.suppress(Exception):
                        await handle.retire()
                    continue  # the next recv() should observe the child exiting
                self._free_slots.put_nowait(slot_id)
                continue

            # SubworkerExited: a crash, or the exit we asked for via retire()/kill().
            request_id = self._occupancy.get(slot_id)
            if request_id is not None:
                self._resolve(request_id, msg)
            self._occupancy[slot_id] = None
            if self._closed:
                return
            logger.info("subworker slot %d exited (exit_code=%s); respawning", slot_id, msg.exit_code)
            await self._spawn_slot(slot_id)
            return  # _spawn_slot started this slot's replacement reader loop

    def _resolve(self, request_id: str, msg: ResultMsg | SubworkerExited) -> None:
        future = self._futures.get(request_id)
        if future is None or future.done():
            return
        if isinstance(msg, ResultMsg):
            if msg.ok:
                future.set_result(msg.result)
            else:
                error = msg.error or SubworkerTaskError(
                    "Unknown", "task failed with no error detail", "", None
                )
                future.set_exception(SubworkerTaskFailure(error))
        else:
            future.set_exception(SubworkerCrashedError(request_id, msg.exit_code))
