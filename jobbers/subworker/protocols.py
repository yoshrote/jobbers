"""
Protocol definitions for subworker transport implementations.

A subworker handle owns exactly one subprocess and speaks whatever wire format its
transport uses; a future ``SubworkerPool`` (occupancy tracking, TTL recycling, capacity
gating, cancel/shutdown escalation policy) is meant to be written once against this
protocol and be transport-agnostic. See
``.claude/plans/subworker-handle-protocol-design.md`` for the full design.

- ``SubworkerHandleProtocol`` — one live subprocess bound to at most one in-flight task.
- ``ResultMsg`` / ``HeartbeatMsg`` / ``SubworkerExited`` — the three possible ``recv()`` outcomes.
- ``SubworkerTaskError`` — marshalled failure from inside the child.
"""

from __future__ import annotations

from typing import Any, NamedTuple, Protocol, runtime_checkable


class SubworkerTaskError(NamedTuple):
    """Marshalled failure from inside the child — never a live exception object crossing the boundary."""

    error_type: str  # stable string tag ("ValueError", "panic", "timeout"), not a class path
    message: str
    traceback_text: str
    retryable: bool | None  # child's opinion, if any; a pool's own retry policy still decides


class ResultMsg(NamedTuple):
    """A finished dispatch: the child is idle again."""

    request_id: str
    ok: bool
    result: Any
    error: SubworkerTaskError | None


class HeartbeatMsg(NamedTuple):
    """Liveness ping for a still-in-flight request; call recv() again."""

    request_id: str


class SubworkerExited(NamedTuple):
    """
    Sentinel returned by ``recv()`` when the process is gone.

    Covers both clean exit and crash, with no ``ResultMsg`` delivered for whatever
    request was in flight, if any. Distinct from a *received* ``ResultMsg``, which
    always means the process is still alive and idle.
    """

    exit_code: int | None


SubworkerMessage = HeartbeatMsg | ResultMsg | SubworkerExited


@runtime_checkable
class SubworkerHandleProtocol(Protocol):
    """One long-lived subprocess, reused across many dispatches, one task at a time."""

    @property
    def pid(self) -> int | None:
        """None before start() and after the process has been reaped."""
        ...

    async def start(self) -> None:
        """Spawn the child and complete any handshake. Not called twice for the same handle."""
        ...

    async def dispatch(
        self, request_id: str, task_name: str, task_version: int, kwargs: dict[str, Any]
    ) -> None:
        """
        Send a dispatch for exactly one task.

        Caller (a pool) guarantees the handle is currently idle — occupancy is
        pool-level state, not this protocol's concern.
        """
        ...

    async def recv(self) -> SubworkerMessage:
        """
        Block for the next inbound event and return it.

        A ``HeartbeatMsg`` means "call recv() again, same request still in flight."  A
        ``ResultMsg`` means the task is done and the handle is idle again.  A
        ``SubworkerExited`` means the process is gone; the caller must resolve any
        in-flight request as a crash and retire this handle.

        This is the one blocking call a per-slot reader loop needs — each implementation
        internally races or collapses whatever liveness signal its transport has
        (``process.sentinel`` vs. stdout EOF) rather than exposing that race to the caller.
        """
        ...

    async def cancel(self, request_id: str) -> None:
        """
        Best-effort cooperative cancel of the in-flight request.

        Must not raise if the handle has already exited or already produced a result —
        a grace-period timer is the actual backstop, not this call's success.
        """
        ...

    async def current_request_id(self) -> str | None:
        """
        Best-effort, out-of-band read of which request the subworker is processing (None if idle).

        Backed by a status file the child updates independently of the message
        pipe/socket (see ``jobbers.subworker.status_file``), so it stays meaningful even
        if the child is stuck and never gets a ``ResultMsg`` out. Used as a tie-breaker
        by a pool's cancel-escalation logic: if this reports the subworker already moved
        off the cancelled request, skip the hard kill even though no ``ResultMsg`` has
        arrived yet.
        """
        ...

    async def retire(self) -> None:
        """
        Tell the child "don't expect another dispatch; exit once idle" without an abrupt kill.

        Safe to call while a task is in flight — the exit happens the next time the
        handle would otherwise go idle. Called by TTL recycling and by graceful worker
        shutdown.
        """
        ...

    async def kill(self, grace_period: float) -> int | None:
        """
        Escalate to a forced stop: soft-stop, wait up to grace_period seconds, then a hard kill.

        Returns the exit code once known, or None if it couldn't be determined.
        """
        ...
