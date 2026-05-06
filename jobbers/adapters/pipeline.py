"""
Pipeline abstractions for deferred, transactional adapter operations.

``StagingPipeline``  – minimal Protocol satisfied by both Redis ``Pipeline``
                       and ``SqlPipeline``.  Used as the parameter type in all
                       ``stage_*`` adapter methods so callers can compose
                       operations without knowing the backend.

``SqlPipeline``      – collects async SQL callables and executes them inside a
                       single SQLAlchemy transaction on ``execute()``.  Mirrors
                       enough of the Redis Pipeline interface (``watch``,
                       ``unwatch``, ``multi``) that ``StateManager`` can treat
                       it identically to a Redis pipeline.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Awaitable, Protocol, runtime_checkable

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


@runtime_checkable
class StagingPipeline(Protocol):
    """
    Minimal interface shared by Redis ``Pipeline`` and ``SqlPipeline``.

    Every concrete pipeline must implement ``execute()``.  The ``watch`` /
    ``unwatch`` / ``multi`` trio is included so that ``StateManager``'s
    optimistic-locking flow works without branching on backend type.
    """

    async def execute(self) -> list[Any]: ...
    async def watch(self, *keys: str) -> None: ...
    async def unwatch(self) -> None: ...
    def multi(self) -> None: ...


class SqlPipeline:
    """
    Deferred SQL operation collector.

    SQL adapter ``stage_*`` methods call ``pipe.add(async_fn)`` instead of
    calling Redis commands directly.  On ``execute()`` all queued callables
    run inside a single ``BEGIN … COMMIT`` transaction.

    Usage::

        pipe = SqlPipeline(engine)
        adapter.stage_save(pipe, task)
        dead_queue.stage_add(pipe, task, failed_at)
        await pipe.execute()          # both ops in one transaction
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._ops: list[Callable[[AsyncConnection], Awaitable[Any]]] = []

    def add(self, op: Callable[[AsyncConnection], Awaitable[Any]]) -> None:
        """Queue a single async SQL operation."""
        self._ops.append(op)

    async def execute(self) -> list[Any]:
        """Run all queued operations in one transaction, then clear the queue."""
        results: list[Any] = []
        if self._ops:
            async with self._engine.begin() as conn:
                for op in self._ops:
                    results.append(await op(conn))
        self._ops.clear()
        return results

    # ------------------------------------------------------------------
    # Redis Pipeline compatibility shims
    # These are no-ops because SQL transactions provide isolation
    # automatically; they exist so StateManager can call them without
    # branching on backend type.
    # ------------------------------------------------------------------

    async def watch(self, *_keys: str) -> None:
        """No-op: SQL transactions serialise conflicting writes natively."""

    async def unwatch(self) -> None:
        """No-op."""

    def multi(self) -> None:
        """No-op: every SqlPipeline.execute() is already a single transaction."""
