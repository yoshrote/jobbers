"""
SQLTransactionBatch — a TransactionHandle backed by a SQLAlchemy session.

Collects async callables staged by ``stage_*`` adapter methods and executes them
all within a single database transaction when ``execute()`` is called.  Satisfies
``TransactionHandle`` structurally: it exposes ``async execute() -> list[object]``.

Usage::

    batch = SQLTransactionBatch(session_factory)
    adapter.stage_save(batch, task)
    adapter.stage_submit_task(batch, task)
    await batch.execute()  # commits atomically; rollback on any error
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class SQLTransactionBatch:
    """Staged SQL operations executed in a single transaction."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._ops: list[Callable[[AsyncSession], Awaitable[None]]] = []

    async def _get_session(self) -> AsyncSession:
        """Return the open session, creating and beginning a transaction if needed."""
        if self._session is None:
            self._session = self._session_factory()
            await self._session.begin()
        return self._session

    def add_op(self, op: Callable[[AsyncSession], Awaitable[None]]) -> None:
        """Append a coroutine-returning callable to the batch."""
        self._ops.append(op)

    async def execute(self) -> list[object]:
        """Run all staged operations in order and commit; rollback on any error."""
        session = await self._get_session()
        try:
            for op in self._ops:
                await op(session)
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
            self._session = None
        return []
