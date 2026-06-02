"""Read-only in-process cron DAG scheduler — no database required."""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import TYPE_CHECKING

from jobbers.protocols import RoutingBackendReadOnlyError

if TYPE_CHECKING:
    from ulid import ULID

    from jobbers.models.cron_dag import CronDAGEntry


class StaticCronDAGScheduler:
    """
    Read-only cron DAG scheduler backed by in-memory state.

    Implements ``CronDAGSchedulerProtocol`` only — no ``backend_key`` or pipeline
    staging, because there is no shared backend to co-locate with the task-state adapter.
    ``StateManager`` falls back to sequential async calls when the cron scheduler does
    not implement ``AtomicCronDAGSchedulerProtocol``.

    Entries are fixed at construction time; ``add`` and ``remove`` raise
    ``RoutingBackendReadOnlyError``.  Runtime state (next_run_at tracking and active-run
    markers) lives in in-memory dicts protected by an ``asyncio.Lock``, so state resets
    on process restart.
    """

    def __init__(
        self,
        entries: list[CronDAGEntry] | None = None,
        initial_next_run_at: dict[ULID, dt.datetime] | None = None,
    ) -> None:
        self._entries: dict[ULID, CronDAGEntry] = {e.id: e for e in (entries or [])}
        self._next_run_at: dict[ULID, dt.datetime] = dict(initial_next_run_at or {})
        self._active_runs: dict[ULID, str] = {}
        self._lock = asyncio.Lock()

    async def add(self, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        raise RoutingBackendReadOnlyError(
            "StaticCronDAGScheduler is read-only; use SQLCronDAGScheduler or "
            "RedisCronDAGScheduler for runtime cron entry management."
        )

    async def remove(self, cron_id: ULID) -> None:
        raise RoutingBackendReadOnlyError(
            "StaticCronDAGScheduler is read-only; use SQLCronDAGScheduler or "
            "RedisCronDAGScheduler for runtime cron entry management."
        )

    async def get(self, cron_id: ULID) -> CronDAGEntry | None:
        return self._entries.get(cron_id)

    async def next_due_bulk(self, n: int) -> list[tuple[CronDAGEntry, dt.datetime]]:
        """Acquire up to n due entries (those with next_run_at <= now)."""
        now = dt.datetime.now(dt.UTC)
        async with self._lock:
            due = sorted(
                (
                    (cron_id, run_at)
                    for cron_id, run_at in self._next_run_at.items()
                    if run_at <= now and cron_id in self._entries
                ),
                key=lambda t: t[1],
            )[:n]
            results: list[tuple[CronDAGEntry, dt.datetime]] = []
            for cron_id, run_at in due:
                del self._next_run_at[cron_id]
                results.append((self._entries[cron_id], run_at))
        return results

    async def reschedule(self, cron_id: ULID, next_run_at: dt.datetime) -> None:
        async with self._lock:
            self._next_run_at[cron_id] = next_run_at

    async def get_active_run(self, cron_id: ULID) -> str | None:
        return self._active_runs.get(cron_id)

    async def set_active_run(self, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False) -> bool:
        async with self._lock:
            if nx and cron_id in self._active_runs:
                return False
            self._active_runs[cron_id] = str(task_id)
            return True

    async def clear_active_run(self, cron_id: ULID) -> None:
        async with self._lock:
            self._active_runs.pop(cron_id, None)

    async def get_next_run_at(self, cron_id: ULID) -> dt.datetime | None:
        return self._next_run_at.get(cron_id)

    async def list(
        self, offset: int = 0, limit: int = 50
    ) -> tuple[list[tuple[CronDAGEntry, dt.datetime]], int]:
        """Return scheduled (non-acquired) entries sorted by next_run_at, with total count."""
        async with self._lock:
            scheduled = sorted(
                (
                    (self._entries[cron_id], run_at)
                    for cron_id, run_at in self._next_run_at.items()
                    if cron_id in self._entries
                ),
                key=lambda t: t[1],
            )
        total = len(scheduled)
        return scheduled[offset : offset + limit], total
