"""Read-only in-process adapters — no database required."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ulid import ULID

from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task_routing import RoutingConfig, RoutingStrategy
from jobbers.protocols import RoutingBackendReadOnlyError

if TYPE_CHECKING:
    from jobbers.models.cron_dag import CronDAGEntry

_DEFAULT_QUEUE = QueueConfig(name="default", max_concurrent=10)
_DEFAULT_ROLES: dict[str, set[str]] = {"default": {"default"}}


def _load_file(path: str) -> dict[str, Any]:
    p = Path(path)
    text = p.read_text()
    if p.suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError("PyYAML is required for YAML config files: pip install jobbers[yaml]") from exc
        return yaml.safe_load(text)  # type: ignore[no-any-return]
    return json.loads(text)  # type: ignore[no-any-return]


def _parse_queues(raw: list[dict[str, Any]]) -> list[QueueConfig]:
    return [
        QueueConfig(
            name=q["name"],
            max_concurrent=q.get("max_concurrent"),
            rate_numerator=q.get("rate_numerator"),
            rate_denominator=q.get("rate_denominator"),
            rate_period=RatePeriod(q["rate_period"]) if q.get("rate_period") else None,
        )
        for q in raw
    ]


def _parse_roles(raw: dict[str, list[str]]) -> dict[str, set[str]]:
    return {role: set(queues) for role, queues in raw.items()}


def _parse_routing(raw: list[dict[str, Any]]) -> list[RoutingConfig]:
    return [
        RoutingConfig(
            task_name=r["task_name"],
            task_version=r["task_version"],
            strategy=RoutingStrategy(r["strategy"]),
            queues=r["queues"],
            weights=r.get("weights"),
        )
        for r in raw
    ]


class StaticRoutingBackend:
    """
    RoutingBackendProtocol backed by in-process memory; config fixed at startup.

    Write operations raise RoutingBackendReadOnlyError. Intended for Celery-like
    deployments where queues and roles are defined once and never changed at runtime.

    Configuration priority (highest to lowest):
      1. Constructor arguments
      2. STATIC_CONFIG_FILE env var → JSON or YAML file
      3. STATIC_QUEUES / STATIC_ROLES / STATIC_ROUTING inline JSON env vars
      4. Built-in defaults: one "default" queue and "default" role
    """

    def __init__(
        self,
        queues: list[QueueConfig] | None = None,
        roles: dict[str, set[str]] | None = None,
        routing_configs: list[RoutingConfig] | None = None,
    ) -> None:
        self._queues: dict[str, QueueConfig] = {q.name: q for q in (queues or [_DEFAULT_QUEUE])}
        self._roles: dict[str, set[str]] = roles if roles is not None else dict(_DEFAULT_ROLES)
        self._routing: dict[tuple[str, int], RoutingConfig] = {
            (rc.task_name, rc.task_version): rc for rc in (routing_configs or [])
        }
        self._refresh_tag = ULID()

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_file(cls, path: str) -> StaticRoutingBackend:
        data = _load_file(path)
        return cls(
            queues=_parse_queues(data.get("queues", [])) or None,
            roles=_parse_roles(data.get("roles", {})) or None,
            routing_configs=_parse_routing(data.get("routing", [])),
        )

    @classmethod
    def from_env(cls) -> StaticRoutingBackend:
        config_file = os.environ.get("STATIC_CONFIG_FILE")
        if config_file:
            return cls.from_file(config_file)

        queues: list[QueueConfig] | None = None
        roles: dict[str, set[str]] | None = None
        routing_configs: list[RoutingConfig] = []

        raw_queues = os.environ.get("STATIC_QUEUES")
        if raw_queues:
            queues = _parse_queues(json.loads(raw_queues))

        raw_roles = os.environ.get("STATIC_ROLES")
        if raw_roles:
            roles = _parse_roles(json.loads(raw_roles))

        raw_routing = os.environ.get("STATIC_ROUTING")
        if raw_routing:
            routing_configs = _parse_routing(json.loads(raw_routing))

        return cls(queues=queues, roles=roles, routing_configs=routing_configs)

    # ── Queue reads ───────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        return self._queues.get(queue)

    async def get_all_queues(self) -> list[str]:
        return sorted(self._queues)

    # ── Role reads ────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        return self._roles.get(role, set())

    async def get_all_roles(self) -> list[str]:
        return sorted(self._roles)

    # ── Refresh tags (static — workers never re-poll) ─────────────────────────

    async def get_refresh_tag(self, role: str) -> ULID:
        return self._refresh_tag

    # ── Routing config reads ──────────────────────────────────────────────────

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        return self._routing.get((task_name, task_version))

    # ── Write operations (not supported) ─────────────────────────────────────

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def delete_queue(self, queue_name: str) -> None:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def delete_role(self, role: str) -> None:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def bump_refresh_tag(self, role: str) -> str:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        return False


# ---------------------------------------------------------------------------
# StaticCronDAGScheduler — in-memory, read-only cron DAG scheduler
# ---------------------------------------------------------------------------


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
