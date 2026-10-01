"""Read-only in-process routing backend — no database required."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.protocols import RoutingBackendReadOnlyError

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


class StaticRoutingBackend:
    """
    RoutingBackendProtocol backed by in-process memory; config fixed at startup.

    Write operations raise RoutingBackendReadOnlyError. Intended for Celery-like
    deployments where queues and roles are defined once and never changed at runtime.

    Configuration priority (highest to lowest):
      1. Constructor arguments
      2. STATIC_CONFIG_FILE env var → JSON or YAML file
      3. Built-in defaults: one "default" queue and "default" role
    """

    def __init__(
        self,
        queues: list[QueueConfig] | None = None,
        roles: dict[str, set[str]] | None = None,
    ) -> None:
        self._queues: dict[str, QueueConfig] = {q.name: q for q in (queues or [_DEFAULT_QUEUE])}
        self._roles: dict[str, set[str]] = roles if roles is not None else dict(_DEFAULT_ROLES)

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_file(cls, path: str) -> StaticRoutingBackend:
        data = _load_file(path)
        roles = _parse_roles(data.get("roles", {})) or None
        queues = _parse_queues(data.get("queues", [])) or None
        known_queues = {q.name for q in (queues or [_DEFAULT_QUEUE])}

        if roles is not None:
            for role_name, role_queues in roles.items():
                for q in role_queues:
                    if q not in known_queues:
                        raise ValueError(f"Role '{role_name}' references unknown queue '{q}'")

        return cls(queues=queues, roles=roles)

    async def drop_stale_indexes(self) -> list[str]:
        """No-op: in-process backend holds no search index."""
        return []

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

    # ── Role discovery ────────────────────────────────────────────────────────

    async def get_roles_for_queue(self, queue_name: str) -> list[str]:
        return [role for role, queues in self._roles.items() if queue_name in queues]

    # ── Write operations (not supported) ─────────────────────────────────────

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def create_queue_config(self, queue_config: QueueConfig) -> bool:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def delete_queue(self, queue_name: str) -> list[str]:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def create_role(self, role: str, queues_set: set[str]) -> bool:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )

    async def delete_role(self, role: str) -> None:
        raise RoutingBackendReadOnlyError(
            "Static routing backend is read-only. Use ROUTING_BACKEND=sql or ROUTING_BACKEND=redis for dynamic config."
        )
