"""Shared helpers for the redis_json adapter package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pydantic import BaseModel


def _escape_tag(value: str) -> str:
    """Escape special characters for a RediSearch TAG query value."""
    special = set(r',.<>{}[]"\':;!@#$%^&*()\-+=~| ')
    return "".join(f"\\{c}" if c in special else c for c in value)


def _pack(model: BaseModel) -> dict[str, Any]:
    """Pack a Pydantic model to a dict with RedisJSON-compatible values (e.g. ULIDs as strings)."""
    return model.model_dump(mode="json", context={"mode": "redis_json"})
