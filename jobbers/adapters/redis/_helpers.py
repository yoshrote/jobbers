"""Shared helpers for the redis adapter package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from jobbers.utils.serialization import serialize

if TYPE_CHECKING:
    from pydantic import BaseModel


def _pack(obj: BaseModel, exclude: set[str] | None = None) -> bytes:
    """Serialize any Pydantic model to msgpack bytes."""
    kw: dict[str, Any] = {"context": {"mode": "msgpack"}}
    if exclude:
        kw["exclude"] = exclude
    return serialize(obj.model_dump(**kw))
