"""Shared helpers for the redis_json adapter package."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pydantic import BaseModel
    from redis.asyncio.client import Redis


def _escape_tag(value: str) -> str:
    """Escape special characters for a RediSearch TAG query value."""
    special = set(r',.<>{}[]"\':;!@#$%^&*()\-+=~| ')
    return "".join(f"\\{c}" if c in special else c for c in value)


def _pack(model: BaseModel) -> dict[str, Any]:
    """Pack a Pydantic model to a dict with RedisJSON-compatible values (e.g. ULIDs as strings)."""
    return model.model_dump(mode="json", context={"mode": "redis_json"})


async def _get_schema_version(client: Redis, version_key: str) -> int:
    """Return the schema version stored at version_key, or 0 if it has never been set."""
    stored = await client.get(version_key)
    return int(stored) if stored is not None else 0


async def _set_schema_version(client: Redis, version_key: str, version: int) -> None:
    """Record that version_key's schema has been migrated up to version."""
    await client.set(version_key, version)


def _stale_index_names(existing_names: Iterable[str], current_name: str, current_version: int) -> list[str]:
    """
    Return names from existing_names that share current_name's naming scheme but carry an older version.

    current_name is expected to end with f"v{current_version}" (e.g. "task-idx-v2");
    the shared prefix before that suffix is used to match other generations (e.g. "task-idx-v1").
    """
    prefix = current_name.removesuffix(f"v{current_version}")
    pattern = re.compile(rf"^{re.escape(prefix)}v(\d+)$")
    stale = []
    for name in existing_names:
        match = pattern.match(name)
        if match and int(match.group(1)) < current_version:
            stale.append(name)
    return stale


async def _drop_stale_indexes(client: Redis, current_names: Iterable[str], current_version: int) -> list[str]:
    """Drop RediSearch indexes from older schema generations of any index in current_names."""
    raw_names = await client.execute_command("FT._LIST")  # type: ignore[no-untyped-call]
    existing = [n.decode() if isinstance(n, bytes) else n for n in raw_names]
    dropped: list[str] = []
    for current_name in current_names:
        for stale_name in _stale_index_names(existing, current_name, current_version):
            await client.ft(stale_name).dropindex()
            dropped.append(stale_name)
    return dropped
