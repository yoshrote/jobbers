"""Redis-backed store for recurring cron+DAG schedule entries."""

from __future__ import annotations

import datetime as dt
import json
from typing import TYPE_CHECKING, Any, cast

from ulid import ULID

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.cron_dag import CronDAGEntry


class CronDAGScheduler:
    """
    Manages recurring cron-scheduled DAG entries in Redis.

    Keys:
    - `cron-dag:{cron_id}` hash — serialised CronDAGEntry fields.
    - `cron-schedule` sorted set — member: cron_id bytes, score: next_run_at Unix timestamp.
    - `cron-active:{id}` string — active root task ID for skip_if_running entries (with TTL).

    The efficient "what's due?" query is a single ZRANGEBYSCORE on `cron-schedule`,
    giving O(log N + K) where N = total entries and K = entries due now.
    """

    CRON_DAG_KEY = "cron-dag:{cron_id}".format
    CRON_SCHEDULE = "cron-schedule"
    CRON_ACTIVE_KEY = "cron-active:{cron_id}".format

    # Atomically acquire up to ARGV[2] entries with score <= ARGV[1] (now).
    # Returns flat list: [cron_id_bytes, score_str, cron_id_bytes, score_str, ...]
    _ACQUIRE_SCRIPT = """
        local now = ARGV[1]
        local limit = tonumber(ARGV[2])
        local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'WITHSCORES', 'LIMIT', 0, limit)
        for i = 1, #items, 2 do
            redis.call('ZREM', KEYS[1], items[i])
        end
        return items
    """

    def __init__(self, data_store: Redis) -> None:
        self.data_store = data_store

    def stage_add(self, pipe: Pipeline, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        """Queue HSET cron-dag:{id} + ZADD cron-schedule onto pipe (no execute)."""
        cron_id_str = str(entry.id)
        cron_id_bytes = bytes(entry.id)
        pipe.hset(
            self.CRON_DAG_KEY(cron_id=cron_id_str),
            mapping={
                "name": entry.name,
                "cron_expr": entry.cron_expr,
                "dag_spec": entry.dag_spec.model_dump_json(),
                "enabled": "1" if entry.enabled else "0",
                "concurrency_policy": entry.concurrency_policy.value,
                "created_at": entry.created_at.isoformat(),
            },
        )
        pipe.zadd(self.CRON_SCHEDULE, {cron_id_bytes: next_run_at.timestamp()})

    def stage_remove(self, pipe: Pipeline, cron_id: ULID) -> None:
        """Queue DEL cron-dag:{id} + ZREM cron-schedule onto pipe (no execute)."""
        pipe.delete(self.CRON_DAG_KEY(cron_id=str(cron_id)))
        pipe.zrem(self.CRON_SCHEDULE, bytes(cron_id))

    async def get(self, cron_id: ULID) -> CronDAGEntry | None:
        """Fetch and deserialize a single CronDAGEntry from its hash, or None if missing."""
        from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
        from jobbers.models.dag import DAGTaskSpec

        raw: dict[bytes, bytes] = await cast(
            "Awaitable[dict[bytes, bytes]]",
            self.data_store.hgetall(self.CRON_DAG_KEY(cron_id=str(cron_id))),
        )
        if not raw:
            return None
        return CronDAGEntry(
            id=cron_id,
            name=raw[b"name"].decode(),
            cron_expr=raw[b"cron_expr"].decode(),
            dag_spec=DAGTaskSpec.model_validate(json.loads(raw[b"dag_spec"])),
            enabled=raw[b"enabled"] == b"1",
            concurrency_policy=ConcurrencyPolicy(raw[b"concurrency_policy"].decode()),
            created_at=dt.datetime.fromisoformat(raw[b"created_at"].decode()),
        )

    async def next_due_bulk(self, n: int) -> list[tuple[CronDAGEntry, dt.datetime]]:
        """
        Atomically acquire and return up to n due cron entries paired with their scheduled run_at.

        Entries are removed from `cron-schedule` atomically; caller is responsible for
        rescheduling via `stage_reschedule` after dispatching.
        """
        now = dt.datetime.now(dt.UTC).timestamp()
        raw: list[Any] = await cast(
            "Awaitable[list[Any]]",
            self.data_store.eval(self._ACQUIRE_SCRIPT, 1, self.CRON_SCHEDULE, str(now), n),
        )

        results: list[tuple[CronDAGEntry, dt.datetime]] = []
        for i in range(0, len(raw), 2):
            cron_id = ULID.from_bytes(raw[i])
            run_at = dt.datetime.fromtimestamp(float(raw[i + 1]), dt.UTC)
            entry = await self.get(cron_id)
            if entry is not None:
                results.append((entry, run_at))
        return results

    def stage_reschedule(self, pipe: Pipeline, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Queue ZADD cron-schedule with updated next_run_at score onto pipe (no execute)."""
        pipe.zadd(self.CRON_SCHEDULE, {bytes(cron_id): next_run_at.timestamp()})

    async def get_active_run(self, cron_id: ULID) -> str | None:
        """Return the active root task ID string for a skip_if_running entry, or None."""
        raw: bytes | None = await cast(
            "Awaitable[bytes | None]",
            self.data_store.get(self.CRON_ACTIVE_KEY(cron_id=str(cron_id))),
        )
        return raw.decode() if raw is not None else None

    def stage_set_active_run(self, pipe: Pipeline, cron_id: ULID, task_id: ULID, ttl: int = 86400) -> None:
        """Queue SET cron-active:{id} with TTL onto pipe (no execute)."""
        pipe.set(self.CRON_ACTIVE_KEY(cron_id=str(cron_id)), str(task_id), ex=ttl)

    def stage_clear_active_run(self, pipe: Pipeline, cron_id: ULID) -> None:
        """Queue DEL cron-active:{id} onto pipe (no execute)."""
        pipe.delete(self.CRON_ACTIVE_KEY(cron_id=str(cron_id)))
