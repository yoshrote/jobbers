"""
Plain Redis cron DAG scheduler adapter.

- `RedisCronDAGScheduler` — CronDAGSchedulerProtocol + AtomicCronDAGSchedulerProtocol backed by Redis.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import TYPE_CHECKING, Any, cast

from ulid import ULID

from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGTaskSpec

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.protocols import TransactionHandle

logger = logging.getLogger(__name__)


class RedisCronDAGScheduler:
    """
    CronDAGSchedulerProtocol and AtomicCronDAGSchedulerProtocol backed by plain Redis.

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
        local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'WITHSCORES', 'LIMIT', '0', ARGV[2])
        for i = 1, #items, 2 do
            redis.call('ZREM', KEYS[1], items[i])
        end
        return items
    """

    def __init__(self, data_store: Redis) -> None:
        self.data_store = data_store

    @property
    def backend_key(self) -> str:
        """Stable identifier matching other adapters that use the same Redis client."""
        return str(id(self.data_store))

    def pipeline(self, transaction: bool = True) -> Pipeline:
        """Return a Redis pipeline for atomic staging of cron ops."""
        return self.data_store.pipeline(transaction=transaction)

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

    async def add(self, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        """Persist a cron entry and schedule it (direct, non-staged version)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_add(pipe, entry, next_run_at)
        await pipe.execute()

    async def remove(self, cron_id: ULID) -> None:
        """Delete a cron entry and remove it from the schedule (direct, non-staged version)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_remove(pipe, cron_id)
        await pipe.execute()

    async def get(self, cron_id: ULID) -> CronDAGEntry | None:
        """Fetch and deserialize a single CronDAGEntry from its hash, or None if missing."""
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
            if entry is None:
                # The hash was deleted (e.g. by an admin) after the Lua script removed the
                # entry from cron-schedule.  Re-add it with a short retry delay so it is
                # not permanently lost; the caller will skip dispatch because there is no entry.
                retry_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=60)
                pipe = self.data_store.pipeline(transaction=True)
                pipe.zadd(self.CRON_SCHEDULE, {bytes(cron_id): retry_at.timestamp()})
                await pipe.execute()
                logger.error(
                    "Cron entry %s removed from schedule but hash is missing; "
                    "re-added with 60s retry delay. Check for concurrent deletion.",
                    cron_id,
                )
                continue
            results.append((entry, run_at))
        return results

    def stage_reschedule(self, pipe: TransactionHandle, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Queue ZADD cron-schedule with updated next_run_at score onto pipe (no execute)."""
        p: Any = pipe
        p.zadd(self.CRON_SCHEDULE, {bytes(cron_id): next_run_at.timestamp()})

    async def reschedule(self, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Update the cron entry's next scheduled run time (non-pipeline version for saga path)."""
        await self.data_store.zadd(self.CRON_SCHEDULE, {bytes(cron_id): next_run_at.timestamp()})

    async def get_active_run(self, cron_id: ULID) -> str | None:
        """Return the active root task ID string for a skip_if_running entry, or None."""
        raw: bytes | None = await cast(
            "Awaitable[bytes | None]",
            self.data_store.get(self.CRON_ACTIVE_KEY(cron_id=str(cron_id))),
        )
        return raw.decode() if raw is not None else None

    async def set_active_run(self, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False) -> bool:
        """Set the active-run marker for a cron entry. Returns False when nx=True and key exists."""
        result = await self.data_store.set(
            self.CRON_ACTIVE_KEY(cron_id=str(cron_id)), str(task_id), ex=ttl, nx=nx
        )
        return result is not None

    def stage_set_active_run(
        self, pipe: TransactionHandle, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False
    ) -> None:
        """
        Queue SET cron-active:{id} with TTL onto pipe (no execute).

        When *nx=True* the SET is conditional (SET NX): it only succeeds if the key
        does not already exist, providing an atomic guard against concurrent dispatches.
        """
        p: Any = pipe
        p.set(self.CRON_ACTIVE_KEY(cron_id=str(cron_id)), str(task_id), ex=ttl, nx=nx)

    def stage_clear_active_run(self, pipe: TransactionHandle, cron_id: ULID) -> None:
        """Queue DEL cron-active:{id} onto pipe (no execute)."""
        p: Any = pipe
        p.delete(self.CRON_ACTIVE_KEY(cron_id=str(cron_id)))

    async def clear_active_run(self, cron_id: ULID) -> None:
        """Delete the active-run marker for a cron entry (non-pipeline version for saga path)."""
        await self.data_store.delete(self.CRON_ACTIVE_KEY(cron_id=str(cron_id)))

    async def get_next_run_at(self, cron_id: ULID) -> dt.datetime | None:
        """Return the next scheduled run time for a cron entry, or None if not scheduled."""
        score: float | None = await cast(
            "Awaitable[float | None]",
            self.data_store.zscore(self.CRON_SCHEDULE, bytes(cron_id)),
        )
        return dt.datetime.fromtimestamp(score, dt.UTC) if score is not None else None

    async def list(
        self, offset: int = 0, limit: int = 50
    ) -> tuple[list[tuple[CronDAGEntry, dt.datetime]], int]:
        """
        Return a page of cron entries ordered by next_run_at ascending.

        Returns (entry, next_run_at) pairs plus the total count of all entries in the schedule.
        """
        total: int = await self.data_store.zcard(self.CRON_SCHEDULE)
        raw: list[tuple[bytes, float]] = await self.data_store.zrange(
            self.CRON_SCHEDULE, offset, offset + limit - 1, withscores=True
        )
        results: list[tuple[CronDAGEntry, dt.datetime]] = []
        for cron_id_bytes, score in raw:
            cron_id = ULID.from_bytes(cron_id_bytes)
            entry = await self.get(cron_id)
            if entry is not None:
                next_run_at = dt.datetime.fromtimestamp(score, dt.UTC)
                results.append((entry, next_run_at))
        return results, total
