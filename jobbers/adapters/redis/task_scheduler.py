"""
Plain Redis task scheduler adapter.

- `RedisTaskScheduler` — TaskSchedulerProtocol + AtomicTaskSchedulerProtocol backed by Redis sorted sets.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING, Any, cast

from ulid import ULID

from jobbers.models.task_status import TaskStatus

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.task import Task
    from jobbers.protocols import TaskStateProtocol, TransactionHandle


class RedisTaskScheduler:
    """
    TaskSchedulerProtocol and AtomicTaskSchedulerProtocol backed by plain Redis (sorted sets).

    Reuses ``task:<task_id>`` keys for task data; stores schedules in sorted sets.

    Keys:
    - `schedule-queue:{queue}` sorted set — member: task_id bytes, score: run_at Unix timestamp.
    - `schedule-task-queue` hash — field: task_id bytes, value: queue name.
    """

    SCHEDULE_QUEUE = "schedule-queue:{queue}".format
    SCHEDULE_TASK_QUEUE = "schedule-task-queue"

    # Atomically acquire up to ARGV[2] tasks with score <= ARGV[1] (now) across all KEYS.
    # Each key is a "schedule-queue:{queue}" sorted set.
    # Returns a flat list: [task_id_bytes, score_str, task_id_bytes, score_str, ...]
    _ACQUIRE_SCRIPT = """
        local now = ARGV[1]
        local limit = tonumber(ARGV[2])
        local results = {}
        local collected = 0
        for _, key in ipairs(KEYS) do
            if collected >= limit then break end
            local remaining = limit - collected
            local members = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'LIMIT', 0, remaining)
            for i = 1, #members do
                local score = redis.call('ZSCORE', key, members[i])
                redis.call('ZREM', key, members[i])
                table.insert(results, members[i])
                table.insert(results, score)
                collected = collected + 1
            end
        end
        return results
    """

    def __init__(
        self,
        data_store: Redis,
        task_adapter: TaskStateProtocol,
        get_all_queues: Callable[[], Awaitable[list[str]]],
    ) -> None:
        self.data_store = data_store
        self.ta = task_adapter
        self._get_all_queues = get_all_queues

    @property
    def backend_key(self) -> str:
        return str(id(self.data_store))

    def pipeline(self, transaction: bool = True) -> Pipeline:
        return self.data_store.pipeline(transaction=transaction)

    def stage_add(self, pipe: TransactionHandle, task: Task, run_at: dt.datetime) -> None:
        """Queue ZADD schedule-queue + HSET schedule-task-queue onto pipe (no execute)."""
        p: Any = pipe
        p.zadd(self.SCHEDULE_QUEUE(queue=task.queue), {bytes(task.id): run_at.timestamp()})
        p.hset(self.SCHEDULE_TASK_QUEUE, str(task.id), task.queue)

    async def add(self, task: Task, run_at: dt.datetime) -> None:
        """Add a task to the scheduler (non-pipeline version for saga path)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_add(pipe, task, run_at)
        await pipe.execute()

    def stage_remove(self, pipe: TransactionHandle, task_id: ULID, queue: str) -> None:
        """Queue ZREM schedule-queue + HDEL schedule-task-queue onto pipe (no execute)."""
        p: Any = pipe
        p.zrem(self.SCHEDULE_QUEUE(queue=queue), bytes(task_id))
        p.hdel(self.SCHEDULE_TASK_QUEUE, str(task_id))

    async def remove(self, task_id: ULID, queue: str) -> None:
        """Remove a task from the scheduler (non-pipeline version for saga path)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_remove(pipe, task_id, queue)
        await pipe.execute()

    async def get_run_at(self, task_id: ULID) -> dt.datetime | None:
        """Return the scheduled run_at for a single task, or None if not found."""
        queue_raw: bytes | None = await cast(
            "Awaitable[bytes | None]", self.data_store.hget(self.SCHEDULE_TASK_QUEUE, str(task_id))
        )
        if queue_raw is None:
            return None
        score: float | None = await self.data_store.zscore(
            self.SCHEDULE_QUEUE(queue=queue_raw.decode()), bytes(task_id)
        )
        return dt.datetime.fromtimestamp(score, dt.UTC) if score is not None else None

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
        start_after: str | None = None,
    ) -> list[tuple[Task, dt.datetime]]:
        """
        Fetch scheduled entries matching the given filter criteria.

        `start_after` is an exclusive ULID cursor for page-by-page iteration.
        Returns each task paired with its scheduled run_at timestamp.
        """
        if queue is not None:
            pairs: list[tuple[bytes, float]] = await cast(
                "Awaitable[list[tuple[bytes, float]]]",
                self.data_store.zrange(self.SCHEDULE_QUEUE(queue=queue), 0, -1, withscores=True),
            )
            score_map: dict[bytes, float] = dict(pairs)
        else:
            score_map = {}
            all_queues = await self._get_all_queues()
            for q in all_queues:
                pairs = await cast(
                    "Awaitable[list[tuple[bytes, float]]]",
                    self.data_store.zrange(self.SCHEDULE_QUEUE(queue=q), 0, -1, withscores=True),
                )
                score_map.update(pairs)

        raw_ids = sorted(score_map.keys())

        if start_after is not None:
            cursor = bytes(ULID.from_str(start_after))
            raw_ids = [r for r in raw_ids if r > cursor]

        ulid_list = [ULID.from_bytes(r) for r in raw_ids]
        fetched: list[Task | None] = await self.ta.get_tasks_bulk(ulid_list)
        results: list[tuple[Task, dt.datetime]] = []
        for task, id_bytes in zip(fetched, raw_ids, strict=True):
            if len(results) >= limit:
                break
            if task is None:
                continue
            if task_name is not None and task.name != task_name:
                continue
            if task_version is not None and task.version != task_version:
                continue
            run_at = dt.datetime.fromtimestamp(score_map[id_bytes], dt.UTC)
            results.append((task, run_at))

        return results

    async def recover_orphans(self, now: dt.datetime) -> None:
        """
        Re-add tasks that were acquired by the Lua script but never dispatched.

        Detection: task_id in schedule-task-queue hash + SCHEDULED blob + absent from sorted set.
        Also cleans up stale hash entries for tasks that are no longer SCHEDULED (dispatch
        completed but saga HDEL failed).
        """
        raw_hash: dict[bytes, bytes] = await cast(
            "Awaitable[dict[bytes, bytes]]",
            self.data_store.hgetall(self.SCHEDULE_TASK_QUEUE),
        )
        if not raw_hash:
            return

        task_id_strs = [k.decode() for k in raw_hash]
        queue_names = [v.decode() for v in raw_hash.values()]
        task_ids = [ULID.from_str(s) for s in task_id_strs]

        tasks: list[Any] = await self.ta.get_tasks_bulk(task_ids)

        scheduled: list[tuple[str, str, bytes]] = []  # (task_id_str, queue, task_id_bytes)
        stale_ids: list[str] = []
        for task_id_str, queue, task_id, task in zip(task_id_strs, queue_names, task_ids, tasks):
            if task is None or task.status != TaskStatus.SCHEDULED:
                stale_ids.append(task_id_str)
            else:
                scheduled.append((task_id_str, queue, bytes(task_id)))

        orphan_entries: list[tuple[str, bytes]] = []  # (queue, task_id_bytes)
        if scheduled:
            pipe = self.data_store.pipeline(transaction=False)
            for _, queue, task_id_bytes in scheduled:
                pipe.zscore(self.SCHEDULE_QUEUE(queue=queue), task_id_bytes)
            scores: list[float | None] = await pipe.execute()
            for (_, queue, task_id_bytes), score in zip(scheduled, scores):
                if score is None:
                    orphan_entries.append((queue, task_id_bytes))

        if orphan_entries:
            pipe = self.data_store.pipeline(transaction=False)
            ts = now.timestamp()
            for queue, task_id_bytes in orphan_entries:
                pipe.zadd(self.SCHEDULE_QUEUE(queue=queue), {task_id_bytes: ts})
            await pipe.execute()
            logger.info("Recovered %d orphaned scheduled task(s).", len(orphan_entries))

        if stale_ids:
            pipe = self.data_store.pipeline(transaction=False)
            for task_id_str in stale_ids:
                pipe.hdel(self.SCHEDULE_TASK_QUEUE, task_id_str)
            await pipe.execute()
            logger.info("Removed %d stale schedule-task-queue hash entry/entries.", len(stale_ids))

    async def next_due(self, queues: list[str] | None = None) -> Task | None:
        """
        Atomically acquire and return the earliest due task, or None.

        - `queues=None` — match any queue
        - `queues=[]` — return None immediately
        - `queues=[...]` — only match tasks in the given queues
        """
        results = await self.next_due_bulk(1, queues=queues)
        return results[0][0] if results else None

    async def next_due_bulk(self, n: int, queues: list[str] | None = None) -> list[tuple[Task, dt.datetime]]:
        """
        Atomically acquire and return up to n due tasks paired with their scheduled run_at.

        - `queues=None` — match any queue
        - `queues=[]` — return [] immediately
        - `queues=[...]` — only match tasks in the given queues
        """
        if queues is not None and not queues:
            return []

        if queues is None:
            queues = await self._get_all_queues()
            if not queues:
                return []

        now = dt.datetime.now(dt.UTC).timestamp()
        keys = [self.SCHEDULE_QUEUE(queue=q) for q in queues]
        raw: list[Any] = await cast(
            "Awaitable[list[Any]]",
            self.data_store.eval(self._ACQUIRE_SCRIPT, len(keys), *keys, str(now), n),
        )

        task_ids = [ULID.from_bytes(raw[i]) for i in range(0, len(raw), 2)]
        run_ats = [dt.datetime.fromtimestamp(float(raw[i + 1]), dt.UTC) for i in range(0, len(raw), 2)]
        tasks: list[Task | None] = await self.ta.get_tasks_bulk(task_ids)
        return [(task, run_at) for task, run_at in zip(tasks, run_ats) if task is not None]
