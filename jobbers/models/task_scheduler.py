from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from ulid import ULID

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Awaitable

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.adapters.task_adapter import TaskAdapterProtocol, TaskPipeline
    from jobbers.models.task import Task


class TaskScheduler:
    """
    Manages scheduled tasks in Redis, reusing task:<task_id> keys for task data.

    Keys:
      ``schedule-queue:{queue}``  sorted set — member: task_id bytes, score: run_at Unix timestamp.
      ``schedule-task-queue``     hash       — field: task_id bytes, value: queue name.
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
            local items = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'WITHSCORES', 'LIMIT', 0, remaining)
            for i = 1, #items, 2 do
                redis.call('ZREM', key, items[i])
                table.insert(results, items[i])
                table.insert(results, items[i + 1])
                collected = collected + 1
            end
        end
        return results
    """

    def __init__(self, data_store: Redis, task_adapter: TaskAdapterProtocol) -> None:
        self.data_store = data_store
        self.ta = task_adapter

    def stage_add(self, pipe: TaskPipeline, task: Task, run_at: dt.datetime) -> None:
        """Queue ZADD schedule-queue + HSET schedule-task-queue onto pipe (no execute)."""
        p = cast("Pipeline", pipe)
        p.zadd(self.SCHEDULE_QUEUE(queue=task.queue), {bytes(task.id): run_at.timestamp()})
        p.hset(self.SCHEDULE_TASK_QUEUE, str(task.id), task.queue)

    def stage_remove(self, pipe: TaskPipeline, task_id: ULID, queue: str) -> None:
        """Queue ZREM schedule-queue + HDEL schedule-task-queue onto pipe (no execute)."""
        p = cast("Pipeline", pipe)
        p.zrem(self.SCHEDULE_QUEUE(queue=queue), bytes(task_id))
        p.hdel(self.SCHEDULE_TASK_QUEUE, str(task_id))

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
        start_after: str | None = None,
    ) -> list[Task]:
        """
        Fetch scheduled entries matching the given filter criteria.

        ``start_after`` is an exclusive ULID cursor for page-by-page iteration.
        """
        if queue is not None:
            raw_ids: list[bytes] = await cast(
                "Awaitable[list[bytes]]",
                self.data_store.zrange(self.SCHEDULE_QUEUE(queue=queue), 0, -1),
            )
        else:
            all_queue_bytes: set[bytes] = await cast(
                "Awaitable[set[bytes]]",
                self.data_store.smembers("all-queues"),
            )
            raw_ids = []
            for q_bytes in all_queue_bytes:
                ids: list[bytes] = await cast(
                    "Awaitable[list[bytes]]",
                    self.data_store.zrange(self.SCHEDULE_QUEUE(queue=q_bytes.decode()), 0, -1),
                )
                raw_ids.extend(ids)

        # ULID bytes are time-ordered, so sorting by bytes matches task_id ascending.
        raw_ids.sort()

        if start_after is not None:
            cursor = bytes(ULID.from_str(start_after))
            raw_ids = [r for r in raw_ids if r > cursor]

        ulid_list = [ULID.from_bytes(r) for r in raw_ids]
        fetched: list[Task | None] = await self.ta.get_tasks_bulk(ulid_list)
        results: list[Task] = []
        for task in fetched:
            if len(results) >= limit:
                break
            if task is None:
                continue
            if task_name is not None and task.name != task_name:
                continue
            if task_version is not None and task.version != task_version:
                continue
            results.append(task)

        return results

    async def next_due(self, queues: list[str] | None = None) -> Task | None:
        """
        Atomically acquire and return the earliest due task, or None.

        - ``queues=None`` — match any queue
        - ``queues=[]``   — return None immediately
        - ``queues=[...]`` — only match tasks in the given queues
        """
        results = await self.next_due_bulk(1, queues=queues)
        return results[0][0] if results else None

    async def next_due_bulk(self, n: int, queues: list[str] | None = None) -> list[tuple[Task, dt.datetime]]:
        """
        Atomically acquire and return up to n due tasks paired with their scheduled run_at.

        - ``queues=None`` — match any queue
        - ``queues=[]``   — return [] immediately
        - ``queues=[...]`` — only match tasks in the given queues
        """
        import datetime as dt

        if queues is not None and not queues:
            return []

        if queues is None:
            all_queue_bytes: set[bytes] = await cast(
                "Awaitable[set[bytes]]",
                self.data_store.smembers("all-queues"),
            )
            queues = [q.decode() for q in all_queue_bytes]
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
