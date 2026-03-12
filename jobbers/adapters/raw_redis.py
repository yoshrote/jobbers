"""
Plain Redis backed implementations (no Redis Stack modules required).

``MsgpackTaskAdapter``  – stores tasks as msgpack-encoded binary strings, queries
                          via sorted-set range commands.  Works with any standard
                          Redis instance.
``DeadQueue``           – dead letter queue backed by Redis sorted sets, sets, and
                          a hash for secondary indexes.  Works with any standard
                          Redis instance.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any, cast

from ulid import ULID

from jobbers.adapters.task_adapter import _BaseTaskAdapter
from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_status import TaskStatus
from jobbers.utils.serialization import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.adapters.task_adapter import TaskAdapterProtocol
    from jobbers.models.queue_config import QueueConfig


# ---------------------------------------------------------------------------
# MsgpackTaskAdapter  (plain Redis: binary strings)
# ---------------------------------------------------------------------------


class MsgpackTaskAdapter(_BaseTaskAdapter):
    """
    Stores tasks as msgpack-encoded binary strings; queries via sorted-set range commands.

    Works with any standard Redis instance (no Redis Stack required).
    ``get_all_tasks`` applies ``task_name``, ``task_version``, and ``status`` filters
    in Python after fetching candidate task IDs from the queue sorted set.
    """

    # Atomically enqueue a new task (no rate limiting).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # ARGV[1] = submitted_at timestamp
    # ARGV[2] = task_id bytes
    # ARGV[3] = '1'/'0' for type index
    # ARGV[4] = msgpack-encoded task blob
    SUBMIT_SCRIPT = """
        local exists = redis.call('EXISTS', KEYS[2])
        if exists == 0 then
            redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
        end
        redis.call('SET', KEYS[2], ARGV[4])
        if ARGV[3] == '1' then
            redis.call('SADD', KEYS[3], ARGV[2])
        else
            redis.call('SREM', KEYS[3], ARGV[2])
        end
        return 1
    """

    # Atomically check rate limit and enqueue.
    # KEYS[1] = rate-limiter:{queue}
    # KEYS[2] = task-queues:{queue}
    # KEYS[3] = task:{task_id}
    # KEYS[4] = task-type-idx:{name}
    # ARGV[1] = earliest_time
    # ARGV[2] = rate_numerator
    # ARGV[3] = submitted_at timestamp
    # ARGV[4] = task_id bytes
    # ARGV[5] = '1'/'0' for type index
    # ARGV[6] = msgpack-encoded task blob
    # Returns: 1 if enqueued, 0 if rate-limited
    SUBMIT_RATE_LIMITED_SCRIPT = """
        local enqueued = 0
        local exists = redis.call('EXISTS', KEYS[3])
        local numerator = tonumber(ARGV[2])
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
        if exists == 0 then
            local count = redis.call('ZCARD', KEYS[1])
            if numerator ~= 0 and count < numerator then
                redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
                redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
                redis.call('SET', KEYS[3], ARGV[6])
                enqueued = 1
            end
        else
            redis.call('SET', KEYS[3], ARGV[6])
            enqueued = 1
        end
        if enqueued == 1 and ARGV[5] == '1' then
            redis.call('SADD', KEYS[4], ARGV[4])
        else
            redis.call('SREM', KEYS[4], ARGV[4])
        end
        return enqueued
    """

    def _pack(self, task: Task) -> bytes:
        """Serialize task to msgpack bytes."""
        return serialize(task.to_dict())

    def _unpack(self, task_id: ULID, data: bytes) -> Task:
        """Deserialize task from msgpack bytes."""
        return Task.unpack(task_id, deserialize(data))

    async def submit_task(self, task: Task) -> bool:
        """Atomically enqueue a new task with no rate limiting. Status must already be SUBMITTED."""
        assert task.submitted_at  # noqa: S101
        is_active = "1" if task.status in TaskStatus.active_statuses() else "0"
        result: int = await cast(
            "Awaitable[int]",
            self.data_store.eval(
                self.SUBMIT_SCRIPT,
                3,
                self.TASKS_BY_QUEUE(queue=task.queue),
                self.TASK_DETAILS(task_id=task.id),
                self.TASK_BY_TYPE_IDX(name=task.name),
                task.submitted_at.timestamp(),
                bytes(task.id),
                is_active,
                self._pack(task),
            ),
        )
        return result == 1

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """Atomically check the rate limit and enqueue the task if there is room."""
        assert task.submitted_at  # noqa: S101
        now = dt.datetime.now(dt.UTC)
        earliest_time = now - dt.timedelta(seconds=queue_config.period_in_seconds() or 0)
        is_active = "1" if task.status in TaskStatus.active_statuses() else "0"
        result: int = await cast(
            "Awaitable[int]",
            self.data_store.eval(
                self.SUBMIT_RATE_LIMITED_SCRIPT,
                4,
                self.QUEUE_RATE_LIMITER(queue=task.queue),
                self.TASKS_BY_QUEUE(queue=task.queue),
                self.TASK_DETAILS(task_id=task.id),
                self.TASK_BY_TYPE_IDX(name=task.name),
                earliest_time.timestamp(),
                queue_config.rate_numerator or 0,
                task.submitted_at.timestamp(),
                bytes(task.id),
                is_active,
                self._pack(task),
            ),
        )
        return result == 1

    def stage_save(self, pipe: Pipeline, task: Task) -> None:
        """Queue SET task-details + type-index update onto pipe (no execute)."""
        pipe.set(self.TASK_DETAILS(task_id=task.id), self._pack(task))
        if task.status in TaskStatus.active_statuses():
            pipe.sadd(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        else:
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))

    async def _fetch_task_data_bulk(self, task_ids: list[ULID]) -> list[Any]:
        pipe = self.data_store.pipeline(transaction=False)
        for task_id in task_ids:
            pipe.get(self.TASK_DETAILS(task_id=task_id))
        return await pipe.execute()

    def _decode_task(self, task_id: ULID, raw: Any) -> Task:
        return self._unpack(task_id, raw)

    async def get_task(self, task_id: ULID) -> Task | None:
        raw_data: bytes | None = await self.data_store.get(self.TASK_DETAILS(task_id=task_id))
        if not raw_data:
            return None
        task = self._unpack(task_id, raw_data)
        heartbeat_score: float | None = await self.data_store.zscore(
            self.HEARTBEAT_SCORES(queue=task.queue), bytes(task_id)
        )
        if heartbeat_score is not None:
            task.heartbeat_at = dt.datetime.fromtimestamp(heartbeat_score, dt.UTC)
        return task

    async def read_for_watch(self, pipe: Pipeline, task_id: ULID) -> Task | None:
        """Read task data via a WATCH pipeline (msgpack format)."""
        raw_data: bytes | None = await pipe.get(self.TASK_DETAILS(task_id=task_id))
        if not raw_data:
            return None
        return self._unpack(task_id, raw_data)

    async def ensure_index(self) -> None:
        """No-op: msgpack backend does not use a search index."""

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Fetch tasks from the queue sorted set and filter in Python."""
        from jobbers.models.task import PaginationOrder

        if pagination.order_by == PaginationOrder.SUBMITTED_AT:
            raw_ids = await self.data_store.zrangebyscore(
                self.TASKS_BY_QUEUE(queue=pagination.queue),
                "-inf",
                "+inf",
                start=pagination.offset,
                num=pagination.limit * 5,  # over-fetch to allow for Python-side filtering
            )
        else:
            raw_ids = await self.data_store.zrange(
                self.TASKS_BY_QUEUE(queue=pagination.queue),
                pagination.offset,
                pagination.offset + pagination.limit * 5 - 1,
            )

        results: list[Task] = []
        for raw_id in raw_ids:
            if len(results) >= pagination.limit:
                break
            task_id = ULID.from_bytes(raw_id)
            raw_data: bytes | None = await self.data_store.get(self.TASK_DETAILS(task_id=task_id))
            if raw_data is None:
                continue
            task = self._unpack(task_id, raw_data)
            if pagination.task_name is not None and task.name != pagination.task_name:
                continue
            if pagination.task_version is not None and task.version != pagination.task_version:
                continue
            if pagination.status is not None and task.status != pagination.status:
                continue
            results.append(task)
        return results

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Delete blobs, heartbeat entries, and type-index members for old terminal tasks."""
        terminal_statuses = {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.STALLED,
            TaskStatus.DROPPED,
        }
        cutoff = now - max_age
        async for raw_key in self.data_store.scan_iter("task:*"):
            key = raw_key if isinstance(raw_key, bytes) else raw_key.encode()
            task_data: bytes | None = await self.data_store.get(key)
            if task_data is None:
                continue
            key_str = key.decode()
            task_id_str = key_str.removeprefix("task:")
            try:
                task_id = ULID.from_str(task_id_str)
            except ValueError:
                continue
            task = self._unpack(task_id, task_data)
            if task.status not in terminal_statuses:
                continue
            if task.completed_at is None or task.completed_at >= cutoff:
                continue
            pipe = self.data_store.pipeline(transaction=True)
            pipe.delete(key)
            pipe.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task_id))
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task_id))
            await pipe.execute()


# ---------------------------------------------------------------------------
# DeadQueue  (plain Redis: sorted sets, sets, hash indexes)
# ---------------------------------------------------------------------------


class DeadQueue:
    r"""
    Dead letter queue backed by Redis, reusing task:<task_id> keys for task data.

    Keys:
      ``dlq``                sorted set — member: task_id bytes, score: failed_at Unix timestamp.
      ``dlq-queue:{queue}``  set        — task_id bytes per queue name (queue filter index).
      ``dlq-name:{name}``    set        — task_id bytes per task name (name filter index).
      ``dlq-meta``           hash       — field: task_id bytes, value: b"{queue}\0{name}\0{version}".
    """

    DLQ = "dlq"
    DLQ_QUEUE = "dlq-queue:{queue}".format
    DLQ_NAME = "dlq-name:{name}".format
    DLQ_META = "dlq-meta"

    def __init__(self, data_store: Redis, task_adapter: TaskAdapterProtocol) -> None:
        self.data_store = data_store
        self.ta = task_adapter

    async def ensure_index(self) -> None:
        """No-op: plain-Redis dead queue does not use a search index."""

    def stage_add(self, pipe: Pipeline, task: Task, failed_at: dt.datetime) -> None:
        """Queue all four DLQ index writes onto pipe (no execute)."""
        pipe.zadd(self.DLQ, {bytes(task.id): failed_at.timestamp()})
        pipe.sadd(self.DLQ_QUEUE(queue=task.queue), bytes(task.id))
        pipe.sadd(self.DLQ_NAME(name=task.name), bytes(task.id))
        pipe.hset(self.DLQ_META, str(task.id), f"{task.queue}\0{task.name}\0{task.version}")

    def stage_remove(self, pipe: Pipeline, task_id: ULID, queue: str, name: str) -> None:
        """Queue all four DLQ index deletes onto pipe (no execute)."""
        pipe.zrem(self.DLQ, bytes(task_id))
        pipe.srem(self.DLQ_QUEUE(queue=queue), bytes(task_id))
        pipe.srem(self.DLQ_NAME(name=name), bytes(task_id))
        pipe.hdel(self.DLQ_META, str(task_id))

    async def get_history(self, task_id: str) -> list[dict[str, Any]]:
        """Return the per-attempt error history for a DLQ task from its stored task blob."""
        u = ULID.from_str(task_id)
        task = await self.ta.get_task(u)
        if task is None:
            return []
        return [{"attempt": i, "error": e} for i, e in enumerate(task.errors)]

    async def get_by_ids(self, task_ids: list[str]) -> list[Task]:
        """Fetch DLQ entries by explicit task ID list."""
        if not task_ids:
            return []
        ulids = [ULID.from_str(tid) for tid in task_ids]
        # Check which IDs are actually in the DLQ
        pipe = self.data_store.pipeline(transaction=False)
        for u in ulids:
            pipe.zscore(self.DLQ, bytes(u))
        scores = await pipe.execute()
        valid_ulids = [u for u, s in zip(ulids, scores) if s is not None]
        if not valid_ulids:
            return []
        tasks: list[Task | None] = await self.ta.get_tasks_bulk(valid_ulids)
        return [t for t in tasks if t is not None]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list[Task]:
        """Fetch DLQ entries matching the given filter criteria."""
        if queue is not None and task_name is not None:
            raw_ids: set[bytes] = await cast(
                "Awaitable[set[bytes]]",
                self.data_store.sinter([self.DLQ_QUEUE(queue=queue), self.DLQ_NAME(name=task_name)]),
            )
        elif queue is not None:
            raw_ids = await cast(
                "Awaitable[set[bytes]]",
                self.data_store.smembers(self.DLQ_QUEUE(queue=queue)),
            )
        elif task_name is not None:
            raw_ids = await cast(
                "Awaitable[set[bytes]]",
                self.data_store.smembers(self.DLQ_NAME(name=task_name)),
            )
        else:
            raw_ids = set(
                await cast(
                    "Awaitable[list[bytes]]",
                    self.data_store.zrange(self.DLQ, 0, -1),
                )
            )
        if not raw_ids:
            return []
        ulid_list = [ULID.from_bytes(b) for b in raw_ids]
        fetched: list[Task | None] = await self.ta.get_tasks_bulk(ulid_list)
        results: list[Task] = []
        for task in fetched:
            if len(results) >= limit:
                break
            if task is None:
                continue
            if task_version is not None and task.version != task_version:
                continue
            results.append(task)
        return results

    async def remove_many(self, task_ids: list[str]) -> None:
        """Remove multiple entries from the dead letter queue in a single transaction."""
        if not task_ids:
            return
        ulids = [ULID.from_str(tid) for tid in task_ids]
        # Batch-fetch metadata for all IDs
        read_pipe = self.data_store.pipeline(transaction=False)
        for u in ulids:
            read_pipe.hget(self.DLQ_META, str(u))
        meta_list = await read_pipe.execute()
        write_pipe = self.data_store.pipeline(transaction=True)
        for u, meta_bytes in zip(ulids, meta_list):
            if meta_bytes is None:
                continue
            queue, name, _ = meta_bytes.decode().split("\0")
            self.stage_remove(write_pipe, u, queue, name)
        await write_pipe.execute()

    async def clean(self, earlier_than: dt.datetime) -> None:
        """Remove failed tasks older than the specified datetime."""
        old_ids: list[bytes] = await cast(
            "Awaitable[list[bytes]]",
            self.data_store.zrangebyscore(self.DLQ, "-inf", earlier_than.timestamp()),
        )
        if not old_ids:
            return
        # Batch-fetch metadata so we can clean up secondary indexes
        old_ulid_strs = [str(ULID.from_bytes(b)) for b in old_ids]
        pipe = self.data_store.pipeline(transaction=False)
        for uid_str in old_ulid_strs:
            pipe.hget(self.DLQ_META, uid_str)
        meta_list = await pipe.execute()
        pipe2 = self.data_store.pipeline(transaction=True)
        for task_id_bytes, uid_str, meta_bytes in zip(old_ids, old_ulid_strs, meta_list):
            pipe2.zrem(self.DLQ, task_id_bytes)
            if meta_bytes is not None:
                queue, name, _ = meta_bytes.decode().split("\0")
                pipe2.srem(self.DLQ_QUEUE(queue=queue), task_id_bytes)
                pipe2.srem(self.DLQ_NAME(name=name), task_id_bytes)
                pipe2.hdel(self.DLQ_META, uid_str)
        await pipe2.execute()
