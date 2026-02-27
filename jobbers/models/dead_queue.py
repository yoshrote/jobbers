import datetime as dt
from typing import TYPE_CHECKING, Any, cast

from redis.asyncio.client import Pipeline, Redis
from ulid import ULID

if TYPE_CHECKING:
    from collections.abc import Awaitable

from jobbers.models.task import Task


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

    def __init__(self, data_store: Redis) -> None:
        self.data_store = data_store

    def stage_add(self, pipe: Pipeline, task: "Task", failed_at: dt.datetime) -> None:
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

    async def add(self, task: "Task", failed_at: dt.datetime) -> None:
        """Insert or replace the DLQ entry for a task (latest failure wins)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_add(pipe, task, failed_at)
        await pipe.execute()

    async def get_history(self, task_id: str) -> list[dict[str, Any]]:
        """Return the per-attempt error history for a DLQ task from its stored task blob."""
        u = ULID.from_str(task_id)
        task_data: bytes | None = await self.data_store.get(f"task:{u}")
        if task_data is None:
            return []
        task = Task.unpack(u, task_data)
        return [{"attempt": i, "error": e} for i, e in enumerate(task.errors)]

    async def get_by_ids(self, task_ids: list[str]) -> list["Task"]:
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
        # Batch-fetch task data for confirmed DLQ members
        pipe2 = self.data_store.pipeline(transaction=False)
        for u in valid_ulids:
            pipe2.get(f"task:{u}")
        task_bytes_list = await pipe2.execute()
        return [Task.unpack(u, tb) for u, tb in zip(valid_ulids, task_bytes_list) if tb is not None]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list["Task"]:
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
        # Batch-fetch task data
        ulid_list = [ULID.from_bytes(b) for b in raw_ids]
        pipe = self.data_store.pipeline(transaction=False)
        for u in ulid_list:
            pipe.get(f"task:{u}")
        task_bytes_list = await pipe.execute()
        results: list[Task] = []
        for u, tb in zip(ulid_list, task_bytes_list):
            if len(results) >= limit:
                break
            if tb is None:
                continue
            task = Task.unpack(u, tb)
            if task_version is not None and task.version != task_version:
                continue
            results.append(task)
        return results

    async def remove(self, task_id: str) -> None:
        """Remove a single entry from the dead letter queue."""
        u = ULID.from_str(task_id)
        meta_bytes: bytes | None = await cast(
            "Awaitable[bytes | None]",
            self.data_store.hget(self.DLQ_META, str(u)),
        )
        if meta_bytes is None:
            return
        queue, name, _ = meta_bytes.decode().split("\0")
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_remove(pipe, u, queue, name)
        await pipe.execute()

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
