"""
Protocols and shared base for task storage and dead-letter-queue adapters.

``TaskAdapterProtocol``  – interface for task storage and querying.
``DeadQueueProtocol``    – interface for dead letter queue operations.
``_BaseTaskAdapter``     – shared key helpers and non-storage methods used by
                           both concrete task-adapter implementations.

Concrete implementations live in the sibling modules:

* ``jobbers.adapters.json_redis``  – Redis Stack (RedisJSON + RediSearch) backed classes.
* ``jobbers.adapters.raw_redis``   – plain Redis (msgpack / sorted-set) backed classes.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from opentelemetry import metrics
from ulid import ULID

from jobbers.constants import TIME_ZERO

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task import Task, TaskPagination

logger = logging.getLogger(__name__)
tasks_missing_data = metrics.get_meter(__name__).create_counter("tasks_missing_data", unit="1")


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class TaskAdapterProtocol(Protocol):
    """Interface for task storage and querying."""

    # -- key helpers (both implementations use the same Redis key names) ----
    TASKS_BY_QUEUE: Any
    TASK_DETAILS: Any
    HEARTBEAT_SCORES: Any
    TASK_BY_TYPE_IDX: Any
    QUEUE_RATE_LIMITER: Any
    DLQ_MISSING_DATA: str

    # -- write path ----------------------------------------------------------
    async def submit_task(self, task: Task) -> bool: ...
    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool: ...
    def stage_save(self, pipe: Pipeline, task: Task) -> None: ...
    def stage_requeue(self, pipe: Pipeline, task: Task) -> None: ...
    async def save_task(self, task: Task) -> None: ...

    # -- read path -----------------------------------------------------------
    async def get_task(self, task_id: ULID) -> Task | None: ...
    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]: ...
    async def read_for_watch(self, pipe: Pipeline, task_id: ULID) -> Task | None: ...
    async def task_exists(self, task_id: ULID) -> bool: ...
    async def get_active_tasks(self, queues: set[str]) -> list[Task]: ...
    def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]: ...
    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]: ...
    async def get_next_task(self, queues: set[str], pop_timeout: int) -> Task | None: ...

    # -- queue management ----------------------------------------------------
    def stage_remove_from_queue(self, pipe: Pipeline, task: Task) -> None: ...
    async def update_task_heartbeat(self, task: Task) -> None: ...
    async def remove_task_heartbeat(self, task: Task) -> None: ...

    # -- lifecycle -----------------------------------------------------------
    async def ensure_index(self) -> None: ...
    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None: ...
    async def clean(
        self,
        queues: set[bytes],
        now: dt.datetime,
        min_queue_age: dt.datetime | None,
        max_queue_age: dt.datetime | None,
    ) -> None: ...


class DeadQueueProtocol(Protocol):
    """Interface for dead letter queue operations."""

    async def ensure_index(self) -> None: ...
    def stage_add(self, pipe: Pipeline, task: Task, failed_at: dt.datetime) -> None: ...
    def stage_remove(self, pipe: Pipeline, task_id: ULID, queue: str, name: str) -> None: ...
    async def get_history(self, task_id: str) -> list[dict[str, Any]]: ...
    async def get_by_ids(self, task_ids: list[str]) -> list[Task]: ...
    async def get_by_filter(
        self,
        queue: str | None,
        task_name: str | None,
        task_version: int | None,
        limit: int,
    ) -> list[Task]: ...
    async def remove_many(self, task_ids: list[str]) -> None: ...
    async def clean(self, earlier_than: dt.datetime) -> None: ...


# ---------------------------------------------------------------------------
# Shared base helpers (not part of public Protocol, internal reuse)
# ---------------------------------------------------------------------------


class _BaseTaskAdapter:
    """Shared key-name helpers and non-storage methods used by both implementations."""

    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    HEARTBEAT_SCORES = "task-heartbeats:{queue}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    DLQ_MISSING_DATA = "dlq-missing-data"

    def __init__(self, data_store: Redis) -> None:
        self.data_store: Redis = data_store

    def stage_requeue(self, pipe: Pipeline, task: Task) -> None:
        """Queue ZADD task-queue + save-task commands onto pipe (no execute)."""
        assert task.submitted_at  # noqa: S101
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
        self.stage_save(pipe, task)

    def stage_save(self, pipe: Pipeline, task: Task) -> None:
        raise NotImplementedError

    async def save_task(self, task: Task) -> None:
        """Save task state to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_save(pipe, task)
        await pipe.execute()

    def stage_remove_from_queue(self, pipe: Pipeline, task: Task) -> None:
        """Queue ZREM task-queue + SREM type-index commands onto pipe (no execute)."""
        pipe.zrem(self.TASKS_BY_QUEUE(queue=task.queue), bytes(task.id))
        pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))

    async def update_task_heartbeat(self, task: Task) -> None:
        """Update the heartbeat for a task."""
        assert task.heartbeat_at  # noqa: S101
        pipe = self.data_store.pipeline(transaction=True)
        pipe.zadd(self.HEARTBEAT_SCORES(queue=task.queue), {bytes(task.id): task.heartbeat_at.timestamp()})
        await pipe.execute()

    async def remove_task_heartbeat(self, task: Task) -> None:
        """Remove a task from the heartbeat sorted set."""
        await self.data_store.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        """Fetch multiple tasks in 2 batched round-trips instead of 2N individual calls."""
        if not task_ids:
            return []
        # Round-trip 1: batch fetch all task documents
        raws = await self._fetch_task_data_bulk(task_ids)
        tasks: list[Task | None] = []
        valid: list[tuple[int, Task]] = []
        for i, (task_id, raw) in enumerate(zip(task_ids, raws)):
            if raw is None:
                tasks.append(None)
            else:
                task = self._decode_task(task_id, raw)
                tasks.append(task)
                valid.append((i, task))
        if not valid:
            return tasks
        # Round-trip 2: batch fetch heartbeat scores for all found tasks
        pipe = self.data_store.pipeline(transaction=False)
        for _, task in valid:
            pipe.zscore(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))
        scores = await pipe.execute()
        for (_, task), score in zip(valid, scores):
            if score is not None:
                task.heartbeat_at = dt.datetime.fromtimestamp(score, dt.UTC)
        return tasks

    async def _fetch_task_data_bulk(self, _task_ids: list[ULID]) -> list[Any]:
        raise NotImplementedError

    def _decode_task(self, _task_id: ULID, _raw: Any) -> Task:
        raise NotImplementedError

    async def get_active_tasks(self, queues: set[str]) -> list[Task]:
        """Return all tasks currently present in any heartbeat sorted set."""
        task_id_bytes: set[bytes] = set()
        for queue in queues:
            members: list[bytes] = await self.data_store.zrange(self.HEARTBEAT_SCORES(queue=queue), 0, -1)
            task_id_bytes.update(members)
        if not task_id_bytes:
            return []
        fetched = await self.get_tasks_bulk([ULID.from_bytes(b) for b in task_id_bytes])
        return [t for t in fetched if t is not None]

    async def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]:
        """Get tasks that have not had a heartbeat update in the stale time."""
        now = dt.datetime.now(dt.UTC)
        cutoff_time = now - stale_time
        stale_task_ids: set[bytes] = set()
        for queue in queues:
            task_ids: list[bytes] = await self.data_store.zrangebyscore(
                self.HEARTBEAT_SCORES(queue=queue), min=0, max=cutoff_time.timestamp()
            )
            stale_task_ids.update(task_ids)
        fetched = await self.get_tasks_bulk([ULID.from_bytes(b) for b in stale_task_ids])
        for task in fetched:
            if task is not None:
                yield task

    async def task_exists(self, task_id: ULID) -> bool:
        does_exists: int = await self.data_store.exists(self.TASK_DETAILS(task_id=task_id))
        return does_exists == 1

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        # TODO: Shuffle/rotate the order of queues to avoid starving any of them
        task_queues = {self.TASKS_BY_QUEUE(queue=queue) for queue in queues}
        pop_result = await self.data_store.bzpopmin(task_queues, timeout=pop_timeout)
        while pop_result:
            task = await self.get_task(ULID.from_bytes(pop_result[1]))
            if task:
                return task
            logger.error("Task %s popped from queue but data not found; adding to %s", pop_result[1], self.DLQ_MISSING_DATA)
            tasks_missing_data.add(1)
            now = dt.datetime.now(dt.UTC)
            await self.data_store.zadd(self.DLQ_MISSING_DATA, {pop_result[1]: now.timestamp()})
            pop_result = await self.data_store.bzpopmin(task_queues, timeout=pop_timeout)
        logger.info("task query timed out")
        return None

    async def clean(
        self,
        queues: set[bytes],
        now: dt.datetime,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
    ) -> None:
        """Remove queue entries within a time range."""
        if max_queue_age or min_queue_age:
            earliest_time = min_queue_age or TIME_ZERO
            latest_time = max_queue_age or now
            for queue in queues:
                pipe = self.data_store.pipeline(transaction=True)
                if earliest_time <= latest_time:
                    pipe.zremrangebyscore(
                        self.TASKS_BY_QUEUE(queue=queue.decode()),
                        min=earliest_time.timestamp(), max=latest_time.timestamp(),
                    )
                else:
                    pipe.zremrangebyscore(
                        self.TASKS_BY_QUEUE(queue=queue.decode()),
                        min=0, max=earliest_time.timestamp(),
                    )
                    pipe.zremrangebyscore(
                        self.TASKS_BY_QUEUE(queue=queue.decode()),
                        min=latest_time.timestamp(), max=now.timestamp(),
                    )
                await pipe.execute()

    async def _add_task_to_results(self, task_id: ULID, results: list[Task]) -> list[Task]:
        task = await self.get_task(task_id)
        if task:
            results.append(task)
        return results

    # Abstract in subclasses
    async def get_task(self, task_id: ULID) -> Task | None:
        raise NotImplementedError

    async def read_for_watch(self, pipe: Pipeline, task_id: ULID) -> Task | None:
        raise NotImplementedError

    async def ensure_index(self) -> None:
        raise NotImplementedError

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        raise NotImplementedError

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        raise NotImplementedError

    async def submit_task(self, task: Task) -> bool:
        raise NotImplementedError

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        raise NotImplementedError
