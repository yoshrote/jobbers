"""
Task adapter interface and concrete implementations.

``TaskAdapterProtocol`` defines the full interface for task storage and querying.

Two implementations are provided:

* ``JsonTaskAdapter``     – stores tasks as RedisJSON documents, queries via RediSearch.
                            Requires a Redis Stack instance (RedisJSON + RediSearch modules).
* ``MsgpackTaskAdapter``  – stores tasks as msgpack-encoded binary strings, queries via
                            sorted-set range commands.  Works with any standard Redis instance.

Select the backend at startup via the ``TASK_ADAPTER_BACKEND`` environment variable
(``"json"`` or ``"msgpack"``; default ``"json"``), or by passing an adapter instance
directly to :class:`~jobbers.state_manager.StateManager`.

``TaskAdapter`` is an alias for ``JsonTaskAdapter`` to preserve existing import sites.
"""

from __future__ import annotations

import datetime as dt
import logging
from asyncio import TaskGroup
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from opentelemetry import metrics
from redis.exceptions import ResponseError
from ulid import ULID

from jobbers.constants import TIME_ZERO
from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_status import TaskStatus
from jobbers.utils.serialization import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.queue_config import QueueConfig

logger = logging.getLogger(__name__)
tasks_missing_data = metrics.get_meter(__name__).create_counter("tasks_missing_data", unit="1")


# ---------------------------------------------------------------------------
# Protocol
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
    async def requeue_task(self, task: Task) -> None: ...
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
    async def remove_from_queue(self, task: Task) -> None: ...
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

    async def requeue_task(self, task: Task) -> None:
        """Re-enqueue an existing task for retry (bypasses the new-task existence check)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_requeue(pipe, task)
        await pipe.execute()

    async def save_task(self, task: Task) -> None:
        """Save task state to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_save(pipe, task)
        await pipe.execute()

    async def remove_from_queue(self, task: Task) -> None:
        """Remove a task from its queue."""
        pipe = self.data_store.pipeline(transaction=True)
        pipe.zrem(self.TASKS_BY_QUEUE(queue=task.queue), bytes(task.id))
        pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        await pipe.execute()

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


# ---------------------------------------------------------------------------
# JsonTaskAdapter  (Redis Stack: RedisJSON + RediSearch)
# ---------------------------------------------------------------------------

class JsonTaskAdapter(_BaseTaskAdapter):
    """
    Stores tasks as RedisJSON documents; queries via RediSearch.

    Requires a Redis Stack instance (RedisJSON + RediSearch modules).
    """

    INDEX_NAME = "task-idx"

    # Atomically enqueue a new task (no rate limiting).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # ARGV[1] = submitted_at timestamp
    # ARGV[2] = task_id bytes
    # ARGV[3] = '1' to SADD type index, '0' to SREM
    # ARGV[4] = JSON-encoded task blob
    SUBMIT_SCRIPT = """
        local exists = redis.call('EXISTS', KEYS[2])
        if exists == 0 then
            redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
        end
        redis.call('JSON.SET', KEYS[2], '$', ARGV[4])
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
    # ARGV[6] = JSON-encoded task blob
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
                redis.call('JSON.SET', KEYS[3], '$', ARGV[6])
                enqueued = 1
            end
        else
            redis.call('JSON.SET', KEYS[3], '$', ARGV[6])
            enqueued = 1
        end
        if enqueued == 1 and ARGV[5] == '1' then
            redis.call('SADD', KEYS[4], ARGV[4])
        else
            redis.call('SREM', KEYS[4], ARGV[4])
        end
        return enqueued
    """

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
                task.pack(),
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
                task.pack(),
            ),
        )
        return result == 1

    def stage_save(self, pipe: Pipeline, task: Task) -> None:
        """Queue JSON.SET task-details + type-index update onto pipe (no execute)."""
        pipe.json().set(self.TASK_DETAILS(task_id=task.id), "$", task.to_dict())
        if task.status in TaskStatus.active_statuses():
            pipe.sadd(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        else:
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))

    async def _fetch_task_data_bulk(self, task_ids: list[ULID]) -> list[Any]:
        pipe = self.data_store.pipeline(transaction=False)
        for task_id in task_ids:
            pipe.json().get(self.TASK_DETAILS(task_id=task_id))
        return await pipe.execute()  # type: ignore[no-any-return]

    def _decode_task(self, task_id: ULID, raw: Any) -> Task:
        return Task.unpack(task_id, raw)

    async def get_task(self, task_id: ULID) -> Task | None:
        raw_data: dict[str, Any] | None = await self.data_store.json().get(  # type: ignore[misc]
            self.TASK_DETAILS(task_id=task_id)
        )
        if raw_data is None:
            return None
        task = Task.unpack(task_id, raw_data)
        heartbeat_score: float | None = await self.data_store.zscore(
            self.HEARTBEAT_SCORES(queue=task.queue), bytes(task_id)
        )
        if heartbeat_score is not None:
            task.heartbeat_at = dt.datetime.fromtimestamp(heartbeat_score, dt.UTC)
        return task

    async def read_for_watch(self, pipe: Pipeline, task_id: ULID) -> Task | None:
        """Read task data via a WATCH pipeline (JSON format)."""
        raw_data: dict[str, Any] | None = await pipe.json().get(  # type: ignore[misc]
            self.TASK_DETAILS(task_id=task_id)
        )
        if raw_data is None:
            return None
        return Task.unpack(task_id, raw_data)

    async def ensure_index(self) -> None:
        """Create the RediSearch index on task JSON documents if it does not exist."""
        from redis.commands.search.field import NumericField, TagField
        from redis.commands.search.index_definition import (
            IndexDefinition,
            IndexType,
        )

        try:
            await self.data_store.ft(self.INDEX_NAME).info()  # type: ignore[no-untyped-call]
        except ResponseError:
            await self.data_store.ft(self.INDEX_NAME).create_index(
                fields=[
                    TagField("$.name", as_name="name"),
                    TagField("$.queue", as_name="queue"),
                    TagField("$.status", as_name="status"),
                    NumericField("$.version", as_name="version"),
                    NumericField("$.submitted_at", as_name="submitted_at", sortable=True),
                ],
                definition=IndexDefinition(prefix=["task:"], index_type=IndexType.JSON),  # type: ignore[no-untyped-call]
            )

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Query tasks via the RediSearch index with optional filters."""
        from redis.commands.search.query import Query as SearchQuery

        from jobbers.models.task import PaginationOrder

        def _escape_tag(value: str) -> str:
            """Escape special characters for a RediSearch TAG query value."""
            special = set(r',.<>{}[]"\':;!@#$%^&*()\-+=~| ')
            return "".join(f"\\{c}" if c in special else c for c in value)

        query_parts = [f"@queue:{{{_escape_tag(pagination.queue)}}}"]
        if pagination.task_name:
            query_parts.append(f"@name:{{{_escape_tag(pagination.task_name)}}}")
        if pagination.task_version is not None:
            query_parts.append(f"@version:[{pagination.task_version} {pagination.task_version}]")
        if pagination.status is not None:
            query_parts.append(f"@status:{{{_escape_tag(str(pagination.status))}}}")

        q: SearchQuery = (
            SearchQuery(" ".join(query_parts))
            .no_content()
            .paging(pagination.offset, pagination.limit)
        )
        if pagination.order_by == PaginationOrder.SUBMITTED_AT:
            q = q.sort_by("submitted_at", asc=True)

        search_results = await self.data_store.ft(self.INDEX_NAME).search(q)
        if not search_results.docs:
            return []

        results: list[Task] = []
        async with TaskGroup() as group:
            for doc in search_results.docs:
                task_id = ULID.from_str(doc.id.removeprefix("task:"))
                group.create_task(self._add_task_to_results(task_id, results))
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
            key_str = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            task_id_str = key_str.removeprefix("task:")
            try:
                task_id = ULID.from_str(task_id_str)
            except ValueError:
                continue
            task_data: dict[str, Any] | None = await self.data_store.json().get(key_str)  # type: ignore[misc]
            if task_data is None:
                continue
            task = Task.unpack(task_id, task_data)
            if task.status not in terminal_statuses:
                continue
            if task.completed_at is None or task.completed_at >= cutoff:
                continue
            pipe = self.data_store.pipeline(transaction=True)
            pipe.delete(key_str)
            pipe.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task_id))
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task_id))
            await pipe.execute()


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
        return await pipe.execute()  # type: ignore[no-any-return]

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
            task_id = ULID(raw_id)
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
# Backward-compat alias
# ---------------------------------------------------------------------------

TaskAdapter = JsonTaskAdapter
