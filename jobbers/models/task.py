import datetime as dt
import inspect
import json
import logging
from asyncio import TaskGroup
from collections.abc import AsyncGenerator, Awaitable
from enum import StrEnum
from typing import Any, Self, cast

from opentelemetry import metrics
from pydantic import BaseModel, Field
from redis.asyncio.client import Pipeline, Redis
from redis.exceptions import ResponseError
from ulid import ULID

from jobbers.constants import TIME_ZERO
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy

from .queue_config import QueueConfig
from .task_config import TaskConfig
from .task_status import TaskStatus

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
tasks_missing_data = meter.create_counter("tasks_missing_data", unit="1")

class Task(BaseModel):
    """A task to be executed."""

    id: ULID
    # task mapping fields
    name: str
    queue: str = "default"
    version: int = 0
    parameters: dict[Any, Any] = {}
    results: dict[Any, Any] = {}
    errors: list[str] = []
    # status fields
    retry_attempt: int = 0  # Number of times this task has been retried
    status: TaskStatus = Field(default=TaskStatus.UNSUBMITTED)
    submitted_at: dt.datetime | None = None
    retried_at: dt.datetime | None = None
    started_at: dt.datetime | None = None
    heartbeat_at: dt.datetime | None = None
    completed_at: dt.datetime | None = None

    task_config: TaskConfig | None = Field(default=None, exclude=True)

    def valid_task_params(self) -> bool:
        if not self.task_config:
            # Safer to fail here than chance something funky downstream
            return True
        signature = inspect.get_annotations(self.task_config.function)
        for param, psig in signature.items():
            # Skip the return type annotation
            if param != "return" and not isinstance(self.parameters[param], psig):
                return False
        return True

    def shutdown(self) -> None:
        if self.task_config is None:
            return
        match self.task_config.on_shutdown:
            case TaskShutdownPolicy.CONTINUE:
                # NOOP: The execution of the task function needs to be wrapped
                # in `shield()` already.
                # TODO: maybe warn or panic since this should be unreachable
                pass
            case TaskShutdownPolicy.STOP:
                self.set_status(TaskStatus.STALLED)
            case TaskShutdownPolicy.RESUBMIT:
                # Direct assignment: shutdown-triggered resubmit should not increment retry_attempt
                self.status = TaskStatus.UNSUBMITTED

    def should_retry(self) -> bool:
        if not self.task_config:
            # safer to fail here than chance something funky downstream
            return False
        return self.retry_attempt < self.task_config.max_retries

    def should_schedule(self) -> bool:
        """Return True if the retry should be delayed (SCHEDULED) rather than immediate (UNSUBMITTED)."""
        if not self.task_config:
            return False
        return self.task_config.retry_delay is not None

    def has_callbacks(self) -> bool:
        return False

    def generate_callbacks(self) -> list[Self]:
        return []

    def summarized(self) -> dict[str, Any]:
        summary = self.model_dump(include={"id", "name", "parameters", "status", "retry_attempt", "submitted_at"})
        summary["id"] = str(self.id)
        if self.errors:
            summary["last_error"] =  self.errors[-1]
        return summary

    @property
    def _ta(self) -> "TaskAdapter":
        from jobbers.db import get_client

        return TaskAdapter(get_client())

    async def heartbeat(self) -> None:
        self.heartbeat_at = dt.datetime.now(dt.UTC)
        await self._ta.update_task_heartbeat(self)

    def to_dict(self) -> dict[str, Any]:
        """Serialize task fields to a dict for RedisJSON storage."""
        def _ts(d: dt.datetime | None) -> float | None:
            return d.timestamp() if d is not None else None

        return {
            "name": self.name,
            "queue": self.queue,
            "version": self.version,
            "parameters": self.parameters or {},
            "results": self.results or {},
            "errors": self.errors,
            "retry_attempt": self.retry_attempt,
            "status": str(self.status),
            "submitted_at": _ts(self.submitted_at),
            "retried_at": _ts(self.retried_at),
            "started_at": _ts(self.started_at),
            "heartbeat_at": _ts(self.heartbeat_at),
            "completed_at": _ts(self.completed_at),
        }

    def pack(self) -> str:
        """Serialize task fields to a JSON string (used for Lua script ARGV values)."""
        return json.dumps(self.to_dict())

    @classmethod
    def unpack(cls, task_id: ULID, data: str | dict[str, Any]) -> "Self":
        """Deserialize a task from a JSON string or dict."""
        raw: dict[str, Any] = json.loads(data) if isinstance(data, str) else data

        def _dt(ts: float | None) -> dt.datetime | None:
            return dt.datetime.fromtimestamp(ts, dt.UTC) if ts is not None else None

        return cls(
            id=task_id,
            name=raw.get("name", ""),
            queue=raw.get("queue", "default"),
            version=raw.get("version", 0),
            parameters=raw.get("parameters") or {},
            results=raw.get("results") or {},
            errors=raw.get("errors") or [],
            retry_attempt=raw.get("retry_attempt", 0),
            status=raw.get("status", TaskStatus.UNSUBMITTED),
            submitted_at=_dt(raw.get("submitted_at")),
            retried_at=_dt(raw.get("retried_at")),
            started_at=_dt(raw.get("started_at")),
            heartbeat_at=_dt(raw.get("heartbeat_at")),
            completed_at=_dt(raw.get("completed_at")),
        )

    def set_status(self, status: TaskStatus) -> None:
        match status:
            case TaskStatus.STARTED:
                if not self.started_at:
                    self.started_at = dt.datetime.now(dt.UTC)
                else:
                    self.retried_at = dt.datetime.now(dt.UTC)
            case TaskStatus.SUBMITTED:
                self.submitted_at = dt.datetime.now(dt.UTC)
            case TaskStatus.COMPLETED | TaskStatus.FAILED | \
                 TaskStatus.CANCELLED | TaskStatus.STALLED | TaskStatus.DROPPED:
                self.completed_at = dt.datetime.now(dt.UTC)
            case TaskStatus.SCHEDULED | TaskStatus.UNSUBMITTED:
                self.retry_attempt += 1
        self.status = status


class PaginationOrder(StrEnum):
    "Supported fields to order task list by."

    SUBMITTED_AT = "submitted_at"
    TASK_ID = "task_id"

class TaskPagination(BaseModel):
    "Pagination details."

    queue: str = Field()
    limit: int = Field(default=10, gt=0, le=100)
    offset: int = Field(default=0, ge=0)
    start: ULID | None = Field(default=None)
    order_by: PaginationOrder = Field(default=PaginationOrder.SUBMITTED_AT)
    task_name: str | None = Field(default=None)
    task_version: int | None = Field(default=None)
    status: TaskStatus | None = Field(default=None)


class TaskAdapter:
    """
    Manages how tasks can be added, pulled, or queried from a Redis data store.

    - `task-queues:<queue>`: Sorted set of task ID => submitted at timestamp for each queue
        - ZPOPMIN to get the oldest task from a set of queues
    - `task:<task_id>`: RedisJSON document containing the task state (name, status, etc).
    - `task-idx`: RediSearch index over task JSON documents for filtered queries.
    """

    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    HEARTBEAT_SCORES = "task-heartbeats:{queue}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    DLQ_MISSING_DATA = "dlq-missing-data"
    INDEX_NAME = "task-idx"

    # Atomically enqueue a new task (no rate limiting).
    # Skips the ZADD if the task details key already exists (idempotent re-submit guard).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # ARGV[1] = submitted_at timestamp (score for the queue sorted set)
    # ARGV[2] = task_id bytes (member for sorted sets)
    # ARGV[3] = '1' to SADD the type index, '0' to SREM
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

    # Atomically check the rate limit and enqueue a new task.
    # Skips the queue ZADD when either the task already exists or the rate limit is reached.
    # Always writes task details (JSON.SET) and updates the type index.
    # KEYS[1] = rate-limiter:{queue}
    # KEYS[2] = task-queues:{queue}
    # KEYS[3] = task:{task_id}
    # KEYS[4] = task-type-idx:{name}
    # ARGV[1] = earliest_time (entries with score <= this are outside the window)
    # ARGV[2] = rate_numerator (max tasks allowed in the window)
    # ARGV[3] = submitted_at timestamp (score for rate-limiter and task-queues sorted sets)
    # ARGV[4] = task_id bytes (member for sorted sets)
    # ARGV[5] = '1' to SADD the type index, '0' to SREM
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

    def __init__(self, data_store: Redis) -> None:
        self.data_store: Redis = data_store

    async def submit_task(self, task: "Task") -> bool:
        """Atomically enqueue a new task with no rate limiting. Status must already be SUBMITTED."""
        assert task.submitted_at  # noqa: S101
        is_active = "1" if task.status in TaskStatus.active_statuses() else "0"
        result: int = await cast(
            "Awaitable[int]",
            self.data_store.eval(
                self.SUBMIT_SCRIPT,
                3,
                self.TASKS_BY_QUEUE(queue=task.queue),    # KEYS[1]
                self.TASK_DETAILS(task_id=task.id),       # KEYS[2]
                self.TASK_BY_TYPE_IDX(name=task.name),    # KEYS[3]
                task.submitted_at.timestamp(),            # ARGV[1]
                bytes(task.id),                           # ARGV[2]
                is_active,                                # ARGV[3]
                task.pack(),                              # ARGV[4]
            )
        )
        return result == 1

    async def submit_rate_limited_task(self, task: "Task", queue_config: QueueConfig) -> bool:
        """
        Atomically check the rate limit and enqueue the task if there is room.

        Returns True if the task was enqueued, False if the rate limit was reached.
        Task details are only written if the task is enqueued.
        """
        assert task.submitted_at  # noqa: S101
        now = dt.datetime.now(dt.UTC)
        earliest_time = now - dt.timedelta(seconds=queue_config.period_in_seconds() or 0)
        is_active = "1" if task.status in TaskStatus.active_statuses() else "0"
        result: int = await cast(
            "Awaitable[int]",
            self.data_store.eval(
                self.SUBMIT_RATE_LIMITED_SCRIPT,
                4,
                self.QUEUE_RATE_LIMITER(queue=task.queue),  # KEYS[1]
                self.TASKS_BY_QUEUE(queue=task.queue),      # KEYS[2]
                self.TASK_DETAILS(task_id=task.id),         # KEYS[3]
                self.TASK_BY_TYPE_IDX(name=task.name),      # KEYS[4]
                earliest_time.timestamp(),                  # ARGV[1]
                queue_config.rate_numerator or 0,           # ARGV[2]
                task.submitted_at.timestamp(),              # ARGV[3]
                bytes(task.id),                             # ARGV[4]
                is_active,                                  # ARGV[5]
                task.pack(),                                # ARGV[6]
            )
        )
        return result == 1

    def stage_save(self, pipe: Pipeline, task: Task) -> None:
        """Queue JSON.SET task-details + type-index update onto pipe (no execute)."""
        pipe.json().set(self.TASK_DETAILS(task_id=task.id), "$", task.to_dict())
        if task.status in TaskStatus.active_statuses():
            pipe.sadd(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        else:
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))

    def stage_requeue(self, pipe: Pipeline, task: Task) -> None:
        """Queue ZADD task-queue + save-task commands onto pipe (no execute)."""
        assert task.submitted_at  # noqa: S101
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
        self.stage_save(pipe, task)

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

    async def get_active_tasks(self, queues: set[str]) -> list["Task"]:
        """Return all tasks currently present in any heartbeat sorted set."""
        task_id_bytes: set[bytes] = set()
        for queue in queues:
            members: list[bytes] = await self.data_store.zrange(self.HEARTBEAT_SCORES(queue=queue), 0, -1)
            task_id_bytes.update(members)
        if not task_id_bytes:
            return []
        results: list[Task] = []
        async with TaskGroup() as group:
            for raw_id in task_id_bytes:
                group.create_task(self._add_task_to_results(ULID.from_bytes(raw_id), results))
        return results

    async def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]:
        """Get tasks that have not had a heartbeat update in the stale time."""
        now = dt.datetime.now(dt.UTC)
        cutoff_time = now - stale_time
        stale_task_ids = set()
        for queue in queues:
            task_ids = await self.data_store.zrangebyscore(self.HEARTBEAT_SCORES(queue=queue), min=0, max=cutoff_time.timestamp())
            stale_task_ids.update(task_ids)

        async def fetch_task(task_id: bytes) -> Task | None:
            return await self.get_task(ULID.from_bytes(task_id))

        async with TaskGroup() as group:
            tasks = [group.create_task(fetch_task(task_id)) for task_id in stale_task_ids]

        for task in tasks:
            if (result := task.result()) is not None:
                yield result

    async def task_exists(self, task_id: ULID) -> bool:
        does_exists: int = await self.data_store.exists(self.TASK_DETAILS(task_id=task_id))
        return does_exists == 1

    async def ensure_index(self) -> None:
        """Create the RediSearch index on task JSON documents if it does not exist."""
        from redis.commands.search.field import NumericField, TagField
        from redis.commands.search.indexDefinition import (  # type: ignore[import-not-found]
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
                definition=IndexDefinition(prefix=["task:"], index_type=IndexType.JSON),
            )

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Query tasks via the RediSearch index with optional filters."""
        from redis.commands.search.query import Query as SearchQuery

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

    async def _add_task_to_results(self, task_id: ULID, results: list[Task]) -> list[Task]:
        task = await self.get_task(task_id)
        if task:
            results.append(task)
        return results

    async def get_next_task(self, queues: set[str], pop_timeout: int=0) -> Task | None:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        # Try to pop from each queue until we find a task
        # TODO: Shuffle/rotate the order of queues to avoid starving any of them
        # see https://redis.io/docs/latest/commands/blpop/#what-key-is-served-first-what-client-what-element-priority-ordering-details
        # for details of how the order of keys impact how tasks are popped
        task_queues = {self.TASKS_BY_QUEUE(queue=queue) for queue in queues}
        pop_result = await self.data_store.bzpopmin(task_queues, timeout=pop_timeout)
        while pop_result:
            task = await self.get_task(ULID.from_bytes(pop_result[1]))
            if task:
                return task
            # Task ID was popped from the queue but its data is missing.
            # Record a metric, move the bare ID to the missing-data DLQ for
            # investigation, and keep waiting for a valid task.
            logger.error("Task %s popped from queue but data not found; adding to %s", pop_result[1], self.DLQ_MISSING_DATA)
            tasks_missing_data.add(1)
            now = dt.datetime.now(dt.UTC)
            await self.data_store.zadd(self.DLQ_MISSING_DATA, {pop_result[1]: now.timestamp()})
            pop_result = await self.data_store.bzpopmin(task_queues, timeout=pop_timeout)
        logger.info("task query timed out")
        return None

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """
        Delete task blobs, heartbeat entries, and type-index members for old terminal tasks.

        Scans all ``task:*`` keys. For each task in a terminal status whose
        ``completed_at`` is older than ``max_age``, atomically removes:
          - ``task:{task_id}``
          - its entry in ``task-heartbeats:{queue}``
          - its entry in ``task-type-idx:{name}`` (safety net for stale orphans)
        """
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

    async def clean(self, queues: set[bytes], now: dt.datetime, min_queue_age: dt.datetime | None=None, max_queue_age: dt.datetime | None=None) -> None:
        """Clean up the state manager."""
        if max_queue_age or min_queue_age:
            earliest_time = min_queue_age or TIME_ZERO
            latest_time = max_queue_age or now
            for queue in queues:
                # TODO: Batch X queues together per pipeline. Just can't let X be so big that
                # the pipeline grows too large.
                pipe = self.data_store.pipeline(transaction=True)
                if earliest_time <= latest_time:
                    pipe.zremrangebyscore(self.TASKS_BY_QUEUE(queue=queue.decode()), min=earliest_time.timestamp(), max=latest_time.timestamp())
                else:
                    pipe.zremrangebyscore(self.TASKS_BY_QUEUE(queue=queue.decode()), min=0, max=earliest_time.timestamp())
                    pipe.zremrangebyscore(self.TASKS_BY_QUEUE(queue=queue.decode()), min=latest_time.timestamp(), max=now.timestamp())
                await pipe.execute()
