import datetime as dt
import inspect
import logging
from asyncio import TaskGroup
from collections.abc import AsyncGenerator, Awaitable
from enum import StrEnum
from typing import Any, Self, cast

from opentelemetry import metrics
from pydantic import BaseModel, Field, PrivateAttr
from redis.asyncio.client import Pipeline, Redis
from ulid import ULID

from jobbers.constants import TIME_ZERO
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.utils.serialization import (
    deserialize,
    serialize,
)

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
    _last_status: TaskStatus | None = PrivateAttr(default=None)

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

    def pack(self) -> bytes:
        """Serialize all task fields to a single msgpack blob for Redis storage."""
        return serialize({
            "name": self.name,
            "queue": self.queue,
            "version": self.version,
            "parameters": self.parameters or {},
            "results": self.results or {},
            "errors": self.errors,
            "retry_attempt": self.retry_attempt,
            "status": self.status,
            "submitted_at": self.submitted_at,
            "retried_at": self.retried_at,
            "started_at": self.started_at,
            "heartbeat_at": self.heartbeat_at,
            "completed_at": self.completed_at,
        })

    @classmethod
    def unpack(cls, task_id: ULID, data: bytes) -> "Self":
        """Deserialize a task from a single msgpack blob."""
        raw = deserialize(data)
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
            submitted_at=raw.get("submitted_at"),
            retried_at=raw.get("retried_at"),
            started_at=raw.get("started_at"),
            heartbeat_at=raw.get("heartbeat_at"),
            completed_at=raw.get("completed_at"),
        )

    def set_status(self, status: TaskStatus) -> None:
        self._last_status = self.status
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

    queue: str | None = Field(default=None)
    queues: list[str] | None = Field(default=None)
    limit: int = Field(default=10, gt=0, le=100)
    start: ULID | None = Field(default=None)
    order_by: PaginationOrder = Field(default=PaginationOrder.SUBMITTED_AT)
    task_name: str | None = Field(default=None)
    task_version: int | None = Field(default=None)
    status: list[TaskStatus] | None = Field(default=None)
    submitted_after: dt.datetime | None = Field(default=None)
    submitted_before: dt.datetime | None = Field(default=None)

    def start_param(self) -> int:
        if not self.start:
            return 0
        return int(self.start.datetime.timestamp())


class TaskAdapter:
    """
    Manages how tasks can be added, pulled, or queried from a Redis data store.

    - `task-queues:<queue>`: Sorted set of task ID => submitted at timestamp for each queue
        - ZPOPMIN to get the oldest task from a set of queues
    - `task:<task_id>`: String containing the packed task state (name, status, etc).
    """

    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    HEARTBEAT_SCORES = "task-heartbeats:{queue}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format
    TASK_STATUS_IDX = "task-status-idx:{status}".format
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    DLQ_MISSING_DATA = "dlq-missing-data"

    # Atomically enqueue a new task (no rate limiting).
    # Skips the ZADD if the task details key already exists (idempotent re-submit guard).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # KEYS[4] = task-status-idx:{status}
    # ARGV[1] = submitted_at timestamp (score for the queue sorted set)
    # ARGV[2] = task_id bytes (member for sorted sets)
    # ARGV[3] = '1' to SADD the type index, '0' to SREM
    # ARGV[4] = packed task blob (msgpack)
    # ARGV[5] = submitted_at timestamp (score for the status index)
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
        redis.call('ZADD', KEYS[4], ARGV[5], ARGV[2])
        return 1
    """

    # Atomically check the rate limit and enqueue a new task.
    # Skips the queue ZADD when either the task already exists or the rate limit is reached.
    # Always writes task details (SET) and updates the type index.
    # KEYS[1] = rate-limiter:{queue}
    # KEYS[2] = task-queues:{queue}
    # KEYS[3] = task:{task_id}
    # KEYS[4] = task-type-idx:{name}
    # KEYS[5] = task-status-idx:{status}
    # ARGV[1] = earliest_time (entries with score <= this are outside the window)
    # ARGV[2] = rate_numerator (max tasks allowed in the window)
    # ARGV[3] = submitted_at timestamp (score for rate-limiter and task-queues sorted sets)
    # ARGV[4] = task_id bytes (member for sorted sets)
    # ARGV[5] = '1' to SADD the type index, '0' to SREM
    # ARGV[6] = packed task blob (msgpack)
    # ARGV[7] = submitted_at timestamp (score for the status index)
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
        if enqueued == 1 then
            redis.call('ZADD', KEYS[5], ARGV[7], ARGV[4])
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
                4,
                self.TASKS_BY_QUEUE(queue=task.queue),          # KEYS[1]
                self.TASK_DETAILS(task_id=task.id),             # KEYS[2]
                self.TASK_BY_TYPE_IDX(name=task.name),          # KEYS[3]
                self.TASK_STATUS_IDX(status=task.status),       # KEYS[4]
                task.submitted_at.timestamp(),                  # ARGV[1]
                bytes(task.id),                                 # ARGV[2]
                is_active,                                      # ARGV[3]
                task.pack(),                                    # ARGV[4]
                task.submitted_at.timestamp(),                  # ARGV[5]
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
                5,
                self.QUEUE_RATE_LIMITER(queue=task.queue),      # KEYS[1]
                self.TASKS_BY_QUEUE(queue=task.queue),          # KEYS[2]
                self.TASK_DETAILS(task_id=task.id),             # KEYS[3]
                self.TASK_BY_TYPE_IDX(name=task.name),          # KEYS[4]
                self.TASK_STATUS_IDX(status=task.status),       # KEYS[5]
                earliest_time.timestamp(),                      # ARGV[1]
                queue_config.rate_numerator or 0,               # ARGV[2]
                task.submitted_at.timestamp(),                  # ARGV[3]
                bytes(task.id),                                 # ARGV[4]
                is_active,                                      # ARGV[5]
                task.pack(),                                    # ARGV[6]
                task.submitted_at.timestamp(),                  # ARGV[7]
            )
        )
        return result == 1

    def stage_save(self, pipe: Pipeline, task: Task) -> None:
        """Queue SET task-details + type-index + status-index updates onto pipe (no execute)."""
        pipe.set(self.TASK_DETAILS(task_id=task.id), task.pack())
        if task.status in TaskStatus.active_statuses():
            pipe.sadd(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        else:
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        if task._last_status is not None:
            pipe.zrem(self.TASK_STATUS_IDX(status=task._last_status), bytes(task.id))
        score = task.submitted_at.timestamp() if task.submitted_at else 0.0
        pipe.zadd(self.TASK_STATUS_IDX(status=task.status), {bytes(task.id): score})

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
        raw_data: bytes | None = await self.data_store.get(self.TASK_DETAILS(task_id=task_id))
        if not raw_data:
            return None
        task = Task.unpack(task_id, raw_data)
        heartbeat_score: float | None = await self.data_store.zscore(
            self.HEARTBEAT_SCORES(queue=task.queue), bytes(task_id)
        )
        if heartbeat_score is not None:
            task.heartbeat_at = dt.datetime.fromtimestamp(heartbeat_score, dt.UTC)
        return task

    async def update_task_heartbeat(self, task: Task) -> None:
        """Update the heartbeat for a task."""
        assert task.heartbeat_at  # noqa: S101
        pipe = self.data_store.pipeline(transaction=True)
        pipe.zadd(self.HEARTBEAT_SCORES(queue=task.queue), {bytes(task.id): task.heartbeat_at.timestamp()})
        await pipe.execute()

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

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        # Map date bounds to score bounds used by both queue and status sorted sets
        min_score: float = float(pagination.start_param())
        max_score: float | str = "+inf"
        if pagination.submitted_after:
            min_score = max(min_score, pagination.submitted_after.timestamp())
        if pagination.submitted_before:
            max_score = pagination.submitted_before.timestamp()

        task_id_bytes: list[bytes]
        if pagination.status:
            task_id_bytes = await self._query_via_status_index(pagination, min_score, max_score)
        else:
            task_id_bytes = await self._query_via_queue_sets(pagination, min_score, max_score)

        if not task_id_bytes:
            return []

        # Fetch task details concurrently
        # TODO: do some batching in case there are tons of tasks
        results: list[Task] = []
        async with TaskGroup() as group:
            for raw_id in task_id_bytes:
                group.create_task(self._add_task_to_results(ULID(raw_id), results))

        # Sort and paginate
        if pagination.order_by == PaginationOrder.SUBMITTED_AT:
            results.sort(key=lambda t: t.submitted_at or dt.datetime.min.replace(tzinfo=dt.UTC))
        else:
            results.sort(key=lambda t: t.id)

        return results[:pagination.limit]

    async def _resolve_queues(self, pagination: TaskPagination) -> list[str] | None:
        """Return the effective queue list, or None to mean 'all queues'."""
        if pagination.queues:
            return pagination.queues
        if pagination.queue:
            return [pagination.queue]
        return None

    async def _query_via_status_index(
        self,
        pagination: TaskPagination,
        min_score: float,
        max_score: float | str,
    ) -> list[bytes]:
        """
        Use task-status-idx:{status} as the primary lookup set.

        - All queues + status: ZRANGEBYSCORE on each status index, union IDs in Python.
        - Single queue + status: ZINTERSTORE(queue_set, status_idx) → temp key.
        - Multiple queues + status: ZUNIONSTORE(queue_sets) → ZINTERSTORE(status_idx) → temp key.
        """
        from uuid import uuid4

        assert pagination.status  # noqa: S101

        queue_list = await self._resolve_queues(pagination)

        if queue_list is None:
            # All queues: query each status index directly, no intersection needed
            id_set: set[bytes] = set()
            for status in pagination.status:
                ids: list[bytes] = await self.data_store.zrangebyscore(
                    self.TASK_STATUS_IDX(status=status), min_score, max_score
                )
                id_set.update(ids)
            return list(id_set)

        # Specific queues: intersect with each status index via a temp key
        queue_keys = [self.TASKS_BY_QUEUE(queue=q) for q in queue_list]
        id_set = set()

        for status in pagination.status:
            status_key = self.TASK_STATUS_IDX(status=status)
            temp_key = f"_tmp:task-query:{uuid4().hex}"
            pipe = self.data_store.pipeline()
            if len(queue_keys) == 1:
                pipe.zinterstore(temp_key, {queue_keys[0]: 1, status_key: 0})
            else:
                temp_union = f"{temp_key}:union"
                pipe.zunionstore(temp_union, queue_keys)
                pipe.zinterstore(temp_key, {temp_union: 1, status_key: 0})
                pipe.expire(temp_union, 30)
            pipe.expire(temp_key, 30)
            await pipe.execute()

            ids = await self.data_store.zrangebyscore(temp_key, min_score, max_score)
            id_set.update(ids)
            await self.data_store.delete(temp_key)

        return list(id_set)

    async def _query_via_queue_sets(
        self,
        pagination: TaskPagination,
        min_score: float,
        max_score: float | str,
    ) -> list[bytes]:
        """Use task-queues:{queue} sorted sets directly (no status filter)."""
        queue_list = await self._resolve_queues(pagination)

        if queue_list is None:
            queue_bytes: set[bytes] = await cast(
                "Awaitable[set[bytes]]",
                self.data_store.smembers("all-queues"),
            )
            queue_list = [q.decode() for q in queue_bytes]

        if not queue_list:
            return []

        all_ids: list[bytes] = []
        for queue in queue_list:
            if pagination.order_by == PaginationOrder.SUBMITTED_AT:
                ids: list[bytes] = await self.data_store.zrangebyscore(
                    self.TASKS_BY_QUEUE(queue=queue), min_score, max_score
                )
            else:
                ids = await self.data_store.zrange(self.TASKS_BY_QUEUE(queue=queue), 0, -1)
            all_ids.extend(ids)
        return all_ids

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
        Delete task blobs, heartbeat entries, type-index, and status-index members for old terminal tasks.

        Scans all ``task:*`` keys. For each task in a terminal status whose
        ``completed_at`` is older than ``max_age``, atomically removes:
          - ``task:{task_id}``
          - its entry in ``task-heartbeats:{queue}``
          - its entry in ``task-type-idx:{name}`` (safety net for stale orphans)
          - its entry in ``task-status-idx:{status}``
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
            task = Task.unpack(task_id, task_data)
            if task.status not in terminal_statuses:
                continue
            if task.completed_at is None or task.completed_at >= cutoff:
                continue
            pipe = self.data_store.pipeline(transaction=True)
            pipe.delete(key)
            pipe.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task_id))
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task_id))
            pipe.zrem(self.TASK_STATUS_IDX(status=task.status), bytes(task_id))
            await pipe.execute()

    async def rebuild_status_indexes(self) -> None:
        """
        Backfill task-status-idx:{status} from all existing task:* blobs.

        Scans every ``task:*`` key and (re)inserts the task into the correct
        ``task-status-idx:{status}`` sorted set using ``submitted_at`` as the
        score (0 when not set).  Existing entries are overwritten in-place by
        ZADD, so the operation is idempotent and safe to run on a live instance.
        """
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
            task = Task.unpack(task_id, task_data)
            score = task.submitted_at.timestamp() if task.submitted_at else 0.0
            await self.data_store.zadd(
                self.TASK_STATUS_IDX(status=task.status), {bytes(task_id): score}
            )

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
