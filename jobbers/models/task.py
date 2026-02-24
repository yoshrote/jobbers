import datetime as dt
import inspect
import logging
from asyncio import TaskGroup
from collections.abc import AsyncGenerator, Awaitable
from enum import StrEnum
from typing import Any, Self, cast

from pydantic import BaseModel, Field
from redis.asyncio.client import Redis
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
    start: ULID | None = Field(default=None)
    order_by: PaginationOrder = Field(default=PaginationOrder.SUBMITTED_AT)
    task_name: str | None = Field(default=None)
    task_version: int | None = Field(default=None)
    # status: TaskStatus | None = Field(default=None)

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
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format

    # Atomically enqueue a new task (no rate limiting).
    # Skips the ZADD if the task details key already exists (idempotent re-submit guard).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # ARGV[1] = submitted_at timestamp (score for the queue sorted set)
    # ARGV[2] = task_id bytes (member for sorted sets)
    # ARGV[3] = '1' to SADD the type index, '0' to SREM
    # ARGV[4] = packed task blob (msgpack)
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

    # Atomically check the rate limit and enqueue a new task.
    # Skips the queue ZADD when either the task already exists or the rate limit is reached.
    # Always writes task details (SET) and updates the type index.
    # KEYS[1] = rate-limiter:{queue}
    # KEYS[2] = task-queues:{queue}
    # KEYS[3] = task:{task_id}
    # KEYS[4] = task-type-idx:{name}
    # ARGV[1] = earliest_time (entries with score <= this are outside the window)
    # ARGV[2] = rate_numerator (max tasks allowed in the window)
    # ARGV[3] = submitted_at timestamp (score for rate-limiter and task-queues sorted sets)
    # ARGV[4] = task_id bytes (member for sorted sets)
    # ARGV[5] = '1' to SADD the type index, '0' to SREM
    # ARGV[6] = packed task blob (msgpack)
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

    async def requeue_task(self, task: Task) -> None:
        """Re-enqueue an existing task for retry (bypasses the new-task existence check)."""
        assert task.submitted_at  # noqa: S101
        pipe = self.data_store.pipeline(transaction=True)
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
        pipe.set(self.TASK_DETAILS(task_id=task.id), task.pack())
        if task.status in TaskStatus.active_statuses():
            pipe.sadd(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        else:
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        await pipe.execute()

    async def save_task(self, task: Task) -> None:
        """Submit a task to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        pipe.set(self.TASK_DETAILS(task_id=task.id), task.pack())
        if task.status in TaskStatus.active_statuses():
            pipe.sadd(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        else:
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
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
        if pagination.order_by == "submitted_at":
            # tasks will be ordered by SUBMISSION time
            task_ids = await self.data_store.zrangebyscore(
                self.TASKS_BY_QUEUE(queue=pagination.queue),
                pagination.start_param(), '+inf',
                start=pagination.start_param(),
                num=pagination.limit
            )
        else:
            # tasks will be ordered by TASK ID, which is roughly creation time but not exactly
            task_ids = await self.data_store.zrange(
                self.TASKS_BY_QUEUE(queue=pagination.queue),
                pagination.start_param(), int(dt.datetime.now(dt.UTC).timestamp()),
                num=pagination.limit
            )
        if not task_ids:
            return []
        results: list[Task] = []
        # TODO: do some batching in case there are tons of tasks
        async with TaskGroup() as group:
            for task_id in task_ids:
                group.create_task(self._add_task_to_results(ULID(task_id), results))
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
        task_id = await self.data_store.bzpopmin(task_queues, timeout=pop_timeout)
        if task_id:
            task = await self.get_task(ULID.from_bytes(task_id[1]))
            if task:
                return task
            logger.warning("Task with ID %s not found.", task_id)
        else:
            logger.info("task query timed out")
        return None

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
