import datetime as dt
import inspect
import logging
from asyncio import TaskGroup
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any, Self, cast

from pydantic import BaseModel, Field
from redis.asyncio.client import Pipeline, Redis
from ulid import ULID

from jobbers.constants import TIME_ZERO
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.utils.serialization import (
    EMPTY_DICT,
    NONE,
    deserialize,
    serialize,
)

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
    error: str | None = None
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
                self.status = TaskStatus.STALLED
                self.completed_at = dt.datetime.now(dt.timezone.utc)
            case TaskShutdownPolicy.RESUBMIT:
                self.status = TaskStatus.UNSUBMITTED

    def should_retry(self) -> bool:
        if not self.task_config:
            # safer to fail here than chance something funky downstream
            return False
        return self.retry_attempt < self.task_config.max_retries

    def has_callbacks(self) -> bool:
        return False

    def generate_callbacks(self) -> list[Self]:
        return []

    def summarized(self) -> dict[str, Any]:
        summary = self.model_dump(include={"id", "name", "parameters", "status", "retry_attempt", "submitted_at"})
        summary["id"] = str(self.id)
        return summary

    @property
    def _ta(self) -> "TaskAdapter":
        from jobbers.db import get_client

        return TaskAdapter(get_client())

    async def heartbeat(self) -> None:
        self.heartbeat_at = dt.datetime.now(dt.timezone.utc)
        await self._ta.update_task_heartbeat(self)

    @classmethod
    def from_redis(cls, task_id: ULID, raw_task_data: dict[bytes, bytes]) -> Self:
        # Try to set good defaults for missing fields so when new fields are added to the task model, we don't break
        return cls(
            id=task_id,
            name=raw_task_data.get(b"name", b"").decode(),
            version=int(raw_task_data.get(b"version", b"0")),
            parameters=deserialize(raw_task_data.get(b"parameters") or EMPTY_DICT),
            results=deserialize(raw_task_data.get(b"results") or EMPTY_DICT),
            error=deserialize(raw_task_data.get(b"error") or NONE),
            status=TaskStatus.from_bytes(raw_task_data.get(b"status")),
            submitted_at=deserialize(raw_task_data.get(b"submitted_at") or NONE),
            started_at=deserialize(raw_task_data.get(b"started_at") or NONE),
            heartbeat_at=deserialize(raw_task_data.get(b"heartbeat_at") or NONE),
            completed_at=deserialize(raw_task_data.get(b"completed_at") or NONE),
        )

    def to_redis(self) -> dict[bytes, bytes | int]:
        return {
            b"name": self.name.encode(),
            b"version": self.version,
            b"parameters": serialize(self.parameters or {}),
            b"results": serialize(self.results or {}),
            b"error": serialize(self.error),
            b"status": self.status.to_bytes(),
            b"submitted_at": serialize(self.submitted_at),
            b"started_at": serialize(self.started_at),
            b"heartbeat_at": serialize(self.heartbeat_at),
            b"completed_at": serialize(self.completed_at),
        }

    def set_status(self, status: TaskStatus) -> None:
        match status:
            case TaskStatus.STARTED:
                if not self.started_at:
                    self.started_at = dt.datetime.now(dt.timezone.utc)
                else:
                    self.retried_at = dt.datetime.now(dt.timezone.utc)
            case TaskStatus.SUBMITTED:
                self.submitted_at = dt.datetime.now(dt.timezone.utc)
            case TaskStatus.COMPLETED | TaskStatus.FAILED | \
                 TaskStatus.CANCELLED | TaskStatus.STALLED | TaskStatus.DROPPED:
                self.completed_at = dt.datetime.now(dt.timezone.utc)
        self.status = status

    def set_to_retry(self) -> None:
        self.status = TaskStatus.UNSUBMITTED
        self.retry_attempt += 1

# TODO: test pagination
class TaskPagination(BaseModel):
    "Pagination details."

    limit: int = Field(default=10, gt=0, le=100)
    start: ULID | None = Field(default=None)

    def start_param(self) -> int:
        if not self.start:
            return 0
        return int(self.start.datetime.timestamp())


class TaskAdapter:
    """
    Manages how tasks can be added, pulled, or queried from a Redis data store.

    - `task-queues:<queue>`: Sorted set of task ID => submitted at timestamp for each queue
        - ZPOPMIN to get the oldest task from a set of queues
    - `task:<task_id>`: Hash containing the task state (name, status, etc).
    """

    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    HEARTBEAT_SCORES = "task-heartbeats:{queue}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format

    def __init__(self, data_store: Redis) -> None:
        self.data_store: Redis = data_store

    async def submit_task(self, task: Task, extra_check: Callable[[Pipeline], bool] | None) -> None:
        """Submit a task to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        # Avoid pushing a task onto the queue multiple times
        if task.status == TaskStatus.UNSUBMITTED and not await self.task_exists(task.id):
            if extra_check is None or extra_check(pipe):
                task.set_status(TaskStatus.SUBMITTED)
                # the assert is to make mypy play nice.
                assert task.submitted_at  # noqa: S101
                pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})

        pipe.hset(self.TASK_DETAILS(task_id=task.id), mapping=task.to_redis())
        if task.status in TaskStatus.active_statuses():
            pipe.sadd(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        else:
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))

        await pipe.execute()

    async def get_task(self, task_id: ULID) -> Task | None:
        raw_task_data: dict[bytes, Any] = await cast("Awaitable[dict[bytes, Any]]", self.data_store.hgetall(self.TASK_DETAILS(task_id=task_id)))
        heartbeat_score: float | None = await self.data_store.zscore(self.HEARTBEAT_SCORES(queue="default"), bytes(task_id))
        if raw_task_data and heartbeat_score is not None:
            raw_task_data[b"heartbeat_at"] = serialize(dt.datetime.fromtimestamp(heartbeat_score, dt.timezone.utc))

        if raw_task_data:
            return Task.from_redis(task_id, raw_task_data)

        return None

    async def update_task_heartbeat(self, task: Task) -> None:
        """Update the heartbeat for a task."""
        assert task.heartbeat_at  # noqa: S101
        pipe = self.data_store.pipeline(transaction=True)
        pipe.zadd(self.HEARTBEAT_SCORES(queue=task.queue), {bytes(task.id): task.heartbeat_at.timestamp()})
        await pipe.execute()

    async def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]:
        """Get tasks that have not had a heartbeat update in the stale time."""
        now = dt.datetime.now(dt.timezone.utc)
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
        task_ids = await self.data_store.zrangebyscore(
            self.TASKS_BY_QUEUE(queue="default"),
            pagination.start_param(), '+inf',
            start=pagination.start_param(),
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

    async def get_next_task(self, queues: set[str], timeout: int=0) -> Task | None:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        # Try to pop from each queue until we find a task
        # TODO: Shuffle/rotate the order of queues to avoid starving any of them
        # see https://redis.io/docs/latest/commands/blpop/#what-key-is-served-first-what-client-what-element-priority-ordering-details
        # for details of how the order of keys impact how tasks are popped
        task_queues = {self.TASKS_BY_QUEUE(queue=queue) for queue in queues}
        task_id = await self.data_store.bzpopmin(task_queues, timeout=timeout)
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
