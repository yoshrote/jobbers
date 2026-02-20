import asyncio
import datetime as dt
import logging
from collections import defaultdict
from collections.abc import Awaitable, Iterator
from contextlib import contextmanager
from typing import Any, cast

from opentelemetry import metrics
from redis.asyncio.client import Pipeline, Redis
from ulid import ULID

from jobbers import registry
from jobbers.models.dead_queue import DeadQueue
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task, TaskAdapter
from jobbers.models.task_config import DeadLetterPolicy
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_status import TaskStatus

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
tasks_dead_lettered = meter.create_counter("tasks_dead_lettered", unit="1")

class TaskException(Exception):
    "Top-level exception for stuff gone wrong."

    pass

class UserCancellationError(Exception):
    """Exception raised when a task is cancelled by user request."""

    pass

class QueueConfigAdapter:
    """
    Manages queue configuration in a Redis data store.

    - `worker-queues:<role>`: Set of queues for a given role, used to manage which queues are available for task submission.
    - `queue-config:<queue>`: Hash of queue configuration data which is shared for by all roles using this queue
    - `all_queues`: a list of all queues used across all roles.
    """

    QUEUES_BY_ROLE = "worker-queues:{role}".format
    QUEUE_CONFIG = "queue-config:{queue}".format
    ALL_QUEUES = "all-queues"

    def __init__(self, data_store: Redis) -> None:
        self.data_store: Redis = data_store

    async def get_queues(self, role: str) -> set[str]:
        return {role.decode() for role in await cast("Awaitable[set[bytes]]", self.data_store.smembers(self.QUEUES_BY_ROLE(role=role)))}

    async def set_queues(self, role: str, queues: set[str]) -> None:
        pipe: Pipeline = self.data_store.pipeline(transaction=True)
        pipe.delete(self.QUEUES_BY_ROLE(role=role))
        pipe.sadd(self.ALL_QUEUES, *queues)
        pipe.sadd(self.QUEUES_BY_ROLE(role=role), *queues)
        await pipe.execute()

    async def get_all_queues(self) -> list[str]:
        # find the union of the queues for all roles
        # this query approach is not ideal for large numbers of roles or queues
        roles = await self.get_all_roles()
        if not roles:
            return []

        return [
            queue.decode()
            for queue in await cast("Awaitable[list[bytes]]", self.data_store.sunion(
                [self.QUEUES_BY_ROLE(role=role) for role in roles]
            ))
        ]

    async def get_all_roles(self) -> list[str]:
        roles = []
        async for key in self.data_store.scan_iter(match=self.QUEUES_BY_ROLE(role="*").encode()):
            roles.append(key.decode().split(":")[1])
        return roles

    async def get_queue_config(self, queue: str) -> QueueConfig:
        raw_data: dict[bytes, Any] = await cast(
            "Awaitable[dict[bytes, Any]]",
            self.data_store.hgetall(self.QUEUE_CONFIG(queue=queue))
        )  # Ensure the queue config exists in the store
        return QueueConfig.from_redis(queue, raw_data)

    async def get_queue_limits(self, queues: set[str]) -> dict[str, int | None]:
        # TODO: replace with a redis query
        result_gen: Iterator[Awaitable[QueueConfig | None]] = (
            self.get_queue_config(q)
            for q in queues
        )

        return {
            conf.name: conf.max_concurrent
            for conf in await asyncio.gather(*result_gen)
            if conf is not None
        }



class StateManager:
    """Manages tasks in memory and a Redis data store."""

    def __init__(self, data_store: Any, task_scheduler: TaskScheduler, dead_queue: DeadQueue) -> None:
        self.data_store = data_store
        self.qca = QueueConfigAdapter(data_store)
        self.ta = TaskAdapter(data_store)
        self.submission_limiter = SubmissionRateLimiter(self.qca)
        self.current_tasks_by_queue: dict[str, set[ULID]] = defaultdict(set)
        self.dead_queue = dead_queue
        self.task_scheduler = task_scheduler

    @property
    def active_tasks_per_queue(self) -> dict[str, int]:
        return {q: len(ids) for q, ids in self.current_tasks_by_queue.items()}

    @contextmanager
    def task_in_registry(self, task: Task) -> Iterator[None]:
        """Context manager to add a task to the registry."""
        self.current_tasks_by_queue[task.queue].add(task.id)
        try:
            yield
        finally:
            # TODO: Do we need to clean this up before handle_success?
            # Need to consider interactions with callbacks of the task
            self.current_tasks_by_queue[task.queue].remove(task.id)

    async def clean(self, rate_limit_age: dt.timedelta | None=None, min_queue_age: dt.datetime | None=None, max_queue_age: dt.datetime | None=None, stale_time: dt.timedelta | None=None) -> None:
        """Clean up the state manager."""
        now = dt.datetime.now(dt.timezone.utc)
        queues = await self.data_store.smembers(self.qca.ALL_QUEUES)

        if rate_limit_age:
            await self.submission_limiter.clean(queues, now, rate_limit_age)

        if max_queue_age or min_queue_age:
            await self.ta.clean(queues, now, min_queue_age, max_queue_age)

        if stale_time:
            stale_tasks_by_type: dict[tuple[str, int], list[Task]] = defaultdict(list)
            async for task in self.ta.get_stale_tasks(queues, stale_time):
                stale_tasks_by_type[(task.name, task.version)].append(task)

            for (task_type, task_version), tasks in stale_tasks_by_type.items():
                task_config = registry.get_task_config(task_type, task_version)
                if task_config and task_config.max_heartbeat_interval:
                    for task in tasks:
                        if task.heartbeat_at and (now - task.heartbeat_at) > task_config.max_heartbeat_interval:
                            task.set_status(TaskStatus.STALLED)
                            await self.save_task(task)

    # TODO: refactor the refresh tag to be an implementation detail of QueueConfigAdapter
    async def get_refresh_tag(self, role: str) -> ULID:
        tag: bytes|None = await self.data_store.get(f"worker-queues:{role}:refresh_tag")
        if tag:
            return ULID(tag)

        # initialize the refresh tag
        init_tag = ULID()
        await self.data_store.set(f"worker-queues:{role}:refresh_tag", bytes(init_tag))
        return init_tag



    async def get_next_task(self, queues: set[str], timeout: int=0) -> Task | None:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        if not queues:
            logger.info("no queues defined")
            return None

        queues = await self.submission_limiter.concurrency_limits(queues, self.current_tasks_by_queue)

        scheduled_task = self.task_scheduler.next_due(queues=list(queues))
        if scheduled_task:
            return scheduled_task
        return await self.ta.get_next_task(queues, timeout)

    # Proxy methods

    async def submit_task(self, task: Task) -> None:
        try:
            task.task_config = registry.get_task_config(task.name, task.version)
        except KeyError as ex:
            logger.error("Task configuration not found for task %s version %d", task.name, task.version)
            raise TaskException(f"Unregistered task {task.name} version {task.version}") from ex

        if not task.valid_task_params():
            raise TaskException(f"Invalid parameters for task {task.name} v{task.version}")

        queue_config = await self.qca.get_queue_config(queue=task.queue)

        def extra_check(pipe: Pipeline) -> bool:
            # The submission linter will add operations to the pipe transaction
            if not queue_config:
                return True  # No config means no rate limiting
            if self.submission_limiter.has_room_in_queue_queue(queue_config):
                self.submission_limiter.add_task_to_queue(task, pipe=pipe)
                return True
            return False

        task.set_status(TaskStatus.SUBMITTED)
        return await self.ta.submit_task(task=task, extra_check=extra_check)

    async def save_task(self, task: Task) -> Task:
        """Save the task state without extra validation."""
        await self.ta.save_task(task=task)
        return task

    async def fail_task(self, task: Task) -> Task:
        """Persist a failed task, handling any dead letter queue side effects."""
        if task.task_config and task.task_config.dead_letter_policy == DeadLetterPolicy.SAVE:
            logger.info("Task %s sent to dead letter queue.", task.id)
            self.dead_queue.add(task, dt.datetime.now(dt.timezone.utc))
            tasks_dead_lettered.add(1, {"queue": task.queue, "task": task.name, "version": task.version})
        return await self.save_task(task)

    async def resubmit_dead_tasks(self, tasks: list[Task], reset_retry_count: bool = True) -> list[Task]:
        """Re-enqueue tasks from the dead letter queue into their active queues."""
        resubmitted = []
        for task in tasks:
            if reset_retry_count:
                task.retry_attempt = 0
            task.error = None
            task.set_status(TaskStatus.SUBMITTED)
            await self.ta.requeue_task(task)
            self.dead_queue.remove(str(task.id))
            resubmitted.append(task)
        return resubmitted

    async def complete_task(self, task: Task) -> Task:
        """Persist a completed task."""
        #TODO set a ttl for the data in redis after completion
        return await self.save_task(task)

    async def schedule_retry_task(self, task: Task, run_at: dt.datetime) -> Task:
        """Persist a delayed retry: add to the task scheduler and save."""
        self.task_scheduler.add(task, run_at)
        logger.info("Task %s scheduled for retry at %s.", task.id, run_at)
        await self.save_task(task)
        return task

    async def queue_retry_task(self, task: Task) -> Task:
        """Persist an immediate retry: re-enqueue the task without full re-validation."""
        task.set_status(TaskStatus.SUBMITTED)
        logger.info("Task %s requeued for immediate retry.", task.id)
        await self.ta.requeue_task(task)
        return task

    async def monitor_task_cancellation(self, task_id: ULID) -> None:
        """Monitor for user cancellation of the task."""
        async with self.data_store.pubsub() as pubsub:
            await pubsub.subscribe(f"task_cancel_{task_id}")
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message is not None:
                logger.info("Received cancellation for task %s", task_id)
                raise UserCancellationError(f"Task {task_id} was cancelled by user request.")

class SubmissionRateLimiter:
    """
    Rate limiter for tasks in a Redis data store.

    The rate limiter is stored in a sorted set with the following key:
    - `rate-limiter:<queue>`: Sorted set of task id => task name/hash key. This should be updated when the task-queues are updated.
        - ZADD/ZREM by task id: O(log(N))
        - ZCOUNT by hash key: O(log(N)) to get the current number of tasks with that hash key
    """

    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format

    def __init__(self, queue_config_adapter: QueueConfigAdapter):
        self.data_store = queue_config_adapter.data_store
        self.qca = queue_config_adapter

    async def has_room_in_queue_queue(self, queue: QueueConfig) -> bool:
        """Check if there is room in the queue for a task."""
        if queue.rate_numerator and queue.rate_denominator and queue.rate_period:
            now = dt.datetime.now(dt.timezone.utc)
            # May be better as a lua script to include adding the task to the queue in the same transaction
            earliest_time = now - dt.timedelta(seconds=queue.period_in_seconds() or 0)
            # Count the number of tasks in the queue that are older than the earliest time
            task_count = await self.data_store.zcount(
                self.QUEUE_RATE_LIMITER(queue=queue.name),
                min=earliest_time.timestamp(),
                max=now.timestamp(),
            )

            if task_count >= queue.rate_numerator:
                return False

        return True

    def add_task_to_queue(self, task: Task, pipe: Pipeline | None=None) -> Pipeline:
        """Add a task to the queue."""
        pipe = pipe or self.data_store.pipeline()

        # Add the task to the rate limiter for the queue
        if task.submitted_at:
            pipe.zadd(self.QUEUE_RATE_LIMITER(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})

        return pipe

    async def concurrency_limits(self, task_queues: set[str], current_tasks_by_queue: dict[str, set[ULID]]) -> set[str]:
        """Limit the number of concurrent tasks in each queue."""
        queues_to_use = set()
        # TODO: Consider ways to check each queue in a single transaction or in parallel
        for queue in task_queues:
            config = await self.qca.get_queue_config(queue=queue)
            if config and config.max_concurrent:
                if len(current_tasks_by_queue[queue]) < config.max_concurrent:
                    queues_to_use.add(queue)

        return queues_to_use

    async def clean(self, queues: set[bytes], now: dt.datetime, rate_limit_age: dt.timedelta) -> None:
        """Clean up the tasks from the rate limiter that are older than the rate limit age."""
        earliest_time = now - rate_limit_age
        pipe = self.data_store.pipeline(transaction=True)
        for queue in queues:
            pipe.zremrangebyscore(self.QUEUE_RATE_LIMITER(queue=queue.decode()), min=0, max=earliest_time.timestamp())
        await pipe.execute()

def build_sm() -> StateManager:
    from jobbers import db
    return db.get_state_manager()
