import datetime as dt
import logging
from asyncio import TaskGroup
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Optional

from ulid import ULID

from jobbers.models import Task, TaskStatus
from jobbers.models.queue_config import QueueConfig

logger = logging.getLogger(__name__)

TIME_ZERO = dt.datetime.fromtimestamp(0, dt.timezone.utc)

class StateManager:
    """
    Manages tasks in a Redis data store.

    The state is stored across a number of different key types:
    - `task-queues:<queue>`: Sorted set of task ID => submitted at timestamp for each queue
        - ZPOPMIN to get the oldest task from a set of queues
    - `task:<task_id>`: Hash containing the task state (name, status, etc).
    - `worker-queues:<role>`: Set of queues for a given role, used to manage which queues are available for task submission.

    - `rate-limiter:<queue>`: Sorted set of task id => task name/hash key. This should be updated when the task-queues are updated.
        - ZADD/ZREM by task id: O(log(N))
        - ZCOUNT by hash key: O(log(N)) to get the current number of tasks with that hash key
    """

    TASKS_BY_QUEUE = "task-queues:{queue}".format
    QUEUES_BY_ROLE = "worker-queues:{role}".format
    TASK_DETAILS = "task:{task_id}".format
    ALL_QUEUES = "all-queues"

    def __init__(self, data_store):
        self.data_store = data_store
        self.rate_limiter = RateLimiter(data_store)
        self.current_tasks_by_queue: dict[str, set[ULID]] = defaultdict(set)

    @asynccontextmanager
    def task_in_registry(self, task: Task):
        """Context manager to add a task to the registry."""
        self.current_tasks_by_queue[task.queue].add(task.id)
        try:
            yield
        finally:
            # TODO: Do we need to clean this up before handle_success?
            # Need to consider interactions with callbacks of the task
            self.current_tasks_by_queue[task.queue].remove(task.id)

    async def clean(self, rate_limit_age: Optional[dt.timedelta]=None, min_queue_age: Optional[dt.datetime]=None, max_queue_age: Optional[dt.datetime]=None):
        """Clean up the state manager."""
        now = dt.datetime.now(dt.timezone.utc)
        if rate_limit_age:
            # Remove tasks from the rate limiter that are older than the rate limit age
            earliest_time = now - rate_limit_age
            pipe = self.data_store.pipeline(transaction=True)
            for queue in await self.data_store.smembers(self.ALL_QUEUES):
                pipe.zremrangebyscore(self.rate_limiter.QUEUE_RATE_LIMITER(queue=queue.decode()), min=0, max=earliest_time.timestamp())
            await pipe.execute()

        if max_queue_age or min_queue_age:
            earliest_time = min_queue_age or TIME_ZERO
            latest_time = max_queue_age or now
            for queue in await self.data_store.smembers(self.ALL_QUEUES):
                pipe = self.data_store.pipeline(transaction=True)
                if earliest_time <= latest_time:
                    pipe.zremrangebyscore(self.TASKS_BY_QUEUE(queue=queue.decode()), min=earliest_time.timestamp(), max=latest_time.timestamp())
                else:
                    pipe.zremrangebyscore(self.TASKS_BY_QUEUE(queue=queue.decode()), min=0, max=earliest_time.timestamp())
                    pipe.zremrangebyscore(self.TASKS_BY_QUEUE(queue=queue.decode()), min=latest_time.timestamp(), max=now.timestamp())
                await pipe.execute()




    async def submit_task(self, task: Task):
        """Submit a task to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        # Avoid pushing a task onto the queue multiple times
        if task.status == TaskStatus.UNSUBMITTED and not await self.task_exists(task.id):
            if self.rate_limiter.has_room_in_queue_queue(task.queue):
                task.submitted_at = dt.datetime.now(dt.timezone.utc)
                task.status = TaskStatus.SUBMITTED
                pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
                self.rate_limiter.add_task_to_queue(task, pipe=pipe)

        pipe.hset(self.TASK_DETAILS(task_id=task.id), mapping=task.to_redis())
        await pipe.execute()

    async def get_task(self, task_id: ULID) -> Task | None:
        raw_task_data: dict = await self.data_store.hgetall(self.TASK_DETAILS(task_id=task_id))

        if raw_task_data:
            return Task.from_redis(task_id, raw_task_data)

        return None

    async def task_exists(self, task_id: ULID) -> bool:
        return await self.data_store.exists(self.TASK_DETAILS(task_id=task_id)) == 1

    async def get_all_tasks(self) -> list[ULID]:
        task_ids = await self.data_store.zrange(self.TASKS_BY_QUEUE(queue="default"), 0, -1)
        if not task_ids:
            return []
        results = []
        async with TaskGroup() as group:
            for task_id in task_ids:
                group.create_task(self._add_task_to_results(ULID(task_id), results))
        return results

    async def _add_task_to_results(self, task_id: ULID, results: list[Task]):
        task = await self.get_task(task_id)
        if task:
            results.append(task)
        return results

    async def get_queues(self, role: str) -> set[str]:
        return {role.decode() for role in await self.data_store.smembers(self.QUEUES_BY_ROLE(role=role))}

    async def get_refresh_tag(self, role: str) -> Optional[ULID]:
        tag = await self.data_store.get(f"worker-queues:{role}:refresh_tag")
        return ULID.from_bytes(tag) if tag else ULID()

    async def get_next_task(self, queues: list[str], timeout=0) -> Optional[Task]:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        if not queues:
            logger.info("no queues defined")
            return None

        queues = await self.rate_limiter.concurrency_limits(queues, self.current_tasks_by_queue)
        task_queues = [self.TASKS_BY_QUEUE(queue=queue) for queue in queues]

        # Try to pop from each queue until we find a task
        # TODO: Shuffle/rotate the order of queues to avoid starving any of them
        # see https://redis.io/docs/latest/commands/blpop/#what-key-is-served-first-what-client-what-element-priority-ordering-details
        # for details of how the order of keys impact how tasks are popped
        task_id = await self.data_store.bzpopmin(task_queues, timeout=timeout)
        if task_id:
            task = await self.get_task(ULID.from_bytes(task_id[1]))
            if task:
                return task
            logger.warning("Task with ID %s not found.", task_id)
        else:
            logger.info("task query timed out")
        return None

    async def set_queues(self, role: str, queues: set[str]):
        pipe = self.data_store.pipeline(transaction=True)
        pipe.delete(self.QUEUES_BY_ROLE(role=role))
        pipe.sadd(self.ALL_QUEUES, *queues)
        for queue in queues:
            pipe.sadd(self.QUEUES_BY_ROLE(role=role), queue)
        await pipe.execute()

    async def get_all_queues(self) -> list[str]:
        # find the union of the queues for all roles
        # this query approach is not ideal for large numbers of roles or queues
        roles = await self.get_all_roles()
        if not roles:
            return []

        return [
            queue.decode()
            for queue in await self.data_store.sunion(
                [self.QUEUES_BY_ROLE(role=role) for role in roles]
            )
        ]

    async def get_all_roles(self) -> list[str]:
        roles = []
        async for key in self.data_store.scan_iter(match=self.QUEUES_BY_ROLE(role="*").encode()):
            roles.append(key.decode().split(":")[1])
        return roles


class RateLimiter:
    """
    Rate limiter for tasks in a Redis data store.

    The rate limiter is stored in a sorted set with the following key:
    """

    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    QUEUE_CONFIG = "queue-config:{queue}".format

    def __init__(self, data_store):
        self.data_store = data_store

    async def has_room_in_queue_queue(self, queue: str) -> bool:
        """Check if there is room in the queue for a task."""
        config = await self.get_queue_config(queue=queue)
        if not config:
            return True  # No config means no rate limiting

        if config.rate_numerator and config.rate_denominator and config.rate_period:
            now = dt.datetime.now(dt.timezone.utc)
            # May be better as a lua script to include adding the task to the queue in the same transaction
            earliest_time = now - dt.timedelta(seconds=config.period_in_seconds())
            # Count the number of tasks in the queue that are older than the earliest time
            task_count = await self.data_store.zcount(
                self.QUEUE_RATE_LIMITER(queue=queue),
                min=earliest_time.timestamp(),
                max=now.timestamp(),
            )

            if task_count >= config.rate_numerator:
                return False

        return True

    async def get_queue_config(self, queue: str) -> QueueConfig:
        raw_data = await self.data_store.hgetall(self.QUEUE_CONFIG(queue=queue))  # Ensure the queue config exists in the store
        return QueueConfig.from_redis(queue, raw_data)

    async def add_task_to_queue(self, task: Task, pipe=None):
        """Add a task to the queue."""
        pipe = pipe or self.data_store

        # Add the task to the rate limiter for the queue
        pipe.zadd(self.QUEUE_RATE_LIMITER(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})

        return pipe

    async def concurrency_limits(self, task_queues: list[str], current_tasks_by_queue: dict[str, set[ULID]]) -> list[str]:
        """Limit the number of concurrent tasks in each queue."""
        queues_to_use = []
        # TODO: Consider ways to check each queue in a single transaction or in parallel
        for queue in task_queues:
            config = await self.get_queue_config(queue=queue)
            if config and config.max_concurrent:
                if len(current_tasks_by_queue[queue]) < config.max_concurrent:
                    queues_to_use.append(queue)

        return queues_to_use

def build_sm() -> StateManager:
    from jobbers import db
    return StateManager(db.get_client())
