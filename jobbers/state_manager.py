import datetime as dt
import logging
from asyncio import TaskGroup
from typing import Optional

from ulid import ULID

from jobbers.models import Task, TaskStatus

logger = logging.getLogger(__name__)

class StateManager:
    """
    Manages tasks in a Redis data store.

    The state is stored across a number of different key types:
    - `task-queues:<queue>`: Sorted set of task ID => submitted at timestamp for each queue
        - ZPOPMIN to get the oldest task from a set of queues
    - `task:<task_id>`: Hash containing the task state (name, status, etc).
    - `worker-queues:<role>`: Set of queues for a given role, used to manage which queues are available for task submission.
    """

    TASKS_BY_QUEUE = "task-queues:{queue}".format
    QUEUES_BY_ROLE = "worker-queues:{role}".format
    TASK_DETAILS = "task:{task_id}".format
    ALL_QUEUES = "all-queues"

    def __init__(self, data_store):
        self.data_store = data_store

    async def submit_task(self, task: Task):
        """Submit a task to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        # Avoid pushing a task onto the queue multiple times
        if task.status == TaskStatus.UNSUBMITTED and not await self.task_exists(task.id):
            task.submitted_at = dt.datetime.now(dt.timezone.utc)
            task.status = TaskStatus.SUBMITTED
            pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})

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
        return ULID.from_bytes(tag) if tag else None

    async def get_next_task(self, queues: list[str], timeout=0) -> Optional[Task]:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        if not queues:
            return None

        # Try to pop from each queue until we find a task
        # TODO: Shuffle/rotate the order of queues to avoid starving any of them
        # see https://redis.io/docs/latest/commands/blpop/#what-key-is-served-first-what-client-what-element-priority-ordering-details
        # for details of how the order of keys impact how tasks are popped
        task_queues = [self.TASKS_BY_QUEUE(queue=queue) for queue in queues]
        task_id = await self.data_store.bzpopmin(task_queues, timeout=timeout)
        if task_id:
            task = await self.get_task(ULID.from_bytes(task_id[1]))
            if task:
                return task
            logger.warning("Task with ID %s not found.", task_id)
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

def build_sm() -> StateManager:
    from jobbers import db
    return StateManager(db.get_client())
