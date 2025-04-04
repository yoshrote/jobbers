import datetime as dt
import logging
from asyncio import TaskGroup

from ulid import ULID

from jobbers.models import Task, TaskConfig, TaskStatus

logger = logging.getLogger(__name__)

class StateManager:
    """Manages tasks in a Redis data store."""

    def __init__(self, data_store):
        self.data_store = data_store

    async def submit_task(self, task: Task):
        """Submit a task to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        # Avoid pushing a task onto the queue multiple times
        if task.status == TaskStatus.UNSUBMITTED and not await self.task_exists(task.id):
            pipe.lpush(f"task-list:{task.queue}", bytes(task.id))
            task.submitted_at = dt.datetime.now(dt.timezone.utc)
            task.status = TaskStatus.SUBMITTED

        pipe.hset(f"task:{task.id}", mapping=task.to_redis())
        await pipe.execute()

    async def get_task(self, task_id: ULID) -> Task | None:
        raw_task_data: dict = await self.data_store.hgetall(f"task:{task_id}".encode())

        if raw_task_data:
            return Task.from_redis(task_id, raw_task_data)

        return None

    async def get_task_config(self, name: str, version: int) -> Task | None:
        raw_task_data: dict = await self.data_store.hgetall(f"task_config:{name}:{version}".encode())

        if raw_task_data:
            return TaskConfig.from_redis(name, version, raw_task_data)

        return None

    async def task_exists(self, task_id: ULID) -> bool:
        return await self.data_store.exists(f"task:{task_id}")

    async def get_all_tasks(self) -> list[ULID]:
        task_ids = await self.data_store.lrange("task-list:default", 0, -1)
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

    async def get_queues(self, role: str) -> list[str]:
        return [role.decode() for role in await self.data_store.smembers(f"worker-queues:{role}")]

    async def set_queues(self, role: str, queues: list[str]):
        pipe = self.data_store.pipeline(transaction=True)
        pipe.delete(f"worker-queues:{role}")
        pipe.sadd("all-queues", *queues)
        for queue in queues:
            pipe.sadd(f"worker-queues:{role}", queue)
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
                [f"worker-queues:{role}" for role in roles]
            )
        ]

    async def get_all_roles(self) -> list[str]:
        roles = []
        async for key in self.data_store.scan_iter(match="worker-queues:*"):
            roles.append(key.decode().split(":")[1])
        return roles

def build_sm() -> StateManager:
    from jobbers import db
    return StateManager(db.get_client())
