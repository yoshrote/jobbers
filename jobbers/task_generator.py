import datetime as dt
import os
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Optional

from jobbers.state_manager import StateManager

if TYPE_CHECKING:
    from ulid import ULID


class LocalTTL:
    """
    A context manager to manage time-to-live (TTL) for local operations.

    Attributes
    ----------
    config_ttl : int
        The TTL duration in seconds.
    last_refreshed : Optional[datetime]
        The last time the TTL was refreshed.
    """

    def __init__(self, config_ttl: int):
        self.config_ttl = config_ttl
        self.last_refreshed: Optional[dt.datetime] = None
        self._now: Optional[dt.datetime] = None

    async def __aenter__(self):
        self._now = dt.datetime.now(dt.timezone.utc)
        return self._older_than_ttl(self._now)

    async def __aexit__(self, exc_type, exc, tb):
        if self._older_than_ttl(self._now):
            self.last_refreshed = self._now

    def _older_than_ttl(self, now: dt.datetime) -> bool:
        if self.last_refreshed and self.config_ttl:
            return (now - self.last_refreshed).total_seconds() >= self.config_ttl
        return True

class MaxTaskCounter:
    """
    A counter to track the number of tasks processed, with a maximum limit.

    Attributes
    ----------
    max_tasks : int
        The maximum number of tasks allowed.

    Methods
    -------
    limit_reached() -> bool
        Check if the maximum task limit has been reached.
    """

    def __init__(self, max_tasks: int=0):
        self.max_tasks: int = max_tasks
        self._task_count: int = 0

    def limit_reached(self) -> bool:
        return self.max_tasks > 0 and self._task_count >= self.max_tasks

    def __enter__(self):
        if self.limit_reached():
            raise StopAsyncIteration
        # Increment immediately so consuming tasks have an accurate count
        if self.max_tasks > 0:
            self._task_count += 1
        return self._task_count

    def __exit__(self, exc_type, exc, tb):
        pass

class TaskGenerator:
    """Generates tasks from the Redis list 'task-list'."""

    DEFAULT_QUEUES = {"default"}

    def __init__(self, state_manager, role="default", max_tasks=100, config_ttl=60):
        self.role: str = role
        self.state_manager: StateManager = state_manager
        self.ttl = LocalTTL(config_ttl)
        self.max_task_check = MaxTaskCounter(max_tasks)
        self.task_queues: set[str] = None
        self.refresh_tag: ULID = None

    async def find_queues(self) -> Awaitable[set[str]]:
        """Find all queues we should listen to via Redis."""
        if self.role == "default":
            return self.DEFAULT_QUEUES
        queues = await self.state_manager.get_queues(self.role)
        return queues or set()

    async def queues(self) -> set[str]:
        async with self.ttl as needs_refresh:
            if not needs_refresh:
                # TODO: filter out queues if we are at capacity running tasks from them
                return self.task_queues
            new_refresh_tag = await self.state_manager.get_refresh_tag(self.role)
            if new_refresh_tag != self.refresh_tag:
                self.refresh_tag = new_refresh_tag
                self.task_queues = {
                    queue
                    for queue in await self.find_queues()
                }
        # TODO: filter out queues if we are at capacity running tasks from them
        return self.task_queues

    def __aiter__(self):
        return self

    async def __anext__(self):
        with self.max_task_check:
            task_queues = await self.queues()
            task = await self.state_manager.get_next_task(task_queues)
        if not task:
            # TODO: We need to monitor how often the generator dies this way
            raise StopAsyncIteration
        return task

def build_task_generator(state_manager: StateManager):
    """Consume tasks from the Redis list 'task-list'."""
    role = os.environ.get("WORKER_ROLE", "default")
    worker_ttl = int(os.environ.get("WORKER_TTL", 50)) # if 0, will run indefinitely

    return TaskGenerator(state_manager, role, max_tasks=worker_ttl)
