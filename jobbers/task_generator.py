import datetime as dt
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from opentelemetry import metrics

from jobbers.models.queue_config import QueueConfigAdapter
from jobbers.models.task import Task
from jobbers.state_manager import StateManager

if TYPE_CHECKING:
    from ulid import ULID

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
time_in_queue = meter.create_histogram("time_in_queue", unit="ms")
tasks_selected = meter.create_counter("tasks_selected", unit="1")

class LocalTTL:
    """
    A context manager to manage time-to-live (TTL) for local operations.

    Attributes
    ----------
    config_ttl : int
        The TTL duration in seconds.
    last_refreshed : datetime | None
        The last time the TTL was refreshed.
    """

    def __init__(self, config_ttl: int):
        self.config_ttl = config_ttl
        self.last_refreshed: dt.datetime | None = None
        self._now: dt.datetime = dt.datetime.now(dt.timezone.utc)

    def __enter__(self) -> bool:
        self._now = dt.datetime.now(dt.timezone.utc)
        return self._older_than_ttl(self._now)

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object | None) -> None:
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

    def __enter__(self) -> int:
        if self.limit_reached():
            logger.info("Limit reached; exiting")
            raise StopAsyncIteration
        # Increment immediately so consuming tasks have an accurate count
        if self.max_tasks > 0:
            self._task_count += 1
        return self._task_count

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object | None) -> None:
        pass

class TaskGenerator:
    """Generates tasks from the Redis list 'task-list'."""

    DEFAULT_QUEUES = {"default"}

    def __init__(self,
                 state_manager: StateManager,
                 query_config_adapter: QueueConfigAdapter,
                 role: str="default",
                 max_tasks: int=100,
                 config_ttl: int=60) -> None:
        self.role: str = role
        self.state_manager: StateManager = state_manager
        self.query_config_adapter = query_config_adapter
        self.ttl = LocalTTL(config_ttl)
        self.max_task_check = MaxTaskCounter(max_tasks)
        self.task_queues: set[str] = set()
        self.refresh_tag: ULID | None = None
        self._run: bool = True

    async def find_queues(self) -> set[str]:
        """Find all queues we should listen to via Redis."""
        if self.role == "default":
            return self.DEFAULT_QUEUES
        queues = await self.query_config_adapter.get_queues(self.role)
        return queues or set()

    async def filter_by_worker_queue_capacity(self, queues: set[str]) -> set[str]:
        if not queues:
            return queues

        active_tasks = self.state_manager.active_tasks_per_queue
        queue_worker_limits = await self.query_config_adapter.get_queue_limits(queues)
        logger.debug("Queues: %s; Active: %s; Limits: %s", queues, active_tasks, queue_worker_limits)
        # TODO: write tests to make sure this filters correctly
        return {
            q for q in queues
            if not queue_worker_limits.get(q, 0) or active_tasks.get(q, 0) < (queue_worker_limits.get(q) or 0)
        }

    async def queues(self) -> set[str]:
        # store the full set of tasks in self.task_queues, but emit the
        # queues that meet configured limits so that we evaluate that aspect
        # between configuration refresh

        # with self.ttl as needs_refresh:
        #     if not needs_refresh:
        #         return self.filter_by_worker_queue_capacity(self.task_queues)
        new_refresh_tag = await self.state_manager.get_refresh_tag(self.role)
        if new_refresh_tag != self.refresh_tag:
            self.refresh_tag = new_refresh_tag
            self.task_queues = {
                queue
                for queue in await self.find_queues()
                if queue
            }
            logger.info("Refreshed to v %s: %s", self.refresh_tag, self.task_queues)
        return await self.filter_by_worker_queue_capacity(self.task_queues)

    def stop(self) -> None:
        self._run = False

    def __aiter__(self) -> AsyncIterator[Task]:
        return self

    async def __anext__(self) -> Task:
        if not self._run:
            raise StopAsyncIteration
        with self.max_task_check:
            task_queues = await self.queues()
            logger.debug("Checking queues %s", task_queues)
            # try:
            task = await self.state_manager.get_next_task(task_queues)
            # except asyncio.CancelledError:
            #    # put task back on queue
        if not task:
            # TODO: We need to monitor how often the generator dies this way
            logger.warning("Strange stop")
            raise StopAsyncIteration
        metric_tags = {
            "queue": task.queue,
            "role": self.role,
            "task": task.name,
        }
        if task.submitted_at is None: # This should never happen
            logger.fatal("Task %s v%s id=%s is missing a submitted_at timestamp.", task.name, task.version, task.id)
            raise RuntimeError("Pulled a task that was never submitted")
        time_in_queue.record(
            (dt.datetime.now(dt.timezone.utc) - task.submitted_at).total_seconds() * 1000,
            metric_tags
        )
        tasks_selected.add(1, metric_tags)
        return task
