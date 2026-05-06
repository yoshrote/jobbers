import asyncio
import datetime as dt
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics

from jobbers.models.task import Task
from jobbers.state_manager import StateManager

if TYPE_CHECKING:
    from ulid import ULID

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
time_in_queue = meter.create_histogram("time_in_queue", unit="ms")
tasks_selected = meter.create_counter("tasks_selected", unit="1")
queue_config_refreshes = meter.create_counter("queue_config_refreshes", unit="1")
refresh_lag_ms = meter.create_histogram("refresh_lag_ms", unit="ms")

_CAPACITY_BACKOFF_SECS: float = 1.0


class MaxTaskCounter:
    """
    A counter to track the number of tasks processed, with a maximum limit.

    **Attributes:**

    - `max_tasks: int` — the maximum number of tasks allowed.

    **Methods:**

    - `limit_reached() -> bool` — check if the maximum task limit has been reached.
    """

    def __init__(self, max_tasks: int = 0):
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

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object | None
    ) -> None:
        pass


class TaskGenerator:
    """Generates tasks from the Redis list 'task-list'."""

    DEFAULT_QUEUES = {"default"}

    def __init__(
        self,
        state_manager: StateManager,
        role: str = "default",
        max_tasks: int = 100,
    ) -> None:
        self.role: str = role
        self.state_manager: StateManager = state_manager
        self.max_task_check = MaxTaskCounter(max_tasks)
        self.task_queues: set[str] = set()
        self.refresh_tag: ULID | None = None
        self.routing_version: ULID | None = None
        self._run: bool = True
        self._refresh_pubsub: Any | None = None

    async def _get_refresh_pubsub(self) -> Any:
        if self._refresh_pubsub is None:
            self._refresh_pubsub = self.state_manager.job_store.pubsub()
            await self._refresh_pubsub.subscribe(f"queue-config-refresh:{self.role}")
        return self._refresh_pubsub

    async def find_queues(self) -> set[str]:
        """Find all queues we should listen to via Redis."""
        if self.role == "default":
            return self.DEFAULT_QUEUES
        queues = await self.state_manager.get_queues(self.role)
        return queues or set()

    async def filter_by_worker_queue_capacity(self, queues: set[str]) -> set[str]:
        if not queues:
            return queues

        active_tasks = self.state_manager.active_tasks_per_queue
        queue_worker_limits = await self.state_manager.get_queue_limits(queues)
        logger.debug("Queues: %s; Active: %s; Limits: %s", queues, active_tasks, queue_worker_limits)
        return {
            q
            for q in queues
            if not (limit := queue_worker_limits.get(q, 0)) or active_tasks.get(q, 0) < limit
        }

    async def queues(self) -> set[str]:
        # store the full set of tasks in self.task_queues, but emit the
        # queues that meet configured limits so that we evaluate that aspect
        # between configuration refresh

        new_routing_version = await self.state_manager.get_routing_version()
        if new_routing_version != self.routing_version:
            self.routing_version = new_routing_version
            self.state_manager.invalidate_all_routing_config()
            logger.info("Routing config invalidated at version %s", new_routing_version)

        pubsub = await self._get_refresh_pubsub()
        if await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.0):
            self.refresh_tag = None  # force tag re-read below

        new_refresh_tag = await self.state_manager.get_refresh_tag(self.role)
        if new_refresh_tag != self.refresh_tag:
            self.refresh_tag = new_refresh_tag
            self.task_queues = {queue for queue in await self.find_queues() if queue}
            for queue in self.task_queues:
                self.state_manager.invalidate_queue_config(queue)
            lag = (dt.datetime.now(dt.UTC) - new_refresh_tag.datetime).total_seconds() * 1000
            queue_config_refreshes.add(1, {"role": self.role})
            refresh_lag_ms.record(lag, {"role": self.role})
            logger.info("Refreshed to v %s: %s", self.refresh_tag, self.task_queues)
        return await self.filter_by_worker_queue_capacity(self.task_queues)

    def stop(self) -> None:
        self._run = False
        self._refresh_pubsub = None

    def __aiter__(self) -> AsyncIterator[Task]:
        return self

    async def __anext__(self) -> Task:
        if not self._run:
            raise StopAsyncIteration
        with self.max_task_check:
            while True:
                task_queues = await self.queues()
                logger.debug("Checking queues %s", task_queues)
                task = None
                try:
                    task = await self.state_manager.get_next_task(task_queues)
                except asyncio.CancelledError:
                    if task:
                        pipe = self.state_manager.job_store.pipeline()
                        self.state_manager.ta.stage_requeue(pipe, task)
                        await pipe.execute()
                    raise
                if not task:
                    logger.info(
                        "All queues filtered out; sleeping before retry. "
                        "role=%s configured_queues=%s capacity_available_queues=%s tasks_processed=%d",
                        self.role,
                        self.task_queues,
                        task_queues,
                        self.max_task_check._task_count,
                    )
                    await asyncio.sleep(_CAPACITY_BACKOFF_SECS)
                    continue
                break
        metric_tags = {
            "queue": task.queue,
            "role": self.role,
            "task": task.name,
        }
        if task.submitted_at is None:  # This should never happen
            logger.fatal(
                "Task %s v%s id=%s is missing a submitted_at timestamp.", task.name, task.version, task.id
            )
            raise RuntimeError("Pulled a task that was never submitted")
        time_in_queue.record(
            (dt.datetime.now(dt.UTC) - task.submitted_at).total_seconds() * 1000, metric_tags
        )
        tasks_selected.add(1, metric_tags)
        return task
