import asyncio
import datetime as dt
import logging
import os
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ulid import ULID

from asyncio_taskpool import TaskPool

from jobbers.db import get_client
from jobbers.models import Task, TaskConfig, TaskStatus
from jobbers.registry import get_task_config
from jobbers.state_manager import StateManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
Important environment variables:
- WORKER_ROLE: Role of the worker (default is "default")
- WORKER_TTL: Time to live for the worker (in seconds) (default is 50)
- WORKER_CONCURRENT_TASKS: Maximum number of concurrent tasks to process (default is 5)

Rate limiting should be implemented by limiting the creation of tasks rather
than on the consumption of tasks.
"""

class TaskProcessor:
    """TaskProcessor to process tasks from a TaskGenerator."""

    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager

    async def process(self, task: Task) -> Task:
        """Process the task and return the result."""
        logger.debug("Task %s details: %s", task.id, task)
        task_config: TaskConfig = get_task_config(task.name, task.version)
        if not task_config:
            self.handle_dropped_task(task)
        else:
            ex = None
            try:
                task = await self.mark_task_as_started(task)
                async with asyncio.timeout(task_config.timeout):
                    task.results = await task_config.function(**task.parameters)
            except task_config.expected_exceptions as exc:
                self.handle_expected_exception(task, task_config, exc)
            except asyncio.TimeoutError:
                self.handle_timeout_exception(task, task_config)
            except asyncio.CancelledError as exc:
                ex = exc
                self.handle_cancelled_task(task)
            except Exception as exc:
                self.handle_unexpected_exception(task, exc)
            else:
                self.handle_success(task)

        await self.state_manager.submit_task(task)

        if task.status == TaskStatus.COMPLETED:
            await self.post_process(task)
        elif task.status == TaskStatus.CANCELLED:
            raise ex

        return task

    async def mark_task_as_started(self, task: Task) -> Awaitable[Task]:
        task.started_at = dt.datetime.now(dt.timezone.utc)
        task.status = TaskStatus.STARTED
        await self.state_manager.submit_task(task)
        return task

    async def post_process(self, task: Task):
        if task.has_callbacks():
            return

        # TODO: submit tasks in parallel
        # Monitor for when fan-out becomes problematic
        for callback_task in task.generate_callbacks():
            await self.state_manager.submit_task(callback_task)

    def handle_dropped_task(self, task: Task):
        logger.error("Dropping unknown task %s v%s id=%s.", task.name, task.version, task.id)
        task.status = TaskStatus.DROPPED
        task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_cancelled_task(self, task: Task):
        logger.info("Task %s was cancelled.", task.id)
        task.status = TaskStatus.CANCELLED
        task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_unexpected_exception(self, task: Task, exc: Exception):
        logger.exception("Exception occurred while processing task %s: %s", task.id, exc)
        task.status = TaskStatus.FAILED
        task.error = str(exc)

    def handle_expected_exception(self, task: Task, task_config: TaskConfig, exc: Exception):
        logger.warning("Task %s failed with error: %s", task.id, exc)
        # TODO: Set metrics to track expected exceptions
        task.error = str(exc)
        if task.should_retry(task_config):
            # Task status will change to submitted when re-enqueued
            task.retry_attempt += 1
            task.status = TaskStatus.UNSUBMITTED
        else:
            task.status = TaskStatus.FAILED
            task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_timeout_exception(self, task: Task, task_config: TaskConfig):
        logger.warning("Task %s timed out after %d seconds.", task.id, task_config.timeout)
        task.error = f"Task {task.id} timed out after {task_config.timeout} seconds"
        if task.should_retry(task_config):
            # Task status will change to submitted when re-enqueued
            task.status = TaskStatus.UNSUBMITTED
            task.retry_attempt += 1
        else:
            task.status = TaskStatus.FAILED
            task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_success(self, task: Task):
        logger.info("Task %s completed.", task.id)
        task.status = TaskStatus.COMPLETED
        task.completed_at = dt.datetime.now(dt.timezone.utc)

class TaskGenerator:
    """Generates tasks from the Redis list 'task-list'."""

    DEFAULT_QUEUES = {"default"}

    def __init__(self, state_manager, role="default", max_tasks=100, config_ttl=60):
        self.role: str = role
        self.state_manager: StateManager = state_manager
        self.task_queues: set[str] = None
        self.config_ttl: int = config_ttl
        self.refresh_tag: ULID = None
        self._last_refreshed: Optional[dt.datetime] = None
        self.max_tasks: int = max_tasks
        self._task_count: int = 0

    async def find_queues(self) -> Awaitable[set[str]]:
        """Find all queues we should listen to via Redis."""
        if self.role == "default":
            return self.DEFAULT_QUEUES
        queues = await self.state_manager.get_queues(self.role)
        return queues or set()

    async def queues(self) -> set[str]:
        if self.task_queues is None or self.should_reload_queues():
            self.task_queues = {
                queue
                for queue in await self.find_queues()
            }
            self._last_refreshed = dt.datetime.now(dt.timezone.utc)
        # TODO: filter out queues if we are at capacity running tasks from them
        return self.task_queues

    async def should_reload_queues(self) -> bool:
        now = dt.datetime.now(dt.timezone.utc)
        if self.config_ttl and self._last_refreshed and (now - self._last_refreshed).total_seconds() < self.config_ttl:
            return False
        refresh_tag = await self.state_manager.get_refresh_tag(self.role)
        if refresh_tag != self.refresh_tag:
            self.refresh_tag = refresh_tag
            needs_refresh = True
        else:
            needs_refresh = False
        return needs_refresh

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.max_tasks and self._task_count >= self.max_tasks:
            raise StopAsyncIteration
        task_queues = await self.queues()
        task = await self.state_manager.get_next_task(task_queues)
        self._task_count += 1
        if not task:
            # TODO: We need to monitor how often the generator dies this way
            raise StopAsyncIteration
        return task

async def task_consumer():
    """Consume tasks from the Redis list 'task-list'."""
    role = os.environ("WORKER_ROLE", "default")
    worker_ttl = int(os.environ("WORKER_TTL", 50)) # if 0, will run indefinitely
    num_concurrent = float(os.environ("WORKER_CONCURRENT_TASKS", 5))
    redis = await get_client()
    state_manager = StateManager(redis)

    task_generator = TaskGenerator(state_manager, role, max_tasks=worker_ttl)
    try:
        pool = TaskPool()
        pool.map(TaskProcessor(state_manager).process, task_generator, num_concurrent=num_concurrent)
        await pool.gather_and_close()
    # except asyncio.CancelledError:
    #     logger.info("Task consumer killed. Shutting down...")
    #     raise
    # else:
    #     logger.info("Task consumer finished. Shutting down...")
    finally:
        await redis.close()

def run():
    try:
        asyncio.run(task_consumer())
    except KeyboardInterrupt:
        logger.info("Task consumer stopped.")
