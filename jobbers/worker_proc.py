import asyncio
import datetime as dt
import logging
import os

from jobbers.db import get_client
from jobbers.registry import get_task_config
from jobbers.state_manager import StateManager, Task, TaskConfig, TaskStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskProcessor:
    """TaskProcessor to process tasks from a TaskGenerator."""

    def __init__(self, task: Task, state_manager: StateManager):
        self.state_manager = state_manager
        self.task = task

    async def process(self) -> Task:
        """Process the task and return the result."""
        logger.debug("Task %s details: %s", self.task.id, self.task)
        task_config: TaskConfig = get_task_config(self.task.name, self.task.version)
        if not task_config:
            self.handle_dropped_task()
        else:
            try:
                async with asyncio.timeout(task_config.timeout):
                    self.task.results = await task_config.function(**self.task.parameters)
            except task_config.expected_exceptions as exc:
                self.handle_expected_exception(task_config, exc)
            except asyncio.TimeoutError:
                self.handle_timeout_exception(task_config)
            except asyncio.CancelledError:
                self.handle_cancelled_task()
            except Exception as exc:
                self.handle_unexpected_exception(exc)
            else:
                self.handle_success()

        await self.state_manager.submit_task(
            self.task,
        )

        return self.task

    def handle_dropped_task(self):
        logger.error("Dropping unknown task %s v%s id=%s.", self.task.name, self.task.version, self.task.id)
        self.task.status = TaskStatus.DROPPED
        self.task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_cancelled_task(self):
        logger.info("Task %s was cancelled.", self.task.id)
        self.task.status = TaskStatus.CANCELLED
        self.task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_unexpected_exception(self, exc: Exception):
        logger.exception("Exception occurred while processing task %s: %s", self.task.id, exc)
        self.task.status = TaskStatus.FAILED
        self.task.error = str(exc)

    def handle_expected_exception(self, task_config: TaskConfig, exc: Exception):
        logger.warning("Task %s failed with error: %s", self.task.id, exc)
        # TODO: Set metrics to track expected exceptions
        self.task.error = str(exc)
        if self.task.should_retry(task_config):
            # Task status will change to submitted when re-enqueued
            self.task.retry_attempt += 1
            self.task.status = TaskStatus.UNSUBMITTED
        else:
            self.task.status = TaskStatus.FAILED
            self.task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_timeout_exception(self, task_config: TaskConfig):
        logger.warning("Task %s timed out after %d seconds.", self.task.id, task_config.timeout)
        self.task.error = f"Task {self.task.id} timed out after {task_config.timeout} seconds"
        if self.task.should_retry(task_config):
            # Task status will change to submitted when re-enqueued
            self.task.status = TaskStatus.UNSUBMITTED
            self.task.retry_attempt += 1
        else:
            self.task.status = TaskStatus.FAILED
            self.task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_success(self):
        logger.info("Task %s completed.", self.task.id)
        self.task.status = TaskStatus.COMPLETED
        self.task.completed_at = dt.datetime.now(dt.timezone.utc)

# move TaskGenerator to a separate file
class TaskGenerator:
    """Generates tasks from the Redis list 'task-list'."""

    DEFAULT_QUEUES = ["task-list:default"]

    def __init__(self, redis, state_manager, role="default"):
        self.redis = redis
        self.role = role
        self.state_manager = state_manager
        self.task_queues = None
        self.refresh_tag = None

    async def find_queues(self):
        """Find all queues we should listen to via Redis."""
        if self.role == "default":
            return self.DEFAULT_QUEUES
        return await self.redis.smembers(f"worker-queues:{self.role}") or self.DEFAULT_QUEUES

    async def queues(self):
        if not self.task_queues or self.should_reload_queues():
            self.task_queues = await self.find_queues()
        return self.task_queues

    async def should_reload_queues(self):
        if not self.refresh_tag:
            return False
        refresh_tag = await self.redis.get(f"worker-queues:{self.role}:refresh_tag")
        if refresh_tag != self.refresh_tag:
            self.refresh_tag = refresh_tag
            return True
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        task_queues = await self.queues()
        # TODO: Switch from a list to zscore to sort by ULID
        # this lets us sort/query tasks in a queue by created_at, sortof
        task_id = await self.redis.bzpopmin(task_queues, timeout=0)
        if task_id:
            task = await self.state_manager.get_task(int(task_id[1]))
            if not task:
                logger.warning("Task with ID %s not found.", task_id)
                return await self.__anext__()
            return task
        raise StopAsyncIteration


async def task_consumer():
    """Consume tasks from the Redis list 'task-list'."""
    redis = await get_client()
    state_manager = StateManager(redis)
    role = os.environ("WORKER_ROLE", "default")
    task_generator = TaskGenerator(redis, state_manager, role)
    try:
        async for task in task_generator:
            task_status = await TaskProcessor(task, state_manager).process()
            if task_status == "completed" and task.has_callbacks():
                # Monitor for when fan-out becomes problematic
                for callback_task in task.generate_callbacks():
                    await state_manager.submit_task(callback_task)
    except asyncio.CancelledError:
        logger.info("Task consumer shutting down...")
    finally:
        await redis.close()

def run():
    try:
        asyncio.run(task_consumer())
    except KeyboardInterrupt:
        logger.info("Task consumer stopped.")
