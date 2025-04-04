import asyncio
import datetime as dt
import logging
import os

from jobbers.db import get_client
from jobbers.state_manager import StateManager, Task, TaskConfig, TaskStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_task(task: Task, state_manager: StateManager):
    """Process a task given its ID."""
    # Simulate task processing
    logger.info("Task %s details: %s", task.id, task)
    task_config: TaskConfig = state_manager.get_task_config(task.name, task.version)
    should_retry = False
    if not task_config or not task_config.task_function:
        logger.warning("Dropping unknown task %s id=%s.", task.name, task.id)
        return
    try:
        async with asyncio.timeout(task_config.timeout):
            task.result = await task_config.task_function(**task.parameters)
    except task.expected_exceptions as exc:
        logger.exception("Task %s failed with error: %s", task.id, exc)
        if task.should_retry():
            task.status = TaskStatus.RETRIED
            task.error = str(exc)
        else:
            task.status = TaskStatus.FAILED
            task.retry_attempt += 1
            task.error = str(exc)
            task.completed_at = dt.datetime.now(dt.timezone.utc)
    except TimeoutError:
        logger.warning("Task %s timed out after %d seconds.", task.id, task_config.timeout)
        task.error = "Task timed out"
        if task.should_retry():
            # Task status will change to submitted when re-enqueued
            task.retry_attempt += 1
            should_retry = True
        else:
            task.status = TaskStatus.FAILED
            task.completed_at = dt.datetime.now(dt.timezone.utc)
    except asyncio.CancelledError:
        logger.info("Task %s was cancelled.", task.id)
        task.status = TaskStatus.CANCELLED
        task.completed_at = dt.datetime.now(dt.timezone.utc)
    except Exception as exc:
        logger.exception("Task %s failed with unexpected error: %s", task.id, exc)
        task.status = TaskStatus.FAILED
        task.error = str(exc)
    else:
        logger.info("Task %s completed.", task.id)
        task.status = TaskStatus.COMPLETED
        task.completed_at = dt.datetime.now(dt.timezone.utc)

    await state_manager.submit_task(
        task,
        force_reenqueue=should_retry,
    )

    return task.status

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
        task_id = await self.redis.brpop(task_queues, timeout=0)
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
            task_status = await process_task(task, state_manager)
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
