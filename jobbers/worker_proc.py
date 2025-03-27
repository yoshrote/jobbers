import asyncio
import datetime as dt
import logging
import os

from jobbers.db import get_client
from jobbers.registry import get_task_function
from jobbers.state_manager import StateManager, Task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_task(task: Task, state_manager: StateManager):
    """Process a task given its ID."""
    # Simulate task processing
    logger.info("Task %d details: %s", task.id, task)
    task_function = get_task_function(task.name, task.version)
    if not task_function:
        logger.warning("Dropping unknown task %s id=%d.", task.name, task.id)
        return
    try:
        task.result = await task_function(**task.parameters)
    except Exception as exc:
        logger.exception("Task %d failed with error: %s", task.id, exc)
        task.status = "failed"
        task.error = str(exc)
    # except TimeoutError:
    #     logger.exception("Task %d timed out.", task.id)

    logger.info("Task %d completed.", task.id)
    task.status = "completed"
    task.completed_at = dt.datetime.now(dt.timezone.utc)
    await state_manager.submit_task(task)

# move TaskGenerator to a separate file
class TaskGenerator:
    """Generates tasks from the Redis list 'task-list'."""

    DEFAULT_QUEUES = ["task-list:default"]

    def __init__(self, redis, state_manager, role="default"):
        self.redis = redis
        self.role = role
        self.state_manager = state_manager
        self.task_queues = None
        self.hup_tag = None

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
        if not self.hup_tag:
            return False
        current_hup = await self.redis.get("worker-queues:hup")
        if current_hup != self.hup_tag:
            self.hup_tag = current_hup
            return True
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        task_queues = await self.queues()
        task_id = await self.redis.brpop(task_queues, timeout=0)
        if task_id:
            task = await self.state_manager.get_task(int(task_id[1]))
            if not task:
                logger.warning("Task with ID %d not found.", task_id)
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
            await process_task(task, state_manager)
    except asyncio.CancelledError:
        logger.info("Task consumer shutting down...")
    finally:
        await redis.close()

def run():
    try:
        asyncio.run(task_consumer())
    except KeyboardInterrupt:
        logger.info("Task consumer stopped.")

if __name__ == "__main__":
    run()
