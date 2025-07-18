import asyncio
import logging
import os

from jobbers.state_manager import StateManager, build_sm
from jobbers.task_generator import TaskGenerator
from jobbers.task_processor import TaskProcessor

logger = logging.getLogger(__name__)
"""
Important environment variables:
- WORKER_ROLE: Role of the worker (default is "default")
- WORKER_TTL: Time to live for the worker (in seconds) (default is 50)
- WORKER_CONCURRENT_TASKS: Maximum number of concurrent tasks to process (default is 5)

Rate limiting should be implemented by limiting the creation of tasks rather
than on the consumption of tasks.
"""

def build_task_generator(state_manager: StateManager) -> TaskGenerator:
    """Consume tasks from the Redis list 'task-list'."""
    role = os.environ.get("WORKER_ROLE", "default")
    worker_ttl = int(os.environ.get("WORKER_TTL", 50)) # if 0, will run indefinitely

    return TaskGenerator(state_manager, state_manager.qca, role, max_tasks=worker_ttl)

async def main() -> None:
    num_concurrent = int(os.environ.get("WORKER_CONCURRENT_TASKS", 5))
    state_manager: StateManager = build_sm()
    task_generator = build_task_generator(state_manager)

    async def worker_factory(tg: TaskGenerator) -> None:
        async for task in tg:
            await TaskProcessor(state_manager).process(task)

    async with asyncio.TaskGroup() as group:
        for _ in range(num_concurrent):
            group.create_task(worker_factory(task_generator))

def run() -> None:
    import importlib
    import sys

    from jobbers.utils.otel import enable_otel


    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]
    enable_otel(handlers, service_name="jobbers-worker")
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    # register tasks
    task_module = sys.argv[1]
    importlib.import_module(task_module)

    logger = logging.getLogger(__name__)
    try:
        logger.info("Task consumer started.")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Task consumer stopped.")
