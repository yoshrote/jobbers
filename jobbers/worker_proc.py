import asyncio
import logging
import os

from jobbers.state_manager import StateManager, build_sm
from jobbers.task_generator import TaskGenerator, build_task_generator
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


def run():
    import sys

    from jobbers.otel import enable_otel

    handlers = [logging.StreamHandler(stream=sys.stdout)]
    enable_otel(handlers, service_name="jobbers-worker")
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    logger = logging.getLogger(__name__)
    logger.info("\n\n\n")

    num_concurrent = int(os.environ.get("WORKER_CONCURRENT_TASKS", 5))
    # queue = asyncio.Queue(maxsize=100)
    state_manager: StateManager = build_sm()
    task_generator = build_task_generator(state_manager)

    async def worker_factory(tg: TaskGenerator):
        async for task in tg:
            await TaskProcessor(state_manager).process(task)

    workers = [worker_factory(task_generator) for _ in range(num_concurrent)]
    try:
        logger.info("Task consumer started.")
        asyncio.gather(*workers)
    except KeyboardInterrupt:
        logger.info("Task consumer stopped.")
