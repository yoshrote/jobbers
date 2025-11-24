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

async def main() -> None:
    num_concurrent = int(os.environ.get("WORKER_CONCURRENT_TASKS", 5))
    role = os.environ.get("WORKER_ROLE", "default")
    worker_ttl = int(os.environ.get("WORKER_TTL", 50)) # if 0, will run indefinitely

    state_manager: StateManager = build_sm()
    task_generator = TaskGenerator(state_manager, state_manager.qca, role, max_tasks=worker_ttl)
    await task_generator.queues() # warm up the refresh tag once for all workers

    async def worker(name: str) -> None:
        logger.info("%s starting", name)
        async for task in task_generator:
            logger.debug("%s running task: %s[%sv%s]", name, task.id, task.name, task.version)
            await TaskProcessor(state_manager).process(task)
        logger.info("%s stopping", name)

    workers = [asyncio.create_task(worker(f"jobber-worker-{i}"), name=f"jobber-worker-{i}") for i in range(num_concurrent)]
    try:
        await asyncio.gather(*workers)
    finally:
        logger.info("Worker tasks cancelled; shutting down")
        task_generator.stop()
        for w in workers:
            w.cancel()

def run() -> None:
    import importlib
    import sys

    from jobbers.utils.otel import enable_otel

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]
    enable_otel(handlers, service_name="jobbers-worker")
    logging.basicConfig(level=logging.DEBUG, handlers=handlers)
    # logging.getLogger("jobbers").setLevel(logging.DEBUG)

    # register tasks
    task_module = sys.argv[1]
    importlib.import_module(task_module)

    asyncio.run(main(), debug=True)
