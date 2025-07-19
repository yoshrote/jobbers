import asyncio
import logging
import os
import signal

from asyncio_taskpool import TaskPool

from jobbers.models.task import Task
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

async def main(state_manager: StateManager, task_generator: TaskGenerator) -> None:
    num_concurrent = int(os.environ.get("WORKER_CONCURRENT_TASKS", 5))

    async def worker_factory(task: Task) -> None:
        await TaskProcessor(state_manager).process(task)

    pool = TaskPool()
    pool.map(worker_factory, task_generator, num_concurrent=num_concurrent)
    await pool.gather_and_close()

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
    state_manager: StateManager = build_sm()
    task_generator = build_task_generator(state_manager)
    main_coroutine = main(state_manager, task_generator)

    def graceful_shutdown(signum, frame) -> None:
        # turn off the faucet of tasks
        task_generator.stop()
        # propagates CancelledError to workers in task group
        main_coroutine.cancel()

    # Attempt an orderly shutdown generally
    signal.signal(signal.SIGTERM, graceful_shutdown)

    try:
        logger.info("Task consumer started.")
        asyncio.run(main_coroutine)
    except KeyboardInterrupt:
        logger.info("Task consumer stopped.")
