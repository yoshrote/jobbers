from __future__ import annotations

import asyncio
import importlib
import importlib.util
import logging
import os
import sys
from typing import TYPE_CHECKING

from jobbers import db
from jobbers.task_generator import TaskGenerator
from jobbers.task_processor import TaskProcessor

if TYPE_CHECKING:
    from jobbers.models.task import Task

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
    state_manager = await db.init_state_manager()
    task_generator = TaskGenerator(state_manager, state_manager.qca, role, max_tasks=worker_ttl)
    await task_generator.queues() # warm up the refresh tag once

    semaphore = asyncio.Semaphore(num_concurrent)
    active: set[asyncio.Task[None]] = set()

    async def run_task(task: Task) -> None:
        logger.debug("Running task: %s[%sv%s]", task.id, task.name, task.version)
        try:
            await TaskProcessor(state_manager).run(task)
        finally:
            semaphore.release()

    try:
        while True:
            await semaphore.acquire()
            try:
                task = await anext(task_generator)
            except StopAsyncIteration:
                semaphore.release()
                break
            t = asyncio.create_task(run_task(task))
            active.add(t)
            t.add_done_callback(active.discard)
        await asyncio.gather(*active, return_exceptions=True)
    finally:
        logger.info("Worker shutting down")
        task_generator.stop()
        for t in active:
            t.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)

def _load_task_module(arg: str) -> None:
    if os.path.isabs(arg) or arg.endswith(".py"):
        spec = importlib.util.spec_from_file_location("_user_tasks", arg)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load task module from path: {arg}")
        module = importlib.util.module_from_spec(spec)
        sys.modules["_user_tasks"] = module
        spec.loader.exec_module(module)
    else:
        importlib.import_module(arg)

def run() -> None:
    from jobbers.utils.otel import enable_otel

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]
    enable_otel(handlers, service_name="jobbers-worker")
    logging.basicConfig(level=logging.DEBUG, handlers=handlers)
    # logging.getLogger("jobbers").setLevel(logging.DEBUG)

    # register tasks
    _load_task_module(sys.argv[1])

    asyncio.run(main(), debug=True)
