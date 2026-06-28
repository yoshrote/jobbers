from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys
from typing import TYPE_CHECKING

from jobbers import db
from jobbers.adapters.static import StaticRoutingBackend
from jobbers.registry import get_task_config
from jobbers.task_generator import TaskGenerator
from jobbers.task_processor import TaskProcessor
from jobbers.utils.concurrency import has_capacity
from jobbers.utils.module_loading import load_task_module
from jobbers.utils.otel import enable_otel

if TYPE_CHECKING:
    from jobbers.models.task import Task

logger = logging.getLogger(__name__)
"""
Important environment variables:
- WORKER_ROLE: Role of the worker (default is "default")
- WORKER_TTL: Time to live for the worker (in seconds) (default is 50)
- WORKER_CONCURRENT_TASKS: Maximum number of concurrent tasks to process (default is 5)
- WORKER_SYNC_PROCESSES: Maximum number of concurrent sync-task subprocesses (default is 2)

Rate limiting should be implemented by limiting the creation of tasks rather
than on the consumption of tasks.
"""

# Mirrors task_generator.py's _CAPACITY_BACKOFF_SECS: how long to wait before retrying a
# dequeue after pulling a sync task with no free subprocess slot.
_SYNC_CAPACITY_BACKOFF_SECS = 1.0


async def main(task_module: str) -> None:
    num_concurrent = int(os.environ.get("WORKER_CONCURRENT_TASKS", 5))
    num_sync_concurrent = int(os.environ.get("WORKER_SYNC_PROCESSES", 2))
    role = os.environ.get("WORKER_ROLE", "default")
    worker_ttl = int(os.environ.get("WORKER_TTL", 50))  # if 0, will run indefinitely
    state_manager = await db.init_state_manager()
    task_generator = TaskGenerator(state_manager, role, max_tasks=worker_ttl)
    await task_generator.queues()  # warm up the refresh tag once

    semaphore = asyncio.Semaphore(num_concurrent)
    sync_semaphore = asyncio.Semaphore(num_sync_concurrent)
    active: set[asyncio.Task[None]] = set()

    async def run_task(task: Task) -> None:
        logger.debug("Running task: %s[%sv%s]", task.id, task.name, task.version)
        try:
            processor = TaskProcessor(state_manager, task_module=task_module, sync_semaphore=sync_semaphore)
            await processor.run(task)
        finally:
            semaphore.release()

    cancel_listener = asyncio.create_task(state_manager.run_cancel_listener())
    try:
        while True:
            await semaphore.acquire()
            try:
                task = await anext(task_generator)
            except StopAsyncIteration:
                semaphore.release()
                break

            task_config = get_task_config(task.name, task.version)
            if task_config is not None and task_config.is_sync and not has_capacity(sync_semaphore):
                # No sync subprocess slot free right now — push it straight back rather than
                # marking it STARTED and holding this concurrency slot for the whole wait.
                await state_manager.requeue_task(task)
                semaphore.release()
                await asyncio.sleep(_SYNC_CAPACITY_BACKOFF_SECS)
                continue

            t = asyncio.create_task(run_task(task))
            active.add(t)
            t.add_done_callback(active.discard)
        await asyncio.gather(*active, return_exceptions=True)
    finally:
        logger.info("Worker shutting down")
        cancel_listener.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cancel_listener
        task_generator.stop()
        for t in active:
            t.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)


def run() -> None:
    parser = argparse.ArgumentParser(description="Jobbers Worker")
    parser.add_argument("task_module", help="Task module to load (dotted name or file path)")
    parser.add_argument(
        "--static-config",
        metavar="FILE",
        default=None,
        help="Path to a JSON/YAML static routing config file. Implies ROUTING_BACKEND=static.",
    )
    args = parser.parse_args()

    if args.static_config:
        db.register_routing_backend(StaticRoutingBackend.from_file(args.static_config))

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]
    enable_otel(handlers, service_name="jobbers-worker")
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    load_task_module(args.task_module)

    asyncio.run(main(args.task_module), debug=True)
