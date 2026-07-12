from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import importlib.util
import logging
import os
import signal
import sys
from typing import TYPE_CHECKING

from jobbers import db
from jobbers.adapters.static import StaticRoutingBackend
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.task_generator import TaskGenerator
from jobbers.task_processor import TaskProcessor
from jobbers.utils.otel import enable_otel, shutdown_otel

if TYPE_CHECKING:
    from jobbers.models.task import Task
    from jobbers.state_manager import StateManager

logger = logging.getLogger(__name__)
"""
Important environment variables:
- WORKER_ROLE: Role of the worker (default is "default")
- WORKER_TTL: Time to live for the worker (in seconds) (default is 50)
- WORKER_CONCURRENT_TASKS: Maximum number of concurrent tasks to process (default is 5)

Rate limiting should be implemented by limiting the creation of tasks rather
than on the consumption of tasks.
"""


async def _run_cancel_listener_supervised(state_manager: StateManager) -> None:
    """Run the cancellation listener, restarting it (with a short backoff) if it dies unexpectedly."""
    while True:
        try:
            await state_manager.run_cancel_listener()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Cancellation listener crashed; restarting in 1s. "
                "Task cancellation was unavailable in the meantime."
            )
            await asyncio.sleep(1)


async def main() -> None:
    num_concurrent = int(os.environ.get("WORKER_CONCURRENT_TASKS", 5))
    role = os.environ.get("WORKER_ROLE", "default")
    worker_ttl = int(os.environ.get("WORKER_TTL", 50))  # if 0, will run indefinitely
    state_manager = await db.init_state_manager()
    task_generator = TaskGenerator(state_manager, role, max_tasks=worker_ttl)
    await task_generator.queues()  # warm up the refresh tag once

    semaphore = asyncio.Semaphore(num_concurrent)
    active: dict[asyncio.Task[None], Task] = {}
    shutdown_event = asyncio.Event()
    fetch_task: asyncio.Task[Task] | None = None

    def _request_shutdown() -> None:
        if shutdown_event.is_set():
            return
        logger.info("Shutdown signal received; draining in-flight tasks.")
        shutdown_event.set()
        # Interrupt a blocking queue pop immediately rather than waiting for the
        # next loop iteration to notice the event.
        if fetch_task is not None and not fetch_task.done():
            fetch_task.cancel()

    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
            registered_signals.append(sig)
        except NotImplementedError:
            # add_signal_handler is POSIX-only (e.g. unsupported on Windows);
            # graceful shutdown is best-effort there.
            logger.warning("Signal handling for %s is not supported on this platform.", sig.name)

    async def run_task(task: Task) -> None:
        logger.debug("Running task: %s[%sv%s]", task.id, task.name, task.version)
        try:
            await TaskProcessor(state_manager).run(task)
        finally:
            semaphore.release()

    def _on_task_done(done_task: asyncio.Task[None]) -> None:
        active.pop(done_task, None)

    cancel_listener = asyncio.create_task(_run_cancel_listener_supervised(state_manager))
    try:
        while not shutdown_event.is_set():
            await semaphore.acquire()
            if shutdown_event.is_set():
                semaphore.release()
                break
            fetch_task = asyncio.ensure_future(anext(task_generator))
            try:
                task = await fetch_task
            except (StopAsyncIteration, asyncio.CancelledError):
                semaphore.release()
                break
            finally:
                fetch_task = None
            t = asyncio.create_task(run_task(task))
            active[t] = task
            t.add_done_callback(_on_task_done)
        if active and not shutdown_event.is_set():
            await asyncio.gather(*active, return_exceptions=True)
    finally:
        logger.info("Worker shutting down")
        for sig in registered_signals:
            loop.remove_signal_handler(sig)
        cancel_listener.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cancel_listener
        task_generator.stop()
        for t, running_task in active.items():
            policy = (
                running_task.task_config.on_shutdown
                if running_task.task_config is not None
                else TaskShutdownPolicy.STOP
            )
            if policy == TaskShutdownPolicy.CONTINUE:
                logger.info("Task %s has on_shutdown=continue; letting it finish.", running_task.id)
            else:
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

    _load_task_module(args.task_module)

    try:
        asyncio.run(main(), debug=True)
    finally:
        shutdown_otel()
