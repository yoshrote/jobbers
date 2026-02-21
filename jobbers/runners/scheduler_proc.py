"""
Scheduler runner: promotes due tasks from the TaskScheduler into their Redis queues.

Environment variables:
- SCHEDULER_POLL_INTERVAL: seconds to sleep between polls when no task is due (default 5.0)
- SCHEDULER_BATCH_SIZE: maximum number of tasks to acquire and dispatch per iteration (default 1)
- SCHEDULER_DB_PATH: path to the SQLite schedule database (default task_schedule.db)
- REDIS_URL: Redis connection URL (default redis://localhost:6379)
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING

from jobbers.db import get_state_manager

if TYPE_CHECKING:
    from jobbers.state_manager import StateManager

logger = logging.getLogger(__name__)


async def main(poll_interval: float, role: str, batch_size: int) -> None:
    state_manager: StateManager = get_state_manager()
    queues: list[str] = list(await state_manager.qca.get_queues(role))
    logger.info("Scheduler starting; poll_interval=%.1fs batch_size=%d", poll_interval, batch_size)
    while True:
        tasks = state_manager.task_scheduler.next_due_bulk(batch_size, queues=queues)
        if tasks:
            logger.info("Dispatching %d scheduled task(s)", len(tasks))
            await asyncio.gather(*(state_manager.dispatch_scheduled_task(t) for t in tasks))
        else:
            await asyncio.sleep(poll_interval)


def run() -> None:
    from jobbers.utils.otel import enable_otel

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]
    enable_otel(handlers, service_name="jobbers-scheduler")
    logging.basicConfig(level=logging.INFO, handlers=handlers)

    poll_interval = float(os.environ.get("SCHEDULER_POLL_INTERVAL", "5.0"))
    batch_size = int(os.environ.get("SCHEDULER_BATCH_SIZE", "1"))
    role = os.environ.get("SCHEDULER_ROLE", "default")

    asyncio.run(main(poll_interval, role, batch_size))
