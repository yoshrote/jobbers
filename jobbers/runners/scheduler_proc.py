"""
Scheduler runner: promotes due tasks from the TaskScheduler into their Redis queues.

Environment variables:
- SCHEDULER_POLL_INTERVAL: seconds to sleep between polls when no task is due (default 5.0)
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


async def main(poll_interval: float, role: str) -> None:
    state_manager: StateManager = get_state_manager()
    queues: set[str] = await state_manager.qca.get_queues(role)
    logger.info("Scheduler starting; poll_interval=%.1fs", poll_interval)
    while True:
        task = state_manager.task_scheduler.next_due(queues=queues)
        if task is not None:
            logger.info("Dispatching scheduled task %s (%s v%s)", task.id, task.name, task.version)
            await state_manager.dispatch_scheduled_task(task)
        else:
            await asyncio.sleep(poll_interval)


def run() -> None:
    from jobbers.utils.otel import enable_otel  # noqa: PLC0415

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]
    enable_otel(handlers, service_name="jobbers-scheduler")
    logging.basicConfig(level=logging.INFO, handlers=handlers)

    poll_interval = float(os.environ.get("SCHEDULER_POLL_INTERVAL", "5.0"))
    role = os.environ.get("SCHEDULER_ROLE", "default")

    asyncio.run(main(poll_interval, role))
