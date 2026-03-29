"""
Scheduler runner: promotes due tasks from the TaskScheduler into their Redis queues.

Environment variables:
- SCHEDULER_POLL_INTERVAL: seconds to sleep between polls when no task is due (default 5.0)
- SCHEDULER_BATCH_SIZE: maximum number of tasks to acquire and dispatch per iteration (default 1)
- REDIS_URL: Redis connection URL (default redis://localhost:6379)
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import sys
from typing import TYPE_CHECKING

from opentelemetry import metrics

from jobbers import db

if TYPE_CHECKING:
    from jobbers.state_manager import StateManager

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
dispatch_latency = meter.create_histogram(
    "scheduled_task_dispatch_latency_seconds",
    unit="s",
    description="Seconds between a task's scheduled run_at and when it is dispatched to its queue.",
)


async def main(poll_interval: float, role: str, batch_size: int) -> None:
    state_manager: StateManager = await db.init_state_manager()
    queues: list[str] = list(await state_manager.qca.get_queues(role))
    logger.info("Scheduler starting; poll_interval=%.1fs batch_size=%d", poll_interval, batch_size)
    while True:
        task_entries, cron_entries = await asyncio.gather(
            state_manager.task_scheduler.next_due_bulk(batch_size, queues=queues),
            state_manager.cron_dag_scheduler.next_due_bulk(batch_size),
        )

        now = dt.datetime.now(dt.UTC)
        work_done = False

        if task_entries:
            work_done = True
            logger.info("Dispatching %d scheduled task(s)", len(task_entries))
            await asyncio.gather(*(state_manager.dispatch_scheduled_task(task) for task, _ in task_entries))
            for task, run_at in task_entries:
                dispatch_latency.record(
                    (now - run_at).total_seconds(),
                    {"queue": task.queue, "task_name": task.name},
                )

        if cron_entries:
            work_done = True
            enabled = [(entry, run_at) for entry, run_at in cron_entries if entry.enabled]
            if enabled:
                logger.info("Dispatching %d cron DAG(s)", len(enabled))
                await asyncio.gather(
                    *(state_manager.dispatch_cron_dag(entry, run_at) for entry, run_at in enabled)
                )
                for entry, run_at in enabled:
                    dispatch_latency.record(
                        (now - run_at).total_seconds(),
                        {"queue": entry.dag_spec.queue, "task_name": entry.dag_spec.name},
                    )
            # Disabled entries were already removed from cron-schedule by next_due_bulk;
            # reschedule them so they aren't lost.
            disabled = [(entry, run_at) for entry, run_at in cron_entries if not entry.enabled]
            if disabled:
                from croniter import croniter

                pipe = state_manager.job_store.pipeline(transaction=True)
                for entry, run_at in disabled:
                    next_run_at = croniter(entry.cron_expr, run_at).get_next(dt.datetime)
                    state_manager.cron_dag_scheduler.stage_reschedule(pipe, entry.id, next_run_at)
                await pipe.execute()

        if not work_done:
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
