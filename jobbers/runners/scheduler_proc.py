"""
Scheduler runner: promotes due tasks from the TaskScheduler into their Redis queues.

Environment variables:
- SCHEDULER_POLL_INTERVAL: seconds to sleep between polls when no task is due (default 5.0)
- SCHEDULER_BATCH_SIZE: maximum number of tasks to acquire and dispatch per iteration (default 1)
- REDIS_URL: Redis connection URL (default redis://localhost:6379)
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import os
import sys
from typing import TYPE_CHECKING

from croniter import croniter
from opentelemetry import metrics

from jobbers import db
from jobbers.adapters.static import StaticRoutingBackend
from jobbers.utils.otel import enable_otel

if TYPE_CHECKING:
    from jobbers.state_manager import StateManager

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
dispatch_latency = meter.create_histogram(
    "scheduled_task_dispatch_latency_seconds",
    unit="s",
    description="Seconds between a task's scheduled run_at and when it is dispatched to its queue.",
)


async def main(poll_interval: float, config_interval: dt.timedelta, role: str, batch_size: int) -> None:
    state_manager: StateManager = await db.init_state_manager()
    logger.info("Scheduler starting; poll_interval=%.1fs batch_size=%d", poll_interval, batch_size)
    queues: list[str] = []
    queues_fetched_at: dt.datetime = dt.datetime.min.replace(tzinfo=dt.UTC)
    while True:
        now = dt.datetime.now(dt.UTC)
        if now - queues_fetched_at >= config_interval:
            queues = list(await state_manager.get_queues(role))
            queues_fetched_at = now
        task_entries, cron_entries = await asyncio.gather(
            state_manager.task_scheduler.next_due_bulk(batch_size, queues=queues if queues else None),
            state_manager.cron_dag_scheduler.next_due_bulk(batch_size),
        )

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
                await asyncio.gather(*(
                    state_manager.cron_dag_scheduler.reschedule(
                        entry.id, croniter(entry.cron_expr, run_at).get_next(dt.datetime)
                    )
                    for entry, run_at in disabled
                ))

        if not work_done:
            await asyncio.sleep(poll_interval)


def run() -> None:
    parser = argparse.ArgumentParser(description="Jobbers Scheduler")
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
    enable_otel(handlers, service_name="jobbers-scheduler")
    logging.basicConfig(level=logging.INFO, handlers=handlers)

    poll_interval = float(os.environ.get("SCHEDULER_POLL_INTERVAL", "5.0"))
    batch_size = int(os.environ.get("SCHEDULER_BATCH_SIZE", "1"))
    role = os.environ.get("SCHEDULER_ROLE", "default")
    config_interval = dt.timedelta(minutes=int(os.environ.get("SCHEDULER_CONFIG_REFRESH_INTERVAL", "3")))

    asyncio.run(main(poll_interval, config_interval, role, batch_size))
