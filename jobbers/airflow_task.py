"""
Built-in Jobbers task that executes an Apache Airflow task instance.

Workers that need to serve as Airflow executor backends should load this module
so that the ``airflow_task_runner`` task is registered:

    jobbers_worker jobbers.airflow_task

The ``airflow_task_runner`` task receives a CLI ``command`` (the same list of
strings that Airflow's CeleryExecutor would pass to a Celery worker) and runs
it as a subprocess.  It sends periodic heartbeats back to Jobbers while the
subprocess is running so the cleaner does not mark the task as stalled.
"""

import asyncio
import datetime as dt
import logging
from asyncio.subprocess import PIPE
from typing import Any

from jobbers.models.task_config import DeadLetterPolicy
from jobbers.registry import register_task

logger = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL = 30  # seconds between heartbeat updates


@register_task(
    name="airflow_task_runner",
    version=1,
    timeout=None,            # Airflow manages its own task timeouts
    max_retries=0,           # Airflow manages retries; we never retry here
    max_heartbeat_interval=dt.timedelta(seconds=90),
    dead_letter_policy=DeadLetterPolicy.SAVE,
)
async def run_airflow_task(command: list[str], jobbers_task_id: str) -> dict[str, Any]:
    """
    Execute an Airflow task instance by running its CLI command as a subprocess.

    Args:
        command: The Airflow CLI command to run, e.g.
            ``["airflow", "tasks", "run", dag_id, task_id, run_id]``.
        jobbers_task_id: The ULID string of this Jobbers task instance, used to
            send heartbeats while the subprocess is running.

    Returns:
        A dict with ``returncode`` and ``stdout`` from the subprocess.

    Raises:
        RuntimeError: If the subprocess exits with a non-zero return code.
    """
    from ulid import ULID

    from jobbers.db import get_client
    from jobbers.models.task import TaskAdapter

    task_id = ULID.from_str(jobbers_task_id)
    adapter = TaskAdapter(get_client())

    logger.info("Running Airflow command: %s", command)
    proc = await asyncio.create_subprocess_exec(*command, stdout=PIPE, stderr=PIPE)

    async def _heartbeat_loop() -> None:
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            task = await adapter.get_task(task_id)
            if task:
                await task.heartbeat()

    heartbeat_task = asyncio.create_task(_heartbeat_loop())
    try:
        stdout, stderr = await proc.communicate()
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

    if proc.returncode != 0:
        stderr_text = stderr.decode(errors="replace")
        logger.error("Airflow task failed (returncode=%s): %s", proc.returncode, stderr_text)
        raise RuntimeError(stderr_text)

    return {
        "returncode": proc.returncode,
        "stdout": stdout.decode(errors="replace"),
    }
