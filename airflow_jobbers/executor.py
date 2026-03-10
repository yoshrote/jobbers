"""Airflow executor that runs tasks via the Jobbers task execution framework.

Configure Airflow to use this executor by setting in ``airflow.cfg``::

    [core]
    executor = airflow_jobbers.executor.JobbersExecutor

Environment variables
---------------------
JOBBERS_MANAGER_URL
    Base URL of the Jobbers Manager API (default: ``http://localhost:8000``).
JOBBERS_QUEUE
    Name of the Jobbers queue to submit Airflow tasks to (default: ``airflow``).
    Individual DAG tasks can override this via their ``queue`` attribute.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

import httpx
from ulid import ULID

from airflow.executors.base_executor import BaseExecutor

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey

logger = logging.getLogger(__name__)

_AIRFLOW_TASK_NAME = "airflow_task_runner"
_AIRFLOW_TASK_VERSION = 1

# Jobbers status values that map to Airflow terminal states
_SUCCESS_STATUSES = frozenset({"completed"})
_FAILURE_STATUSES = frozenset({"failed", "stalled", "dropped", "cancelled"})


class JobbersExecutor(BaseExecutor):
    """Airflow executor that submits tasks to the Jobbers task execution framework.

    Each Airflow task instance is submitted to the Jobbers Manager API as an
    ``airflow_task_runner`` task.  Jobbers workers run the task by executing
    the Airflow CLI command supplied by the scheduler.

    The Jobbers worker must have ``jobbers.airflow_task`` loaded, e.g.::

        jobbers_worker jobbers.airflow_task
    """

    def __init__(self, parallelism: int = 0) -> None:
        super().__init__(parallelism=parallelism)
        self._manager_url: str = os.environ.get("JOBBERS_MANAGER_URL", "http://localhost:8000")
        self._queue: str = os.environ.get("JOBBERS_QUEUE", "airflow")
        # Maps Airflow TaskInstanceKey → Jobbers task_id (ULID string)
        self._key_to_task_id: dict[TaskInstanceKey, str] = {}

    def start(self) -> None:
        logger.info(
            "JobbersExecutor started (manager=%s, queue=%s)",
            self._manager_url,
            self._queue,
        )

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: list[str],
        queue: str | None = None,
        executor_config: Any = None,
    ) -> None:
        """Submit an Airflow task instance to Jobbers.

        The *command* is the Airflow CLI invocation the scheduler wants to run,
        e.g. ``["airflow", "tasks", "run", dag_id, task_id, run_id]``.
        """
        task_id = str(ULID())
        target_queue = queue or self._queue
        payload = {
            "id": task_id,
            "name": _AIRFLOW_TASK_NAME,
            "version": _AIRFLOW_TASK_VERSION,
            "queue": target_queue,
            "parameters": {
                "command": command,
                "jobbers_task_id": task_id,
            },
        }
        try:
            with httpx.Client() as client:
                resp = client.post(f"{self._manager_url}/submit-task", json=payload)
                resp.raise_for_status()
            self._key_to_task_id[key] = task_id
            logger.info("Submitted Airflow task %s as Jobbers task %s", key, task_id)
        except httpx.HTTPError:
            logger.exception("Failed to submit task %s to Jobbers", key)
            self.fail(key)

    def sync(self) -> None:
        """Poll Jobbers for task status and update Airflow task states."""
        if not self._key_to_task_id:
            return

        task_ids = list(self._key_to_task_id.values())
        try:
            with httpx.Client() as client:
                resp = client.post(
                    f"{self._manager_url}/task-status/bulk",
                    json={"task_ids": task_ids},
                )
                resp.raise_for_status()
                statuses: dict[str, str] = resp.json()["statuses"]
        except httpx.HTTPError:
            logger.exception("Failed to poll task statuses from Jobbers")
            return

        for key, task_id in list(self._key_to_task_id.items()):
            status = statuses.get(task_id)
            if status in _SUCCESS_STATUSES:
                self.success(key)
                del self._key_to_task_id[key]
            elif status in _FAILURE_STATUSES:
                self.fail(key)
                del self._key_to_task_id[key]

    def end(self) -> None:
        logger.info("JobbersExecutor shutting down")

    def terminate(self) -> None:
        logger.info("JobbersExecutor terminated")
