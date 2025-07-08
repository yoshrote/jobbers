import asyncio
import datetime as dt
import logging

from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.models.task_status import TaskStatus
from jobbers.registry import get_task_config
from jobbers.state_manager import StateManager

logger = logging.getLogger(__name__)

class TaskProcessor:
    """TaskProcessor to process tasks from a TaskGenerator."""

    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager

    async def process(self, task: Task) -> Task:
        """Process the task and return the result."""
        logger.debug("Task %s details: %s", task.id, task)
        task_config = get_task_config(task.name, task.version)
        if not task_config:
            self.handle_dropped_task(task)
        else:
            ex: BaseException | None = None
            try:
                task = await self.mark_task_as_started(task)
                with self.state_manager.task_in_registry(task):
                    async with asyncio.timeout(task_config.timeout):
                        task.results = await task_config.function(**task.parameters)
            except asyncio.TimeoutError as exc:
                # ex = exc
                self.handle_timeout_exception(task, task_config)
            except asyncio.CancelledError as exc:
                ex = exc
                self.handle_cancelled_task(task)
            except Exception as exc:
                if task_config.expected_exceptions and isinstance(exc, task_config.expected_exceptions):
                    self.handle_expected_exception(task, task_config, exc)
                else:
                    ex = exc
                    self.handle_unexpected_exception(task, exc)
            else:
                self.handle_success(task)

        await self.state_manager.submit_task(task)

        if task.status == TaskStatus.COMPLETED:
            await self.post_process(task)
        elif ex is not None:
            raise ex

        return task

    async def mark_task_as_started(self, task: Task) -> Task:
        task.started_at = dt.datetime.now(dt.timezone.utc)
        task.status = TaskStatus.STARTED
        await self.state_manager.submit_task(task)
        return task

    async def post_process(self, task: Task) -> None:
        if not task.has_callbacks():
            return

        # TODO: configure max concurrent callbacks
        # Monitor for when fan-out becomes problematic
        # callback_pool = TaskPool()
        # callback_pool.map(self.state_manager.submit_task, task.generate_callbacks(), num_concurrent=5)
        # await callback_pool.gather_and_close()

    def handle_dropped_task(self, task: Task) -> None:
        logger.error("Dropping unknown task %s v%s id=%s.", task.name, task.version, task.id)
        task.status = TaskStatus.DROPPED
        task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_cancelled_task(self, task: Task) -> None:
        logger.info("Task %s was cancelled.", task.id)
        task.status = TaskStatus.CANCELLED
        task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_unexpected_exception(self, task: Task, exc: Exception) -> None:
        logger.exception("Exception occurred while processing task %s: %s", task.id, exc)
        task.status = TaskStatus.FAILED
        task.error = str(exc)

    def handle_expected_exception(self, task: Task, task_config: TaskConfig, exc: Exception) -> None:
        logger.warning("Task %s failed with error: %s", task.id, exc)
        # TODO: Set metrics to track expected exceptions
        task.error = str(exc)
        if task.should_retry(task_config):
            # Task status will change to submitted when re-enqueued
            task.retry_attempt += 1
            task.status = TaskStatus.UNSUBMITTED
        else:
            task.status = TaskStatus.FAILED
            task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_timeout_exception(self, task: Task, task_config: TaskConfig) -> None:
        logger.warning("Task %s timed out after %d seconds.", task.id, task_config.timeout)
        task.error = f"Task {task.id} timed out after {task_config.timeout} seconds"
        if task.should_retry(task_config):
            # Task status will change to submitted when re-enqueued
            task.status = TaskStatus.UNSUBMITTED
            task.retry_attempt += 1
        else:
            task.status = TaskStatus.FAILED
            task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_success(self, task: Task) -> None:
        logger.info("Task %s completed.", task.id)
        task.status = TaskStatus.COMPLETED
        task.completed_at = dt.datetime.now(dt.timezone.utc)
