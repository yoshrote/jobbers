import asyncio
import datetime as dt
import logging
from typing import TYPE_CHECKING, Any

from jobbers.models.task import Task
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.models.task_status import TaskStatus
from jobbers.registry import get_task_config
from jobbers.state_manager import StateManager

if TYPE_CHECKING:
    from collections.abc import Awaitable

logger = logging.getLogger(__name__)

class TaskProcessor:
    """TaskProcessor to process tasks from a TaskGenerator."""

    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager
        self._current_promise: Awaitable[Any] | None = None

    async def process(self, task: Task) -> Task:
        """Process the task and return the result."""
        logger.debug("Task %s details: %s", task.id, task)
        task.task_config = get_task_config(task.name, task.version)
        ex: BaseException | None = None

        if not task.task_config:
            self.handle_dropped_task(task)
        else:
            try:
                task = await self.mark_task_as_started(task)
                with self.state_manager.task_in_registry(task):
                    async with asyncio.timeout(task.task_config.timeout):
                        self._current_promise = task.task_config.function(**task.parameters)
                        if task.task_config.on_shutdown == TaskShutdownPolicy.CONTINUE:
                            self._current_promise = asyncio.shield(self._current_promise)
                        task.results = await self._current_promise
            except asyncio.TimeoutError:
                self.handle_timeout_exception(task)
            except asyncio.CancelledError as exc:
                ex = exc
                self.handle_cancelled_task(task)
            except Exception as exc:
                if task.task_config.expected_exceptions and isinstance(exc, task.task_config.expected_exceptions):
                    self.handle_expected_exception(task, exc)
                else:
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
        # callback_pool.map(self.state_manager.submit_task, task.generate_callbacks(), num_concurrent=5)


    def handle_dropped_task(self, task: Task) -> None:
        logger.error("Dropping unknown task %s v%s id=%s.", task.name, task.version, task.id)
        task.status = TaskStatus.DROPPED
        task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_cancelled_task(self, task: Task) -> None:
        logger.info("Task %s was cancelled.", task.id)
        task.shutdown()

    def handle_unexpected_exception(self, task: Task, exc: Exception) -> None:
        logger.exception("Exception occurred while processing task %s: %s", task.id, exc)
        task.status = TaskStatus.FAILED
        task.error = str(exc)

    def handle_expected_exception(self, task: Task, exc: Exception) -> None:
        logger.warning("Task %s failed with error: %s", task.id, exc)
        # TODO: Set metrics to track expected exceptions
        task.error = str(exc)
        if task.should_retry():
            # Task status will change to submitted when re-enqueued
            task.retry_attempt += 1
            task.status = TaskStatus.UNSUBMITTED
        else:
            task.status = TaskStatus.FAILED
            task.completed_at = dt.datetime.now(dt.timezone.utc)

    def handle_timeout_exception(self, task: Task) -> None:
        timeout = task.task_config.timeout
        logger.warning("Task %s timed out after %d seconds.", task.id, timeout)
        task.error = f"Task {task.id} timed out after {timeout} seconds"
        if task.should_retry():
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
