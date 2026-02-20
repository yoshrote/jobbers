import asyncio
import logging
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics

from jobbers.models.task import Task
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.models.task_status import TaskStatus
from jobbers.registry import get_task_config
from jobbers.state_manager import StateManager, UserCancellationError

if TYPE_CHECKING:
    from collections.abc import Awaitable


logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
tasks_processed = meter.create_counter("tasks_processed", unit="1")
tasks_retried = meter.create_counter("tasks_retried", unit="1")
execution_time = meter.create_histogram("task_execution_time", unit="ms")
end_to_end_latency = meter.create_histogram("task_end_to_end_latency", unit="ms")

class TaskProcessor:
    """TaskProcessor to process tasks from a TaskGenerator."""

    def __init__(self, state_manager: StateManager) -> None:
        self.state_manager = state_manager
        self._current_promise: Awaitable[Any] | None = None

    async def run(self, task: Task) -> None:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.process(task))
                tg.create_task(self.monitor_task_cancellation(task))
        except UserCancellationError:
            pass

    async def process(self, task: Task) -> Task:
        """Process the task and return the result."""
        logger.debug("Task %s details: %s", task.id, task)
        task.task_config = get_task_config(task.name, task.version)
        ex: BaseException | None = None

        if task.task_config is None:
            await self.handle_dropped_task(task)
        else:
            self.mark_task_as_started(task)
            await self.state_manager.save_task(task)

            with self.state_manager.task_in_registry(task):
                self._current_promise = task.task_config.function(**task.parameters)
                if task.task_config.on_shutdown == TaskShutdownPolicy.CONTINUE:
                    self._current_promise = asyncio.shield(self._current_promise)

                # Run the task and handle exceptions
                try:
                    async with asyncio.timeout(task.task_config.timeout):
                        task.results = await self._current_promise
                except asyncio.TimeoutError:
                    task = await self.handle_timeout_exception(task)
                except asyncio.CancelledError as exc:
                    ex = exc
                    await self.handle_system_cancelled_task(task)
                except Exception as exc:
                    if task.task_config and task.task_config.expected_exceptions and isinstance(exc, task.task_config.expected_exceptions):
                        task = await self.handle_expected_exception(task, exc)
                    else:
                        await self.handle_unexpected_exception(task, exc)
                else:
                    await self.handle_success(task)

        # Metrics recording
        tasks_processed.add(1, {"queue": task.queue, "task": task.name, "status": task.status})
        if task.status != TaskStatus.UNSUBMITTED:
            tasks_retried.add(task.retry_attempt, {"queue": task.queue, "task": task.name})
        if task.started_at and task.completed_at:
            execution_time.record(
                (task.completed_at - task.started_at).total_seconds() * 1000,
                {"queue": task.queue, "task": task.name, "status": task.status}
            )
        if task.submitted_at and task.completed_at:
            end_to_end_latency.record(
                (task.completed_at - task.submitted_at).total_seconds() * 1000,
                {"queue": task.queue, "task": task.name, "status": task.status}
            )

        if task.status == TaskStatus.COMPLETED:
            await self.post_process(task)
        elif ex is not None:
            raise ex

        return task

    async def monitor_task_cancellation(self, task: Task) -> None:
        """Monitor for task cancellation and handle it."""
        try:
            await self.state_manager.monitor_task_cancellation(task.id)
        except UserCancellationError:
            await self.handle_user_cancelled_task(task)
            raise # Re-raise to exit the TaskGroup in run()

    def mark_task_as_started(self, task: Task) -> None:
        task.set_status(TaskStatus.STARTED)

    async def post_process(self, task: Task) -> None:
        pass
        # if not task.has_callbacks():
        #     return

        # TODO: configure max concurrent callbacks
        # Monitor for when fan-out becomes problematic
        # callback_pool.map(self.state_manager.submit_task, task.generate_callbacks(), num_concurrent=5)

    async def handle_dropped_task(self, task: Task) -> None:
        logger.error("Dropping unknown task %s v%s id=%s.", task.name, task.version, task.id)
        task.set_status(TaskStatus.DROPPED)
        await self.state_manager.save_task(task)

    async def handle_system_cancelled_task(self, task: Task) -> None:
        logger.info("Task %s was cancelled.", task.id)
        task.shutdown()
        await self.state_manager.save_task(task)

    async def handle_user_cancelled_task(self, task: Task) -> None:
        logger.info("Task %s was cancelled by user.", task.id)
        task.set_status(TaskStatus.CANCELLED)
        await self.state_manager.save_task(task)

    async def handle_unexpected_exception(self, task: Task, exc: Exception) -> None:
        logger.exception("Exception occurred while processing task %s: %s", task.id, exc)
        task.set_status(TaskStatus.FAILED)
        task.errors.append(str(exc))
        await self.state_manager.fail_task(task)

    async def handle_expected_exception(self, task: Task, exc: Exception) -> Task:
        logger.warning("Task %s failed with error: %s", task.id, exc)
        task.errors.append(str(exc))
        tasks_retried.add(1, {"queue": task.queue, "task": task.name})
        if not task.should_retry():
            task.set_status(TaskStatus.FAILED)
            await self.state_manager.fail_task(task)
            return task

        if task.should_schedule():
            task.set_status(TaskStatus.SCHEDULED)
            run_at = task.task_config.compute_retry_at(task.retry_attempt)  # type: ignore[union-attr]
            return await self.state_manager.schedule_retry_task(task, run_at)
        else:
            task.set_status(TaskStatus.UNSUBMITTED)
            return await self.state_manager.queue_retry_task(task)

    async def handle_timeout_exception(self, task: Task) -> Task:
        timeout: int | None
        if task.task_config is None:
            timeout = None
        else:
            timeout = task.task_config.timeout
        logger.warning("Task %s timed out after %s seconds.", task.id, timeout)
        task.errors.append(f"Task {task.id} timed out after {timeout} seconds")
        tasks_retried.add(1, {"queue": task.queue, "task": task.name})
        if not task.should_retry():
            task.set_status(TaskStatus.FAILED)
            await self.state_manager.fail_task(task)
            return task

        if task.should_schedule():
            task.set_status(TaskStatus.SCHEDULED)
            run_at = task.task_config.compute_retry_at(task.retry_attempt)  # type: ignore[union-attr]
            return await self.state_manager.schedule_retry_task(task, run_at)
        else:
            task.set_status(TaskStatus.UNSUBMITTED)
            return await self.state_manager.queue_retry_task(task)

    async def handle_success(self, task: Task) -> None:
        logger.info("Task %s completed.", task.id)
        task.set_status(TaskStatus.COMPLETED)
        await self.state_manager.complete_task(task)
