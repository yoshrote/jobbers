import asyncio
import logging
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics

from jobbers.context import _current_task as _current_task_cv
from jobbers.models.dag import DynamicFanOut, TaskResult
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
                process_task = tg.create_task(self.process(task))
                monitor_task = tg.create_task(self.monitor_task_cancellation(task))
                monitor_task.add_done_callback(lambda t: process_task.cancel())
                process_task.add_done_callback(lambda t: monitor_task.cancel())
        except ExceptionGroup as eg:
            for exc in eg.exceptions:
                # Treat UserCancellationError as a normal control flow signal to exit the TaskGroup;
                # re-raise any other exceptions.
                if not isinstance(exc, UserCancellationError):
                    raise  # Re-raise to exit the TaskGroup in run()


    async def process(self, task: Task) -> Task:
        """Process the task and return the result."""
        logger.debug("Task %s details: %s", task.id, task)
        task.task_config = get_task_config(task.name, task.version)
        ex: BaseException | None = None

        dynamic_fanout: DynamicFanOut | None = None
        if task.task_config is None:
            await self.handle_dropped_task(task)
        else:
            self.mark_task_as_started(task)
            await self.state_manager.save_task(task)

            with self.state_manager.task_in_registry(task):
                await self.state_manager.update_task_heartbeat(task)
                _token = _current_task_cv.set(task)
                kwargs = dict(task.parameters)
                if task.inject_parent_results and task.parent_ids:
                    kwargs["parent_results"] = await task.parent_results()
                self._current_promise = task.task_config.function(**kwargs)
                if task.task_config.on_shutdown == TaskShutdownPolicy.CONTINUE:
                    self._current_promise = asyncio.shield(self._current_promise)

                # Run the task and handle exceptions
                try:
                    async with asyncio.timeout(task.task_config.timeout):
                        raw_result = await self._current_promise
                    if isinstance(raw_result, TaskResult):
                        task.results = raw_result.results
                        dynamic_fanout = raw_result.fanout
                        if raw_result.parent_ids:
                            task.parent_ids = raw_result.parent_ids
                    else:
                        task.results = {}
                except TimeoutError:
                    task = await self.handle_timeout_exception(task)
                except asyncio.CancelledError as exc:
                    if task.status == TaskStatus.CANCELLED:
                        pass  # user cancellation already handled; keep CANCELLED status
                    else:
                        ex = exc
                        await self.handle_system_cancelled_task(task)
                except Exception as exc:
                    if (
                        task.task_config
                        and task.task_config.expected_exceptions
                        and isinstance(exc, task.task_config.expected_exceptions)
                    ):
                        task = await self.handle_expected_exception(task, exc)
                    else:
                        await self.handle_unexpected_exception(task, exc)
                else:
                    await self.handle_success(task)
                finally:
                    _current_task_cv.reset(_token)

            await self.state_manager.remove_task_heartbeat(task)

        # Metrics recording
        tasks_processed.add(1, {"queue": task.queue, "task": task.name, "status": task.status})
        if task.started_at and task.completed_at:
            execution_time.record(
                (task.completed_at - task.started_at).total_seconds() * 1000,
                {"queue": task.queue, "task": task.name, "status": task.status},
            )
        if task.submitted_at and task.completed_at:
            end_to_end_latency.record(
                (task.completed_at - task.submitted_at).total_seconds() * 1000,
                {"queue": task.queue, "task": task.name, "status": task.status},
            )

        if task.status == TaskStatus.COMPLETED:
            await self.post_process(task, dynamic_fanout)
        else:
            # Only FAILED triggers error callbacks. CANCELLED means the user
            # deliberately stopped the task; STALLED and DROPPED are system-level
            # outcomes where the task function never ran to completion — firing an
            # error callback would be surprising and is intentionally not supported.
            if task.status == TaskStatus.FAILED:
                await self.post_process_error(task)
            if ex is not None:
                raise ex

        return task

    async def monitor_task_cancellation(self, task: Task) -> None:
        """Monitor for task cancellation and handle it."""
        try:
            await self.state_manager.monitor_task_cancellation(task.id)
        except UserCancellationError:
            await self.handle_user_cancelled_task(task)
            raise  # Re-raise to exit the TaskGroup in run()

    def mark_task_as_started(self, task: Task) -> None:
        task.set_status(TaskStatus.STARTED)
        logger.info("Task %s started (attempt %d).", task.id, task.retry_attempt + 1)

    async def post_process(self, task: Task, dynamic_fanout: DynamicFanOut | None = None) -> None:
        if dynamic_fanout is not None:
            await self._handle_dynamic_fanout(task, dynamic_fanout)
        if task.has_callbacks():
            callbacks = await task.generate_callbacks(self.state_manager.ta)
            pipe = self.state_manager.job_store.pipeline(transaction=True)
            for submitted_task in callbacks:
                self.state_manager.stage_submit_task(pipe, submitted_task, queue_config=None)
            await pipe.execute()

    async def post_process_error(self, task: Task) -> None:
        """Submit error callback tasks for a permanently-failed task."""
        error_callbacks = task.generate_error_callbacks()
        if error_callbacks:
            pipe = self.state_manager.job_store.pipeline(transaction=True)
            for cb in error_callbacks:
                self.state_manager.stage_submit_task(pipe, cb, queue_config=None)
            await pipe.execute()

    async def _handle_dynamic_fanout(self, parent: Task, fanout: DynamicFanOut) -> None:
        """
        Wire and submit a runtime fan-out produced by a task function.

        Calls `DAGNode.merge` to attach `FanInCallback`s to each child,
        initialises the Redis tracking + members sets, pre-saves the collector
        so it exists when the first child finishes, then submits all children
        atomically in a single pipeline.

        **Best practice:** assign child tasks to queues without rate limiting.
        Once a DAG is executing there is no safe recourse if a child submission
        is rejected — the fan-in would be initialised but never complete.
        Rate limits on child queues are bypassed with a warning logged.
        Capacity limits (max_concurrent) are not enforced at submission time
        but a low limit may cause queue backpressure that delays fan-in completion.
        """
        from jobbers.models.dag import DAGNode

        if not fanout.children:
            # Degenerate case: no children — submit the collector immediately.
            solo = fanout.collector.to_task(dag_run_id=parent.dag_run_id)
            await self.state_manager.submit_task(solo)
            return

        fan_in_key = f"dag:fan-in:{fanout.collector.id}"
        DAGNode.merge(*fanout.children, into=fanout.collector)

        child_ids = {child.id for child in fanout.children}
        child_tasks = [
            child.to_task(parent_id=parent.id, dag_run_id=parent.dag_run_id) for child in fanout.children
        ]
        collector_task = fanout.collector.to_task(dag_run_id=parent.dag_run_id)
        collector_task.parent_ids = list(child_ids)

        await self.state_manager.init_fan_in(fan_in_key, child_ids, ttl=fanout.fan_in_ttl)
        # Pre-save the collector so it exists in Redis when the first child completes.
        await self.state_manager.save_task(collector_task)

        # Warn if any child queue has rate limiting — we bypass it below.
        child_queues = list({ct.queue for ct in child_tasks})
        configs = await asyncio.gather(
            *(self.state_manager.qca.get_queue_config(queue=q) for q in child_queues)
        )
        for queue, config in zip(child_queues, configs):
            if config and config.rate_numerator and config.rate_denominator and config.rate_period:
                logger.warning(
                    "Queue '%s' has rate limiting configured but DAG child task submission "
                    "bypasses rate limits. Assign child tasks to queues without rate limiting.",
                    queue,
                )

        # Submit all children atomically in a single pipeline. A transactional pipeline
        # (MULTI/EXEC) is all-or-nothing: either every child is enqueued or none are,
        # with no partial state possible on a crash — strictly safer than sequential submission.
        pipe = self.state_manager.job_store.pipeline(transaction=True)
        for ct in child_tasks:
            ct.set_status(TaskStatus.SUBMITTED)
            self.state_manager.ta.stage_submit_task(pipe, ct)
        await pipe.execute()

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
        if not task.should_retry():
            task.set_status(TaskStatus.FAILED)
            await self.state_manager.fail_task(task)
            return task

        tasks_retried.add(1, {"queue": task.queue, "task": task.name, "version": task.version})
        if task.should_schedule():
            run_at = task.task_config.compute_retry_at(task.retry_attempt)  # type: ignore[union-attr]
            task.set_status(TaskStatus.SCHEDULED)
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
        if not task.should_retry():
            task.set_status(TaskStatus.FAILED)
            await self.state_manager.fail_task(task)
            return task

        tasks_retried.add(1, {"queue": task.queue, "task": task.name, "version": task.version})
        if task.should_schedule():
            run_at = task.task_config.compute_retry_at(task.retry_attempt)  # type: ignore[union-attr]
            task.set_status(TaskStatus.SCHEDULED)
            return await self.state_manager.schedule_retry_task(task, run_at)
        else:
            task.set_status(TaskStatus.UNSUBMITTED)
            return await self.state_manager.queue_retry_task(task)

    async def handle_success(self, task: Task) -> None:
        logger.info("Task %s completed.", task.id)
        task.set_status(TaskStatus.COMPLETED)
        if task.cron_id is not None:
            pipe = self.state_manager.job_store.pipeline(transaction=True)
            self.state_manager.ta.stage_save(pipe, task)
            self.state_manager.cron_dag_scheduler.stage_clear_active_run(pipe, task.cron_id)
            await pipe.execute()
        else:
            await self.state_manager.save_task(task)
