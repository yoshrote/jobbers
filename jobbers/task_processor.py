import asyncio
import contextlib
import logging
import multiprocessing
import queue as queue_module
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics

from jobbers import registry
from jobbers.context import _current_task as _current_task_cv
from jobbers.models.dag import DAGNode, DynamicFanOut, TaskResult
from jobbers.models.task import Task
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.models.task_status import TaskStatus
from jobbers.registry import get_task_config
from jobbers.state_manager import StateManager, UserCancellationError
from jobbers.sync_runner import run_sync_task
from jobbers.utils.di import DependencyResolver, merge_resolved_kwargs

if TYPE_CHECKING:
    from collections.abc import Awaitable


logger = logging.getLogger(__name__)


meter = metrics.get_meter(__name__)
tasks_processed = meter.create_counter("tasks_processed", unit="1")
tasks_retried = meter.create_counter("tasks_retried", unit="1")
execution_time = meter.create_histogram("task_execution_time", unit="ms")
end_to_end_latency = meter.create_histogram("task_end_to_end_latency", unit="ms")


def _await_sync_result(
    proc: multiprocessing.Process, result_q: "multiprocessing.Queue[Any]", poll_interval: float = 0.5
) -> tuple[str, Any]:
    """
    Block (in a worker thread, via asyncio.to_thread) until the subprocess reports a result.

    Polls instead of a bare `result_q.get()` so that once the parent kills the process on
    timeout/cancellation, this call returns promptly instead of blocking forever waiting for
    an item that will never arrive.
    """
    while True:
        try:
            result: tuple[str, Any] = result_q.get(timeout=poll_interval)
            return result
        except queue_module.Empty:
            if not proc.is_alive():
                return (
                    "error",
                    RuntimeError(f"sync task process exited (code {proc.exitcode}) without a result"),
                )


class TaskProcessor:
    """TaskProcessor to process tasks from a TaskGenerator."""

    def __init__(
        self,
        state_manager: StateManager,
        task_module: str | None = None,
        sync_semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        self.state_manager = state_manager
        self._current_promise: Awaitable[Any] | None = None
        # Only required for sync (non-async) task functions.
        self.task_module = task_module
        self.sync_semaphore = sync_semaphore or asyncio.Semaphore(1)

    async def run(self, task: Task) -> None:
        with self.state_manager.cancel_event(task.id):
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
                task._adapter = self.state_manager.task_state
                _token = _current_task_cv.set(task)
                kwargs = dict(task.parameters)
                if task.inject_parent_results and task.parent_ids:
                    kwargs["parent_results"] = await task.parent_results()

                try:
                    if task.task_config.is_sync:
                        task, dynamic_fanout, ex = await self._execute_sync(task, kwargs)
                    else:
                        task, dynamic_fanout, ex = await self._execute_async(task, kwargs)
                finally:
                    _current_task_cv.reset(_token)

            await self.state_manager.remove_task_heartbeat(task)
            await self._maybe_cleanup(task)

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

    async def _execute_async(
        self, task: Task, kwargs: dict[str, Any]
    ) -> tuple[Task, DynamicFanOut | None, BaseException | None]:
        """Resolve DI and await the registered async task function in-process."""
        dynamic_fanout: DynamicFanOut | None = None
        ex: BaseException | None = None

        resolver = DependencyResolver(task.task_config.dependency_graph)  # type: ignore[union-attr]
        async with resolver:
            dep_cache = await resolver.resolve_all()
            kwargs = merge_resolved_kwargs(task.task_config.function, dep_cache, kwargs)  # type: ignore[union-attr]

            self._current_promise = task.task_config.function(**kwargs)  # type: ignore[union-attr]
            if task.task_config.on_shutdown == TaskShutdownPolicy.CONTINUE:  # type: ignore[union-attr]
                self._current_promise = asyncio.shield(self._current_promise)

            try:
                async with asyncio.timeout(task.task_config.timeout):  # type: ignore[union-attr]
                    raw_result: dict[Any, Any] | TaskResult | None = await self._current_promise
                if isinstance(raw_result, TaskResult):
                    task.results = raw_result.results
                    dynamic_fanout = raw_result.fanout
                    if raw_result.parent_ids:
                        task.parent_ids = raw_result.parent_ids
                else:
                    if task.parent_ids:
                        task.parent_ids = list(set(task.parent_ids))  # deduplicate parent IDs
                    task.results = raw_result or {}
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

        return task, dynamic_fanout, ex

    async def _execute_sync(
        self, task: Task, kwargs: dict[str, Any]
    ) -> tuple[Task, DynamicFanOut | None, BaseException | None]:
        """
        Run a sync (non-async) registered task function in a dedicated subprocess.

        DI resolves inside the child (see jobbers/sync_runner.py) since dependency providers
        aren't picklable across the process boundary. `heartbeat_q` carries explicit
        `ctx.heartbeat()` calls back to this process, which holds the live state adapter;
        `cancel_evt` carries cooperative-cancellation requests the other way; `result_q`
        carries the final outcome. Timeout/cancellation forcibly `.terminate()`/`.kill()` the
        process — see CLAUDE.md for why a shared ProcessPoolExecutor can't do that safely.
        """
        dynamic_fanout: DynamicFanOut | None = None
        ex: BaseException | None = None
        task_config = task.task_config
        if task_config is None:
            raise RuntimeError("_execute_sync called without a resolved task_config")

        sync_task = task.model_copy(deep=True)
        sync_task._adapter = None
        # The child re-derives task_config from its own registry after loading the task
        # module (see sync_runner.run_sync_task) — pickling it here would carry `function`
        # across the process boundary by reference, which is exactly the trap documented
        # in CLAUDE.md (fails for file-path-loaded `_user_tasks` modules).
        sync_task.task_config = None

        heartbeat_q: multiprocessing.Queue[Any] = multiprocessing.Queue()
        cancel_evt = multiprocessing.Event()
        result_q: multiprocessing.Queue[Any] = multiprocessing.Queue()

        proc = multiprocessing.Process(
            target=run_sync_task,
            args=(self.task_module, sync_task, kwargs, heartbeat_q, cancel_evt, result_q),
            daemon=True,
        )

        async def _drain_heartbeats() -> None:
            # Polls with a timeout (rather than a bare blocking get()) so that once the
            # process is gone, this loop notices and returns on its own — a plain
            # `heartbeat_q.get()` would leave the underlying thread (and this task's
            # cancellation) blocked forever, since nothing will ever be queued again.
            while True:
                try:
                    heartbeat_task_id = await asyncio.to_thread(heartbeat_q.get, True, 0.5)
                except queue_module.Empty:
                    if not proc.is_alive():
                        return
                    continue
                if heartbeat_task_id == task.id:
                    await self.state_manager.update_task_heartbeat(task)

        async with self.sync_semaphore:
            proc.start()
            drain_task = asyncio.create_task(_drain_heartbeats())
            self._current_promise = asyncio.to_thread(_await_sync_result, proc, result_q)
            if task_config.on_shutdown == TaskShutdownPolicy.CONTINUE:
                self._current_promise = asyncio.shield(self._current_promise)

            try:
                async with asyncio.timeout(task_config.timeout):
                    status, payload = await self._current_promise
                if status == "error":
                    raise payload
                raw_result: dict[Any, Any] | TaskResult | None = payload
                if isinstance(raw_result, TaskResult):
                    task.results = raw_result.results
                    dynamic_fanout = raw_result.fanout
                    if raw_result.parent_ids:
                        task.parent_ids = raw_result.parent_ids
                else:
                    if task.parent_ids:
                        task.parent_ids = list(set(task.parent_ids))  # deduplicate parent IDs
                    task.results = raw_result or {}
            except TimeoutError:
                # Shielded (on_shutdown=CONTINUE) tasks keep running in the background, same as
                # the async path — don't kill the process out from under them.
                if task_config.on_shutdown != TaskShutdownPolicy.CONTINUE:
                    proc.terminate()
                task = await self.handle_timeout_exception(task)
            except asyncio.CancelledError as exc:
                if task_config.on_shutdown != TaskShutdownPolicy.CONTINUE:
                    cancel_evt.set()
                    proc.terminate()
                if task.status == TaskStatus.CANCELLED:
                    pass  # user cancellation already handled; keep CANCELLED status
                else:
                    ex = exc
                    await self.handle_system_cancelled_task(task)
            except Exception as exc:
                if task_config.expected_exceptions and isinstance(exc, task_config.expected_exceptions):
                    task = await self.handle_expected_exception(task, exc)
                else:
                    await self.handle_unexpected_exception(task, exc)
            else:
                await self.handle_success(task)
            finally:
                drain_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await drain_task
                if task_config.on_shutdown != TaskShutdownPolicy.CONTINUE or not proc.is_alive():
                    await asyncio.to_thread(proc.join, 5)
                    if proc.is_alive():
                        proc.kill()
                        await asyncio.to_thread(proc.join)

        return task, dynamic_fanout, ex

    async def monitor_task_cancellation(self, task: Task) -> None:
        """Monitor for task cancellation and handle it."""
        try:
            await self.state_manager.monitor_task_cancellation(task.id)
        except UserCancellationError:
            await self.handle_user_cancelled_task(task)
            raise  # Re-raise to exit the TaskGroup in run()

    async def _maybe_cleanup(self, task: Task) -> None:
        """
        Delete the task record if its final status is in cleanup_on.

        For DAG tasks, waits until all tasks in the run are terminal before
        deleting any of them (parent results may still be needed by siblings).
        """
        if task.task_config is None or not task.task_config.cleanup_on:
            return
        if task.status not in task.task_config.cleanup_on:
            return

        if task.dag_run_id is None:
            await self.state_manager.delete_task(task)
            return

        run = await self.state_manager.get_dag_run(task.dag_run_id)
        if run is None:
            # Orphaned run index — clean up immediately.
            await self.state_manager.delete_task(task)
            return

        _, task_ids = run
        sibling_tasks = await self.state_manager.task_state.get_tasks_bulk(task_ids)
        if any(t is None or t.status in TaskStatus.active_statuses() for t in sibling_tasks):
            return  # DAG still in flight; the last task to finish will trigger cleanup.

        # All DAG tasks are terminal — delete those whose config says to.
        to_delete: list[Task] = []
        for sibling in sibling_tasks:
            if sibling is None:
                continue
            cfg = registry.get_task_config(sibling.name, sibling.version)
            if cfg and cfg.cleanup_on and sibling.status in cfg.cleanup_on:
                to_delete.append(sibling)
        if to_delete:
            await asyncio.gather(*(self.state_manager.delete_task(t) for t in to_delete))

    def mark_task_as_started(self, task: Task) -> None:
        task.set_status(TaskStatus.STARTED)
        logger.info("Task %s started (attempt %d).", task.id, task.retry_attempt + 1)

    async def post_process(self, task: Task, dynamic_fanout: DynamicFanOut | None = None) -> None:
        if dynamic_fanout is not None:
            await self._handle_dynamic_fanout(task, dynamic_fanout)
        if task.has_callbacks():
            callbacks = await task.generate_callbacks(self.state_manager.task_state)
            for cb in callbacks:
                cb.queue = await self.state_manager.resolve_queue(cb)
            await self.state_manager.submit_tasks_batch(callbacks)

    async def post_process_error(self, task: Task) -> None:
        """Submit error callback tasks for a permanently-failed task."""
        error_callbacks = task.generate_error_callbacks()
        if error_callbacks:
            for cb in error_callbacks:
                cb.queue = await self.state_manager.resolve_queue(cb)
            await self.state_manager.submit_tasks_batch(error_callbacks)

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
        configs = await asyncio.gather(*(self.state_manager.get_queue_config(q) for q in child_queues))
        for queue, config in zip(child_queues, configs):
            if config and config.rate_numerator and config.rate_denominator and config.rate_period:
                logger.warning(
                    "Queue '%s' has rate limiting configured but DAG child task submission "
                    "bypasses rate limits. Assign child tasks to queues without rate limiting.",
                    queue,
                )

        await self.state_manager.submit_tasks_batch(child_tasks)

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

    async def _handle_retry(self, task: Task, error_message: str) -> Task:
        task.errors.append(error_message)
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

    async def handle_expected_exception(self, task: Task, exc: Exception) -> Task:
        logger.warning("Task %s failed with error: %s", task.id, exc)
        return await self._handle_retry(task, str(exc))

    async def handle_timeout_exception(self, task: Task) -> Task:
        timeout: int | None = None if task.task_config is None else task.task_config.timeout
        logger.warning("Task %s timed out after %s seconds.", task.id, timeout)
        return await self._handle_retry(task, f"Task {task.id} timed out after {timeout} seconds")

    async def handle_success(self, task: Task) -> None:
        logger.info("Task %s completed.", task.id)
        task.set_status(TaskStatus.COMPLETED)
        if task.cron_id is not None:
            await self.state_manager.complete_cron_task(task)
        else:
            await self.state_manager.save_task(task)
