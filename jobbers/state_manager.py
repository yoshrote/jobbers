from __future__ import annotations

import asyncio
import datetime as dt
import logging
import random
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from croniter import croniter
from opentelemetry import metrics
from ulid import ULID

from jobbers import registry
from jobbers.models.cron_dag import ConcurrencyPolicy
from jobbers.models.dag import collect_fan_in_keys
from jobbers.models.task import Task
from jobbers.models.task_config import DeadLetterPolicy
from jobbers.models.task_routing import RoutingStrategy
from jobbers.models.task_status import TaskStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

    from jobbers.models.cron_dag import CronDAGEntry
    from jobbers.models.dag import DAGNode, DAGRunPagination
    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task_routing import RoutingConfig
    from jobbers.protocols import (
        AtomicCronDAGSchedulerProtocol,
        AtomicDeadQueueProtocol,
        AtomicTaskSchedulerProtocol,
        AtomicTaskStateProtocol,
        CancellationBusProtocol,
        CronDAGSchedulerProtocol,
        DeadQueueProtocol,
        RoutingBackendProtocol,
        RoutingNotificationProtocol,
        TaskSchedulerProtocol,
        TaskStateProtocol,
        TaskSubmitProtocol,
        TransactionHandle,
    )

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
tasks_dead_lettered = meter.create_counter("tasks_dead_lettered", unit="1")


@dataclass
class ConcurrencyStager:
    """
    Yielded by ``StateManager._concurrency_guard``.

    ``skipped`` is True when the cron entry was suppressed because a previous
    run is still active.  Return immediately without building or submitting a
    new task.

    ``stage_active_run(pipe, task_id)`` stages the SET NX for
    SKIP_IF_RUNNING policy, or is a no-op for ALWAYS policy.
    Call it unconditionally during pipeline construction.
    """

    skipped: bool
    _stage_fn: Callable[[TransactionHandle, ULID], None]

    def stage_active_run(self, pipe: TransactionHandle, task_id: ULID) -> None:
        self._stage_fn(pipe, task_id)


class TaskException(Exception):
    "Top-level exception for stuff gone wrong."

    pass


class UserCancellationError(Exception):
    """Exception raised when a task is cancelled by user request."""

    pass


class StateManager:
    """Coordinator for managing task state across the job store."""

    def __init__(
        self,
        routing_backend: RoutingBackendProtocol,
        task_state: TaskStateProtocol,
        task_submit: TaskSubmitProtocol,
        *,
        dead_queue: DeadQueueProtocol,
        task_scheduler: TaskSchedulerProtocol,
        cron_dag_scheduler: CronDAGSchedulerProtocol,
        cancellation_bus: CancellationBusProtocol,
        routing_notifications: RoutingNotificationProtocol,
        force_saga: bool = False,
    ) -> None:
        self.routing: RoutingBackendProtocol = routing_backend
        self.cancellation_bus: CancellationBusProtocol = cancellation_bus
        self.routing_notifications: RoutingNotificationProtocol = routing_notifications
        self.task_state: TaskStateProtocol = task_state
        self.task_submit: TaskSubmitProtocol = task_submit
        self._queue_config_cache: dict[str, QueueConfig | None] = {}
        self._routing_config_cache: dict[tuple[str, int], RoutingConfig | None] = {}
        self.submission_limiter = SubmissionRateLimiter(self.get_queue_config)
        self.current_tasks_by_queue: dict[str, set[ULID]] = defaultdict(set)
        self._cancel_events: dict[ULID, asyncio.Event] = {}
        self.dead_queue: DeadQueueProtocol = dead_queue
        self.task_scheduler = task_scheduler
        self.cron_dag_scheduler = cron_dag_scheduler

        # Same-backend detection: if the task adapter, scheduler, and DLQ all implement the
        # Atomic sub-protocols, StateManager uses atomic pipelines.  When stores differ, or
        # when force_saga=True, it falls back to saga coordination.
        from jobbers.protocols import (  # local import avoids circular at module level
            AtomicCronDAGSchedulerProtocol,
            AtomicDeadQueueProtocol,
            AtomicTaskSchedulerProtocol,
            AtomicTaskStateProtocol,
        )

        self._atomic_state: AtomicTaskStateProtocol | None = (
            task_state if isinstance(task_state, AtomicTaskStateProtocol) else None
        )
        self._atomic_scheduler: AtomicTaskSchedulerProtocol | None = (
            self.task_scheduler if isinstance(self.task_scheduler, AtomicTaskSchedulerProtocol) else None
        )
        self._atomic_dlq: AtomicDeadQueueProtocol | None = (
            self.dead_queue if isinstance(self.dead_queue, AtomicDeadQueueProtocol) else None
        )
        self._atomic_mode: bool = (
            False
            if force_saga
            else all(x is not None for x in (self._atomic_state, self._atomic_scheduler, self._atomic_dlq))
        )
        # Cron-specific same-backend detection: True when the cron scheduler shares a
        # backend with the task-state adapter, enabling cron ops to be folded into the
        # same atomic pipeline as task-state ops.
        self._atomic_cron: AtomicCronDAGSchedulerProtocol | None = (
            cron_dag_scheduler
            if (
                not force_saga
                and isinstance(cron_dag_scheduler, AtomicCronDAGSchedulerProtocol)
                and self._atomic_state is not None
                and cron_dag_scheduler.backend_key == self._atomic_state.backend_key
            )
            else None
        )

    @property
    def active_tasks_per_queue(self) -> dict[str, int]:
        return {q: len(ids) for q, ids in self.current_tasks_by_queue.items()}

    @contextmanager
    def task_in_registry(self, task: Task) -> Iterator[None]:
        """Context manager to add a task to the registry."""
        self.current_tasks_by_queue[task.queue].add(task.id)
        try:
            yield
        finally:
            self.current_tasks_by_queue[task.queue].remove(task.id)

    @contextmanager
    def cancel_event(self, task_id: ULID) -> Iterator[None]:
        self._cancel_events[task_id] = asyncio.Event()
        try:
            yield
        finally:
            self._cancel_events.pop(task_id, None)

    def signal_cancel(self, task_id: ULID) -> bool:
        event = self._cancel_events.get(task_id)
        if event is not None:
            event.set()
            return True
        return False

    async def clean(
        self,
        rate_limit_age: dt.timedelta | None = None,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
        stale_time: dt.timedelta | None = None,
        dlq_age: dt.timedelta | None = None,
        completed_task_age: dt.timedelta | None = None,
        recover_orphaned_scheduled: bool = False,
    ) -> None:
        """Clean up the state manager."""
        now = dt.datetime.now(dt.UTC)
        queues = set(await self.get_all_queues())

        clean_ops = []
        if rate_limit_age:
            clean_ops.append(self.task_submit.clean_rate_limiter(queues, now, rate_limit_age))

        if max_queue_age or min_queue_age:
            clean_ops.append(self.task_state.clean(queues, now, min_queue_age, max_queue_age))

        if dlq_age:
            clean_ops.append(self.dead_queue.clean(now - dlq_age))

        if completed_task_age:
            clean_ops.append(self.task_state.clean_terminal_tasks(now, completed_task_age))
            clean_ops.append(self.task_state.clean_dag_runs(now, completed_task_age))

        if recover_orphaned_scheduled:
            clean_ops.append(self.task_scheduler.recover_orphans(now))

        if stale_time:
            stale_tasks_by_type: dict[tuple[str, int], list[Task]] = defaultdict(list)
            async for task in self.task_state.get_stale_tasks(queues, stale_time):
                if task.status != TaskStatus.STARTED:
                    continue
                stale_tasks_by_type[(task.name, task.version)].append(task)

            stale_pipes = []
            stale_saga_tasks: list[Task] = []
            for (task_type, task_version), tasks in stale_tasks_by_type.items():
                task_config = registry.get_task_config(task_type, task_version)
                if task_config and task_config.max_heartbeat_interval:
                    for task in tasks:
                        if (
                            task.heartbeat_at
                            and (now - task.heartbeat_at) > task_config.max_heartbeat_interval
                        ):
                            task.set_status(TaskStatus.STALLED)
                            if self._atomic_state is not None:
                                pipe = self._atomic_state.pipeline(transaction=True)
                                self._atomic_state.stage_save(pipe, task)
                                self._atomic_state.stage_remove_heartbeat(pipe, task)
                                stale_pipes.append(pipe.execute())
                            else:
                                stale_saga_tasks.append(task)
            if stale_pipes:
                await asyncio.gather(*stale_pipes)
            for stale_task in stale_saga_tasks:
                await self.task_state.save_task(stale_task)
                await self.task_state.remove_task_heartbeat(stale_task)

        await asyncio.gather(*clean_ops)

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        if not queues:
            logger.info("no queues defined")
            return None

        queues = await self.submission_limiter.concurrency_limits(queues, self.current_tasks_by_queue)

        return await self.task_submit.get_next_task(queues, pop_timeout)

    async def resubmit_dead_tasks(self, tasks: list[Task], reset_retry_count: bool = True) -> list[Task]:
        """Re-enqueue DLQ tasks and remove them from the DLQ in a single atomic transaction."""
        for task in tasks:
            if reset_retry_count:
                task.retry_attempt = 0
            task.errors = []
            task.set_status(TaskStatus.SUBMITTED)
        if self._atomic_state is not None and self._atomic_dlq is not None:
            pipe = self._atomic_state.pipeline(transaction=True)
            for task in tasks:
                self._atomic_state.stage_requeue(pipe, task)
                self._atomic_dlq.stage_remove(pipe, task.id, task.queue, task.name)
            await pipe.execute()
        else:
            # Saga: persist blobs (source of truth), then remove from DLQ.
            # If remove fails, Cleaner reconciles DLQ entries for SUBMITTED/STARTED tasks.
            await asyncio.gather(*(self.task_state.save_task(t) for t in tasks))
            for task in tasks:
                await self.dead_queue.remove_from_dlq(task.id, task.queue, task.name)
        return tasks

    async def fail_task(self, task: Task) -> Task:
        """Persist a failed task and any DLQ side effects in a single atomic transaction."""
        now = dt.datetime.now(dt.UTC)
        needs_dlq = bool(task.task_config and task.task_config.dead_letter_policy == DeadLetterPolicy.SAVE)
        if self._atomic_state is not None and self._atomic_dlq is not None:
            pipe = self._atomic_state.pipeline(transaction=True)
            self._atomic_state.stage_save(pipe, task)
            if needs_dlq:
                logger.info("Task %s sent to dead letter queue.", task.id)
                self._atomic_dlq.stage_add(pipe, task, now)
            await pipe.execute()
        else:
            await self.task_state.save_task(task)
            if needs_dlq:
                logger.info("Task %s sent to dead letter queue.", task.id)
                await self.dead_queue.add_to_dlq(task, now)
        if needs_dlq:
            tasks_dead_lettered.add(1, {"queue": task.queue, "task": task.name, "version": task.version})
        return task

    async def _run_schedule_pipeline(self, task: Task, run_at: dt.datetime) -> None:
        if self._atomic_state is not None and self._atomic_scheduler is not None:
            pipe = self._atomic_state.pipeline(transaction=True)
            self._atomic_scheduler.stage_add(pipe, task, run_at)
            self._atomic_state.stage_save(pipe, task)
            await pipe.execute()
        else:
            await self.task_state.save_task(task)
            await self.task_scheduler.add(task, run_at)

    async def schedule_new_task(self, task: Task, run_at: dt.datetime) -> Task:
        """Save a brand-new task directly into the scheduler in a single atomic transaction."""
        task.queue = await self.resolve_queue(task)
        task.status = TaskStatus.SCHEDULED
        await self._run_schedule_pipeline(task, run_at)
        logger.info("Task %s scheduled to run at %s.", task.id, run_at)
        return task

    async def schedule_retry_task(self, task: Task, run_at: dt.datetime) -> Task:
        """Add a task to the scheduler and save its state in a single atomic transaction."""
        await self._run_schedule_pipeline(task, run_at)
        logger.info("Task %s scheduled for retry at %s.", task.id, run_at)
        return task

    async def queue_retry_task(self, task: Task) -> Task:
        """Persist an immediate retry: re-enqueue the task without full re-validation."""
        task.set_status(TaskStatus.SUBMITTED)
        logger.info("Task %s requeued for immediate retry.", task.id)
        await self.requeue_task(task)
        return task

    async def dispatch_scheduled_task(self, task: Task) -> Task:
        """
        Move a due scheduled task into its queue and clean up the scheduler atomically.

        Same-backend mode: uses WATCH/MULTI for optimistic locking — if the task is modified
        concurrently (e.g. cancelled), the pipeline detects the conflict and retries.

        Cross-store mode: uses compare_and_set_status for the optimistic guard, then enqueues
        and removes from the scheduler as separate steps.

        next_due_bulk's Lua script already removed the task from the schedule-queue sorted
        set before this is called, so stage_remove's ZREM is a no-op; the HDEL cleans up
        the residual schedule-task-queue hash entry.
        """
        if self._atomic_state is not None and self._atomic_scheduler is not None:
            scheduler = self._atomic_scheduler

            def _stage_scheduler_remove(pipe: TransactionHandle) -> None:
                scheduler.stage_remove(pipe, task.id, task.queue)

            dispatched = await self._atomic_state.atomic_dispatch_scheduled(task, _stage_scheduler_remove)
            if dispatched:
                logger.info("Task %s dispatched to queue %s.", task.id, task.queue)
            return task
        else:
            # Saga path: compare-and-set status guards against concurrent cancellation.
            applied = await self.task_state.compare_and_set_status(
                task.id, TaskStatus.SCHEDULED, TaskStatus.SUBMITTED
            )
            if not applied:
                return task  # task was cancelled or already processed
            task.set_status(TaskStatus.SUBMITTED)
            await self.task_scheduler.remove(task.id, task.queue)
            logger.info("Task %s dispatched to queue %s.", task.id, task.queue)
            return task

    @asynccontextmanager
    async def _concurrency_guard(
        self,
        entry: CronDAGEntry,
        next_run_at: dt.datetime,
    ) -> AsyncIterator[ConcurrencyStager]:
        """
        Async context manager encapsulating all ConcurrencyPolicy logic for a cron dispatch.

        Yields a ``ConcurrencyStager`` with:
        - ``skipped=True`` if the previous run is still active (reschedule-only pipeline
          already fired internally; caller should return immediately).
        - ``skipped=False`` and a ``stage_active_run(pipe, task_id)`` callable otherwise.
          Call it unconditionally during pipeline construction; it is a no-op for ALWAYS
          policy and stages the NX guard for SKIP_IF_RUNNING.
        """
        from jobbers.protocols import AtomicCronDAGSchedulerProtocol

        if entry.concurrency_policy == ConcurrencyPolicy.SKIP_IF_RUNNING:
            active_task_id_str = await self.cron_dag_scheduler.get_active_run(entry.id)
            if active_task_id_str is not None:
                active_task = await self.task_state.get_task(ULID.from_str(active_task_id_str))
                if active_task is not None and active_task.status in {
                    TaskStatus.SUBMITTED,
                    TaskStatus.STARTED,
                }:
                    logger.info(
                        "Cron entry %s skipped: previous run %s still active (%s).",
                        entry.id,
                        active_task_id_str,
                        active_task.status,
                    )
                    await self.cron_dag_scheduler.reschedule(entry.id, next_run_at)
                    yield ConcurrencyStager(skipped=True, _stage_fn=lambda _p, _t: None)
                    return

            if isinstance(self.cron_dag_scheduler, AtomicCronDAGSchedulerProtocol):
                # Scheduler supports pipeline staging: NX guard goes into the pipeline.
                _atomic_sched = self.cron_dag_scheduler  # narrow for mypy

                def _stage_skip(pipe: TransactionHandle, task_id: ULID) -> None:
                    _atomic_sched.stage_set_active_run(pipe, entry.id, task_id, nx=True)

                yield ConcurrencyStager(skipped=False, _stage_fn=_stage_skip)
            else:
                # No pipeline (e.g. StaticCronDAGScheduler): set_active_run called
                # directly in dispatch_cron_dag; stager is a no-op here.
                yield ConcurrencyStager(skipped=False, _stage_fn=lambda _p, _t: None)
        else:
            yield ConcurrencyStager(skipped=False, _stage_fn=lambda _p, _t: None)

    async def dispatch_cron_dag(self, entry: CronDAGEntry, run_at: dt.datetime) -> None:
        """
        Fire a cron-scheduled DAG run and reschedule the entry for its next occurrence.

        A fresh copy of the DAG spec (new ULIDs for every node) is generated on each call
        so repeated runs never share Redis fan-in keys.  If the entry's concurrency policy
        is SKIP_IF_RUNNING and the previous root task is still active, the run is skipped
        but the entry is still rescheduled.

        Ordering guarantee: reschedule + fan-in init are written atomically in a single
        pipeline *before* the root task is submitted.  A crash after the pipeline but
        before submit means the entry fires again on the next poll (tolerable duplicate),
        but the cron schedule is never permanently lost.
        """
        next_run_at = croniter(entry.cron_expr, run_at).get_next(dt.datetime)

        async with self._concurrency_guard(entry, next_run_at) as stager:
            if stager.skipped:
                return

            fresh_spec, _ = entry.dag_spec.fresh_copy()
            fan_ins = collect_fan_in_keys(fresh_spec)
            queue_config = await self.get_queue_config(fresh_spec.queue)

            task = Task(
                id=fresh_spec.id,
                name=fresh_spec.name,
                queue=fresh_spec.queue,
                version=fresh_spec.version,
                parameters=fresh_spec.parameters,
                dag_callbacks=fresh_spec.dag_callbacks,
                cron_id=entry.id,
                dag_run_id=ULID(),
            )

            is_rate_limited = bool(
                queue_config
                and queue_config.rate_numerator
                and queue_config.rate_denominator
                and queue_config.rate_period
            )

            # Write reschedule + fan-in sets atomically BEFORE submitting the root task.
            # This ensures the cron entry is never permanently lost even if submit_task crashes.
            # For SKIP_IF_RUNNING, SET NX is the authoritative guard against concurrent dispatches.
            # For non-rate-limited queues, submission is included in the same pipeline.
            from jobbers.protocols import AtomicCronDAGSchedulerProtocol

            if self._atomic_cron is not None:
                # Same-backend: cron + task-state ops in one atomic pipeline.
                pipe = self._atomic_state.pipeline(transaction=True)  # type: ignore[union-attr]
                self._atomic_cron.stage_reschedule(pipe, entry.id, next_run_at)
                stager.stage_active_run(pipe, task.id)
                for fan_in_key, predecessor_ids in fan_ins.items():
                    self._atomic_state.stage_init_fan_in(pipe, fan_in_key, predecessor_ids)  # type: ignore[union-attr]
                if not is_rate_limited:
                    self.stage_submit_task(pipe, task, queue_config)
                await pipe.execute()
                if is_rate_limited:
                    await self.submit_task(task)
            elif isinstance(self.cron_dag_scheduler, AtomicCronDAGSchedulerProtocol):
                # Cross-backend: cron ops are atomic internally; task-state ops follow.
                cron_pipe = self.cron_dag_scheduler.pipeline(transaction=True)
                self.cron_dag_scheduler.stage_reschedule(cron_pipe, entry.id, next_run_at)
                stager.stage_active_run(cron_pipe, task.id)
                await cron_pipe.execute()
                await asyncio.gather(*(self.task_state.init_fan_in(k, ids) for k, ids in fan_ins.items()))
                await self.submit_task(task)
            else:
                # No pipeline support (e.g. StaticCronDAGScheduler): sequential calls.
                await self.cron_dag_scheduler.reschedule(entry.id, next_run_at)
                if entry.concurrency_policy == ConcurrencyPolicy.SKIP_IF_RUNNING:
                    await self.cron_dag_scheduler.set_active_run(entry.id, task.id, nx=True)
                await asyncio.gather(*(self.task_state.init_fan_in(k, ids) for k, ids in fan_ins.items()))
                await self.submit_task(task)
            logger.info("Cron entry %s dispatched as task %s (run_at=%s).", entry.id, task.id, run_at)

    async def request_task_cancellation(self, task_id: ULID) -> Task | None:
        """
        Publish a cancellation request for a task.

        Returns None if the task does not exist.
        Raises TaskException if the task status does not permit cancellation.
        """
        task = await self.task_state.get_task(task_id)
        if task is None:
            return None
        match task.status:
            case TaskStatus.SCHEDULED:
                # For scheduled tasks, we can just remove them from the scheduler without a pub/sub dance
                task.set_status(TaskStatus.CANCELLED)
                if self._atomic_state is not None and self._atomic_scheduler is not None:
                    pipe = self._atomic_state.pipeline(transaction=True)
                    self._atomic_scheduler.stage_remove(pipe, task_id, task.queue)
                    self._atomic_state.stage_save(pipe, task)
                    await pipe.execute()
                else:
                    await self.task_scheduler.remove(task_id, task.queue)
                    await self.task_state.save_task(task)
            case TaskStatus.SUBMITTED:
                # remove from the queue immediately so it can't be claimed by a worker
                task.set_status(TaskStatus.CANCELLED)
                if self._atomic_state is not None:
                    pipe = self._atomic_state.pipeline(transaction=True)
                    self._atomic_state.stage_remove_from_queue(pipe, task)
                    self._atomic_state.stage_save(pipe, task)
                    await pipe.execute()
                else:
                    # Saga: save CANCELLED state; queue entry is cleaned up by Cleaner.
                    await self.task_state.save_task(task)
            case TaskStatus.STARTED:
                await self.cancellation_bus.publish_cancellation(task_id)
            case _:
                raise TaskException(f"Task has status '{task.status}' and cannot be cancelled.")
        return task

    async def monitor_task_cancellation(self, task_id: ULID) -> None:
        """Wait for a cancel signal for this task and raise UserCancellationError when it arrives."""
        event = self._cancel_events.get(task_id)
        if event is None:
            return
        await event.wait()
        logger.info("Received cancellation signal for task %s", task_id)
        raise UserCancellationError(f"Task {task_id} was cancelled by user request.")

    async def run_cancel_listener(self) -> None:
        """Subscribe to the shared cancellations channel and signal matching active tasks."""
        async for task_id in self.cancellation_bus.listen_cancellations():
            self.signal_cancel(task_id)

    # Proxy methods
    async def get_refresh_tag(self, role: str) -> ULID:
        return await self.routing_notifications.get_refresh_tag(role)

    async def poll_refresh_signal(self, role: str) -> ULID:
        return await self.routing_notifications.poll_refresh_signal(role)

    def stage_submit_task(
        self, pipe: TransactionHandle, task: Task, queue_config: QueueConfig | None
    ) -> None:
        """
        Set task status to SUBMITTED and stage ZADD + save onto pipe (no execute).

        Raises ValueError if the queue has rate-limiting configured — callers must
        use submit_task() for rate-limited queues, which enforces limits via Lua script.
        The queue_config must be pre-fetched by the caller before building the pipeline.
        """
        if (
            queue_config
            and queue_config.rate_numerator
            and queue_config.rate_denominator
            and queue_config.rate_period
        ):
            raise ValueError(
                f"Queue '{task.queue}' is rate-limited; use submit_task() instead of stage_submit_task()."
            )
        task.set_status(TaskStatus.SUBMITTED)
        assert self._atomic_state is not None  # noqa: S101
        self._atomic_state.stage_submit_task(pipe, task)

    # ── New cross-store coordination methods ─────────────────────────────────

    async def submit_tasks_batch(self, tasks: list[Task]) -> None:
        """
        Set tasks to SUBMITTED and persist them.

        Atomic pipeline (MULTI/EXEC) when all adapters share the same Redis backend;
        sequential saga (save blob → enqueue) otherwise.  Queues must already be
        resolved on each task before calling this method.
        """
        for task in tasks:
            task.set_status(TaskStatus.SUBMITTED)

        if self._atomic_state is not None:
            pipe = self._atomic_state.pipeline(transaction=True)
            for task in tasks:
                self._atomic_state.stage_submit_task(pipe, task)
            await pipe.execute()
        else:
            # Saga: persist blobs first (source of truth), then enqueue via TaskQueueProtocol.
            await asyncio.gather(*(self.task_state.save_task(t) for t in tasks))

    async def requeue_task(self, task: Task) -> None:
        """
        Re-enqueue a task that was popped but not yet started (e.g. on CancelledError).

        Atomic ZADD+save in single-backend mode; sequential save→enqueue in saga mode.
        """
        if self._atomic_state is not None:
            pipe = self._atomic_state.pipeline(transaction=True)
            self._atomic_state.stage_requeue(pipe, task)
            await pipe.execute()
        else:
            task.set_status(TaskStatus.SUBMITTED)
            await self.task_state.save_task(task)

    async def complete_cron_task(self, task: Task) -> None:
        """
        Persist a COMPLETED cron task and clear its active-run marker atomically.

        When cron and task-state share a backend, both ops go in one pipeline.
        When task-state is atomic but cron is on a separate backend, task save is
        atomic and the active-run clear is a sequential follow-up call.
        Falls back to sequential saga when neither adapter supports atomic staging.
        """
        assert task.cron_id is not None  # noqa: S101
        if self._atomic_cron is not None:
            # Same-backend: save + clear-active-run in one pipeline.
            pipe = self._atomic_state.pipeline(transaction=True)  # type: ignore[union-attr]
            self._atomic_state.stage_save(pipe, task)  # type: ignore[union-attr]
            self._atomic_cron.stage_clear_active_run(pipe, task.cron_id)
            await pipe.execute()
        elif self._atomic_state is not None:
            # Task-state atomic, cron is on a separate backend.
            pipe = self._atomic_state.pipeline(transaction=True)
            self._atomic_state.stage_save(pipe, task)
            await pipe.execute()
            await self.cron_dag_scheduler.clear_active_run(task.cron_id)
        else:
            await self.task_state.save_task(task)
            await self.cron_dag_scheduler.clear_active_run(task.cron_id)

    async def reschedule_cron_entries_bulk(self, entries: list[tuple[CronDAGEntry, dt.datetime]]) -> None:
        """
        Reschedule a batch of disabled cron entries to their next run time.

        Uses the cron scheduler's own pipeline when it supports atomic staging;
        falls back to sequential calls otherwise.
        """
        from jobbers.protocols import AtomicCronDAGSchedulerProtocol

        if isinstance(self.cron_dag_scheduler, AtomicCronDAGSchedulerProtocol):
            pipe = self.cron_dag_scheduler.pipeline(transaction=True)
            for entry, run_at in entries:
                next_run_at = croniter(entry.cron_expr, run_at).get_next(dt.datetime)
                self.cron_dag_scheduler.stage_reschedule(pipe, entry.id, next_run_at)
            await pipe.execute()
        else:
            for entry, run_at in entries:
                next_run_at = croniter(entry.cron_expr, run_at).get_next(dt.datetime)
                await self.cron_dag_scheduler.reschedule(entry.id, next_run_at)

    # ── End cross-store coordination methods ─────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        """Return queue config, reading from cache on hit."""
        if queue not in self._queue_config_cache:
            self._queue_config_cache[queue] = await self.routing.get_queue_config(queue)
        return self._queue_config_cache.get(queue)

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        """Return routing config, reading from cache on hit."""
        key = (task_name, task_version)
        if key not in self._routing_config_cache:
            self._routing_config_cache[key] = await self.routing.get_routing_config(task_name, task_version)
        return self._routing_config_cache.get(key)

    def invalidate_queue_config(self, queue: str) -> None:
        self._queue_config_cache.pop(queue, None)

    def invalidate_routing_config(self, task_name: str, task_version: int) -> None:
        self._routing_config_cache.pop((task_name, task_version), None)

    def invalidate_all_routing_config(self) -> None:
        self._routing_config_cache.clear()

    async def get_routing_version(self) -> ULID | None:
        return await self.routing_notifications.get_routing_version()

    async def bump_routing_version(self) -> None:
        await self.routing_notifications.bump_routing_version()

    async def bump_refresh_tag(self, role: str) -> str:
        return await self.routing_notifications.bump_refresh_tag(role)

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> None:
        roles = await self.routing.get_roles_for_queue(queue_name)
        for role in roles:
            await self.routing_notifications.bump_refresh_tag(role)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        """Save queue config, invalidate local cache, and bump refresh_tags for affected roles."""
        await self.routing.save_queue_config(queue_config)
        self.invalidate_queue_config(queue_config.name)
        await self.bump_refresh_tags_for_queue(queue_config.name)

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        """Save routing config, invalidate local cache entry, and bump routing version."""
        await self.routing.save_routing_config(routing_config)
        self.invalidate_routing_config(routing_config.task_name, routing_config.task_version)
        await self.bump_routing_version()

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        """Delete routing config, invalidate local cache entry, and bump routing version."""
        deleted = await self.routing.delete_routing_config(task_name, task_version)
        self.invalidate_routing_config(task_name, task_version)
        await self.bump_routing_version()
        return deleted

    async def get_queues(self, role: str) -> set[str]:
        """Return the set of queues assigned to a role."""
        return await self.routing.get_queues(role)

    async def get_queue_limits(self, queues: set[str]) -> dict[str, int | None]:
        """Return per-queue max_concurrent limits, reusing the queue-config cache."""
        results = await asyncio.gather(*(self.get_queue_config(q) for q in queues))
        return {q: (cfg.max_concurrent if cfg else None) for q, cfg in zip(queues, results)}

    async def get_all_queues(self) -> list[str]:
        """Return the list of all configured queue names."""
        return await self.routing.get_all_queues()

    async def get_all_roles(self) -> list[str]:
        return await self.routing.get_all_roles()

    async def save_role(self, role: str, queues_set: set[str]) -> None:
        await self.routing.save_role(role, queues_set)
        await self.routing_notifications.bump_refresh_tag(role)

    async def delete_queue(self, queue_name: str) -> None:
        self.invalidate_queue_config(queue_name)
        affected_roles = await self.routing.delete_queue(queue_name)
        for role in affected_roles:
            await self.routing_notifications.bump_refresh_tag(role)

    async def delete_role(self, role: str) -> None:
        await self.routing.delete_role(role)

    async def resolve_queue(self, task: Task) -> str:
        """Return the queue name to use for *task*, applying routing config if one is set."""
        routing = await self.get_routing_config(task.name, task.version)
        if routing is None:
            return task.queue
        match routing.strategy:
            case RoutingStrategy.SINGLE:
                final = routing.queues[0]
            case RoutingStrategy.WEIGHTED:
                final = random.choices(routing.queues, weights=routing.weights, k=1)[0]
        if final != task.queue:
            logger.info(
                "Routing override: task=%s v%d original=%s resolved=%s strategy=%s",
                task.name,
                task.version,
                task.queue,
                final,
                routing.strategy,
            )
        return final

    async def submit_task(self, task: Task) -> None:
        task.queue = await self.resolve_queue(task)
        queue_config = await self.get_queue_config(task.queue)
        task.set_status(TaskStatus.SUBMITTED)
        if (
            queue_config
            and queue_config.rate_numerator
            and queue_config.rate_denominator
            and queue_config.rate_period
        ):
            await self.task_submit.submit_rate_limited_task(task=task, queue_config=queue_config)
        else:
            await self.task_submit.submit_task(task=task)

    async def init_fan_in(self, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400) -> None:
        await self.task_state.init_fan_in(fan_in_key, predecessor_ids, ttl)

    async def add_cron_dag(self, entry: CronDAGEntry) -> None:
        """
        Register a new (or replace an existing) cron DAG entry and schedule its first run.

        The first run is computed from ``entry.cron_expr`` relative to now.
        If an entry with the same ``id`` already exists it is overwritten and
        its schedule is reset.
        """
        now = dt.datetime.now(dt.UTC)
        first_run_at = croniter(entry.cron_expr, now).get_next(dt.datetime)
        await self.cron_dag_scheduler.add(entry, first_run_at)
        logger.info(
            "Cron DAG entry '%s' (%s) registered, first run at %s.", entry.name, entry.id, first_run_at
        )

    async def remove_cron_dag(self, cron_id: ULID) -> None:
        """Remove a cron DAG entry from the schedule."""
        await self.cron_dag_scheduler.remove(cron_id)
        logger.info("Cron DAG entry %s removed.", cron_id)

    async def submit_dag(self, *roots: DAGNode) -> tuple[ULID, list[Task]]:
        """
        Initialise fan-in sets and submit all root tasks of a DAG.

        All fan-in Redis sets are populated *before* any task is enqueued so
        that a fast-completing predecessor cannot decrement a set that does not
        yet exist.
        """
        all_fan_ins: dict[str, set[ULID]] = {}
        for root in roots:
            for key, ids in root.fan_in_predecessors().items():
                all_fan_ins.setdefault(key, set()).update(ids)

        await asyncio.gather(*(self.init_fan_in(k, ids) for k, ids in all_fan_ins.items()))

        dag_run_id = ULID()
        submitted: list[Task] = []
        for root in roots:
            task = root.to_task(dag_run_id=dag_run_id)
            await self.submit_task(task)
            submitted.append(task)
        return dag_run_id, submitted

    async def list_dag_runs(self, pagination: DAGRunPagination) -> tuple[list[tuple[ULID, dt.datetime]], int]:
        """Return a paginated list of DAG runs ordered by submission time."""
        return await self.task_state.get_dag_runs(pagination)

    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run, or None if not found."""
        return await self.task_state.get_dag_run(dag_run_id)

    async def delete_task(self, task: Task) -> None:
        """Delete a task record and remove it from all indexes."""
        await self.task_state.delete_task(task)

    async def save_task(self, task: Task) -> Task:
        """Save the task state without extra validation."""
        if self._atomic_state is not None:
            pipe = self._atomic_state.pipeline(transaction=True)
            self._atomic_state.stage_save(pipe, task)
            await pipe.execute()
        else:
            await self.task_state.save_task(task)
        return task

    async def update_task_heartbeat(self, task: Task) -> None:
        """Update the heartbeat timestamp for a task."""
        task.heartbeat_at = dt.datetime.now(dt.UTC)
        await self.task_state.update_task_heartbeat(task)

    async def remove_task_heartbeat(self, task: Task) -> None:
        """Remove a task from the heartbeat sorted set."""
        await self.task_state.remove_task_heartbeat(task)

    async def get_active_tasks(self, queues: set[str]) -> list[Task]:
        """Return all tasks currently present in any heartbeat sorted set."""
        return await self.task_state.get_active_tasks(queues)


class SubmissionRateLimiter:
    """Concurrency guard: filters task queues down to those below their max_concurrent limit."""

    def __init__(self, get_queue_config: Callable[[str], Awaitable[QueueConfig | None]]) -> None:
        self._get_queue_config = get_queue_config

    async def concurrency_limits(
        self, task_queues: set[str], current_tasks_by_queue: dict[str, set[ULID]]
    ) -> set[str]:
        """Limit the number of concurrent tasks in each queue."""
        queues_to_use = set()
        queues = list(task_queues)
        configs = await asyncio.gather(*(self._get_queue_config(q) for q in queues))
        for queue, config in zip(queues, configs, strict=True):
            if config and config.max_concurrent:
                if len(current_tasks_by_queue[queue]) < config.max_concurrent:
                    queues_to_use.add(queue)
            else:
                queues_to_use.add(queue)

        return queues_to_use
