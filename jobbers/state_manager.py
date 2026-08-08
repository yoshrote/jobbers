from __future__ import annotations

import asyncio
import datetime as dt
import logging
import random
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from croniter import croniter
from opentelemetry import metrics
from ulid import ULID

from jobbers import registry
from jobbers.models.cron_dag import ConcurrencyPolicy
from jobbers.models.dag import (
    DAGCancelResult,
    DAGCancelTaskResult,
    DAGResumePrecheck,
    DAGResumeReason,
    DAGResumeResult,
    DynamicFanOutCallback,
    FanInCallback,
    collect_fan_in_keys,
)
from jobbers.models.task import Task
from jobbers.models.task_config import DeadLetterPolicy
from jobbers.models.task_routing import RoutingStrategy
from jobbers.models.task_status import TaskStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

    from jobbers.models.cron_dag import CronDAGEntry
    from jobbers.models.dag import DAGNode, DAGRunDetail, DagRunOutcome, DAGRunPagination, DAGRunSummary
    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task_routing import RoutingConfig
    from jobbers.protocols import (
        AtomicCronDAGSchedulerProtocol,
        AtomicDagRunProtocol,
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


def _dag_run_uses_fan_in(tasks: list[Task]) -> bool:
    """
    Whether any task in a DAG run registers a fan-in edge.

    Used by StateManager.can_resume_dag_run to decide whether a missing
    DAG_RUN_FANIN tracking hash is actually a problem -- a run built entirely from
    SimpleCallback chains never calls init_fan_in, so it never had one to expire.
    """
    return any(
        isinstance(cb, (FanInCallback, DynamicFanOutCallback)) for t in tasks for cb in t.dag_callbacks
    )


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


@dataclass
class _CancelHandle:
    """Per-in-flight-task cancel wait state, registered for the duration of TaskProcessor.run()."""

    event: asyncio.Event
    dag_run_id: ULID | None


class TaskException(Exception):
    "Top-level exception for stuff gone wrong."

    pass


class UserCancellationError(Exception):
    """Exception raised when a task is cancelled by user request."""

    pass


class TaskRateLimitedError(TaskException):
    """Raised by submit_task() when the queue's rate limiter rejects the submission."""

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
        self._cancel_events: dict[ULID, _CancelHandle] = {}
        self.dead_queue: DeadQueueProtocol = dead_queue
        self.task_scheduler = task_scheduler
        self.cron_dag_scheduler = cron_dag_scheduler

        # Same-backend detection: if the task adapter, scheduler, and DLQ all implement the
        # Atomic sub-protocols, StateManager uses atomic pipelines.  When stores differ, or
        # when force_saga=True, it falls back to saga coordination.
        from jobbers.protocols import (  # local import avoids circular at module level
            AtomicCronDAGSchedulerProtocol,
            AtomicDagRunProtocol,
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
        # DAG-run-terminal-specific: True when the task-state adapter can fold
        # record_dag_run_task_terminal + close_dag_run_task into one pipelined round
        # trip (Redis/RedisJSON). Checked independently of _atomic_mode -- a backend
        # without this (e.g. SQL) still gets full atomic-mode pipelining for
        # everything else, it just doesn't have a "same pipeline" concept for these
        # two calls to fold into.
        self._atomic_dag_run: AtomicDagRunProtocol | None = (
            self._atomic_state
            if (not force_saga and isinstance(self._atomic_state, AtomicDagRunProtocol))
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
    def cancel_event(self, task_id: ULID, dag_run_id: ULID | None = None) -> Iterator[None]:
        self._cancel_events[task_id] = _CancelHandle(asyncio.Event(), dag_run_id)
        try:
            yield
        finally:
            self._cancel_events.pop(task_id, None)

    def signal_cancel(self, task_id: ULID) -> bool:
        handle = self._cancel_events.get(task_id)
        if handle is not None:
            handle.event.set()
            return True
        return False

    def signal_cancel_dag(self, dag_run_id: ULID) -> int:
        """Fire the cancel event for every in-flight task on this worker belonging to dag_run_id."""
        n = 0
        for handle in self._cancel_events.values():
            if handle.dag_run_id == dag_run_id:
                handle.event.set()
                n += 1
        return n

    async def clean(
        self,
        rate_limit_age: dt.timedelta | None = None,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
        stale_time: dt.timedelta | None = None,
        dlq_age: dt.timedelta | None = None,
        completed_task_age: dt.timedelta | None = None,
        recover_orphaned_scheduled: bool = False,
        drop_stale_indexes: bool = False,
        clean_orphaned_dlq: bool = False,
    ) -> None:
        """Clean up the state manager."""
        now = dt.datetime.now(dt.UTC)
        queues = set(await self.get_all_queues())

        clean_ops = []

        if drop_stale_indexes:
            clean_ops.append(self._drop_stale_indexes())
        if clean_orphaned_dlq:
            clean_ops.append(self._clean_orphaned_dlq())
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
            dlq_saga_tasks: list[Task] = []
            for (task_type, task_version), tasks in stale_tasks_by_type.items():
                task_config = registry.get_task_config(task_type, task_version)
                if task_config and task_config.max_heartbeat_interval:
                    needs_dlq = task_config.dead_letter_policy == DeadLetterPolicy.SAVE
                    for task in tasks:
                        if (
                            task.heartbeat_at
                            and (now - task.heartbeat_at) > task_config.max_heartbeat_interval
                        ):
                            # A stalled task never calls generate_callbacks(), so it
                            # never closes out of its DAG run's pending counter — a
                            # run containing it simply stays open (see
                            # TaskProcessor._maybe_cleanup), preserving fan-in
                            # tracking and sibling task records within their TTLs.
                            task.set_status(TaskStatus.STALLED)
                            if self._atomic_state is not None:
                                pipe = self._atomic_state.pipeline(transaction=True)
                                self._atomic_state.stage_save(pipe, task)
                                self._atomic_state.stage_remove_heartbeat(pipe, task)
                                if needs_dlq and self._atomic_dlq is not None:
                                    self._atomic_dlq.stage_add(pipe, task, now)
                                elif needs_dlq:
                                    dlq_saga_tasks.append(task)
                                stale_pipes.append(pipe.execute())
                            else:
                                stale_saga_tasks.append(task)
                                if needs_dlq:
                                    dlq_saga_tasks.append(task)
                            if needs_dlq:
                                logger.info("Task %s sent to dead letter queue.", task.id)
                                tasks_dead_lettered.add(
                                    1, {"queue": task.queue, "task": task.name, "version": task.version}
                                )
            if stale_pipes:
                await asyncio.gather(*stale_pipes)
            for stale_task in stale_saga_tasks:
                await self.task_state.save_task(stale_task)
                await self.task_state.remove_task_heartbeat(stale_task)
            if dlq_saga_tasks:
                await asyncio.gather(*(self.dead_queue.add_to_dlq(t, now) for t in dlq_saga_tasks))

        await asyncio.gather(*clean_ops)

    async def _drop_stale_indexes(self) -> None:
        """Drop RediSearch indexes from older schema generations across all adapters."""
        dropped = []
        for names in await asyncio.gather(
            self.routing.drop_stale_indexes(),
            self.task_state.drop_stale_indexes(),
            self.dead_queue.drop_stale_indexes(),
        ):
            dropped.extend(names)
        if dropped:
            logger.info("Dropped stale RediSearch indexes: %s", dropped)

    async def _clean_orphaned_dlq(self) -> None:
        """Remove DLQ index entries whose task blob no longer exists (Redis/RedisJSON only)."""
        removed = await self.dead_queue.clean_orphaned_entries()
        if removed:
            logger.info("Removed %d orphaned DLQ entries", removed)

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
            # Saga: persist blobs (source of truth), then enqueue via the saga-safe
            # enqueue() — not submit_task(), whose "skip if blob exists" guard would
            # silently no-op the ZADD once save_task() has already written the blob.
            # Then remove from DLQ. If remove fails, Cleaner reconciles DLQ entries
            # for SUBMITTED/STARTED tasks.
            await asyncio.gather(*(self.task_state.save_task(t) for t in tasks))
            await asyncio.gather(*(self.task_submit.enqueue(t) for t in tasks))
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
          policy and stages the active-run marker write for SKIP_IF_RUNNING.

        Callers must already hold the entry's dispatch lock (see ``dispatch_cron_dag``)
        before calling this — that lock, not this method, is what actually serializes
        concurrent dispatchers, so the active-run marker write below is a plain
        unconditional set (``nx=False``), not a race guard in its own right.
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
                # Scheduler supports pipeline staging. nx=False: the caller's dispatch
                # lock already serializes concurrent dispatchers, so this is just a
                # plain set, not a race guard (see _concurrency_guard's docstring).
                _atomic_sched = self.cron_dag_scheduler  # narrow for mypy

                def _stage_skip(pipe: TransactionHandle, task_id: ULID) -> None:
                    _atomic_sched.stage_set_active_run(pipe, entry.id, task_id, nx=False)

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

        The whole method runs under a short-TTL dispatch lock (``try_acquire_dispatch_lock``)
        so two dispatchers racing to fire the same due occurrence (e.g. during a rolling
        scheduler restart) can't both submit a duplicate run — see
        ``CronDAGSchedulerProtocol.try_acquire_dispatch_lock``'s docstring for why this is
        a separate, much shorter-lived lock than the SKIP_IF_RUNNING active-run marker.
        """
        if not await self.cron_dag_scheduler.try_acquire_dispatch_lock(entry.id):
            logger.info("Cron entry %s: another dispatcher already claimed this run; skipping.", entry.id)
            return
        try:
            await self._dispatch_cron_dag_locked(entry, run_at)
        finally:
            await self.cron_dag_scheduler.release_dispatch_lock(entry.id)

    async def _dispatch_cron_dag_locked(self, entry: CronDAGEntry, run_at: dt.datetime) -> None:
        """Run dispatch_cron_dag's body while its dispatch lock is held."""
        next_run_at = croniter(entry.cron_expr, run_at).get_next(dt.datetime)

        async with self._concurrency_guard(entry, next_run_at) as stager:
            if stager.skipped:
                return

            fresh_spec, _ = entry.dag_spec.fresh_copy()
            fan_ins = collect_fan_in_keys(fresh_spec)
            queue_config = await self.get_queue_config(fresh_spec.queue)

            dag_run_id = ULID()
            task = Task(
                id=fresh_spec.id,
                name=fresh_spec.name,
                queue=fresh_spec.queue,
                version=fresh_spec.version,
                parameters=fresh_spec.parameters,
                dag_callbacks=fresh_spec.dag_callbacks,
                cron_id=entry.id,
                dag_run_id=dag_run_id,
                dag_run_name=f"{entry.name} @ {run_at.isoformat()}",
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
                    self._atomic_state.stage_init_fan_in(  # type: ignore[union-attr]
                        pipe, dag_run_id, fan_in_key, predecessor_ids
                    )
                if not is_rate_limited:
                    self.stage_submit_task(pipe, task, queue_config)
                await pipe.execute()
                if is_rate_limited:
                    await self._dispatch_cron_root_task(entry, task)
            elif isinstance(self.cron_dag_scheduler, AtomicCronDAGSchedulerProtocol):
                # Cross-backend: cron ops are atomic internally; task-state ops follow.
                cron_pipe = self.cron_dag_scheduler.pipeline(transaction=True)
                self.cron_dag_scheduler.stage_reschedule(cron_pipe, entry.id, next_run_at)
                stager.stage_active_run(cron_pipe, task.id)
                await cron_pipe.execute()
                await asyncio.gather(
                    *(self.task_state.init_fan_in(dag_run_id, k, ids) for k, ids in fan_ins.items())
                )
                await self._dispatch_cron_root_task(entry, task)
            else:
                # No pipeline support (e.g. StaticCronDAGScheduler): sequential calls.
                # nx=False: the dispatch lock already serializes concurrent dispatchers.
                await self.cron_dag_scheduler.reschedule(entry.id, next_run_at)
                if entry.concurrency_policy == ConcurrencyPolicy.SKIP_IF_RUNNING:
                    await self.cron_dag_scheduler.set_active_run(entry.id, task.id, nx=False)
                await asyncio.gather(
                    *(self.task_state.init_fan_in(dag_run_id, k, ids) for k, ids in fan_ins.items())
                )
                await self._dispatch_cron_root_task(entry, task)
            logger.info("Cron entry %s dispatched as task %s (run_at=%s).", entry.id, task.id, run_at)

    async def _dispatch_cron_root_task(self, entry: CronDAGEntry, task: Task) -> None:
        """
        Submit a cron DAG's root task, swallowing (and logging) a rate-limit rejection.

        The cron entry has already been rescheduled by the time this runs, so a
        rejected root task only skips this one firing -- the entry's next run is
        unaffected. Letting TaskRateLimitedError propagate here would crash the
        Scheduler's dispatch loop, since it runs via an unguarded asyncio.gather().
        """
        try:
            await self.submit_task(task)
        except TaskRateLimitedError:
            logger.warning(
                "Cron entry %s's root task %s was rejected by queue '%s' rate limiting; skipping this run.",
                entry.id,
                task.id,
                task.queue,
            )

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

    async def request_dag_cancellation(self, dag_run_id: ULID) -> DAGCancelResult | None:
        """
        Cancel every non-terminal task belonging to a DAG run.

        Marks the run cancelling (idempotent) before touching any task, so the
        TaskProcessor.post_process / _handle_retry gates (which check
        is_dag_run_cancelling) start rejecting new descendants/retries as early as
        possible. SCHEDULED/SUBMITTED (and the practically-unreachable UNSUBMITTED —
        see docs/dag-cancellation-design.md) tasks are cancelled directly in this
        sweep; STARTED tasks are left to a single trailing publish_dag_cancellation
        broadcast instead of one publish per task, so a large fan-out's worth of
        concurrently-running arms is cancelled with one pub/sub message.

        Returns None if the run does not exist.
        """
        run = await self.get_dag_run(dag_run_id)
        if run is None:
            return None
        await self.task_state.mark_dag_run_cancelling(dag_run_id)

        fetched = await self.task_state.get_tasks_bulk(run.task_ids)

        already_terminal: list[Task] = []
        scheduled: list[Task] = []
        submitted_or_unsubmitted: list[Task] = []
        started: list[Task] = []
        for task in fetched:
            if task is None:
                continue
            if task.status in TaskStatus.terminal_statuses():
                already_terminal.append(task)
            elif task.status == TaskStatus.STARTED:
                started.append(task)
            elif task.status == TaskStatus.SCHEDULED:
                scheduled.append(task)
            else:
                # SUBMITTED, or UNSUBMITTED (practically unreachable via persisted
                # storage -- see docs/dag-cancellation-design.md §4.4). Cancelling
                # directly with a best-effort, harmless-if-absent queue removal
                # covers both the same way.
                submitted_or_unsubmitted.append(task)

        to_cancel = scheduled + submitted_or_unsubmitted
        for task in to_cancel:
            task.set_status(TaskStatus.CANCELLED)

        if to_cancel:
            if self._atomic_state is not None and self._atomic_scheduler is not None:
                pipe = self._atomic_state.pipeline(transaction=True)
                for task in scheduled:
                    self._atomic_scheduler.stage_remove(pipe, task.id, task.queue)
                    self._atomic_state.stage_save(pipe, task)
                for task in submitted_or_unsubmitted:
                    # A no-op ZREM/DELETE for a task never enqueued (UNSUBMITTED) —
                    # harmless, same as removing an already-absent queue member.
                    self._atomic_state.stage_remove_from_queue(pipe, task)
                    self._atomic_state.stage_save(pipe, task)
                await pipe.execute()
            else:
                for task in scheduled:
                    await self.task_scheduler.remove(task.id, task.queue)
                    await self.task_state.save_task(task)
                for task in submitted_or_unsubmitted:
                    await self.task_state.save_task(task)

            # These tasks never go through TaskProcessor (which is what normally
            # calls finalize_dag_run_task -> record_dag_run_task_terminal on a
            # terminal transition), so the run's aggregate completed/failed
            # counters would otherwise never reflect them -- and get_dag_run's
            # CANCELLING-vs-CANCELLED derivation (§4.4) depends on those counters
            # reaching len(task_ids) to ever report CANCELLED. Mirror what
            # finalize_dag_run_task would have recorded: CANCELLED is a stuck
            # status, so only the counters are updated -- these tasks intentionally
            # stay in DAG_RUN_PENDING like any other stuck-status task.
            await asyncio.gather(*(self.record_dag_run_task_terminal(t) for t in to_cancel))

        if started:
            await self.cancellation_bus.publish_dag_cancellation(dag_run_id)

        tasks_result = (
            [DAGCancelTaskResult(task_id=t.id, status="already_terminal") for t in already_terminal]
            + [DAGCancelTaskResult(task_id=t.id, status="cancelled") for t in to_cancel]
            + [DAGCancelTaskResult(task_id=t.id, status="signalled") for t in started]
        )
        return DAGCancelResult(
            dag_run_id=dag_run_id,
            already_terminal=len(already_terminal),
            cancelled_immediately=len(to_cancel),
            signalled_running=len(started),
            tasks=tasks_result,
        )

    async def can_resume_dag_run(self, dag_run_id: ULID) -> DAGResumePrecheck:
        """
        Read-only check for whether a DAG run can be resumed right now (docs/dag-resume-design.md §4.1).

        Does not mutate anything -- safe to call repeatedly (e.g. to drive a "Resume"
        button's enabled state). ``resume_dag_run`` re-derives the same checks itself
        rather than trusting an earlier precheck result, since the two are separate
        round trips and state could change in between.
        """
        run = await self.get_dag_run(dag_run_id)
        if run is None:
            return DAGResumePrecheck(
                dag_run_id=dag_run_id, resumable=False, reason=DAGResumeReason.DAG_RUN_NOT_FOUND_OR_EXPIRED
            )

        fetched = await self.task_state.get_tasks_bulk(run.task_ids)
        if any(t is None for t in fetched):
            return DAGResumePrecheck(
                dag_run_id=dag_run_id, resumable=False, reason=DAGResumeReason.TASK_HISTORY_INCOMPLETE
            )
        run_tasks = cast("list[Task]", fetched)

        stuck = [t for t in run_tasks if t.status in TaskStatus.stuck_statuses()]
        if not stuck:
            return DAGResumePrecheck(dag_run_id=dag_run_id, resumable=False, reason=DAGResumeReason.NO_STUCK_TASKS)

        if _dag_run_uses_fan_in(run_tasks) and not await self.task_state.dag_run_fan_in_alive(dag_run_id):
            return DAGResumePrecheck(
                dag_run_id=dag_run_id, resumable=False, reason=DAGResumeReason.FAN_IN_TRACKING_EXPIRED
            )

        return DAGResumePrecheck(dag_run_id=dag_run_id, resumable=True, stuck_task_ids=[t.id for t in stuck])

    async def resume_dag_run(self, dag_run_id: ULID) -> DAGResumeResult:
        """
        Retry every stuck task in a DAG run from its stored parameters (docs/dag-resume-design.md §4.3).

        Reuses each stuck task's existing blob (parameters, dag_callbacks, parent_ids
        unchanged) rather than reading from the DLQ, so this works regardless of
        dead_letter_policy and covers CANCELLED tasks, which are never DLQ'd. Once a
        resumed task completes, the normal TaskProcessor.post_process ->
        generate_callbacks() path continues the DAG exactly as it would on a first
        attempt -- no separate graph-replay logic is needed here.

        Raises TaskException if the run isn't currently resumable; see
        can_resume_dag_run for the specific reasons.
        """
        precheck = await self.can_resume_dag_run(dag_run_id)
        if not precheck.resumable:
            raise TaskException(f"DAG run {dag_run_id} is not resumable: {precheck.reason}")

        if await self.is_dag_run_cancelling(dag_run_id):
            # Must happen before the resumed tasks go SUBMITTED, or TaskProcessor's
            # post_process/_handle_retry gates (docs/dag-cancellation-design.md §4.4)
            # would keep treating this run as cancelling and suppress their
            # descendants/retries the moment they run again.
            await self.task_state.clear_dag_run_cancellation(dag_run_id)

        await self.task_state.refresh_dag_run_fan_in_ttl(dag_run_id)

        fetched = await self.task_state.get_tasks_bulk(precheck.stuck_task_ids)
        stuck_tasks = cast("list[Task]", [t for t in fetched if t is not None])

        # finalize_dag_run_task recorded exactly one 'failed' increment per stuck task
        # when it first became stuck (see §2.2) -- undo all of those in one round trip
        # before resubmitting, or the run can never report 'complete' again even if
        # every resumed task goes on to succeed.
        if stuck_tasks:
            await self.task_state.reconcile_dag_run_task_retry(dag_run_id, count=len(stuck_tasks))

        now = dt.datetime.now(dt.UTC)
        for task in stuck_tasks:
            task.errors.append(f"--- resumed by operator, dag_run_id={dag_run_id}, at {now.isoformat()} ---")
            task.retry_attempt = 0
            task.set_status(TaskStatus.SUBMITTED)

        if self._atomic_state is not None:
            pipe = self._atomic_state.pipeline(transaction=True)
            for task in stuck_tasks:
                self._atomic_state.stage_requeue(pipe, task)
            await pipe.execute()
        else:
            # Saga: blob is the source of truth, write it before the queue pointer
            # that references it (same ordering rationale as resubmit_dead_tasks).
            await asyncio.gather(*(self.task_state.save_task(t) for t in stuck_tasks))
            await asyncio.gather(*(self.task_submit.enqueue(t) for t in stuck_tasks))

        return DAGResumeResult(dag_run_id=dag_run_id, resumed_task_ids=[t.id for t in stuck_tasks])

    async def monitor_task_cancellation(self, task_id: ULID) -> None:
        """Wait for a cancel signal for this task and raise UserCancellationError when it arrives."""
        handle = self._cancel_events.get(task_id)
        if handle is None:
            return
        await handle.event.wait()
        logger.info("Received cancellation signal for task %s", task_id)
        raise UserCancellationError(f"Task {task_id} was cancelled by user request.")

    async def run_cancel_listener(self) -> None:
        """Subscribe to the shared cancellations channel and signal matching active tasks."""
        async for msg in self.cancellation_bus.listen_cancellations():
            if msg.kind == "task":
                self.signal_cancel(msg.id)
            else:
                self.signal_cancel_dag(msg.id)

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
            # Saga: persist blobs first (source of truth), then enqueue via the
            # saga-safe enqueue() — not submit_task(), whose "skip if blob exists"
            # guard would silently no-op the ZADD once save_task() has already
            # written the blob.
            await asyncio.gather(*(self.task_state.save_task(t) for t in tasks))
            await asyncio.gather(*(self.task_submit.enqueue(t) for t in tasks))

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
            await self.task_submit.enqueue(task)

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

    async def create_queue_config(self, queue_config: QueueConfig) -> bool:
        """Atomically create a new queue config. Returns False if the name already exists."""
        created = await self.routing.create_queue_config(queue_config)
        if created:
            self.invalidate_queue_config(queue_config.name)
        return created

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

    async def create_role(self, role: str, queues_set: set[str]) -> bool:
        """Atomically create a new role. Returns False if the role already exists."""
        created = await self.routing.create_role(role, queues_set)
        if created:
            await self.routing_notifications.bump_refresh_tag(role)
        return created

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
        """
        Resolve the task's queue and submit it, respecting rate limiting.

        Raises TaskRateLimitedError if the queue's rate limiter rejects the
        submission; the task's status is reverted to its pre-call value in that case.
        """
        task.queue = await self.resolve_queue(task)
        queue_config = await self.get_queue_config(task.queue)
        is_rate_limited = bool(
            queue_config
            and queue_config.rate_numerator
            and queue_config.rate_denominator
            and queue_config.rate_period
        )
        previous_status = task.status
        # Must precede submit: sets submitted_at, which the adapters assert is present.
        task.set_status(TaskStatus.SUBMITTED)
        if is_rate_limited:
            assert queue_config is not None  # noqa: S101
            accepted = await self.task_submit.submit_rate_limited_task(task=task, queue_config=queue_config)
        else:
            accepted = await self.task_submit.submit_task(task=task)
        if not accepted:
            task.status = previous_status
            raise TaskRateLimitedError(
                f"Queue '{task.queue}' rate limit exceeded; task {task.id} was not submitted."
            )

    async def init_fan_in(
        self, dag_run_id: ULID, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None:
        await self.task_state.init_fan_in(dag_run_id, fan_in_key, predecessor_ids, ttl)

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

    async def submit_dag(
        self, *roots: DAGNode, name: str, dag_run_id: ULID | None = None
    ) -> tuple[ULID, list[Task]]:
        """
        Initialise fan-in sets and submit all root tasks of a DAG.

        All fan-in Redis sets are populated *before* any task is enqueued so
        that a fast-completing predecessor cannot decrement a set that does not
        yet exist.

        `name` is required here — callers must resolve any user-facing default
        (e.g. `jobbers/task_routes.py`'s `submit_dag` route defaults an omitted
        name to `str(dag_run_id)`) before calling this method. `dag_run_id` is an
        optional override so a caller that needs to know the ID in advance (in
        order to build that default) can supply it; every other caller lets this
        method self-generate one exactly as before.
        """
        all_fan_ins: dict[str, set[ULID]] = {}
        for root in roots:
            for key, ids in root.fan_in_predecessors().items():
                all_fan_ins.setdefault(key, set()).update(ids)

        dag_run_id = dag_run_id or ULID()
        await asyncio.gather(*(self.init_fan_in(dag_run_id, k, ids) for k, ids in all_fan_ins.items()))

        submitted: list[Task] = []
        for root in roots:
            task = root.to_task(dag_run_id=dag_run_id, dag_run_name=name)
            await self.submit_task(task)
            submitted.append(task)
        return dag_run_id, submitted

    async def list_dag_runs(self, pagination: DAGRunPagination) -> tuple[list[DAGRunSummary], int]:
        """Return a paginated list of DAG runs ordered by submission time."""
        return await self.task_state.get_dag_runs(pagination)

    async def get_dag_run(self, dag_run_id: ULID) -> DAGRunDetail | None:
        """Return details for a DAG run, or None if not found."""
        return await self.task_state.get_dag_run(dag_run_id)

    async def finalize_dag_run_task(self, task: Task) -> None:
        """
        Record *task*'s terminal outcome, close it out of its run's pending set, and sweep.

        This is the single entry point TaskProcessor calls per DAG-task completion
        (any terminal status). It owns the ordering between the two steps so callers
        don't have to reproduce it: record_dag_run_task_terminal's recompute
        unconditionally rewrites status to running/partial_failure/failed (never
        complete), so running it *after* mark_dag_run_complete on the run's last task
        would immediately clobber that 'complete' write back to a non-terminal
        status. Calling record first for every task, and only ever calling
        mark_dag_run_complete afterwards, means the 'complete' write is always the
        last one to happen for a given task — and since pending only reaches zero
        once every sibling's own close has already run (each preceded by its own
        record call, in-order within that sibling's coroutine), every sibling's
        record call is guaranteed to have already posted by the time the run's
        pending count reaches zero, regardless of how concurrent completions
        interleave.

        A DAG task in a stuck status (``TaskStatus.stuck_statuses()`` — FAILED,
        STALLED, CANCELLED, or DROPPED) never closes out of the pending counter: such
        a task never calls ``generate_callbacks()``, so its ``FanInCallback``/
        ``DynamicFanOutCallback`` never fires and the DAG can't complete on its own.
        Leaving the counter open preserves the run's fan-in tracking and sibling task
        records (within their TTLs) instead of sweeping them away.

        When the task-state adapter implements ``AtomicDagRunProtocol`` (Redis/
        RedisJSON), the record + close steps for non-stuck tasks are folded into one
        pipelined round trip instead of two sequential ones.
        """
        assert task.dag_run_id is not None  # noqa: S101
        if task.status in TaskStatus.stuck_statuses():
            await self.record_dag_run_task_terminal(task)
            return
        if self._atomic_dag_run is None:
            await self.record_dag_run_task_terminal(task)
            await self.close_dag_run_task_and_sweep(task)
            return
        remaining = await self._record_terminal_and_close_atomic(task)
        await self._finish_dag_run_if_terminal(task, remaining)

    async def _record_terminal_and_close_atomic(self, task: Task) -> int:
        """
        Fold record_dag_run_task_terminal + close_dag_run_task into one pipelined round trip.

        Only called from finalize_dag_run_task for non-stuck tasks, so outcome is
        always 'completed' here (stuck tasks return before reaching this path).
        """
        assert task.dag_run_id is not None  # noqa: S101
        assert self._atomic_dag_run is not None  # noqa: S101
        pipe = self._atomic_dag_run.pipeline(transaction=True)
        await self._atomic_dag_run.stage_record_dag_run_task_terminal(pipe, task.dag_run_id, "completed")
        await self._atomic_dag_run.stage_close_dag_run_task(pipe, task.dag_run_id, task.id)
        results = await pipe.execute()
        close_result = cast("list[int]", results[-1])
        return int(close_result[1])

    async def record_dag_run_task_terminal(self, task: Task) -> None:
        """
        Record *task*'s terminal outcome against its run's aggregate status counters.

        Wholly additive: does not read or write DAG_RUN_PENDING/CLOSED. Called once
        per terminal DAG task for every terminal status (unlike close_dag_run_task_
        and_sweep, which only fires for non-stuck statuses). Prefer
        ``finalize_dag_run_task`` at call sites — it owns the ordering between this
        and ``close_dag_run_task_and_sweep``.
        """
        assert task.dag_run_id is not None  # noqa: S101
        outcome: DagRunOutcome = "failed" if task.status in TaskStatus.stuck_statuses() else "completed"
        await self.task_state.record_dag_run_task_terminal(task.dag_run_id, outcome)

    async def mark_dag_run_complete(self, dag_run_id: ULID) -> None:
        """Set the run's status to complete iff it has never recorded a failed task."""
        await self.task_state.mark_dag_run_complete(dag_run_id)

    async def mark_dag_run_cancelling(self, dag_run_id: ULID) -> None:
        """Idempotently record that cancellation was requested for this run."""
        await self.task_state.mark_dag_run_cancelling(dag_run_id)

    async def is_dag_run_cancelling(self, dag_run_id: ULID) -> bool:
        """Cheap check: has cancellation been requested for this run."""
        return await self.task_state.is_dag_run_cancelling(dag_run_id)

    async def close_dag_run_task(self, dag_run_id: ULID, task_id: ULID) -> int:
        """Mark task_id resolved in the DAG run's pending set; return the remaining count."""
        return await self.task_state.close_dag_run_task(dag_run_id, task_id)

    async def sweep_dag_run(self, dag_run_id: ULID, *, fallback_task: Task | None = None) -> None:
        """
        Delete every task in a now-fully-terminal DAG run whose config says to clean up.

        If the run's index is orphaned (already swept, e.g. by the age-based
        ``clean_dag_runs`` purge racing this call), fall back to applying
        *fallback_task*'s own ``cleanup_on`` policy to just that task.

        **Precondition:** callers must only pass *fallback_task* once they have
        independently confirmed the run is fully terminal (e.g. ``close_dag_run_task``
        just returned a remaining count of 0 for it). This method does not — and, once
        the run's index is orphaned, cannot — re-verify that on its own; passing
        *fallback_task* for a run that still has siblings in flight would delete a
        task those siblings may still depend on (e.g. via ``parent_results()``).
        The only production caller, ``_finish_dag_run_if_terminal`` (via either
        ``close_dag_run_task_and_sweep`` or ``finalize_dag_run_task``'s pipelined
        path), already guarantees this.
        """
        run = await self.get_dag_run(dag_run_id)
        if run is None:
            if fallback_task is not None:
                cfg = registry.get_task_config(fallback_task.name, fallback_task.version)
                if cfg and cfg.cleanup_on and fallback_task.status in cfg.cleanup_on:
                    await self.delete_task(fallback_task)
            return

        sibling_tasks = await self.task_state.get_tasks_bulk(run.task_ids)
        to_delete: list[Task] = []
        for sibling in sibling_tasks:
            if sibling is None:
                continue
            cfg = registry.get_task_config(sibling.name, sibling.version)
            if cfg and cfg.cleanup_on and sibling.status in cfg.cleanup_on:
                to_delete.append(sibling)
        if to_delete:
            await asyncio.gather(*(self.delete_task(t) for t in to_delete))

    async def close_dag_run_task_and_sweep(self, task: Task) -> None:
        """
        Close *task* out of its DAG run's pending set and sweep siblings if the run just went terminal.

        Only called for tasks reaching a non-stuck terminal status (see
        ``TaskProcessor._maybe_cleanup``) — FAILED/STALLED/CANCELLED/DROPPED tasks never
        close out of ``DAG_RUN_PENDING`` at all, so a run containing one simply never
        reaches a pending count of zero and this method is never invoked for it.
        """
        assert task.dag_run_id is not None  # noqa: S101
        remaining = await self.close_dag_run_task(task.dag_run_id, task.id)
        await self._finish_dag_run_if_terminal(task, remaining)

    async def _finish_dag_run_if_terminal(self, task: Task, remaining: int) -> None:
        """
        Mark the run complete and sweep siblings once *remaining* hits 0.

        *remaining* comes from close_dag_run_task, staged or not. Since only
        ``COMPLETED`` tasks ever reach here, reaching a pending count of
        zero means every task the run has ever registered completed successfully --
        so this also marks the run's aggregate status ``complete`` (via
        ``mark_dag_run_complete``, itself gated on the run having never recorded a
        failed task, defensively).
        """
        assert task.dag_run_id is not None  # noqa: S101
        if remaining != 0:
            # >0: DAG still in flight, the last task to close will trigger the sweep.
            # -1: already closed (duplicate call) or the run's pending entry expired/was
            # swept — either way, do not re-trigger the sweep from here.
            return
        await self.mark_dag_run_complete(task.dag_run_id)
        await self.sweep_dag_run(task.dag_run_id, fallback_task=task)

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
            # Deliberately truthy, not `is not None`: max_concurrent=0 means unlimited,
            # same as None -- see QueueConfig.max_concurrent's docstring.
            if config and config.max_concurrent:
                if len(current_tasks_by_queue[queue]) < config.max_concurrent:
                    queues_to_use.add(queue)
            else:
                queues_to_use.add(queue)

        return queues_to_use
