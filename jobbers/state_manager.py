from __future__ import annotations

import asyncio
import datetime as dt
import logging
import random
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

from opentelemetry import metrics
from redis.exceptions import WatchError
from ulid import ULID

from jobbers import registry
from jobbers.adapters.json_redis import JsonTaskAdapter
from jobbers.adapters.raw_redis import DeadQueue
from jobbers.models.cron_dag_scheduler import CronDAGScheduler
from jobbers.models.task_config import DeadLetterPolicy
from jobbers.models.task_routing import RoutingStrategy
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_status import TaskStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.adapters.routing_backend import RoutingBackendProtocol
    from jobbers.adapters.task_adapter import DeadQueueProtocol, TaskAdapterProtocol
    from jobbers.models.cron_dag import CronDAGEntry
    from jobbers.models.cron_dag_scheduler import ConcurrencyStager
    from jobbers.models.dag import DAGNode, DAGRunPagination
    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task import Task
    from jobbers.models.task_routing import RoutingConfig

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
tasks_dead_lettered = meter.create_counter("tasks_dead_lettered", unit="1")


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
        job_store: Redis,
        routing_backend: RoutingBackendProtocol,
        task_adapter: TaskAdapterProtocol | None = None,
    ) -> None:
        self.job_store: Redis = job_store
        self.routing: RoutingBackendProtocol = routing_backend
        self.ta: TaskAdapterProtocol = (
            task_adapter if task_adapter is not None else JsonTaskAdapter(job_store)
        )
        self._queue_config_cache: dict[str, QueueConfig | None] = {}
        self._routing_config_cache: dict[tuple[str, int], RoutingConfig | None] = {}
        self.submission_limiter = SubmissionRateLimiter(job_store, self.get_queue_config)
        self.current_tasks_by_queue: dict[str, set[ULID]] = defaultdict(set)
        self.dead_queue: DeadQueueProtocol = DeadQueue(job_store, self.ta)
        self.task_scheduler = TaskScheduler(job_store, self.ta, self.get_all_queues)
        self.cron_dag_scheduler = CronDAGScheduler(job_store)

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
            # TODO: Do we need to clean this up before handle_success?
            # Need to consider interactions with callbacks of the task
            self.current_tasks_by_queue[task.queue].remove(task.id)

    async def clean(
        self,
        rate_limit_age: dt.timedelta | None = None,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
        stale_time: dt.timedelta | None = None,
        dlq_age: dt.timedelta | None = None,
        completed_task_age: dt.timedelta | None = None,
    ) -> None:
        """Clean up the state manager."""
        now = dt.datetime.now(dt.UTC)
        queues = {q.encode() for q in await self.get_all_queues()}

        clean_ops = []
        if rate_limit_age:
            clean_ops.append(self.submission_limiter.clean(queues, now, rate_limit_age))

        if max_queue_age or min_queue_age:
            clean_ops.append(self.ta.clean(queues, now, min_queue_age, max_queue_age))

        if dlq_age:
            clean_ops.append(self.dead_queue.clean(now - dlq_age))

        if completed_task_age:
            clean_ops.append(self.ta.clean_terminal_tasks(now, completed_task_age))
            clean_ops.append(self.ta.clean_dag_runs(now, completed_task_age))

        if stale_time:
            stale_tasks_by_type: dict[tuple[str, int], list[Task]] = defaultdict(list)
            async for task in self.ta.get_stale_tasks({q.decode() for q in queues}, stale_time):
                if task.status != TaskStatus.STARTED:
                    continue
                stale_tasks_by_type[(task.name, task.version)].append(task)

            stale_pipes = []
            for (task_type, task_version), tasks in stale_tasks_by_type.items():
                task_config = registry.get_task_config(task_type, task_version)
                if task_config and task_config.max_heartbeat_interval:
                    for task in tasks:
                        if (
                            task.heartbeat_at
                            and (now - task.heartbeat_at) > task_config.max_heartbeat_interval
                        ):
                            task.set_status(TaskStatus.STALLED)
                            pipe = self.job_store.pipeline(transaction=True)
                            self.ta.stage_save(pipe, task)
                            pipe.zrem(self.ta.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))
                            stale_pipes.append(pipe.execute())
            if stale_pipes:
                await asyncio.gather(*stale_pipes)

        await asyncio.gather(*clean_ops)

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        if not queues:
            logger.info("no queues defined")
            return None

        queues = await self.submission_limiter.concurrency_limits(queues, self.current_tasks_by_queue)

        return await self.ta.get_next_task(queues, pop_timeout)

    async def resubmit_dead_tasks(self, tasks: list[Task], reset_retry_count: bool = True) -> list[Task]:
        """Re-enqueue DLQ tasks and remove them from the DLQ in a single atomic transaction."""
        for task in tasks:
            if reset_retry_count:
                task.retry_attempt = 0
            task.errors = []
            task.set_status(TaskStatus.SUBMITTED)
        pipe = self.job_store.pipeline(transaction=True)
        for task in tasks:
            self.ta.stage_requeue(pipe, task)
            self.dead_queue.stage_remove(pipe, task.id, task.queue, task.name)
        await pipe.execute()
        return tasks

    async def fail_task(self, task: Task) -> Task:
        """Persist a failed task and any DLQ side effects in a single atomic transaction."""
        now = dt.datetime.now(dt.UTC)
        pipe = self.job_store.pipeline(transaction=True)
        self.ta.stage_save(pipe, task)
        if task.task_config and task.task_config.dead_letter_policy == DeadLetterPolicy.SAVE:
            logger.info("Task %s sent to dead letter queue.", task.id)
            self.dead_queue.stage_add(pipe, task, now)
        await pipe.execute()
        if task.task_config and task.task_config.dead_letter_policy == DeadLetterPolicy.SAVE:
            tasks_dead_lettered.add(1, {"queue": task.queue, "task": task.name, "version": task.version})
        return task

    async def _run_schedule_pipeline(self, task: Task, run_at: dt.datetime) -> None:
        pipe = self.job_store.pipeline(transaction=True)
        self.task_scheduler.stage_add(pipe, task, run_at)
        self.ta.stage_save(pipe, task)
        await pipe.execute()

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
        pipe = self.job_store.pipeline(transaction=True)
        self.ta.stage_requeue(pipe, task)
        await pipe.execute()
        return task

    async def dispatch_scheduled_task(self, task: Task) -> Task:
        """
        Move a due scheduled task into its queue and clean up the scheduler atomically.

        Uses WATCH on the task key for optimistic locking: if the task is modified
        concurrently (e.g. cancelled between scheduler acquisition and dispatch), the
        pipeline detects the conflict and retries. If the task is already CANCELLED at
        read time, dispatch is silently skipped.

        next_due_bulk's Lua script already removed the task from the schedule-queue sorted
        set before this is called, so stage_remove's ZREM is a no-op; the HDEL cleans up
        the residual schedule-task-queue hash entry.
        """
        task_key = self.ta.TASK_DETAILS(task_id=task.id)
        while True:
            pipe = self.job_store.pipeline()
            await pipe.watch(task_key)
            watched_task: Task | None = await self.ta.read_for_watch(pipe, task.id)
            if watched_task is None or watched_task.status == TaskStatus.CANCELLED:
                await pipe.unwatch()  # type: ignore[no-untyped-call]
                return task
            task.set_status(TaskStatus.SUBMITTED)
            pipe.multi()  # type: ignore[no-untyped-call]
            self.ta.stage_requeue(pipe, task)
            self.task_scheduler.stage_remove(pipe, task.id, task.queue)
            try:
                await pipe.execute()
                logger.info("Task %s dispatched to queue %s.", task.id, task.queue)
                return task
            except WatchError:
                continue

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
        from jobbers.models.cron_dag import ConcurrencyPolicy
        from jobbers.models.cron_dag_scheduler import ConcurrencyStager

        if entry.concurrency_policy == ConcurrencyPolicy.SKIP_IF_RUNNING:
            active_task_id_str = await self.cron_dag_scheduler.get_active_run(entry.id)
            if active_task_id_str is not None:
                from ulid import ULID as _ULID

                active_task = await self.ta.get_task(_ULID.from_str(active_task_id_str))
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
                    pipe = self.job_store.pipeline(transaction=True)
                    self.cron_dag_scheduler.stage_reschedule(pipe, entry.id, next_run_at)
                    await pipe.execute()
                    yield ConcurrencyStager(skipped=True, _stage_fn=lambda _p, _t: None)
                    return

            def _stage_skip(pipe: Pipeline, task_id: ULID) -> None:
                self.cron_dag_scheduler.stage_set_active_run(pipe, entry.id, task_id, nx=True)

            yield ConcurrencyStager(skipped=False, _stage_fn=_stage_skip)
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
        from croniter import croniter

        from jobbers.models.dag import collect_fan_in_keys
        from jobbers.models.task import Task

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
            pipe = self.job_store.pipeline(transaction=True)
            self.cron_dag_scheduler.stage_reschedule(pipe, entry.id, next_run_at)
            stager.stage_active_run(pipe, task.id)
            for fan_in_key, predecessor_ids in fan_ins.items():
                self.ta.stage_init_fan_in(pipe, fan_in_key, predecessor_ids)
            if not is_rate_limited:
                self.stage_submit_task(pipe, task, queue_config)
            await pipe.execute()

            if is_rate_limited:
                await self.submit_task(task)
            logger.info("Cron entry %s dispatched as task %s (run_at=%s).", entry.id, task.id, run_at)

    async def request_task_cancellation(self, task_id: ULID) -> Task | None:
        """
        Publish a cancellation request for a task.

        Returns None if the task does not exist.
        Raises TaskException if the task status does not permit cancellation.
        """
        task = await self.ta.get_task(task_id)
        if task is None:
            return None
        match task.status:
            case TaskStatus.SCHEDULED:
                # For scheduled tasks, we can just remove them from the scheduler without a pub/sub dance
                task.set_status(TaskStatus.CANCELLED)
                pipe = self.job_store.pipeline(transaction=True)
                self.task_scheduler.stage_remove(pipe, task_id, task.queue)
                self.ta.stage_save(pipe, task)
                await pipe.execute()
            case TaskStatus.SUBMITTED:
                # remove from the queue immediately so it can't be claimed by a worker
                task.set_status(TaskStatus.CANCELLED)
                pipe = self.job_store.pipeline(transaction=True)
                self.ta.stage_remove_from_queue(pipe, task)
                self.ta.stage_save(pipe, task)
                await pipe.execute()
            case TaskStatus.STARTED:
                await self.job_store.publish(f"task_cancel_{task_id}", "cancel")
            case _:
                raise TaskException(f"Task has status '{task.status}' and cannot be cancelled.")
        return task

    async def monitor_task_cancellation(self, task_id: ULID) -> None:
        """Monitor for user cancellation of the task."""
        async with self.job_store.pubsub() as pubsub:
            await pubsub.subscribe(f"task_cancel_{task_id}")
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    logger.info("Received cancellation message %s for task %s", message, task_id)
                    raise UserCancellationError(f"Task {task_id} was cancelled by user request.")
                await asyncio.sleep(0.01)

    # Proxy methods
    async def get_refresh_tag(self, role: str) -> ULID:
        return await self.routing.get_refresh_tag(role)

    def stage_submit_task(self, pipe: Pipeline, task: Task, queue_config: QueueConfig | None) -> None:
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
        self.ta.stage_submit_task(pipe, task)

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
        raw: bytes | None = await self.job_store.get("routing:version")
        return ULID.from_bytes(raw) if raw else None

    async def bump_routing_version(self) -> None:
        await self.job_store.set("routing:version", ULID().bytes)

    async def bump_refresh_tag(self, role: str) -> str:
        new_tag = await self.routing.bump_refresh_tag(role)
        await self.job_store.publish(f"queue-config-refresh:{role}", new_tag)
        return new_tag

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        return await self.routing.bump_refresh_tags_for_queue(queue_name)

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
        new_tag = await self.routing.save_role(role, queues_set)
        await self.job_store.publish(f"queue-config-refresh:{role}", new_tag)

    async def delete_queue(self, queue_name: str) -> None:
        self.invalidate_queue_config(queue_name)
        await self.routing.delete_queue(queue_name)

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
            await self.ta.submit_rate_limited_task(task=task, queue_config=queue_config)
        else:
            await self.ta.submit_task(task=task)

    async def init_fan_in(self, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400) -> None:
        await self.ta.init_fan_in(fan_in_key, predecessor_ids, ttl)

    async def add_cron_dag(self, entry: CronDAGEntry) -> None:
        """
        Register a new (or replace an existing) cron DAG entry and schedule its first run.

        The first run is computed from ``entry.cron_expr`` relative to now.
        If an entry with the same ``id`` already exists it is overwritten and
        its schedule is reset.
        """
        from croniter import croniter

        now = dt.datetime.now(dt.UTC)
        first_run_at = croniter(entry.cron_expr, now).get_next(dt.datetime)
        pipe = self.job_store.pipeline(transaction=True)
        self.cron_dag_scheduler.stage_add(pipe, entry, first_run_at)
        await pipe.execute()
        logger.info(
            "Cron DAG entry '%s' (%s) registered, first run at %s.", entry.name, entry.id, first_run_at
        )

    async def remove_cron_dag(self, cron_id: ULID) -> None:
        """Remove a cron DAG entry from the schedule."""
        pipe = self.job_store.pipeline(transaction=True)
        self.cron_dag_scheduler.stage_remove(pipe, cron_id)
        await pipe.execute()
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
        return await self.ta.get_dag_runs(pagination)

    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run, or None if not found."""
        return await self.ta.get_dag_run(dag_run_id)

    async def save_task(self, task: Task) -> Task:
        """Save the task state without extra validation."""
        pipe = self.job_store.pipeline(transaction=True)
        self.ta.stage_save(pipe, task)
        await pipe.execute()
        return task

    async def update_task_heartbeat(self, task: Task) -> None:
        """Update the heartbeat timestamp for a task."""
        task.heartbeat_at = dt.datetime.now(dt.UTC)
        await self.ta.update_task_heartbeat(task)

    async def remove_task_heartbeat(self, task: Task) -> None:
        """Remove a task from the heartbeat sorted set."""
        await self.ta.remove_task_heartbeat(task)

    async def get_active_tasks(self, queues: set[str]) -> list[Task]:
        """Return all tasks currently present in any heartbeat sorted set."""
        return await self.ta.get_active_tasks(queues)


class SubmissionRateLimiter:
    """
    Rate limiter for tasks in a Redis data store.

    The rate limiter is stored in a sorted set with the following key:
    - `rate-limiter:<queue>`: Sorted set of task id => task name/hash key. This should be updated when the task-queues are updated.
        - ZADD/ZREM by task id: O(log(N))
        - ZCOUNT by hash key: O(log(N)) to get the current number of tasks with that hash key
    """

    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format

    def __init__(
        self, job_store: Redis, get_queue_config: Callable[[str], Awaitable[QueueConfig | None]]
    ) -> None:
        self.job_store = job_store
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

    async def clean(self, queues: set[bytes], now: dt.datetime, rate_limit_age: dt.timedelta) -> None:
        """Clean up the tasks from the rate limiter that are older than the rate limit age."""
        earliest_time = now - rate_limit_age
        pipe = self.job_store.pipeline(transaction=True)
        for queue in queues:
            pipe.zremrangebyscore(
                self.QUEUE_RATE_LIMITER(queue=queue.decode()), min=0, max=earliest_time.timestamp()
            )
        await pipe.execute()
