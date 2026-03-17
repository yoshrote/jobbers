from __future__ import annotations

import asyncio
import datetime as dt
import logging
from collections import defaultdict
from contextlib import contextmanager
from typing import TYPE_CHECKING

from opentelemetry import metrics
from redis.exceptions import WatchError

from jobbers import registry
from jobbers.adapters.json_redis import JsonTaskAdapter
from jobbers.adapters.raw_redis import DeadQueue
from jobbers.models.queue_config import QueueConfigAdapter
from jobbers.models.task_config import DeadLetterPolicy
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_status import TaskStatus

if TYPE_CHECKING:
    from collections.abc import Iterator

    from redis.asyncio.client import Redis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from ulid import ULID

    from jobbers.adapters.task_adapter import DeadQueueProtocol, TaskAdapterProtocol
    from jobbers.models.task import Task

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
        session_factory: async_sessionmaker[AsyncSession],
        task_adapter: TaskAdapterProtocol | None = None,
    ) -> None:
        self.job_store: Redis = job_store
        self.qca = QueueConfigAdapter(session_factory)
        self.ta: TaskAdapterProtocol = (
            task_adapter if task_adapter is not None else JsonTaskAdapter(job_store)
        )
        self.submission_limiter = SubmissionRateLimiter(job_store, self.qca)
        self.current_tasks_by_queue: dict[str, set[ULID]] = defaultdict(set)
        self.dead_queue: DeadQueueProtocol = DeadQueue(job_store, self.ta)
        self.task_scheduler = TaskScheduler(job_store, self.ta)

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
        queues = {q.encode() for q in await self.qca.get_all_queues()}

        clean_ops = []
        if rate_limit_age:
            clean_ops.append(self.submission_limiter.clean(queues, now, rate_limit_age))

        if max_queue_age or min_queue_age:
            clean_ops.append(self.ta.clean(queues, now, min_queue_age, max_queue_age))

        if dlq_age:
            clean_ops.append(self.dead_queue.clean(now - dlq_age))

        if completed_task_age:
            clean_ops.append(self.ta.clean_terminal_tasks(now, completed_task_age))

        if stale_time:
            stale_tasks_by_type: dict[tuple[str, int], list[Task]] = defaultdict(list)
            async for task in self.ta.get_stale_tasks({q.decode() for q in queues}, stale_time):
                if task.status not in {TaskStatus.STARTED, TaskStatus.HEARTBEAT}:
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

    async def schedule_retry_task(self, task: Task, run_at: dt.datetime) -> Task:
        """Add a task to the scheduler and save its state in a single atomic transaction."""
        pipe = self.job_store.pipeline(transaction=True)
        self.task_scheduler.stage_add(pipe, task, run_at)
        self.ta.stage_save(pipe, task)
        await pipe.execute()
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
                return task
            except WatchError:
                continue

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
            case TaskStatus.STARTED | TaskStatus.HEARTBEAT:
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
        return await self.qca.get_refresh_tag(role)

    async def submit_task(self, task: Task) -> None:
        queue_config = await self.qca.get_queue_config(queue=task.queue)
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

    def __init__(self, job_store: Redis, queue_config_adapter: QueueConfigAdapter):
        self.job_store = job_store
        self.qca = queue_config_adapter

    async def concurrency_limits(
        self, task_queues: set[str], current_tasks_by_queue: dict[str, set[ULID]]
    ) -> set[str]:
        """Limit the number of concurrent tasks in each queue."""
        queues_to_use = set()
        queues = list(task_queues)
        configs = await asyncio.gather(*(self.qca.get_queue_config(queue=q) for q in queues))
        for queue, config in zip(queues, configs):
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
