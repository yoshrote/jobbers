import datetime as dt
import logging
from collections import defaultdict
from collections.abc import Awaitable, Iterator
from contextlib import contextmanager
from typing import cast

from opentelemetry import metrics
from redis.asyncio.client import Redis
from ulid import ULID

from jobbers import registry
from jobbers.models.dead_queue import DeadQueue
from jobbers.models.queue_config import QueueConfigAdapter
from jobbers.models.task import Task, TaskAdapter
from jobbers.models.task_config import DeadLetterPolicy
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_status import TaskStatus

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
    """Manages tasks in memory and a Redis data store."""

    def __init__(self, data_store: Redis, task_scheduler: TaskScheduler, dead_queue: DeadQueue) -> None:
        self.data_store: Redis = data_store
        self.qca = QueueConfigAdapter(data_store)
        self.ta = TaskAdapter(data_store)
        self.submission_limiter = SubmissionRateLimiter(self.qca)
        self.current_tasks_by_queue: dict[str, set[ULID]] = defaultdict(set)
        self.dead_queue = dead_queue
        self.task_scheduler = task_scheduler

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

    async def clean(self, rate_limit_age: dt.timedelta | None=None, min_queue_age: dt.datetime | None=None, max_queue_age: dt.datetime | None=None, stale_time: dt.timedelta | None=None) -> None:
        """Clean up the state manager."""
        now = dt.datetime.now(dt.timezone.utc)
        queues = await cast("Awaitable[set[bytes]]", self.data_store.smembers(self.qca.ALL_QUEUES))

        if rate_limit_age:
            await self.submission_limiter.clean(queues, now, rate_limit_age)

        if max_queue_age or min_queue_age:
            await self.ta.clean(queues, now, min_queue_age, max_queue_age)

        if stale_time:
            stale_tasks_by_type: dict[tuple[str, int], list[Task]] = defaultdict(list)
            async for task in self.ta.get_stale_tasks({q.decode() for q in queues}, stale_time):
                stale_tasks_by_type[(task.name, task.version)].append(task)

            for (task_type, task_version), tasks in stale_tasks_by_type.items():
                task_config = registry.get_task_config(task_type, task_version)
                if task_config and task_config.max_heartbeat_interval:
                    for task in tasks:
                        if task.heartbeat_at and (now - task.heartbeat_at) > task_config.max_heartbeat_interval:
                            task.set_status(TaskStatus.STALLED)
                            await self.save_task(task)

    # TODO: refactor the refresh tag to be an implementation detail of QueueConfigAdapter
    async def get_refresh_tag(self, role: str) -> ULID:
        tag: bytes|None = await self.data_store.get(f"worker-queues:{role}:refresh_tag")
        if tag:
            return ULID(tag)

        # initialize the refresh tag
        init_tag = ULID()
        await self.data_store.set(f"worker-queues:{role}:refresh_tag", bytes(init_tag))
        return init_tag

    async def get_next_task(self, queues: set[str], timeout: int=0) -> Task | None:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        if not queues:
            logger.info("no queues defined")
            return None

        queues = await self.submission_limiter.concurrency_limits(queues, self.current_tasks_by_queue)

        return await self.ta.get_next_task(queues, timeout)

    # Proxy methods

    async def submit_task(self, task: Task) -> None:
        queue_config = await self.qca.get_queue_config(queue=task.queue)
        task.set_status(TaskStatus.SUBMITTED)
        if queue_config and queue_config.rate_numerator and queue_config.rate_denominator and queue_config.rate_period:
            await self.ta.submit_rate_limited_task(task=task, queue_config=queue_config)
        else:
            await self.ta.submit_task(task=task)

    async def save_task(self, task: Task) -> Task:
        """Save the task state without extra validation."""
        await self.ta.save_task(task=task)
        return task

    async def fail_task(self, task: Task) -> Task:
        """
        Persist a failed task, handling any dead letter queue side effects.

        Write ordering: Redis first, then SQLite DLQ.
        If we crash after Redis but before the DLQ write, the task is correctly
        marked FAILED in Redis (no re-execution). The DLQ entry is a best-effort
        record; a missing entry is preferable to a task being re-executed.
        The DLQ write is idempotent (INSERT OR REPLACE), so retries are safe.
        """
        result = await self.save_task(task)
        if task.task_config and task.task_config.dead_letter_policy == DeadLetterPolicy.SAVE:
            logger.info("Task %s sent to dead letter queue.", task.id)
            now = dt.datetime.now(dt.timezone.utc)
            self.dead_queue.add(task, now)
            tasks_dead_lettered.add(1, {"queue": task.queue, "task": task.name, "version": task.version})
        return result

    async def resubmit_dead_tasks(self, tasks: list[Task], reset_retry_count: bool = True) -> list[Task]:
        """
        Re-enqueue tasks from the dead letter queue into their active queues.

        Write ordering: all Redis writes first, then a single SQLite batch delete.
        requeue_task is idempotent (ZADD + HSET), so re-running after a partial
        crash is safe. If we crash between the last Redis write and the SQLite
        delete, tasks are live in Redis and stale DLQ entries are the only
        side-effect (harmless — re-running resubmit is safe).
        """
        resubmitted = []
        for task in tasks:
            if reset_retry_count:
                task.retry_attempt = 0
            task.errors = []
            task.set_status(TaskStatus.SUBMITTED)
            await self.ta.requeue_task(task)
            resubmitted.append(task)

        # Single SQLite transaction for all removes: either all succeed or all
        # remain, keeping DLQ consistent even if we crash here.
        self.dead_queue.remove_many([str(t.id) for t in resubmitted])
        return resubmitted

    async def complete_task(self, task: Task) -> Task:
        """Persist a completed task."""
        #TODO set a ttl for the data in redis after completion
        return await self.save_task(task)

    async def schedule_retry_task(self, task: Task, run_at: dt.datetime) -> Task:
        """
        Persist a delayed retry: add to the task scheduler and save.

        Write ordering: SQLite scheduler first, then Redis.
        If we crash after SQLite but before Redis, the scheduler_proc will still
        dispatch the task via dispatch_scheduled_task → queue_retry_task, which
        writes the SUBMITTED status to Redis. This is self-healing.
        Swapping the order would be unsafe: a crash after Redis (status=SCHEDULED)
        but before SQLite would leave the task stuck in SCHEDULED with nothing to
        dispatch it.
        """
        self.task_scheduler.add(task, run_at)
        logger.info("Task %s scheduled for retry at %s.", task.id, run_at)
        await self.save_task(task)
        return task

    async def queue_retry_task(self, task: Task) -> Task:
        """Persist an immediate retry: re-enqueue the task without full re-validation."""
        task.set_status(TaskStatus.SUBMITTED)
        logger.info("Task %s requeued for immediate retry.", task.id)
        await self.ta.requeue_task(task)
        return task

    async def dispatch_scheduled_task(self, task: Task) -> Task:
        """
        Move a due scheduled task from the scheduler into its Redis queue.

        Write ordering: Redis first, then SQLite scheduler delete.
        If we crash after Redis but before the SQLite delete, the task is already
        enqueued and will be processed. The leftover SQLite row has acquired=1,
        which next_due_bulk filters out (WHERE acquired = 0), so it will never
        be re-dispatched — no duplicate execution. The orphaned row is harmless.
        Swapping the order would be unsafe: deleting from SQLite first and then
        crashing would permanently lose the task.
        """
        task = await self.queue_retry_task(task)
        self.task_scheduler.remove(task.id)
        return task

    async def request_task_cancellation(self, task_id: ULID) -> "Task | None":
        """
        Publish a cancellation request for a task.

        Returns None if the task does not exist.
        Raises TaskException if the task status does not permit cancellation.
        """
        task = await self.ta.get_task(task_id)
        if task is None:
            return None
        if task.status == TaskStatus.SCHEDULED:
            # For scheduled tasks, we can just remove them from the scheduler without a pub/sub dance
            self.task_scheduler.remove(task_id)
            task.set_status(TaskStatus.CANCELLED)
            await self.save_task(task)
            return task
        if task.status not in {TaskStatus.SUBMITTED, TaskStatus.STARTED}:
            raise TaskException(f"Task has status '{task.status}' and cannot be cancelled.")
        await self.data_store.publish(f"task_cancel_{task_id}", "cancel")
        return task

    async def monitor_task_cancellation(self, task_id: ULID) -> None:
        """Monitor for user cancellation of the task."""
        async with self.data_store.pubsub() as pubsub:
            await pubsub.subscribe(f"task_cancel_{task_id}")
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message is not None:
                logger.info("Received cancellation for task %s", task_id)
                raise UserCancellationError(f"Task {task_id} was cancelled by user request.")

class SubmissionRateLimiter:
    """
    Rate limiter for tasks in a Redis data store.

    The rate limiter is stored in a sorted set with the following key:
    - `rate-limiter:<queue>`: Sorted set of task id => task name/hash key. This should be updated when the task-queues are updated.
        - ZADD/ZREM by task id: O(log(N))
        - ZCOUNT by hash key: O(log(N)) to get the current number of tasks with that hash key
    """

    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format

    def __init__(self, queue_config_adapter: QueueConfigAdapter):
        self.data_store = queue_config_adapter.data_store
        self.qca = queue_config_adapter

    async def concurrency_limits(self, task_queues: set[str], current_tasks_by_queue: dict[str, set[ULID]]) -> set[str]:
        """Limit the number of concurrent tasks in each queue."""
        queues_to_use = set()
        # TODO: Consider ways to check each queue in a single transaction or in parallel
        for queue in task_queues:
            config = await self.qca.get_queue_config(queue=queue)
            if config and config.max_concurrent:
                if len(current_tasks_by_queue[queue]) < config.max_concurrent:
                    queues_to_use.add(queue)

        return queues_to_use

    async def clean(self, queues: set[bytes], now: dt.datetime, rate_limit_age: dt.timedelta) -> None:
        """Clean up the tasks from the rate limiter that are older than the rate limit age."""
        earliest_time = now - rate_limit_age
        pipe = self.data_store.pipeline(transaction=True)
        for queue in queues:
            pipe.zremrangebyscore(self.QUEUE_RATE_LIMITER(queue=queue.decode()), min=0, max=earliest_time.timestamp())
        await pipe.execute()

def build_sm() -> StateManager:
    from jobbers import db
    return db.get_state_manager()
