"""
Shared base classes for Redis-backed task adapters.

``SharedTaskAdapterMixin`` is an internal ABC that implements all ``TaskStateProtocol``
and ``AtomicTaskStateProtocol`` logic identical across the plain-Redis and RedisJSON
backends.  It is not part of the public adapter API.

``_SharedRedisTaskSubmitBase`` is an internal base that implements ``TaskSubmitProtocol``
(submit_task, submit_rate_limited_task, get_next_task) shared across both Redis backends.
It requires a ``pack`` callable and a ``get_task`` callable supplied at construction time,
so the state and submit classes remain independent objects.

Concrete adapters inherit ``SharedTaskAdapterMixin`` and implement:
  - Storage primitives: ``_load_raw``, ``_load_raw_watch``, ``_stage_store``,
    ``_stage_load``
  - Serialization: ``pack``, ``unpack``
  - Backend-specific queries: ``ensure_index``, ``get_all_tasks``, ``get_dag_run``

Concrete submit classes inherit ``_SharedRedisTaskSubmitBase`` and define:
  - Lua script class attributes: ``SUBMIT_SCRIPT``, ``SUBMIT_RATE_LIMITED_SCRIPT``
  - ``_extra_submit_keys``, ``_extra_rate_limited_keys`` if needed
"""

from __future__ import annotations

import datetime as dt
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, cast

from opentelemetry import metrics
from redis.exceptions import WatchError
from ulid import ULID

from jobbers.constants import TIME_ZERO
from jobbers.models.task_status import TaskStatus

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.dag import DAGRunPagination
    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task import Task, TaskPagination
    from jobbers.protocols import TransactionHandle

logger = logging.getLogger(__name__)
tasks_missing_data = metrics.get_meter(__name__).create_counter("tasks_missing_data", unit="1")


class SharedTaskAdapterMixin(ABC):
    """ABC mixin implementing all TaskStateProtocol/AtomicTaskStateProtocol logic identical across backends."""

    # -- key helpers (both implementations use the same Redis key names) ----
    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    HEARTBEAT_SCORES = "task-heartbeats:{queue}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    DLQ_MISSING_DATA = "dlq-missing-data"
    DAG_RUNS = "dag-runs"
    DAG_RUN_TASKS = "dag-run:{dag_run_id}:tasks".format

    # Atomically remove task_id from fan-in set and return remaining count.
    # Returns {removed=0, remaining=-1} if the ID was not a member (already
    # processed or key expired), so callers can distinguish a real zero-remaining
    # from a false zero caused by a missing/expired key.
    _FAN_IN_SCRIPT = """
        local removed = redis.call('SREM', KEYS[1], ARGV[1])
        if removed == 0 then
            return {0, -1}
        end
        return {removed, redis.call('SCARD', KEYS[1])}
    """

    def __init__(self, data_store: Redis) -> None:
        self.data_store: Redis = data_store
        self._fan_in_script = self.data_store.register_script(self._FAN_IN_SCRIPT)

    @property
    def backend_key(self) -> str:
        """Stable identifier for the backend instance — same object → same key."""
        return str(id(self.data_store))

    def pipeline(self, transaction: bool = True) -> Pipeline:
        """Return a Redis pipeline for atomic staging (satisfies AtomicTaskStateProtocol)."""
        return self.data_store.pipeline(transaction=transaction)

    # ---------------------------------------------------------------------------
    # Abstract primitives — subclasses must implement these
    # ---------------------------------------------------------------------------

    @abstractmethod
    def pack(self, task: Task) -> str | bytes:
        """Serialize a task to the backend's wire format."""

    @abstractmethod
    def unpack(self, task_id: ULID, data: Any) -> Task:
        """Deserialize a task from the backend's wire format."""

    @abstractmethod
    async def _load_raw(self, key: str) -> Any:
        """Fetch raw task data by string key."""

    @abstractmethod
    async def _load_raw_watch(self, pipe: Pipeline, key: str) -> Any:
        """Fetch raw task data via an active WATCH pipeline (Redis-specific)."""

    @abstractmethod
    def _stage_store(self, pipe: Pipeline, key: str, task: Task) -> None:
        """Stage the backend's write command for task data onto pipe (Redis-specific)."""

    @abstractmethod
    def _stage_load(self, pipe: Pipeline, key: str) -> None:
        """Stage the backend's read command for task data onto pipe (Redis-specific)."""

    @abstractmethod
    async def ensure_index(self) -> None:
        """Create or update any backend search index."""

    @abstractmethod
    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Return a page of tasks matching the pagination filters."""

    @abstractmethod
    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run, or None if not found."""

    # ---------------------------------------------------------------------------
    # Shared implementations (identical across all backends)
    # ---------------------------------------------------------------------------

    def stage_save(self, pipe: TransactionHandle, task: Task) -> None:
        """Queue task-details write + type-index update onto pipe (no execute)."""
        p: Any = pipe
        self._stage_store(p, self.TASK_DETAILS(task_id=task.id), task)
        if task.status in TaskStatus.active_statuses():
            p.sadd(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        else:
            p.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))

    async def get_task(self, task_id: ULID) -> Task | None:
        raw_data = await self._load_raw(self.TASK_DETAILS(task_id=task_id))
        if not raw_data:
            return None
        task = self.unpack(task_id, raw_data)
        heartbeat_score: float | None = await self.data_store.zscore(
            self.HEARTBEAT_SCORES(queue=task.queue), bytes(task_id)
        )
        if heartbeat_score is not None:
            task.heartbeat_at = dt.datetime.fromtimestamp(heartbeat_score, dt.UTC)
        return task

    async def read_for_watch(self, pipe: TransactionHandle, task_id: ULID) -> Task | None:
        """Read task data via a WATCH pipeline."""
        raw_data = await self._load_raw_watch(pipe, self.TASK_DETAILS(task_id=task_id))  # type: ignore[arg-type]
        if not raw_data:
            return None
        return self.unpack(task_id, raw_data)

    async def compare_and_set_status(
        self,
        task_id: ULID,
        expected: TaskStatus,
        new: TaskStatus,
    ) -> bool:
        """
        Atomically transition task status if the current status equals ``expected``.

        Uses WATCH/MULTI for optimistic locking: retries on concurrent modification.
        Returns True if the transition was applied, False if status did not match.
        """
        task_key = self.TASK_DETAILS(task_id=task_id)
        while True:
            pipe = self.data_store.pipeline()
            await pipe.watch(task_key)
            task = await self.read_for_watch(pipe, task_id)
            if task is None or task.status != expected:
                await pipe.unwatch()  # type: ignore[no-untyped-call]
                return False
            task.set_status(new)
            pipe.multi()  # type: ignore[no-untyped-call]
            self.stage_save(pipe, task)
            try:
                await pipe.execute()
                return True
            except WatchError:
                continue

    async def atomic_dispatch_scheduled(
        self,
        task: Task,
        stage_extra: Callable[[TransactionHandle], None],
    ) -> bool:
        """
        Atomically transition a SCHEDULED task to SUBMITTED using WATCH/MULTI.

        Reads the task under WATCH; if it is missing or CANCELLED, unWATCHes and
        returns False.  Otherwise sets status to SUBMITTED, stages a requeue, calls
        stage_extra(pipe) for extra staged operations (e.g. scheduler removal), and
        commits.  Retries transparently on WatchError.
        """
        task_key = self.TASK_DETAILS(task_id=task.id)
        while True:
            pipe = self.data_store.pipeline()
            await pipe.watch(task_key)
            watched_task = await self.read_for_watch(pipe, task.id)
            if watched_task is None or watched_task.status == TaskStatus.CANCELLED:
                await pipe.unwatch()  # type: ignore[no-untyped-call]
                return False
            task.set_status(TaskStatus.SUBMITTED)
            pipe.multi()  # type: ignore[no-untyped-call]
            self.stage_requeue(pipe, task)
            stage_extra(pipe)
            try:
                await pipe.execute()
                return True
            except WatchError:
                continue

    async def _fetch_task_data_bulk(self, task_ids: list[ULID]) -> list[Any]:
        pipe = self.data_store.pipeline(transaction=False)
        for task_id in task_ids:
            self._stage_load(pipe, self.TASK_DETAILS(task_id=task_id))
        return await pipe.execute()

    def _decode_task(self, task_id: ULID, raw: Any) -> Task:
        return self.unpack(task_id, raw)

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Delete blobs, heartbeat entries, and type-index members for old terminal tasks."""
        terminal_statuses = {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.STALLED,
            TaskStatus.DROPPED,
        }
        cutoff = now - max_age
        async for raw_key in self.data_store.scan_iter("task:*"):
            key_str = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            task_id_str = key_str.removeprefix("task:")
            try:
                task_id = ULID.from_str(task_id_str)
            except ValueError:
                continue
            task_data = await self._load_raw(key_str)
            if not task_data:
                continue
            task = self.unpack(task_id, task_data)
            if task.status not in terminal_statuses:
                continue
            if task.completed_at is None or task.completed_at >= cutoff:
                continue
            pipe = self.data_store.pipeline(transaction=True)
            pipe.delete(key_str)
            pipe.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task_id))
            pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task_id))
            await pipe.execute()

    async def delete_task(self, task: Task) -> None:
        """Delete a task blob and remove it from all indexes atomically."""
        key_str = self.TASK_DETAILS(task_id=task.id)
        pipe = self.data_store.pipeline(transaction=True)
        pipe.delete(key_str)
        pipe.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))
        pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))
        await pipe.execute()

    def stage_requeue(self, pipe: TransactionHandle, task: Task) -> None:
        """Queue ZADD task-queue + save-task commands onto pipe (no execute)."""
        assert task.submitted_at  # noqa: S101
        p: Any = pipe
        p.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
        self.stage_save(pipe, task)

    def stage_submit_task(self, pipe: TransactionHandle, task: Task) -> None:
        """Queue ZADD + save-task onto pipe for initial submission (no execute)."""
        assert task.submitted_at  # noqa: S101
        p: Any = pipe
        p.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
        self.stage_save(pipe, task)
        self.stage_register_dag_run(pipe, task)

    def stage_register_dag_run(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage DAG run index updates onto pipe if the task belongs to a DAG run."""
        if task.dag_run_id is None or task.submitted_at is None:
            return
        score = task.submitted_at.timestamp()
        p: Any = pipe
        p.zadd(self.DAG_RUNS, {bytes(task.dag_run_id): score}, nx=True)

    async def get_dag_runs(self, pagination: DAGRunPagination) -> tuple[list[tuple[ULID, dt.datetime]], int]:
        """Return a paginated list of DAG runs ordered by submission time (oldest first)."""
        total: int = await self.data_store.zcard(self.DAG_RUNS)
        raw: list[tuple[bytes, float]] = await self.data_store.zrange(
            self.DAG_RUNS, pagination.offset, pagination.offset + pagination.limit - 1, withscores=True
        )
        return [
            (ULID.from_bytes(dag_id_bytes), dt.datetime.fromtimestamp(score, dt.UTC))
            for dag_id_bytes, score in raw
        ], total

    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Remove DAG run index entries older than ``max_age``."""
        cutoff = (now - max_age).timestamp()
        stale: list[bytes] = await self.data_store.zrangebyscore(self.DAG_RUNS, "-inf", cutoff)
        if stale:
            await self.data_store.zrem(self.DAG_RUNS, *stale)

    async def save_task(self, task: Task) -> None:
        """Save task state to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_save(pipe, task)
        await pipe.execute()

    def stage_remove_from_queue(self, pipe: TransactionHandle, task: Task) -> None:
        """Queue ZREM task-queue + SREM type-index commands onto pipe (no execute)."""
        p: Any = pipe
        p.zrem(self.TASKS_BY_QUEUE(queue=task.queue), bytes(task.id))
        p.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))

    def stage_remove_heartbeat(self, pipe: TransactionHandle, task: Task) -> None:
        """Queue ZREM heartbeat-scores command onto pipe (no execute)."""
        p: Any = pipe
        p.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))

    async def update_task_heartbeat(self, task: Task) -> None:
        """Update the heartbeat for a task."""
        assert task.heartbeat_at  # noqa: S101
        pipe = self.data_store.pipeline(transaction=True)
        pipe.zadd(self.HEARTBEAT_SCORES(queue=task.queue), {bytes(task.id): task.heartbeat_at.timestamp()})
        await pipe.execute()

    async def remove_task_heartbeat(self, task: Task) -> None:
        """Remove a task from the heartbeat sorted set."""
        await self.data_store.zrem(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        """Fetch multiple tasks in 2 batched round-trips instead of 2N individual calls."""
        if not task_ids:
            return []
        raws = await self._fetch_task_data_bulk(task_ids)
        tasks: list[Task | None] = []
        valid: list[tuple[int, Task]] = []
        for i, (task_id, raw) in enumerate(zip(task_ids, raws, strict=True)):
            if raw is None:
                tasks.append(None)
            else:
                task = self._decode_task(task_id, raw)
                tasks.append(task)
                valid.append((i, task))
        if not valid:
            return tasks
        pipe = self.data_store.pipeline(transaction=False)
        for _, task in valid:
            pipe.zscore(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))
        scores = await pipe.execute()
        for (_, task), score in zip(valid, scores, strict=True):
            if score is not None:
                task.heartbeat_at = dt.datetime.fromtimestamp(score, dt.UTC)
        return tasks

    async def get_active_tasks(self, queues: set[str]) -> list[Task]:
        """Return all tasks currently present in any heartbeat sorted set."""
        task_id_bytes: set[bytes] = set()
        for queue in queues:
            members: list[bytes] = await self.data_store.zrange(self.HEARTBEAT_SCORES(queue=queue), 0, -1)
            task_id_bytes.update(members)
        if not task_id_bytes:
            return []
        fetched = await self.get_tasks_bulk([ULID.from_bytes(b) for b in task_id_bytes])
        return [t for t in fetched if t is not None]

    async def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]:
        """Get tasks that have not had a heartbeat update in the stale time."""
        now = dt.datetime.now(dt.UTC)
        cutoff_time = now - stale_time
        stale_task_ids: set[bytes] = set()
        for queue in queues:
            task_ids: list[bytes] = await self.data_store.zrangebyscore(
                self.HEARTBEAT_SCORES(queue=queue), min=0, max=cutoff_time.timestamp()
            )
            stale_task_ids.update(task_ids)
        fetched = await self.get_tasks_bulk([ULID.from_bytes(b) for b in stale_task_ids])
        for task in fetched:
            if task is not None:
                yield task

    def stage_init_fan_in(
        self, pipe: TransactionHandle, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None:
        """Queue fan-in set initialisation commands onto *pipe* without executing."""
        p: Any = pipe
        members_key = f"{fan_in_key}:members"
        encoded = [str(pid).encode() for pid in predecessor_ids]
        p.sadd(fan_in_key, *encoded)
        p.expire(fan_in_key, ttl)
        p.sadd(members_key, *encoded)
        p.expire(members_key, ttl * 2)

    async def init_fan_in(self, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400) -> None:
        """Pre-populate a fan-in tracking set in Redis."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_init_fan_in(pipe, fan_in_key, predecessor_ids, ttl)
        await pipe.execute()

    async def fan_in_complete(self, fan_in_key: str, task_id: ULID) -> int:
        """Atomically remove *task_id* from the fan-in set and return the remaining count."""
        results: list[int] = await self._fan_in_script(keys=[fan_in_key], args=[str(task_id).encode()])
        return int(results[1])

    async def get_fan_in_members(self, fan_in_key: str) -> list[ULID]:
        """Return the permanent list of predecessor IDs for a fan-in collector."""
        members_key = f"{fan_in_key}:members"
        raw: set[bytes] = await cast("Awaitable[set[bytes]]", self.data_store.smembers(members_key))
        return [ULID.from_str(m.decode()) for m in raw]

    async def task_exists(self, task_id: ULID) -> bool:
        does_exists: int = await self.data_store.exists(self.TASK_DETAILS(task_id=task_id))
        return does_exists == 1

    async def clean(
        self,
        queues: set[bytes],
        now: dt.datetime,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
    ) -> None:
        """Remove queue entries within a time range."""
        if max_queue_age or min_queue_age:
            earliest_time = min_queue_age or TIME_ZERO
            latest_time = max_queue_age or now
            for queue in queues:
                pipe = self.data_store.pipeline(transaction=True)
                if earliest_time <= latest_time:
                    pipe.zremrangebyscore(
                        self.TASKS_BY_QUEUE(queue=queue.decode()),
                        min=earliest_time.timestamp(),
                        max=latest_time.timestamp(),
                    )
                else:
                    pipe.zremrangebyscore(
                        self.TASKS_BY_QUEUE(queue=queue.decode()),
                        min=0,
                        max=earliest_time.timestamp(),
                    )
                    pipe.zremrangebyscore(
                        self.TASKS_BY_QUEUE(queue=queue.decode()),
                        min=latest_time.timestamp(),
                        max=now.timestamp(),
                    )
                await pipe.execute()

    async def _add_task_to_results(self, task_id: ULID, results: list[Task]) -> list[Task]:
        task = await self.get_task(task_id)
        if task:
            results.append(task)
        return results


class _SharedRedisTaskSubmitBase:
    """
    Shared TaskSubmitProtocol implementation for both Redis backends.

    Concrete submit classes define ``SUBMIT_SCRIPT`` and ``SUBMIT_RATE_LIMITED_SCRIPT``
    as class attributes and override ``_extra_submit_keys`` / ``_extra_rate_limited_keys``
    when the backend needs additional Lua KEYS arguments.
    """

    SUBMIT_SCRIPT: ClassVar[str]
    SUBMIT_RATE_LIMITED_SCRIPT: ClassVar[str]

    # Key constants — identical to SharedTaskAdapterMixin's constants.
    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    DAG_RUNS = "dag-runs"
    DAG_RUN_TASKS = "dag-run:{dag_run_id}:tasks".format
    DLQ_MISSING_DATA = "dlq-missing-data"

    def __init__(
        self,
        data_store: Redis,
        pack: Callable[[Task], str | bytes],
        get_task: Callable[[ULID], Awaitable[Task | None]],
    ) -> None:
        self._data_store = data_store
        self._pack_fn = pack
        self._get_task_fn = get_task

    def _extra_submit_keys(self, task: Task) -> list[str]:
        """Extra KEYS[] args for SUBMIT_SCRIPT beyond the base 4 keys."""
        return []

    def _extra_rate_limited_keys(self, task: Task) -> list[str]:
        """Extra KEYS[] args for SUBMIT_RATE_LIMITED_SCRIPT beyond the base 5 keys."""
        return []

    async def submit_task(self, task: Task) -> bool:
        """Atomically enqueue a new task with no rate limiting. Status must already be SUBMITTED."""
        assert task.submitted_at  # noqa: S101
        is_active = "1" if task.status in TaskStatus.active_statuses() else "0"
        dag_run_id_bytes = bytes(task.dag_run_id) if task.dag_run_id is not None else b""
        extra_keys = self._extra_submit_keys(task)
        result: int = await cast(
            "Awaitable[int]",
            self._data_store.eval(
                self.SUBMIT_SCRIPT,
                4 + len(extra_keys),
                self.TASKS_BY_QUEUE(queue=task.queue),
                self.TASK_DETAILS(task_id=task.id),
                self.TASK_BY_TYPE_IDX(name=task.name),
                self.DAG_RUNS,
                *extra_keys,
                task.submitted_at.timestamp(),
                bytes(task.id),
                is_active,
                self._pack_fn(task),
                dag_run_id_bytes,
            ),
        )
        return result == 1

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """Atomically check the rate limit and enqueue the task if there is room."""
        assert task.submitted_at  # noqa: S101
        now = dt.datetime.now(dt.UTC)
        earliest_time = now - dt.timedelta(seconds=queue_config.period_in_seconds() or 0)
        is_active = "1" if task.status in TaskStatus.active_statuses() else "0"
        dag_run_id_bytes = bytes(task.dag_run_id) if task.dag_run_id is not None else b""
        extra_keys = self._extra_rate_limited_keys(task)
        result: int = await cast(
            "Awaitable[int]",
            self._data_store.eval(
                self.SUBMIT_RATE_LIMITED_SCRIPT,
                5 + len(extra_keys),
                self.QUEUE_RATE_LIMITER(queue=task.queue),
                self.TASKS_BY_QUEUE(queue=task.queue),
                self.TASK_DETAILS(task_id=task.id),
                self.TASK_BY_TYPE_IDX(name=task.name),
                self.DAG_RUNS,
                *extra_keys,
                earliest_time.timestamp(),
                queue_config.rate_numerator or 0,
                task.submitted_at.timestamp(),
                bytes(task.id),
                is_active,
                self._pack_fn(task),
                dag_run_id_bytes,
            ),
        )
        return result == 1

    async def clean_rate_limiter(
        self, queues: set[bytes], now: dt.datetime, rate_limit_age: dt.timedelta
    ) -> None:
        earliest_time = now - rate_limit_age
        pipe = self._data_store.pipeline(transaction=True)
        for queue in queues:
            pipe.zremrangebyscore(
                self.QUEUE_RATE_LIMITER(queue=queue.decode()), min=0, max=earliest_time.timestamp()
            )
        await pipe.execute()

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """Get the next task from the queues in order of priority."""
        task_queues = {self.TASKS_BY_QUEUE(queue=queue) for queue in queues}
        while pop_result := await self._data_store.bzpopmin(task_queues, timeout=pop_timeout):
            queue_name, task_id_bytes, _ = pop_result
            logger.debug("Popped task %s from %s", task_id_bytes, queue_name)
            task = await self._get_task_fn(ULID.from_bytes(task_id_bytes))
            if task:
                return task
            logger.error(
                "Task %s popped from queue but data not found; adding to %s",
                task_id_bytes,
                self.DLQ_MISSING_DATA,
            )
            tasks_missing_data.add(1)
            now = dt.datetime.now(dt.UTC)
            await self._data_store.zadd(self.DLQ_MISSING_DATA, {task_id_bytes: now.timestamp()})
        logger.info("task query timed out")
        return None
