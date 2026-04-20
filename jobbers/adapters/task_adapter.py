"""
Protocols and shared base for task storage and dead-letter-queue adapters.

- `TaskAdapterProtocol` — interface for task storage and querying.
- `DeadQueueProtocol` — interface for dead letter queue operations.
- `_BaseTaskAdapter` — shared key helpers and non-storage methods used by both
  concrete task-adapter implementations.

Concrete implementations live in the sibling modules:

- `jobbers.adapters.json_redis` — Redis Stack (RedisJSON + RediSearch) backed classes.
- `jobbers.adapters.raw_redis` — plain Redis (msgpack / sorted-set) backed classes.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from opentelemetry import metrics
from ulid import ULID

from jobbers.constants import TIME_ZERO

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.dag import DAGRunPagination
    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task import Task, TaskPagination

logger = logging.getLogger(__name__)
tasks_missing_data = metrics.get_meter(__name__).create_counter("tasks_missing_data", unit="1")


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class TaskAdapterProtocol(Protocol):
    """Interface for task storage and querying."""

    # -- key helpers (both implementations use the same Redis key names) ----
    TASKS_BY_QUEUE: Any
    TASK_DETAILS: Any
    HEARTBEAT_SCORES: Any
    TASK_BY_TYPE_IDX: Any
    QUEUE_RATE_LIMITER: Any
    DLQ_MISSING_DATA: str
    DAG_RUNS: str
    DAG_RUN_TASKS: Any

    # -- write path ----------------------------------------------------------
    async def submit_task(self, task: Task) -> bool: ...
    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool: ...
    def stage_save(self, pipe: Pipeline, task: Task) -> None: ...
    def stage_requeue(self, pipe: Pipeline, task: Task) -> None: ...
    def stage_submit_task(self, pipe: Pipeline, task: Task) -> None: ...

    # -- read path -----------------------------------------------------------
    async def get_task(self, task_id: ULID) -> Task | None: ...
    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]: ...
    async def read_for_watch(self, pipe: Pipeline, task_id: ULID) -> Task | None: ...
    async def task_exists(self, task_id: ULID) -> bool: ...
    async def get_active_tasks(self, queues: set[str]) -> list[Task]: ...
    def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]: ...
    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]: ...
    async def get_next_task(self, queues: set[str], pop_timeout: int) -> Task | None: ...

    # -- queue management ----------------------------------------------------
    def stage_remove_from_queue(self, pipe: Pipeline, task: Task) -> None: ...
    async def update_task_heartbeat(self, task: Task) -> None: ...
    async def remove_task_heartbeat(self, task: Task) -> None: ...

    # -- dag fan-in ----------------------------------------------------------
    def stage_init_fan_in(
        self, pipe: Pipeline, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None: ...
    async def init_fan_in(self, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400) -> None: ...
    async def fan_in_complete(self, fan_in_key: str, task_id: ULID) -> int: ...
    async def get_fan_in_members(self, fan_in_key: str) -> list[ULID]: ...

    # -- dag run index -------------------------------------------------------
    async def get_dag_runs(
        self, pagination: DAGRunPagination
    ) -> tuple[list[tuple[ULID, dt.datetime]], int]: ...
    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None: ...
    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None: ...

    # -- lifecycle -----------------------------------------------------------
    async def ensure_index(self) -> None: ...
    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None: ...
    async def clean(
        self,
        queues: set[bytes],
        now: dt.datetime,
        min_queue_age: dt.datetime | None,
        max_queue_age: dt.datetime | None,
    ) -> None: ...


class DeadQueueProtocol(Protocol):
    """Interface for dead letter queue operations."""

    async def ensure_index(self) -> None: ...
    def stage_add(self, pipe: Pipeline, task: Task, failed_at: dt.datetime) -> None: ...
    def stage_remove(self, pipe: Pipeline, task_id: ULID, queue: str, name: str) -> None: ...
    async def get_history(self, task_id: str) -> list[dict[str, Any]]: ...
    async def get_by_ids(self, task_ids: list[str]) -> list[Task]: ...
    async def get_by_filter(
        self,
        queue: str | None,
        task_name: str | None,
        task_version: int | None,
        limit: int,
    ) -> list[Task]: ...
    async def remove_many(self, task_ids: list[str]) -> None: ...
    async def clean(self, earlier_than: dt.datetime) -> None: ...


# ---------------------------------------------------------------------------
# Shared base helpers (not part of public Protocol, internal reuse)
# ---------------------------------------------------------------------------


class _BaseTaskAdapter:
    """Shared key-name helpers and non-storage methods used by both implementations."""

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

    def stage_requeue(self, pipe: Pipeline, task: Task) -> None:
        """Queue ZADD task-queue + save-task commands onto pipe (no execute)."""
        assert task.submitted_at  # noqa: S101
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
        self.stage_save(pipe, task)

    def stage_submit_task(self, pipe: Pipeline, task: Task) -> None:
        """
        Queue ZADD + save-task onto pipe for initial submission (no execute).

        Identical Redis ops to stage_requeue; use for first-time submission where
        the task ULID is guaranteed fresh and the Lua EXISTS guard is not needed.
        """
        assert task.submitted_at  # noqa: S101
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
        self.stage_save(pipe, task)
        self.stage_register_dag_run(pipe, task)

    def stage_register_dag_run(self, pipe: Pipeline, task: Task) -> None:
        """
        Stage DAG run index updates onto pipe if the task belongs to a DAG run.

        Base implementation registers the dag_run_id in the global ``dag-runs``
        sorted set (score = submitted_at, NX so the score always reflects the
        first root task).  Subclasses may override to stage additional keys.
        """
        if task.dag_run_id is None or task.submitted_at is None:
            return
        score = task.submitted_at.timestamp()
        pipe.zadd(self.DAG_RUNS, {bytes(task.dag_run_id): score}, nx=True)

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

    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run, or None if not found."""
        raise NotImplementedError

    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """
        Remove DAG run index entries older than ``max_age``.

        Uses the same threshold as ``clean_terminal_tasks`` so that a DAG run's
        listing entry is pruned at the same time its task blobs are deleted.
        Subclasses should call ``super()`` and then delete any adapter-specific
        per-run keys.
        """
        cutoff = (now - max_age).timestamp()
        stale: list[bytes] = await self.data_store.zrangebyscore(self.DAG_RUNS, "-inf", cutoff)
        if stale:
            await self.data_store.zrem(self.DAG_RUNS, *stale)

    def stage_save(self, pipe: Pipeline, task: Task) -> None:
        raise NotImplementedError

    async def save_task(self, task: Task) -> None:
        """Save task state to the Redis data store."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_save(pipe, task)
        await pipe.execute()

    def stage_remove_from_queue(self, pipe: Pipeline, task: Task) -> None:
        """Queue ZREM task-queue + SREM type-index commands onto pipe (no execute)."""
        pipe.zrem(self.TASKS_BY_QUEUE(queue=task.queue), bytes(task.id))
        pipe.srem(self.TASK_BY_TYPE_IDX(name=task.name), bytes(task.id))

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
        # Round-trip 1: batch fetch all task documents
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
        # Round-trip 2: batch fetch heartbeat scores for all found tasks
        pipe = self.data_store.pipeline(transaction=False)
        for _, task in valid:
            pipe.zscore(self.HEARTBEAT_SCORES(queue=task.queue), bytes(task.id))
        scores = await pipe.execute()
        for (_, task), score in zip(valid, scores, strict=True):
            if score is not None:
                task.heartbeat_at = dt.datetime.fromtimestamp(score, dt.UTC)
        return tasks

    async def _fetch_task_data_bulk(self, _task_ids: list[ULID]) -> list[Any]:
        raise NotImplementedError

    def _decode_task(self, _task_id: ULID, _raw: Any) -> Task:
        raise NotImplementedError

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
        self, pipe: Pipeline, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None:
        """
        Queue fan-in set initialisation commands onto *pipe* without executing.

        Writes two keys:

        - `fan_in_key` — decrementing tracking set; empty when all predecessors done.
        - `{fan_in_key}:members` — permanent membership record; read by the collector
          via `get_fan_in_members` to retrieve all parents' results even after the
          tracking set has expired.

        The *ttl* (seconds) guards against orphaned keys if the DAG is never fully
        executed.  The members set uses `ttl * 2` so a slow collector can still read
        it after the tracking set has expired.
        """
        members_key = f"{fan_in_key}:members"
        encoded = [str(pid).encode() for pid in predecessor_ids]
        pipe.sadd(fan_in_key, *encoded)
        pipe.expire(fan_in_key, ttl)
        pipe.sadd(members_key, *encoded)
        pipe.expire(members_key, ttl * 2)

    async def init_fan_in(self, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400) -> None:
        """Pre-populate a fan-in tracking set in Redis (see stage_init_fan_in for key details)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_init_fan_in(pipe, fan_in_key, predecessor_ids, ttl)
        await pipe.execute()

    async def fan_in_complete(self, fan_in_key: str, task_id: ULID) -> int:
        """
        Atomically remove *task_id* from the fan-in set and return the remaining count.

        Uses a Lua script so the SREM+SCARD pair is truly atomic across all workers.
        Returns -1 if the task ID was not a member of the set (already processed or
        key expired); callers must treat -1 as "do nothing" and not submit the
        collector.  Returns 0 when this was the last predecessor.
        """
        results: list[int] = await self._fan_in_script(keys=[fan_in_key], args=[str(task_id).encode()])
        return int(results[1])

    async def get_fan_in_members(self, fan_in_key: str) -> list[ULID]:
        """
        Return the permanent list of predecessor IDs for a fan-in collector.

        Reads the `{fan_in_key}:members` set written by `init_fan_in`.
        """
        members_key = f"{fan_in_key}:members"
        raw: set[bytes] = await cast("Awaitable[set[bytes]]", self.data_store.smembers(members_key))
        return [ULID.from_str(m.decode()) for m in raw]

    async def task_exists(self, task_id: ULID) -> bool:
        does_exists: int = await self.data_store.exists(self.TASK_DETAILS(task_id=task_id))
        return does_exists == 1

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """Get the next task from the queues in order of priority (first in the list is highest priority)."""
        # TODO: Shuffle/rotate the order of queues to avoid starving any of them
        task_queues = {self.TASKS_BY_QUEUE(queue=queue) for queue in queues}
        while pop_result := await self.data_store.bzpopmin(task_queues, timeout=pop_timeout):
            queue_name, task_id_bytes, _ = pop_result
            logger.debug("Popped task %s from %s", task_id_bytes, queue_name)
            task = await self.get_task(ULID.from_bytes(task_id_bytes))
            if task:
                return task
            logger.error(
                "Task %s popped from queue but data not found; adding to %s",
                task_id_bytes,
                self.DLQ_MISSING_DATA,
            )
            tasks_missing_data.add(1)
            now = dt.datetime.now(dt.UTC)
            await self.data_store.zadd(self.DLQ_MISSING_DATA, {task_id_bytes: now.timestamp()})
        logger.info("task query timed out")
        return None

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

    # Abstract in subclasses
    async def get_task(self, task_id: ULID) -> Task | None:
        raise NotImplementedError

    async def read_for_watch(self, pipe: Pipeline, task_id: ULID) -> Task | None:
        raise NotImplementedError

    async def ensure_index(self) -> None:
        raise NotImplementedError

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        raise NotImplementedError

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        raise NotImplementedError

    async def submit_task(self, task: Task) -> bool:
        raise NotImplementedError

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        raise NotImplementedError
