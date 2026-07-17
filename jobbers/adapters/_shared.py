"""
Shared base classes for Redis-backed task adapters.

``SharedTaskAdapterMixin`` is an internal ABC that implements the ``TaskStateProtocol``
and ``AtomicTaskStateProtocol`` logic identical across the plain-Redis and RedisJSON
backends. It is not part of the public adapter API.

DAG-run-index methods (``stage_register_dag_run``, ``get_dag_runs``, ``get_dag_run``,
``clean_dag_runs``) are *not* here — they live per-backend in ``redis/task_state.py``
and ``redis_json/task_state.py``, since the two backends store DAG-run name/status
metadata in genuinely different native shapes. Fan-in tracking and the pending/closed
completion counter (``close_dag_run_task`` and friends) are unaffected by that and
remain shared here.

``_SharedRedisTaskSubmitBase`` is an internal base that implements ``TaskSubmitProtocol``
(submit_task, submit_rate_limited_task, get_next_task) shared across both Redis backends.
It requires a ``pack`` callable and a ``get_task`` callable supplied at construction time,
so the state and submit classes remain independent objects.

Concrete adapters inherit ``SharedTaskAdapterMixin`` and implement:
  - Storage primitives: ``_load_raw``, ``_load_raw_watch``, ``_stage_store``,
    ``_stage_load``
  - Serialization: ``pack``, ``unpack``
  - Backend-specific queries: ``ensure_index``, ``get_all_tasks``

Concrete submit classes inherit ``_SharedRedisTaskSubmitBase`` and define:
  - Lua script class attributes: ``SUBMIT_SCRIPT``, ``SUBMIT_RATE_LIMITED_SCRIPT``
  - ``_extra_submit_keys``, ``_extra_rate_limited_keys`` if needed
"""

from __future__ import annotations

import datetime as dt
import json
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

    from jobbers.models.dag import DAGRunDetail, DagRunOutcome, DAGRunPagination, DAGRunSummary
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
    # Per-DAG-run structures — flat key count regardless of task count or fan-out nesting depth.
    DAG_RUN_PENDING = "dag-run:{dag_run_id}:pending".format  # Set: task IDs submitted, not yet closed.
    DAG_RUN_CLOSED = "dag-run:{dag_run_id}:closed".format  # Set: task IDs that have closed.
    # Hash: "remaining:{fan_in_key}" -> int countdown,
    #       "pending:{fan_in_key}:{predecessor_id}" -> "1" (one field per uncompleted predecessor).
    DAG_RUN_FANIN = "dag-run:{dag_run_id}:fanin".format
    # Hash: "{fan_in_key}" -> JSON list of permanent predecessor ULID strings.
    DAG_RUN_FANIN_MEMBERS = "dag-run:{dag_run_id}:fanin-members".format

    # Atomically remove task_id from the run's pending set, record it as closed, and
    # return the remaining pending count. Returns {removed=0, remaining=-1} if the ID
    # was not pending (already closed, or the run's pending key expired/was swept).
    _CLOSE_DAG_RUN_TASK_SCRIPT = """
        local removed = redis.call('SREM', KEYS[1], ARGV[1])
        if removed == 0 then
            return {0, -1}
        end
        redis.call('SADD', KEYS[2], ARGV[1])
        return {removed, redis.call('SCARD', KEYS[1])}
    """

    # Atomically close one predecessor for one fan_in_key within the run's shared fan-in
    # Hash. HDEL on the per-predecessor "pending" field mirrors the old per-collector
    # Set's SREM-on-a-member exactly: it returns 0 (and hence the {0, -1} sentinel) both
    # when this ID was never a registered predecessor and when it already closed.
    _FAN_IN_HASH_SCRIPT = """
        local pending_field = 'pending:' .. ARGV[1] .. ':' .. ARGV[2]
        if redis.call('HDEL', KEYS[1], pending_field) == 0 then
            return {0, -1}
        end
        local remaining_field = 'remaining:' .. ARGV[1]
        local remaining = redis.call('HINCRBY', KEYS[1], remaining_field, -1)
        return {1, remaining}
    """

    # Atomically swap old_id for new_id for one fan_in_key: renames the "pending" field in
    # the fan-in Hash (KEYS[1]) so the new id's future close is recognised, and rewrites the
    # JSON-encoded predecessor list in the members Hash (KEYS[2]). Used by delegate_fan_in
    # when a nested DynamicFanOut transfers outer fan-in responsibility to a grandcollector.
    # Does not touch the remaining count — a delegated ID hasn't closed yet, only its
    # identity within the pending/members tracking changes.
    _DELEGATE_FAN_IN_HASH_SCRIPT = """
        local old_pending = 'pending:' .. ARGV[1] .. ':' .. ARGV[2]
        local new_pending = 'pending:' .. ARGV[1] .. ':' .. ARGV[3]
        if redis.call('HEXISTS', KEYS[1], old_pending) == 1 then
            redis.call('HDEL', KEYS[1], old_pending)
            redis.call('HSET', KEYS[1], new_pending, '1')
        end

        local blob = redis.call('HGET', KEYS[2], ARGV[1])
        if blob then
            local members = cjson.decode(blob)
            for i, m in ipairs(members) do
                if m == ARGV[2] then
                    members[i] = ARGV[3]
                end
            end
            redis.call('HSET', KEYS[2], ARGV[1], cjson.encode(members))
        end
        return 1
    """

    def __init__(self, data_store: Redis) -> None:
        self.data_store: Redis = data_store
        self._close_dag_run_task_script = self.data_store.register_script(self._CLOSE_DAG_RUN_TASK_SCRIPT)
        self._fan_in_hash_script = self.data_store.register_script(self._FAN_IN_HASH_SCRIPT)
        self._delegate_fan_in_hash_script = self.data_store.register_script(self._DELEGATE_FAN_IN_HASH_SCRIPT)

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

    # DAG-run-index primitives — backend-specific because RedisTaskState and
    # RedisJSONTaskState store run name/status/counters in different native shapes
    # (a flat-field Hash vs. a JSON document per run).
    @abstractmethod
    def stage_register_dag_run(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage DAG run index + metadata registration onto pipe if task belongs to a DAG run."""

    @abstractmethod
    async def get_dag_runs(self, pagination: DAGRunPagination) -> tuple[list[DAGRunSummary], int]:
        """Return a paginated list of DAG runs ordered by submission time (oldest first)."""

    @abstractmethod
    async def get_dag_run(self, dag_run_id: ULID) -> DAGRunDetail | None:
        """Return details for a DAG run, or None if not registered."""

    @abstractmethod
    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Remove DAG run index entries and all their per-run structures older than ``max_age``."""

    @abstractmethod
    async def record_dag_run_task_terminal(self, dag_run_id: ULID, outcome: DagRunOutcome) -> None:
        """Atomically increment the run's completed/failed counters and recompute+persist status."""

    @abstractmethod
    async def mark_dag_run_complete(self, dag_run_id: ULID) -> None:
        """Set status='complete' iff failed_count == 0. No-op if the run's record is missing."""

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
        terminal_statuses = TaskStatus.terminal_statuses()
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
            members = cast(
                "list[bytes]", await self.data_store.zrange(self.HEARTBEAT_SCORES(queue=queue), 0, -1)
            )
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
            task_ids = cast(
                "list[bytes]",
                await self.data_store.zrange(
                    self.HEARTBEAT_SCORES(queue=queue), 0, cutoff_time.timestamp(), byscore=True
                ),
            )
            stale_task_ids.update(task_ids)
        fetched = await self.get_tasks_bulk([ULID.from_bytes(b) for b in stale_task_ids])
        for task in fetched:
            if task is not None:
                yield task

    def stage_init_fan_in(
        self,
        pipe: TransactionHandle,
        dag_run_id: ULID,
        fan_in_key: str,
        predecessor_ids: set[ULID],
        ttl: int = 86400,
    ) -> None:
        """Queue fan-in initialisation commands onto *pipe* without executing."""
        p: Any = pipe
        fanin_key = self.DAG_RUN_FANIN(dag_run_id=dag_run_id)
        members_key = self.DAG_RUN_FANIN_MEMBERS(dag_run_id=dag_run_id)
        pending_fields: dict[str, str] = {f"remaining:{fan_in_key}": str(len(predecessor_ids))}
        for pid in predecessor_ids:
            pending_fields[f"pending:{fan_in_key}:{pid}"] = "1"
        p.hset(fanin_key, mapping=pending_fields)
        # Both hashes are shared by every collector registered under this dag_run_id.
        # NX sets the expiry the first time the key is created; GT then only ever
        # extends it on later calls, so a later collector's shorter ttl can never
        # shrink the expiry out from under an earlier, still-pending collector's
        # tracking data (plain EXPIRE would overwrite, not extend).
        p.expire(fanin_key, ttl, nx=True)
        p.expire(fanin_key, ttl, gt=True)
        p.hset(members_key, fan_in_key, json.dumps([str(pid) for pid in predecessor_ids]))
        # Members data must outlive the countdown by a margin so get_fan_in_members
        # can still succeed even if read right as the countdown key expires.
        p.expire(members_key, ttl * 2, nx=True)
        p.expire(members_key, ttl * 2, gt=True)

    async def init_fan_in(
        self, dag_run_id: ULID, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None:
        """Pre-populate a fan-in countdown and its permanent member list."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_init_fan_in(pipe, dag_run_id, fan_in_key, predecessor_ids, ttl)
        await pipe.execute()

    async def fan_in_complete(self, dag_run_id: ULID, fan_in_key: str, task_id: ULID) -> int:
        """Atomically close *task_id* for *fan_in_key* and return the remaining count."""
        results: list[int] = await self._fan_in_hash_script(
            keys=[self.DAG_RUN_FANIN(dag_run_id=dag_run_id)],
            args=[fan_in_key.encode(), str(task_id).encode()],
        )
        return int(results[1])

    async def get_fan_in_members(self, dag_run_id: ULID, fan_in_key: str) -> list[ULID]:
        """Return the permanent list of predecessor IDs for a fan-in collector."""
        raw = await self.data_store.hget(self.DAG_RUN_FANIN_MEMBERS(dag_run_id=dag_run_id), fan_in_key)
        if raw is None:
            return []
        decoded = raw.decode() if isinstance(raw, bytes) else raw
        return [ULID.from_str(s) for s in json.loads(decoded)]

    async def delegate_fan_in(self, dag_run_id: ULID, fan_in_key: str, old_id: ULID, new_id: ULID) -> None:
        """Atomically swap *old_id* for *new_id* in the fan-in pending tracking and members list."""
        await self._delegate_fan_in_hash_script(
            keys=[
                self.DAG_RUN_FANIN(dag_run_id=dag_run_id),
                self.DAG_RUN_FANIN_MEMBERS(dag_run_id=dag_run_id),
            ],
            args=[fan_in_key, str(old_id), str(new_id)],
        )

    async def close_dag_run_task(self, dag_run_id: ULID, task_id: ULID) -> int:
        """Atomically move task_id from the DAG run's pending set to closed; return the remaining count."""
        results: list[int] = await self._close_dag_run_task_script(
            keys=[self.DAG_RUN_PENDING(dag_run_id=dag_run_id), self.DAG_RUN_CLOSED(dag_run_id=dag_run_id)],
            args=[bytes(task_id)],
        )
        return int(results[1])

    async def task_exists(self, task_id: ULID) -> bool:
        does_exists: int = await self.data_store.exists(self.TASK_DETAILS(task_id=task_id))
        return does_exists == 1

    async def clean(
        self,
        queues: set[str],
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
                        self.TASKS_BY_QUEUE(queue=queue),
                        min=earliest_time.timestamp(),
                        max=latest_time.timestamp(),
                    )
                else:
                    pipe.zremrangebyscore(
                        self.TASKS_BY_QUEUE(queue=queue),
                        min=0,
                        max=earliest_time.timestamp(),
                    )
                    pipe.zremrangebyscore(
                        self.TASKS_BY_QUEUE(queue=queue),
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
    DAG_RUN_PENDING = "dag-run:{dag_run_id}:pending".format
    # Identical key pattern on both Redis backends (flat Hash vs. JSON doc, same name).
    DAG_RUN_META = "dag-run:{dag_run_id}:meta".format
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

    async def enqueue(self, task: Task) -> None:
        """
        Add an already-persisted task's ID to its queue (saga-safe re-enqueue).

        Assumes the caller has already saved the task blob via
        ``TaskStateProtocol.save_task()`` — this only stages the queue-membership
        ZADD and DAG-run registration, without rewriting the blob and without
        ``submit_task()``'s "skip the ZADD if the blob already exists" guard (which
        is what makes ``submit_task()`` unsafe to call for a task whose blob was
        just saved, or that was already submitted once before, e.g. a requeue).
        """
        assert task.submitted_at  # noqa: S101
        pipe = self._data_store.pipeline(transaction=True)
        pipe.zadd(self.TASKS_BY_QUEUE(queue=task.queue), {bytes(task.id): task.submitted_at.timestamp()})
        if task.dag_run_id is not None:
            pipe.zadd(self.DAG_RUNS, {bytes(task.dag_run_id): task.submitted_at.timestamp()}, nx=True)
            pipe.sadd(self.DAG_RUN_PENDING(dag_run_id=task.dag_run_id), bytes(task.id))
            self._stage_dag_run_meta_seed(pipe, task)
        await pipe.execute()

    def _stage_dag_run_meta_seed(self, pipe: Pipeline, task: Task) -> None:
        """Backend-specific DAG-run meta seeding; no-op by default (overridden per backend)."""

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
                task.dag_run_name or "",
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
                task.dag_run_name or "",
            ),
        )
        return result == 1

    async def clean_rate_limiter(
        self, queues: set[str], now: dt.datetime, rate_limit_age: dt.timedelta
    ) -> None:
        earliest_time = now - rate_limit_age
        pipe = self._data_store.pipeline(transaction=True)
        for queue in queues:
            pipe.zremrangebyscore(self.QUEUE_RATE_LIMITER(queue=queue), min=0, max=earliest_time.timestamp())
        await pipe.execute()

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """Get the next task from the queues in order of priority."""
        task_queues = {self.TASKS_BY_QUEUE(queue=queue) for queue in queues}
        while pop_result := await self._data_store.bzpopmin(task_queues, timeout=pop_timeout):
            queue_name, task_id_bytes, _ = cast("tuple[bytes, bytes, float]", pop_result)
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
