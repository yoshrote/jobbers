"""
Plain Redis task state adapter.

- `RedisTaskState` — TaskStateProtocol + AtomicTaskStateProtocol backed by plain Redis (msgpack).
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any, cast

from ulid import ULID

from jobbers.adapters._shared import SharedTaskAdapterMixin
from jobbers.adapters.redis._helpers import _pack
from jobbers.models.dag import DAGRunDetail, DagRunStatus, DAGRunSummary
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.utils.serialization import deserialize

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.dag import DagRunOutcome, DAGRunPagination
    from jobbers.protocols import TransactionHandle


# Atomically increments the run's completed/failed field in its per-run meta Hash and
# recomputes+persists the aggregate status. Returns 0 if the run's meta hash is missing
# (already cleaned up), 1 otherwise. Never itself produces 'complete' -- that's written
# separately by mark_dag_run_complete, gated on failed_count == 0.
_RECORD_DAG_RUN_TERMINAL_SCRIPT = """
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    local field = 'failed'
    if ARGV[1] == 'completed' then field = 'completed' end
    redis.call('HINCRBY', KEYS[1], field, 1)
    local completed = tonumber(redis.call('HGET', KEYS[1], 'completed')) or 0
    local failed = tonumber(redis.call('HGET', KEYS[1], 'failed')) or 0
    local status = 'running'
    if failed > 0 then
        if completed > 0 then status = 'partial_failure' else status = 'failed' end
    end
    redis.call('HSET', KEYS[1], 'status', status)
    return 1
"""

# Sets status='complete' iff failed_count == 0. No-op (returns 0) if the run's meta hash
# is missing. Idempotent: safe to call more than once, e.g. under concurrent last-completer
# races.
_MARK_DAG_RUN_COMPLETE_SCRIPT = """
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    local failed = tonumber(redis.call('HGET', KEYS[1], 'failed')) or 0
    if failed == 0 then redis.call('HSET', KEYS[1], 'status', 'complete') end
    return 1
"""


class RedisTaskState(SharedTaskAdapterMixin):
    """
    TaskStateProtocol + AtomicTaskStateProtocol backed by plain Redis (msgpack encoding).

    Works with any standard Redis instance (no Redis Stack required).
    `get_all_tasks` applies `task_name`, `task_version`, and `status` filters in Python
    after fetching candidate task IDs from the queue sorted set.
    """

    DAG_RUNS = "dag-runs"
    # Hash per run: name, status, completed (int), failed (int).
    DAG_RUN_META = "dag-run:{dag_run_id}:meta".format

    def __init__(self, data_store: Redis) -> None:
        super().__init__(data_store)
        self._record_dag_run_terminal_script = data_store.register_script(_RECORD_DAG_RUN_TERMINAL_SCRIPT)
        self._mark_dag_run_complete_script = data_store.register_script(_MARK_DAG_RUN_COMPLETE_SCRIPT)

    # -- Storage primitives --------------------------------------------------

    def pack(self, task: Task) -> bytes:
        """Serialize a task to msgpack bytes."""
        return _pack(task, exclude={"id"})

    def unpack(self, task_id: ULID, data: bytes) -> Task:
        """Deserialize a task from msgpack bytes."""
        return Task.model_validate({"id": task_id, **deserialize(data)})

    async def _load_raw(self, key: str) -> bytes | None:
        return cast("bytes | None", await self.data_store.get(key))

    async def _load_raw_watch(self, pipe: Pipeline, key: str) -> bytes | None:
        return cast("bytes | None", await pipe.get(key))

    def _stage_store(self, pipe: Pipeline, key: str, task: Task) -> None:
        pipe.set(key, self.pack(task))

    def _stage_load(self, pipe: Pipeline, key: str) -> None:
        pipe.get(key)

    # -- Backend-specific queries --------------------------------------------

    async def ensure_index(self) -> None:
        """No-op: msgpack backend does not use a search index."""

    async def drop_stale_indexes(self) -> list[str]:
        """No-op: msgpack backend does not use a search index."""
        return []

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Fetch tasks from the queue sorted set and filter in Python."""
        if pagination.order_by == PaginationOrder.SUBMITTED_AT:
            raw_ids = cast(
                "list[bytes]",
                await self.data_store.zrange(
                    self.TASKS_BY_QUEUE(queue=pagination.queue),
                    "-inf",
                    "+inf",
                    byscore=True,
                    offset=pagination.offset,
                    num=pagination.limit * 5,
                ),
            )
        else:
            raw_ids = cast(
                "list[bytes]",
                await self.data_store.zrange(
                    self.TASKS_BY_QUEUE(queue=pagination.queue),
                    pagination.offset,
                    pagination.offset + pagination.limit * 5 - 1,
                ),
            )

        results: list[Task] = []
        for raw_id in raw_ids:
            if len(results) >= pagination.limit:
                break
            task_id = ULID.from_bytes(raw_id)
            raw_data = cast("bytes | None", await self.data_store.get(self.TASK_DETAILS(task_id=task_id)))
            if raw_data is None:
                continue
            task = self.unpack(task_id, raw_data)
            if pagination.task_name is not None and task.name != pagination.task_name:
                continue
            if pagination.task_version is not None and task.version != pagination.task_version:
                continue
            if pagination.status is not None and task.status != pagination.status:
                continue
            results.append(task)
        return results

    # -- DAG run index --------------------------------------------------------

    def stage_register_dag_run(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage DAG run index + metadata registration onto pipe if the task belongs to a DAG run."""
        if task.dag_run_id is None or task.submitted_at is None:
            return
        score = task.submitted_at.timestamp()
        p: Any = pipe
        p.zadd(self.DAG_RUNS, {bytes(task.dag_run_id): score}, nx=True)
        p.sadd(self.DAG_RUN_PENDING(dag_run_id=task.dag_run_id), bytes(task.id))
        meta_key = self.DAG_RUN_META(dag_run_id=task.dag_run_id)
        p.hsetnx(meta_key, "name", task.dag_run_name or "")
        p.hsetnx(meta_key, "status", DagRunStatus.RUNNING.value)
        p.hsetnx(meta_key, "completed", 0)
        p.hsetnx(meta_key, "failed", 0)

    async def get_dag_runs(self, pagination: DAGRunPagination) -> tuple[list[DAGRunSummary], int]:
        """Return a paginated list of DAG runs ordered by submission time (oldest first)."""
        total: int = await self.data_store.zcard(self.DAG_RUNS)
        raw = cast(
            "list[tuple[bytes, float]]",
            await self.data_store.zrange(
                self.DAG_RUNS, pagination.offset, pagination.offset + pagination.limit - 1, withscores=True
            ),
        )
        pipe = self.data_store.pipeline(transaction=False)
        for dag_id_bytes, _ in raw:
            pipe.hmget(
                self.DAG_RUN_META(dag_run_id=ULID.from_bytes(dag_id_bytes)), "name", "status", "cancelled_at"
            )
        meta_rows = cast("list[list[bytes | None]]", await pipe.execute()) if raw else []
        summaries = [
            DAGRunSummary(
                dag_run_id=ULID.from_bytes(dag_id_bytes),
                name=name.decode() if name else "",
                # Once cancellation is requested, the raw "status" field is superseded
                # here -- see get_dag_run's docstring for why this list view can't
                # afford to distinguish CANCELLING from CANCELLED cheaply.
                status=(
                    DagRunStatus.CANCELLING
                    if cancelled_at
                    else (DagRunStatus(status.decode()) if status else DagRunStatus.RUNNING)
                ),
                submitted_at=dt.datetime.fromtimestamp(score, dt.UTC),
            )
            for (dag_id_bytes, score), (name, status, cancelled_at) in zip(raw, meta_rows, strict=True)
        ]
        return summaries, total

    async def get_dag_run(self, dag_run_id: ULID) -> DAGRunDetail | None:
        """Return details for a DAG run via SUNION(pending, closed), or None if not registered."""
        score: float | None = await self.data_store.zscore(self.DAG_RUNS, bytes(dag_run_id))
        if score is None:
            return None
        submitted_at = dt.datetime.fromtimestamp(score, dt.UTC)
        raw_ids = cast(
            "set[bytes]",
            await self.data_store.sunion(
                [self.DAG_RUN_PENDING(dag_run_id=dag_run_id), self.DAG_RUN_CLOSED(dag_run_id=dag_run_id)]
            ),
        )
        # SUNION has no ordering guarantee; ULIDs sort lexicographically by creation
        # time, so sorting restores the submission-order guarantee callers rely on
        # (e.g. the /dags/{dag_run_id} response) at no extra I/O cost.
        task_ids = sorted(ULID.from_bytes(b) for b in raw_ids)
        name_raw, status_raw, cancelled_raw, completed_raw, failed_raw = cast(
            "list[bytes | None]",
            await self.data_store.hmget(
                self.DAG_RUN_META(dag_run_id=dag_run_id),
                "name",
                "status",
                "cancelled_at",
                "completed",
                "failed",
            ),
        )
        status = DagRunStatus(status_raw.decode()) if status_raw else DagRunStatus.RUNNING
        if cancelled_raw:
            # Once cancellation is requested, the raw "status" field (still being
            # written by record_dag_run_task_terminal's running/partial_failure/failed
            # recompute) is superseded here: a cancelled/cancelling run reports
            # CANCELLED once every registered task has reached a terminal outcome,
            # CANCELLING until then. Cancelled tasks never leave DAG_RUN_PENDING (see
            # finalize_dag_run_task), so "pending count reaches 0" can't be used as the
            # settled signal -- completed+failed reaching the full task count can.
            completed = int(completed_raw or 0)
            failed = int(failed_raw or 0)
            status = (
                DagRunStatus.CANCELLED if (completed + failed) >= len(task_ids) else DagRunStatus.CANCELLING
            )
        return DAGRunDetail(
            dag_run_id=dag_run_id,
            name=name_raw.decode() if name_raw else "",
            status=status,
            submitted_at=submitted_at,
            task_ids=task_ids,
        )

    async def mark_dag_run_cancelling(self, dag_run_id: ULID) -> None:
        """Idempotently record that cancellation was requested for this run (HSETNX)."""
        now = dt.datetime.now(dt.UTC).timestamp()
        await self.data_store.hsetnx(self.DAG_RUN_META(dag_run_id=dag_run_id), "cancelled_at", now)

    async def is_dag_run_cancelling(self, dag_run_id: ULID) -> bool:
        """Cheap check: has cancellation been requested for this run."""
        val = await self.data_store.hget(self.DAG_RUN_META(dag_run_id=dag_run_id), "cancelled_at")
        return val is not None

    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Remove DAG run index entries and all their per-run structures older than ``max_age``."""
        cutoff = (now - max_age).timestamp()
        stale = cast("list[bytes]", await self.data_store.zrange(self.DAG_RUNS, "-inf", cutoff, byscore=True))
        if not stale:
            return
        pipe = self.data_store.pipeline(transaction=False)
        pipe.zrem(self.DAG_RUNS, *stale)
        for dag_id_bytes in stale:
            try:
                dag_run_id = ULID.from_bytes(dag_id_bytes)
            except ValueError:
                continue
            pipe.delete(self.DAG_RUN_PENDING(dag_run_id=dag_run_id))
            pipe.delete(self.DAG_RUN_CLOSED(dag_run_id=dag_run_id))
            pipe.delete(self.DAG_RUN_FANIN(dag_run_id=dag_run_id))
            pipe.delete(self.DAG_RUN_FANIN_MEMBERS(dag_run_id=dag_run_id))
            pipe.delete(self.DAG_RUN_META(dag_run_id=dag_run_id))
        await pipe.execute()

    async def record_dag_run_task_terminal(self, dag_run_id: ULID, outcome: DagRunOutcome) -> None:
        """Atomically increment the run's completed/failed counters and recompute+persist status."""
        await self._record_dag_run_terminal_script(
            keys=[self.DAG_RUN_META(dag_run_id=dag_run_id)], args=[outcome]
        )

    async def stage_record_dag_run_task_terminal(
        self, pipe: TransactionHandle, dag_run_id: ULID, outcome: DagRunOutcome
    ) -> None:
        """
        Stage record_dag_run_task_terminal onto pipe (part of AtomicDagRunProtocol).

        Must be awaited: this only queues the EVALSHA command onto pipe, it does not
        execute it (registered Lua scripts are coroutines under redis-py's async
        client regardless of the target). The real result is only available in the
        list returned by the eventual `await pipe.execute()`.
        """
        p: Any = pipe
        await self._record_dag_run_terminal_script(
            keys=[self.DAG_RUN_META(dag_run_id=dag_run_id)], args=[outcome], client=p
        )

    async def mark_dag_run_complete(self, dag_run_id: ULID) -> None:
        """Set status='complete' iff failed_count == 0. No-op if the run's record is missing."""
        await self._mark_dag_run_complete_script(keys=[self.DAG_RUN_META(dag_run_id=dag_run_id)], args=[])
