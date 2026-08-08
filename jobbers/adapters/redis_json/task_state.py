"""
Redis Stack (RedisJSON + RediSearch) task state adapter.

- `RedisJSONTaskState` — TaskStateProtocol + AtomicTaskStateProtocol backed by Redis Stack.
"""

from __future__ import annotations

import datetime as dt
import json
from asyncio import TaskGroup
from typing import TYPE_CHECKING, Any, cast

from redis.commands.search.field import NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query as SearchQuery
from redis.exceptions import ResponseError
from ulid import ULID

from jobbers.adapters._shared import SharedTaskAdapterMixin
from jobbers.adapters.redis_json._helpers import (
    _drop_stale_indexes,
    _escape_tag,
    _get_schema_version,
    _pack,
    _set_schema_version,
)
from jobbers.models.dag import DAGRunDetail, DagRunStatus, DAGRunSummary
from jobbers.models.task import PaginationOrder, Task, TaskPagination

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.dag import DagRunOutcome, DAGRunPagination
    from jobbers.protocols import TransactionHandle


# Atomically increments the run's completed/failed field in its per-run JSON meta doc and
# recomputes+persists the aggregate status. Returns 0 if the doc is missing (already
# cleaned up), 1 otherwise. Never itself produces 'complete' -- that's written separately
# by mark_dag_run_complete, gated on failed_count == 0.
_RECORD_DAG_RUN_TERMINAL_SCRIPT = """
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    local field = '$.failed'
    if ARGV[1] == 'completed' then field = '$.completed' end
    redis.call('JSON.NUMINCRBY', KEYS[1], field, 1)
    local completed = cjson.decode(redis.call('JSON.GET', KEYS[1], '$.completed'))[1]
    local failed = cjson.decode(redis.call('JSON.GET', KEYS[1], '$.failed'))[1]
    local status = 'running'
    if failed > 0 then
        if completed > 0 then status = 'partial_failure' else status = 'failed' end
    end
    redis.call('JSON.SET', KEYS[1], '$.status', cjson.encode(status))
    return 1
"""

# Sets status='complete' iff failed_count == 0. No-op (returns 0) if the doc is missing.
# Idempotent: safe to call more than once, e.g. under concurrent last-completer races.
_MARK_DAG_RUN_COMPLETE_SCRIPT = """
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    local failed = cjson.decode(redis.call('JSON.GET', KEYS[1], '$.failed'))[1]
    if failed == 0 then redis.call('JSON.SET', KEYS[1], '$.status', cjson.encode('complete')) end
    return 1
"""

# Mirror image of _RECORD_DAG_RUN_TERMINAL_SCRIPT's 'failed' branch: decrements 'failed'
# by ARGV[1] (floored at 0) instead of incrementing either counter, and recomputes status
# the same way. Used by reconcile_dag_run_task_retry to undo count stuck tasks' earlier
# failure records before they're resubmitted (see docs/dag-resume-design.md §2.2), in one
# round trip rather than one call per task. No-op (returns 0) if the doc is missing.
_RECONCILE_DAG_RUN_TASK_RETRY_SCRIPT = """
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    local failed = cjson.decode(redis.call('JSON.GET', KEYS[1], '$.failed'))[1]
    local count = tonumber(ARGV[1])
    failed = math.max(0, failed - count)
    redis.call('JSON.SET', KEYS[1], '$.failed', cjson.encode(failed))
    local completed = cjson.decode(redis.call('JSON.GET', KEYS[1], '$.completed'))[1]
    local status = 'running'
    if failed > 0 then
        if completed > 0 then status = 'partial_failure' else status = 'failed' end
    end
    redis.call('JSON.SET', KEYS[1], '$.status', cjson.encode(status))
    return 1
"""


class RedisJSONTaskState(SharedTaskAdapterMixin):
    """
    TaskStateProtocol + AtomicTaskStateProtocol backed by Redis Stack (JSON encoding).

    Requires a Redis Stack instance (RedisJSON + RediSearch modules).
    """

    SCHEMA_VERSION = 1
    INDEX_NAME = f"task-idx-v{SCHEMA_VERSION}"
    _VERSION_KEY = "schema_version:task_json"

    DAG_RUNS = "dag-runs"
    # JSON doc per run: {name, status, completed, failed}.
    DAG_RUN_META = "dag-run:{dag_run_id}:meta".format

    def __init__(self, data_store: Redis) -> None:
        super().__init__(data_store)
        self._record_dag_run_terminal_script = data_store.register_script(_RECORD_DAG_RUN_TERMINAL_SCRIPT)
        self._mark_dag_run_complete_script = data_store.register_script(_MARK_DAG_RUN_COMPLETE_SCRIPT)
        self._reconcile_dag_run_task_retry_script = data_store.register_script(
            _RECONCILE_DAG_RUN_TASK_RETRY_SCRIPT
        )

    # -- Storage primitives --------------------------------------------------

    def pack(self, task: Task) -> str:
        """Serialize a task to a JSON string (float timestamps for RediSearch, string ULIDs)."""
        return json.dumps(_pack(task))

    def unpack(self, task_id: ULID, data: str | dict[str, Any]) -> Task:
        """Deserialize a task from a JSON string or dict."""
        raw: dict[str, Any] = json.loads(data) if isinstance(data, str) else data
        return Task.model_validate({"id": task_id, **raw})

    async def _load_raw(self, key: str) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", await self.data_store.json().get(key))

    async def _load_raw_watch(self, pipe: Pipeline, key: str) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", await pipe.json().get(key))

    def _stage_store(self, pipe: Pipeline, key: str, task: Task) -> None:
        pipe.json().set(key, "$", json.loads(self.pack(task)))

    def _stage_load(self, pipe: Pipeline, key: str) -> None:
        pipe.json().get(key)

    # -- Backend-specific queries --------------------------------------------

    async def ensure_index(self) -> None:
        """Create the RediSearch index, or add any missing fields, if not already at the current schema version."""
        if await _get_schema_version(self.data_store, self._VERSION_KEY) >= self.SCHEMA_VERSION:
            return
        desired_fields = [
            TagField("$.name", as_name="name"),
            TagField("$.queue", as_name="queue"),
            TagField("$.status", as_name="status"),
            TagField("$.dag_run_id", as_name="dag_run_id"),
            NumericField("$.version", as_name="version"),
            NumericField("$.submitted_at", as_name="submitted_at", sortable=True),
        ]
        try:
            await self.data_store.ft(self.INDEX_NAME).info()  # type: ignore[no-untyped-call]
        except ResponseError:
            await self.data_store.ft(self.INDEX_NAME).create_index(
                fields=desired_fields,
                definition=IndexDefinition(prefix=["task:"], index_type=IndexType.JSON),  # type: ignore[no-untyped-call]
            )
            await _set_schema_version(self.data_store, self._VERSION_KEY, self.SCHEMA_VERSION)
            return

        for field in desired_fields:
            try:
                await self.data_store.ft(self.INDEX_NAME).alter_schema_add([field])
            except ResponseError as e:
                if "duplicate" not in str(e).lower():
                    raise
        await _set_schema_version(self.data_store, self._VERSION_KEY, self.SCHEMA_VERSION)

    async def drop_stale_indexes(self) -> list[str]:
        """Drop RediSearch indexes from older schema generations of INDEX_NAME. Returns names dropped."""
        return await _drop_stale_indexes(self.data_store, [self.INDEX_NAME], self.SCHEMA_VERSION)

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Query tasks via the RediSearch index with optional filters."""
        query_parts = [f"@queue:{{{_escape_tag(pagination.queue)}}}"]
        if pagination.task_name:
            query_parts.append(f"@name:{{{_escape_tag(pagination.task_name)}}}")
        if pagination.task_version is not None:
            query_parts.append(f"@version:[{pagination.task_version} {pagination.task_version}]")
        if pagination.status is not None:
            query_parts.append(f"@status:{{{_escape_tag(str(pagination.status))}}}")

        q: SearchQuery = (
            SearchQuery(" ".join(query_parts)).no_content().paging(pagination.offset, pagination.limit)
        )
        if pagination.order_by == PaginationOrder.SUBMITTED_AT:
            q = q.sort_by("submitted_at", asc=True)

        search_results = await self.data_store.ft(self.INDEX_NAME).search(q)
        if not search_results.docs:
            return []

        results: list[Task] = []
        async with TaskGroup() as group:
            for doc in search_results.docs:
                task_id = ULID.from_str(doc.id.removeprefix("task:"))
                group.create_task(self._add_task_to_results(task_id, results))

        if pagination.order_by == PaginationOrder.SUBMITTED_AT:
            results.sort(key=lambda t: t.submitted_at or dt.datetime.min)
        else:
            results.sort(key=lambda t: t.id)
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
        p.json().set(
            self.DAG_RUN_META(dag_run_id=task.dag_run_id),
            "$",
            {
                "name": task.dag_run_name or "",
                "status": DagRunStatus.RUNNING.value,
                "completed": 0,
                "failed": 0,
            },
            nx=True,
        )

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
            pipe.json().get(self.DAG_RUN_META(dag_run_id=ULID.from_bytes(dag_id_bytes)))
        meta_docs = cast("list[dict[str, Any] | None]", await pipe.execute()) if raw else []
        summaries = [
            DAGRunSummary(
                dag_run_id=ULID.from_bytes(dag_id_bytes),
                name=(meta or {}).get("name", ""),
                # Once cancellation is requested, the raw "status" field is superseded
                # here -- see get_dag_run's docstring for why this list view can't
                # afford to distinguish CANCELLING from CANCELLED cheaply.
                status=(
                    DagRunStatus.CANCELLING
                    if (meta or {}).get("cancelled_at") is not None
                    else DagRunStatus((meta or {}).get("status", DagRunStatus.RUNNING.value))
                ),
                submitted_at=dt.datetime.fromtimestamp(score, dt.UTC),
            )
            for (dag_id_bytes, score), meta in zip(raw, meta_docs, strict=True)
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
        meta = cast(
            "dict[str, Any] | None",
            await self.data_store.json().get(self.DAG_RUN_META(dag_run_id=dag_run_id)),
        )
        status = DagRunStatus((meta or {}).get("status", DagRunStatus.RUNNING.value))
        cancelled_at = (meta or {}).get("cancelled_at")
        if cancelled_at is not None:
            # Once cancellation is requested, the raw "status" field (still being
            # written by record_dag_run_task_terminal's running/partial_failure/failed
            # recompute) is superseded here: a cancelled/cancelling run reports
            # CANCELLED once every registered task has reached a terminal outcome,
            # CANCELLING until then. Cancelled tasks never leave DAG_RUN_PENDING (see
            # finalize_dag_run_task), so "pending count reaches 0" can't be used as the
            # settled signal -- completed+failed reaching the full task count can.
            completed = int((meta or {}).get("completed", 0))
            failed = int((meta or {}).get("failed", 0))
            status = (
                DagRunStatus.CANCELLED if (completed + failed) >= len(task_ids) else DagRunStatus.CANCELLING
            )
        return DAGRunDetail(
            dag_run_id=dag_run_id,
            name=(meta or {}).get("name", ""),
            status=status,
            submitted_at=submitted_at,
            task_ids=task_ids,
        )

    async def mark_dag_run_cancelling(self, dag_run_id: ULID) -> None:
        """Idempotently record that cancellation was requested for this run (JSON.SET NX)."""
        now = dt.datetime.now(dt.UTC).timestamp()
        await self.data_store.json().set(
            self.DAG_RUN_META(dag_run_id=dag_run_id), "$.cancelled_at", now, nx=True
        )

    async def is_dag_run_cancelling(self, dag_run_id: ULID) -> bool:
        """Cheap check: has cancellation been requested for this run."""
        meta = cast(
            "dict[str, Any] | None",
            await self.data_store.json().get(self.DAG_RUN_META(dag_run_id=dag_run_id)),
        )
        return bool(meta and meta.get("cancelled_at") is not None)

    async def clear_dag_run_cancellation(self, dag_run_id: ULID) -> None:
        """Clear a previously-set cancellation marker (JSON.DEL). No-op if it wasn't set."""
        await self.data_store.json().delete(self.DAG_RUN_META(dag_run_id=dag_run_id), "$.cancelled_at")

    async def reconcile_dag_run_task_retry(self, dag_run_id: ULID, count: int = 1) -> None:
        """Undo ``count`` earlier 'failed' terminal-outcome records for tasks about to be retried."""
        await self._reconcile_dag_run_task_retry_script(
            keys=[self.DAG_RUN_META(dag_run_id=dag_run_id)], args=[count]
        )

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
