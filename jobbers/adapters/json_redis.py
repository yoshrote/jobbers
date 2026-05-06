"""
Redis Stack (RedisJSON + RediSearch) backed implementations.

- `JsonTaskAdapter` — stores tasks as RedisJSON documents, queries via RediSearch.
- `JsonDeadQueue` — stores dead-letter entries as RedisJSON documents with a
  RediSearch index for server-side filtering and sorting.

Both require a Redis Stack instance with the RedisJSON and RediSearch modules.
"""

from __future__ import annotations

import datetime as dt
import json
from asyncio import TaskGroup
from typing import TYPE_CHECKING, Any, cast

from redis.exceptions import ResponseError
from ulid import ULID

from jobbers.adapters.task_adapter import SharedTaskAdapterMixin
from jobbers.models.task import Task, TaskPagination

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline, Redis

    from jobbers.adapters.task_adapter import TaskAdapterProtocol


def _escape_tag(value: str) -> str:
    """Escape special characters for a RediSearch TAG query value."""
    special = set(r',.<>{}[]"\':;!@#$%^&*()\-+=~| ')
    return "".join(f"\\{c}" if c in special else c for c in value)


# ---------------------------------------------------------------------------
# JsonTaskAdapter  (Redis Stack: RedisJSON + RediSearch)
# ---------------------------------------------------------------------------


class JsonTaskAdapter(SharedTaskAdapterMixin):
    """
    Stores tasks as RedisJSON documents; queries via RediSearch.

    Requires a Redis Stack instance (RedisJSON + RediSearch modules).
    """

    INDEX_NAME = "task-idx"

    # Atomically enqueue a new task (no rate limiting).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # KEYS[4] = dag-runs
    # ARGV[1] = submitted_at timestamp
    # ARGV[2] = task_id bytes
    # ARGV[3] = '1' to SADD type index, '0' to SREM
    # ARGV[4] = JSON-encoded task blob
    # ARGV[5] = dag_run_id bytes (empty string if task is not part of a DAG run)
    SUBMIT_SCRIPT = """
        local exists = redis.call('EXISTS', KEYS[2])
        if exists == 0 then
            redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
        end
        redis.call('JSON.SET', KEYS[2], '$', ARGV[4])
        if ARGV[3] == '1' then
            redis.call('SADD', KEYS[3], ARGV[2])
        else
            redis.call('SREM', KEYS[3], ARGV[2])
        end
        if ARGV[5] ~= '' then
            redis.call('ZADD', KEYS[4], 'NX', ARGV[1], ARGV[5])
        end
        return 1
    """

    # Atomically check rate limit and enqueue.
    # KEYS[1] = rate-limiter:{queue}
    # KEYS[2] = task-queues:{queue}
    # KEYS[3] = task:{task_id}
    # KEYS[4] = task-type-idx:{name}
    # KEYS[5] = dag-runs
    # ARGV[1] = earliest_time
    # ARGV[2] = rate_numerator
    # ARGV[3] = submitted_at timestamp
    # ARGV[4] = task_id bytes
    # ARGV[5] = '1'/'0' for type index
    # ARGV[6] = JSON-encoded task blob
    # ARGV[7] = dag_run_id bytes (empty string if task is not part of a DAG run)
    # Returns: 1 if enqueued, 0 if rate-limited
    SUBMIT_RATE_LIMITED_SCRIPT = """
        local enqueued = 0
        local exists = redis.call('EXISTS', KEYS[3])
        local numerator = tonumber(ARGV[2])
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
        if exists == 0 then
            local count = redis.call('ZCARD', KEYS[1])
            if numerator ~= 0 and count < numerator then
                redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
                redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
                redis.call('JSON.SET', KEYS[3], '$', ARGV[6])
                enqueued = 1
            end
        else
            redis.call('JSON.SET', KEYS[3], '$', ARGV[6])
            enqueued = 1
        end
        if enqueued == 1 and ARGV[5] == '1' then
            redis.call('SADD', KEYS[4], ARGV[4])
        else
            redis.call('SREM', KEYS[4], ARGV[4])
        end
        if enqueued == 1 and ARGV[7] ~= '' then
            redis.call('ZADD', KEYS[5], 'NX', ARGV[3], ARGV[7])
        end
        return enqueued
    """

    # -- Storage primitives --------------------------------------------------

    def pack(self, task: Task) -> str:
        """Serialize a task to a JSON string."""
        return json.dumps(task.to_dict())

    def unpack(self, task_id: ULID, data: str | dict[str, Any]) -> Task:
        """Deserialize a task from a JSON string or dict."""
        raw: dict[str, Any] = json.loads(data) if isinstance(data, str) else data
        return Task.from_dict(task_id, raw)

    async def _load_raw(self, key: str) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", await self.data_store.json().get(key))  # type: ignore[misc]

    async def _load_raw_watch(self, pipe: Pipeline, key: str) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", await pipe.json().get(key))  # type: ignore[misc]

    def _stage_store(self, pipe: Pipeline, key: str, task: Task) -> None:
        pipe.json().set(key, "$", task.to_dict())

    def _stage_load(self, pipe: Pipeline, key: str) -> None:
        pipe.json().get(key)

    def _extra_submit_keys(self, task: Task) -> list[str]:
        return []

    def _extra_rate_limited_keys(self, task: Task) -> list[str]:
        return []

    # -- Backend-specific queries --------------------------------------------

    async def ensure_index(self) -> None:
        """Create the RediSearch index, or add any missing fields to an existing one."""
        from redis.commands.search.field import NumericField, TagField
        from redis.commands.search.index_definition import (
            IndexDefinition,
            IndexType,
        )

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
            # Index does not exist — create it fresh.
            await self.data_store.ft(self.INDEX_NAME).create_index(
                fields=desired_fields,
                definition=IndexDefinition(prefix=["task:"], index_type=IndexType.JSON),  # type: ignore[no-untyped-call]
            )
            return

        # Index exists — add any fields the live schema is missing.
        # Try each field individually; RediSearch returns ResponseError for duplicates.
        for field in desired_fields:
            try:
                await self.data_store.ft(self.INDEX_NAME).alter_schema_add([field])
            except ResponseError as e:
                if "duplicate" not in str(e).lower():
                    raise

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Query tasks via the RediSearch index with optional filters."""
        from redis.commands.search.query import Query as SearchQuery

        from jobbers.models.task import PaginationOrder

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

        # final sort to ensure correct order after async fetches
        if pagination.order_by == PaginationOrder.SUBMITTED_AT:
            results.sort(key=lambda t: t.submitted_at or dt.datetime.min)
        else:  # default to sorting by ID (which is roughly creation time) if not sorting by submitted_at
            results.sort(key=lambda t: t.id)
        return results

    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run using the RediSearch index."""
        from redis.commands.search.query import Query as SearchQuery

        score: float | None = await self.data_store.zscore(self.DAG_RUNS, bytes(dag_run_id))
        if score is None:
            return None
        submitted_at = dt.datetime.fromtimestamp(score, dt.UTC)

        escaped = _escape_tag(str(dag_run_id))
        q = SearchQuery(f"@dag_run_id:{{{escaped}}}").no_content().paging(0, 10000)
        search_results = await self.data_store.ft(self.INDEX_NAME).search(q)
        task_ids = [ULID.from_str(doc.id.removeprefix("task:")) for doc in (search_results.docs or [])]
        return submitted_at, task_ids


# ---------------------------------------------------------------------------
# JsonDeadQueue  (Redis Stack: RedisJSON + RediSearch)
# ---------------------------------------------------------------------------


class JsonDeadQueue:
    """
    Dead letter queue using Redis JSON documents and RediSearch for filtering/sorting.

    Each entry is stored as a JSON document at `dlq:{task_id}` with fields:

    | Field | Type | Description |
    |-------|------|-------------|
    | `task_id` | string | ULID of the failed task. |
    | `name` | string | task name. |
    | `queue` | string | originating queue name. |
    | `failed_at` | float | Unix timestamp of failure. |

    A RediSearch index (`dlq-json-idx`) on `dlq:*` keys enables server-side filtering
    by `name` and `queue` (tag fields) and sorting by `failed_at` (numeric, sortable).

    Full task data is loaded from the `task_adapter` when Task objects are needed.
    """

    INDEX_NAME = "dlq-json-idx"
    DLQ_KEY = "dlq:{task_id}".format

    def __init__(self, data_store: Redis, task_adapter: TaskAdapterProtocol) -> None:
        self.data_store = data_store
        self.ta = task_adapter

    async def ensure_index(self) -> None:
        """Create the RediSearch index on DLQ JSON documents if it does not exist."""
        from redis.commands.search.field import NumericField, TagField
        from redis.commands.search.index_definition import IndexDefinition, IndexType

        try:
            await self.data_store.ft(self.INDEX_NAME).info()  # type: ignore[no-untyped-call]
        except ResponseError:
            await self.data_store.ft(self.INDEX_NAME).create_index(
                fields=[
                    TagField("$.name", as_name="name"),
                    TagField("$.queue", as_name="queue"),
                    NumericField("$.version", as_name="version"),
                    NumericField("$.failed_at", as_name="failed_at", sortable=True),
                ],
                definition=IndexDefinition(prefix=["dlq:"], index_type=IndexType.JSON),  # type: ignore[no-untyped-call]
            )

    def stage_add(self, pipe: Pipeline, task: Task, failed_at: dt.datetime) -> None:
        """Queue JSON.SET DLQ entry onto pipe (no execute)."""
        pipe.json().set(
            self.DLQ_KEY(task_id=task.id),
            "$",
            {
                "task_id": str(task.id),
                "name": task.name,
                "version": task.version,
                "queue": task.queue,
                "failed_at": failed_at.timestamp(),
            },
        )

    def stage_remove(self, pipe: Pipeline, task_id: ULID, queue: str, name: str) -> None:
        """Queue DELETE of the DLQ JSON document onto pipe (no execute)."""
        pipe.delete(self.DLQ_KEY(task_id=task_id))

    async def get_history(self, task_id: str) -> list[dict[str, Any]]:
        """Return per-attempt error history for a DLQ task from its stored task blob."""
        u = ULID.from_str(task_id)
        task = await self.ta.get_task(u)
        if task is None:
            return []
        return [{"attempt": i, "error": e} for i, e in enumerate(task.errors)]

    async def get_by_ids(self, task_ids: list[str]) -> list[Task]:
        """Fetch DLQ entries by explicit task ID list."""
        if not task_ids:
            return []
        ulids = [ULID.from_str(tid) for tid in task_ids]
        # Check which IDs actually have DLQ entries
        pipe = self.data_store.pipeline(transaction=False)
        for u in ulids:
            pipe.exists(self.DLQ_KEY(task_id=u))
        exist_flags: list[int] = await pipe.execute()
        valid_ulids = [u for u, flag in zip(ulids, exist_flags) if flag]
        if not valid_ulids:
            return []
        tasks: list[Task | None] = await self.ta.get_tasks_bulk(valid_ulids)
        return [t for t in tasks if t is not None]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list[Task]:
        """Fetch DLQ entries matching filter criteria using RediSearch, sorted by failed_at descending."""
        from redis.commands.search.query import Query as SearchQuery

        query_parts = []
        if queue is not None:
            query_parts.append(f"@queue:{{{_escape_tag(queue)}}}")
        if task_name is not None:
            query_parts.append(f"@name:{{{_escape_tag(task_name)}}}")
        if task_version is not None:
            query_parts.append(f"@version:[{task_version} {task_version}]")
        query_str = " ".join(query_parts) if query_parts else "*"

        q: SearchQuery = SearchQuery(query_str).no_content().sort_by("failed_at", asc=False).paging(0, limit)
        search_results = await self.data_store.ft(self.INDEX_NAME).search(q)
        if not search_results.docs:
            return []

        ulids = [ULID.from_str(doc.id.removeprefix("dlq:")) for doc in search_results.docs]
        fetched: list[Task | None] = await self.ta.get_tasks_bulk(ulids)
        return [task for task in fetched if task is not None]

    async def remove_many(self, task_ids: list[str]) -> None:
        """Remove multiple entries from the dead letter queue in a single transaction."""
        if not task_ids:
            return
        pipe = self.data_store.pipeline(transaction=True)
        for tid in task_ids:
            self.stage_remove(pipe, ULID.from_str(tid), "", "")  # queue and name are not needed for removal
        await pipe.execute()

    async def clean(self, earlier_than: dt.datetime) -> None:
        """Remove failed tasks older than the specified datetime."""
        from redis.commands.search.query import Query as SearchQuery

        ts = earlier_than.timestamp()
        q: SearchQuery = SearchQuery(f"@failed_at:[-inf ({ts}]").no_content().paging(0, 10000)
        search_results = await self.data_store.ft(self.INDEX_NAME).search(q)
        if not search_results.docs:
            return
        pipe = self.data_store.pipeline(transaction=True)
        for doc in search_results.docs:
            pipe.delete(doc.id)
        await pipe.execute()
