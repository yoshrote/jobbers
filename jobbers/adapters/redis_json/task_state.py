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
from jobbers.adapters.redis_json._helpers import _escape_tag, _pack
from jobbers.models.task import PaginationOrder, Task, TaskPagination

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline, Redis


_JSON_SUBMIT_SCRIPT = """
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

_JSON_SUBMIT_RATE_LIMITED_SCRIPT = """
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


class RedisJSONTaskState(SharedTaskAdapterMixin):
    """
    TaskStateProtocol + AtomicTaskStateProtocol backed by Redis Stack (JSON encoding).

    Requires a Redis Stack instance (RedisJSON + RediSearch modules).
    """

    INDEX_NAME = "task-idx"

    # -- Storage primitives --------------------------------------------------

    def pack(self, task: Task) -> str:
        """Serialize a task to a JSON string (float timestamps for RediSearch, string ULIDs)."""
        return json.dumps(_pack(task))

    def unpack(self, task_id: ULID, data: str | dict[str, Any]) -> Task:
        """Deserialize a task from a JSON string or dict."""
        raw: dict[str, Any] = json.loads(data) if isinstance(data, str) else data
        return Task.model_validate({"id": task_id, **raw})

    async def _load_raw(self, key: str) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", await self.data_store.json().get(key))  # type: ignore[misc]

    async def _load_raw_watch(self, pipe: Pipeline, key: str) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", await pipe.json().get(key))  # type: ignore[misc]

    def _stage_store(self, pipe: Pipeline, key: str, task: Task) -> None:
        pipe.json().set(key, "$", json.loads(self.pack(task)))

    def _stage_load(self, pipe: Pipeline, key: str) -> None:
        pipe.json().get(key)

    # -- Backend-specific queries --------------------------------------------

    async def ensure_index(self) -> None:
        """Create the RediSearch index, or add any missing fields to an existing one."""
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
            return

        for field in desired_fields:
            try:
                await self.data_store.ft(self.INDEX_NAME).alter_schema_add([field])
            except ResponseError as e:
                if "duplicate" not in str(e).lower():
                    raise

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

    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run using the RediSearch index."""
        score: float | None = await self.data_store.zscore(self.DAG_RUNS, bytes(dag_run_id))
        if score is None:
            return None
        submitted_at = dt.datetime.fromtimestamp(score, dt.UTC)

        escaped = _escape_tag(str(dag_run_id))
        q = SearchQuery(f"@dag_run_id:{{{escaped}}}").no_content().paging(0, 10000)
        search_results = await self.data_store.ft(self.INDEX_NAME).search(q)
        task_ids = [ULID.from_str(doc.id.removeprefix("task:")) for doc in (search_results.docs or [])]
        return submitted_at, task_ids
