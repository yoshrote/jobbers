"""
Redis Stack (RedisJSON + RediSearch) dead-letter queue adapter.

- `RedisJSONDeadQueue` — DeadQueueProtocol + AtomicDeadQueueProtocol backed by Redis Stack.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from redis.commands.search.field import NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query as SearchQuery
from redis.exceptions import ResponseError
from ulid import ULID

from jobbers.adapters.redis_json._helpers import (
    _drop_stale_indexes,
    _escape_tag,
    _get_schema_version,
    _set_schema_version,
)

if TYPE_CHECKING:
    import datetime as dt

    from redis.asyncio.client import Pipeline, Redis

    from jobbers.models.task import Task
    from jobbers.protocols import TaskStateProtocol, TransactionHandle


class RedisJSONDeadQueue:
    """
    DeadQueueProtocol and AtomicDeadQueueProtocol backed by Redis Stack (JSON encoding).

    Each entry is stored as a JSON document at `dlq:{task_id}` with fields:

    | Field | Type | Description |
    |-------|------|-------------|
    | `task_id` | string | ULID of the failed task. |
    | `name` | string | task name. |
    | `queue` | string | originating queue name. |
    | `failed_at` | float | Unix timestamp of failure. |

    A RediSearch index (INDEX_NAME) on `dlq:*` keys enables server-side filtering
    by `name` and `queue` (tag fields) and sorting by `failed_at` (numeric, sortable).
    INDEX_NAME is suffixed with SCHEMA_VERSION so a future non-additive migration
    can build a new index alongside the old one before cutover.
    """

    SCHEMA_VERSION = 1
    INDEX_NAME = f"dlq-json-idx-v{SCHEMA_VERSION}"
    DLQ_KEY = "dlq:{task_id}".format
    _VERSION_KEY = "schema_version:dlq_json"

    def __init__(self, data_store: Redis, task_adapter: TaskStateProtocol) -> None:
        self.data_store = data_store
        self.ta = task_adapter

    @property
    def backend_key(self) -> str:
        return str(id(self.data_store))

    def pipeline(self, transaction: bool = True) -> Pipeline:
        return self.data_store.pipeline(transaction=transaction)

    async def ensure_index(self) -> None:
        """Create the RediSearch index on DLQ JSON documents if not already at the current schema version."""
        if await _get_schema_version(self.data_store, self._VERSION_KEY) >= self.SCHEMA_VERSION:
            return
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
        await _set_schema_version(self.data_store, self._VERSION_KEY, self.SCHEMA_VERSION)

    async def drop_stale_indexes(self) -> list[str]:
        """Drop RediSearch indexes from older schema generations of INDEX_NAME. Returns names dropped."""
        return await _drop_stale_indexes(self.data_store, [self.INDEX_NAME], self.SCHEMA_VERSION)

    def stage_add(self, pipe: TransactionHandle, task: Task, failed_at: dt.datetime) -> None:
        """Queue JSON.SET DLQ entry onto pipe (no execute)."""
        p: Any = pipe
        p.json().set(
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

    async def add_to_dlq(self, task: Task, failed_at: dt.datetime) -> None:
        """Add a task to the DLQ (non-pipeline version for saga path)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_add(pipe, task, failed_at)
        await pipe.execute()

    def stage_remove(self, pipe: TransactionHandle, task_id: ULID, queue: str, name: str) -> None:
        """Queue DELETE of the DLQ JSON document onto pipe (no execute)."""
        p: Any = pipe
        p.delete(self.DLQ_KEY(task_id=task_id))

    async def remove_from_dlq(self, task_id: ULID, _queue: str, _name: str) -> None:
        """Remove a task from the DLQ (non-pipeline version for saga path)."""
        await self.data_store.delete(self.DLQ_KEY(task_id=task_id))

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
            self.stage_remove(pipe, ULID.from_str(tid), "", "")
        await pipe.execute()

    async def clean(self, earlier_than: dt.datetime) -> None:
        """Remove failed tasks older than the specified datetime."""
        ts = earlier_than.timestamp()
        q: SearchQuery = SearchQuery(f"@failed_at:[-inf ({ts}]").no_content().paging(0, 10000)
        search_results = await self.data_store.ft(self.INDEX_NAME).search(q)
        if not search_results.docs:
            return
        pipe = self.data_store.pipeline(transaction=True)
        for doc in search_results.docs:
            pipe.delete(doc.id)
        await pipe.execute()

    async def clean_orphaned_entries(self) -> int:
        """Remove DLQ JSON documents whose task blob no longer exists. Returns count removed."""
        q: SearchQuery = SearchQuery("*").no_content().paging(0, 10000)
        search_results = await self.data_store.ft(self.INDEX_NAME).search(q)
        if not search_results.docs:
            return 0
        doc_ids = [doc.id for doc in search_results.docs]
        ulids = [ULID.from_str(doc_id.removeprefix("dlq:")) for doc_id in doc_ids]
        tasks = await self.ta.get_tasks_bulk(ulids)
        orphaned_ids = [doc_id for doc_id, task in zip(doc_ids, tasks) if task is None]
        if not orphaned_ids:
            return 0
        pipe = self.data_store.pipeline(transaction=True)
        for doc_id in orphaned_ids:
            pipe.delete(doc_id)
        await pipe.execute()
        return len(orphaned_ids)
