"""
Redis Stack (RedisJSON + RediSearch) backed implementations.

Routing sub-adapters:
- `RedisJSONQueueConfigAdapter` — queue/role config and refresh tags in RedisJSON docs
  with RediSearch indexes. Replaces the O(N) set scans in the plain Redis backend.
- `RedisJSONTaskRoutingConfigAdapter` — task routing config in RedisJSON docs.
- `RedisJSONRoutingBackend` — composes the two sub-adapters to satisfy RoutingBackendProtocol.

Task storage:
- `RedisJSONTaskState` — stores tasks as RedisJSON documents; implements
  ``TaskStateProtocol`` and ``AtomicTaskStateProtocol``.
- `RedisJSONTaskSubmit` — submit/pop operations backed by Redis Stack Lua scripts;
  implements ``TaskSubmitProtocol``.

Dead-letter queue:
- `RedisJSONDeadQueue` — stores dead-letter entries as RedisJSON documents with a
  RediSearch index for server-side filtering and sorting.

All classes require a Redis Stack instance with the RedisJSON and RediSearch modules.
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

from jobbers.adapters._shared import SharedTaskAdapterMixin, _SharedRedisTaskSubmitBase
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_routing import RoutingConfig

if TYPE_CHECKING:
    from pydantic import BaseModel
    from redis.asyncio.client import Pipeline, Redis

    from jobbers.protocols import TaskStateProtocol, TransactionHandle


def _escape_tag(value: str) -> str:
    """Escape special characters for a RediSearch TAG query value."""
    special = set(r',.<>{}[]"\':;!@#$%^&*()\-+=~| ')
    return "".join(f"\\{c}" if c in special else c for c in value)


def _pack(model: BaseModel) -> dict[str, Any]:
    """Pack a Pydantic model to a dict with RedisJSON-compatible values (e.g. ULIDs as strings)."""
    return model.model_dump(mode="json", context={"mode": "redis_json"})


# ---------------------------------------------------------------------------
# RedisJSONQueueConfigAdapter  (Redis Stack: RedisJSON + RediSearch)
# ---------------------------------------------------------------------------


class RedisJSONQueueConfigAdapter:
    """
    QueueConfigProtocol backed by RedisJSON. Requires Redis Stack.

    Two RediSearch indexes replace the plain-Redis set indexes used by
    RedisQueueConfigAdapter:

      routing_queue_idx — on routing:queue:* docs; enables enumeration and fast
                          queue-membership queries without a separate index set.
      routing_role_idx  — on routing:role:* docs; enables bump_refresh_tags_for_queue
                          to find affected roles via an indexed query rather than
                          scanning all roles.

    Key scheme:
      routing:queue:{name}   — JSON doc (QueueConfig fields)
      routing:role:{name}    — JSON doc {queues: [...]}
      routing:tag:{name}     — String (ULID, refresh tag)
    """

    QUEUE_IDX = "routing_queue_idx"
    ROLE_IDX = "routing_role_idx"
    QUEUE_KEY = "routing:queue:{name}".format
    ROLE_KEY = "routing:role:{name}".format
    REFRESH_TAG_KEY = "routing:tag:{name}".format

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def ensure_indexes(self) -> None:
        """Create RediSearch indexes for queues and roles if they do not already exist."""
        try:
            await self._client.ft(self.QUEUE_IDX).info()  # type: ignore[no-untyped-call]
        except ResponseError:
            await self._client.ft(self.QUEUE_IDX).create_index(
                fields=[TagField("$.name", as_name="name")],
                definition=IndexDefinition(prefix=["routing:queue:"], index_type=IndexType.JSON),  # type: ignore[no-untyped-call]
            )
        try:
            await self._client.ft(self.ROLE_IDX).info()  # type: ignore[no-untyped-call]
        except ResponseError:
            await self._client.ft(self.ROLE_IDX).create_index(
                fields=[TagField("$.queues[*]", as_name="queues")],
                definition=IndexDefinition(prefix=["routing:role:"], index_type=IndexType.JSON),  # type: ignore[no-untyped-call]
            )

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        raw: dict[str, Any] | None = await self._client.json().get(self.QUEUE_KEY(name=queue))  # type: ignore[misc]
        if raw is None:
            return None
        return QueueConfig.model_validate(raw)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        await self._client.json().set(self.QUEUE_KEY(name=queue_config.name), "$", _pack(queue_config))  # type: ignore[misc]

    async def delete_queue(self, queue_name: str) -> None:
        await self._client.delete(self.QUEUE_KEY(name=queue_name))
        results = await self._client.ft(self.ROLE_IDX).search(
            SearchQuery(f"@queues:{{{_escape_tag(queue_name)}}}").no_content()
        )
        affected = [doc.id.removeprefix("routing:role:") for doc in (results.docs or [])]
        if not affected:
            return
        get_pipe = self._client.pipeline(transaction=False)
        for role in affected:
            get_pipe.json().get(self.ROLE_KEY(name=role))
        role_docs: list[dict[str, Any] | None] = await get_pipe.execute()
        new_tag = str(ULID())
        write_pipe = self._client.pipeline(transaction=True)
        for role, role_doc in zip(affected, role_docs):
            if role_doc is not None:
                new_queues = [q for q in role_doc.get("queues", []) if q != queue_name]
                write_pipe.json().set(self.ROLE_KEY(name=role), "$.queues", new_queues)
            write_pipe.set(self.REFRESH_TAG_KEY(name=role), new_tag)
        await write_pipe.execute()

    async def get_all_queues(self) -> list[str]:
        results = await self._client.ft(self.QUEUE_IDX).search(SearchQuery("*").no_content().paging(0, 10000))
        return sorted(doc.id.removeprefix("routing:queue:") for doc in (results.docs or []))

    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]:
        if not queues_set:
            return {}
        ordered = list(queues_set)
        pipe = self._client.pipeline(transaction=False)
        for name in ordered:
            pipe.json().get(self.QUEUE_KEY(name=name))
        raws: list[dict[str, Any] | None] = await pipe.execute()
        result: dict[str, int | None] = {}
        for name, raw in zip(ordered, raws):
            if raw is None:
                result[name] = None
            else:
                result[name] = QueueConfig.model_validate(raw).max_concurrent
        return result

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        raw: dict[str, Any] | None = await self._client.json().get(self.ROLE_KEY(name=role))  # type: ignore[misc]
        if raw is None:
            return set()
        return set(raw.get("queues", []))

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        new_tag = str(ULID())
        pipe = self._client.pipeline(transaction=True)
        pipe.json().set(self.ROLE_KEY(name=role), "$", {"queues": list(queues_set)})
        pipe.set(self.REFRESH_TAG_KEY(name=role), new_tag)
        await pipe.execute()
        return new_tag

    async def get_all_roles(self) -> list[str]:
        results = await self._client.ft(self.ROLE_IDX).search(SearchQuery("*").no_content().paging(0, 10000))
        return sorted(doc.id.removeprefix("routing:role:") for doc in (results.docs or []))

    async def delete_role(self, role: str) -> None:
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.ROLE_KEY(name=role))
        pipe.delete(self.REFRESH_TAG_KEY(name=role))
        await pipe.execute()

    # ── Refresh tags ──────────────────────────────────────────────────────────

    async def get_refresh_tag(self, role: str) -> ULID:
        raw: bytes | None = await self._client.get(self.REFRESH_TAG_KEY(name=role))
        if raw:
            return ULID.from_str(raw.decode())
        init_tag = ULID()
        await self._client.set(self.REFRESH_TAG_KEY(name=role), str(init_tag), nx=True)
        raw = await self._client.get(self.REFRESH_TAG_KEY(name=role))
        return ULID.from_str(raw.decode()) if raw else init_tag

    async def bump_refresh_tag(self, role: str) -> str:
        new_tag = str(ULID())
        await self._client.set(self.REFRESH_TAG_KEY(name=role), new_tag)
        return new_tag

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        """Find all roles containing queue_name via RediSearch and bump their refresh tags."""
        results = await self._client.ft(self.ROLE_IDX).search(
            SearchQuery(f"@queues:{{{_escape_tag(queue_name)}}}").no_content()
        )
        affected = [doc.id.removeprefix("routing:role:") for doc in (results.docs or [])]
        if affected:
            new_tag = str(ULID())
            pipe = self._client.pipeline(transaction=True)
            for role in affected:
                pipe.set(self.REFRESH_TAG_KEY(name=role), new_tag)
            await pipe.execute()
        return affected


# ---------------------------------------------------------------------------
# RedisJSONTaskRoutingConfigAdapter  (Redis Stack: RedisJSON)
# ---------------------------------------------------------------------------


class RedisJSONTaskRoutingConfigAdapter:
    """TaskRoutingConfigProtocol backed by RedisJSON plain key-value docs."""

    ROUTING_KEY = "routing:config:{task_name}:{task_version}".format

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        raw: dict[str, Any] | None = await self._client.json().get(  # type: ignore[misc]
            self.ROUTING_KEY(task_name=task_name, task_version=task_version)
        )
        if raw is None:
            return None
        return RoutingConfig.model_validate(raw)

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        await self._client.json().set(  # type: ignore[misc]
            self.ROUTING_KEY(task_name=routing_config.task_name, task_version=routing_config.task_version),
            "$",
            _pack(routing_config),
        )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        deleted: int = await self._client.delete(
            self.ROUTING_KEY(task_name=task_name, task_version=task_version)
        )
        return deleted > 0


# ---------------------------------------------------------------------------
# RedisJSONRoutingBackend  (Redis Stack: RedisJSON + RediSearch)
# ---------------------------------------------------------------------------


class RedisJSONRoutingBackend:
    """RoutingBackendProtocol backed by RedisJSON. Delegates to sub-adapters."""

    def __init__(self, client: Redis) -> None:
        self._qca = RedisJSONQueueConfigAdapter(client)
        self._rca = RedisJSONTaskRoutingConfigAdapter(client)

    async def ensure_indexes(self) -> None:
        await self._qca.ensure_indexes()

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        return await self._qca.get_queue_config(queue)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        await self._qca.save_queue_config(queue_config)

    async def delete_queue(self, queue_name: str) -> None:
        await self._qca.delete_queue(queue_name)

    async def get_all_queues(self) -> list[str]:
        return await self._qca.get_all_queues()

    async def get_queues(self, role: str) -> set[str]:
        return await self._qca.get_queues(role)

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        return await self._qca.save_role(role, queues_set)

    async def get_all_roles(self) -> list[str]:
        return await self._qca.get_all_roles()

    async def delete_role(self, role: str) -> None:
        await self._qca.delete_role(role)

    async def get_refresh_tag(self, role: str) -> ULID:
        return await self._qca.get_refresh_tag(role)

    async def bump_refresh_tag(self, role: str) -> str:
        return await self._qca.bump_refresh_tag(role)

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        return await self._qca.bump_refresh_tags_for_queue(queue_name)

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        return await self._rca.get_routing_config(task_name, task_version)

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        await self._rca.save_routing_config(routing_config)

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        return await self._rca.delete_routing_config(task_name, task_version)


# ---------------------------------------------------------------------------
# RedisJSONTaskState  (Redis Stack: RedisJSON + RediSearch — TaskStateProtocol + Atomic)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# RedisJSONTaskSubmit  (Redis Stack: RedisJSON + RediSearch — TaskSubmitProtocol)
# ---------------------------------------------------------------------------


class RedisJSONTaskSubmit(_SharedRedisTaskSubmitBase):
    """
    TaskSubmitProtocol backed by Redis Stack (JSON encoding).

    Lua scripts atomically enqueue the JSON task blob and queue membership.
    Requires the same Redis client as the paired ``RedisJSONTaskState``.
    """

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
    SUBMIT_SCRIPT = _JSON_SUBMIT_SCRIPT

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
    SUBMIT_RATE_LIMITED_SCRIPT = _JSON_SUBMIT_RATE_LIMITED_SCRIPT

    def __init__(self, data_store: Redis, state: RedisJSONTaskState) -> None:
        super().__init__(data_store, state.pack, state.get_task)


# ---------------------------------------------------------------------------
# RedisJSONDeadQueue  (Redis Stack: RedisJSON + RediSearch)
# ---------------------------------------------------------------------------


class RedisJSONDeadQueue:
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
    """

    INDEX_NAME = "dlq-json-idx"
    DLQ_KEY = "dlq:{task_id}".format

    def __init__(self, data_store: Redis, task_adapter: TaskStateProtocol) -> None:
        self.data_store = data_store
        self.ta = task_adapter

    @property
    def backend_key(self) -> str:
        return str(id(self.data_store))

    def pipeline(self, transaction: bool = True) -> Pipeline:
        return self.data_store.pipeline(transaction=transaction)

    async def ensure_index(self) -> None:
        """Create the RediSearch index on DLQ JSON documents if it does not exist."""
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
