"""
Plain Redis backed implementations (no Redis Stack modules required).

Routing sub-adapters:
- `RedisQueueConfigAdapter` — queue/role config and refresh tags in plain Redis keys/sets.
- `RedisTaskRoutingConfigAdapter` — task routing config in plain Redis keys.
- `RedisRoutingBackend` — composes the two sub-adapters to satisfy RoutingBackendProtocol.

Task storage:
- `RedisTaskState` — stores tasks as msgpack-encoded binary strings; implements
  ``TaskStateProtocol`` and ``AtomicTaskStateProtocol``.
- `RedisTaskSubmit` — submit/pop operations backed by plain Redis Lua scripts;
  implements ``TaskSubmitProtocol``.

Dead-letter queue:
- `RedisDeadQueue` — dead letter queue backed by Redis sorted sets, sets, and a
  hash for secondary indexes.

Task scheduler:
- `RedisTaskScheduler` — scheduled/delayed task queue stored in Redis sorted sets;
  implements ``AtomicTaskSchedulerProtocol``.

Cron DAG scheduler:
- `RedisCronDAGScheduler` — recurring cron-scheduled DAG entries stored in Redis
  hashes and sorted sets.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import TYPE_CHECKING, Any, cast

from ulid import ULID

from jobbers.adapters._shared import SharedTaskAdapterMixin, _SharedRedisTaskSubmitBase
from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGTaskSpec
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_routing import RoutingConfig
from jobbers.utils.serialization import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pydantic import BaseModel
    from redis.asyncio.client import Pipeline, Redis

    from jobbers.protocols import TaskStateProtocol, TransactionHandle

logger = logging.getLogger(__name__)


def _pack(obj: BaseModel, exclude: set[str] | None = None) -> bytes:
    """Serialize any Pydantic model to msgpack bytes."""
    kw: dict[str, Any] = {"context": {"mode": "msgpack"}}
    if exclude:
        kw["exclude"] = exclude
    return serialize(obj.model_dump(**kw))


# ---------------------------------------------------------------------------
# RedisQueueConfigAdapter  (plain Redis: string values + sets)
# ---------------------------------------------------------------------------


class RedisQueueConfigAdapter:
    """
    QueueConfigProtocol backed by plain Redis keys and sets. No SQL required.

    Key scheme:
      config:queue:{name}                — msgpack bytes (QueueConfig fields)
      config:queues                      — Set of all queue names
      config:role:{name}:queues          — Set of queue names for the role
      config:roles                       — Set of all role names
      config:role:{name}:refresh_tag     — String (ULID)
    """

    QUEUE_KEY = "config:queue:{name}".format
    QUEUES_INDEX = "config:queues"
    ROLE_QUEUES_KEY = "config:role:{name}:queues".format
    ROLES_INDEX = "config:roles"
    ROLE_REFRESH_TAG_KEY = "config:role:{name}:refresh_tag".format

    def __init__(self, client: Redis) -> None:
        self._client = client

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        raw = await self._client.get(self.QUEUE_KEY(name=queue))
        if raw is None:
            return None
        return QueueConfig.model_validate(deserialize(raw))

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        payload = _pack(queue_config)
        pipe = self._client.pipeline(transaction=True)
        pipe.set(self.QUEUE_KEY(name=queue_config.name), payload)
        pipe.sadd(self.QUEUES_INDEX, queue_config.name)
        await pipe.execute()

    async def delete_queue(self, queue_name: str) -> None:
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.QUEUE_KEY(name=queue_name))
        pipe.srem(self.QUEUES_INDEX, queue_name)
        await pipe.execute()
        raw_roles: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        role_names = [r.decode() for r in raw_roles]
        if not role_names:
            return
        srem_pipe = self._client.pipeline(transaction=False)
        for role in role_names:
            srem_pipe.srem(self.ROLE_QUEUES_KEY(name=role), queue_name)
        removed_counts: list[int] = await srem_pipe.execute()
        affected = [role for role, count in zip(role_names, removed_counts) if count]
        if affected:
            new_tag = str(ULID())
            bump_pipe = self._client.pipeline(transaction=True)
            for role in affected:
                bump_pipe.set(self.ROLE_REFRESH_TAG_KEY(name=role), new_tag)
            await bump_pipe.execute()

    async def get_all_queues(self) -> list[str]:
        raw: set[bytes] = await self._client.smembers(self.QUEUES_INDEX)  # type: ignore[misc]
        return sorted(m.decode() for m in raw)

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        raw: set[bytes] = await self._client.smembers(self.ROLE_QUEUES_KEY(name=role))  # type: ignore[misc]
        return {m.decode() for m in raw}

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        new_tag = str(ULID())
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.ROLE_QUEUES_KEY(name=role))
        if queues_set:
            pipe.sadd(self.ROLE_QUEUES_KEY(name=role), *queues_set)
        pipe.sadd(self.ROLES_INDEX, role)
        pipe.set(self.ROLE_REFRESH_TAG_KEY(name=role), new_tag)
        await pipe.execute()
        return new_tag

    async def get_all_roles(self) -> list[str]:
        raw: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        return sorted(m.decode() for m in raw)

    async def delete_role(self, role: str) -> None:
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self.ROLE_QUEUES_KEY(name=role))
        pipe.delete(self.ROLE_REFRESH_TAG_KEY(name=role))
        pipe.srem(self.ROLES_INDEX, role)
        await pipe.execute()

    # ── Refresh tags ──────────────────────────────────────────────────────────

    async def get_refresh_tag(self, role: str) -> ULID:
        raw: bytes | None = await self._client.get(self.ROLE_REFRESH_TAG_KEY(name=role))
        if raw:
            return ULID.from_str(raw.decode())
        init_tag = ULID()
        # SET NX so two concurrent callers don't clobber each other.
        await self._client.set(self.ROLE_REFRESH_TAG_KEY(name=role), str(init_tag), nx=True)
        raw = await self._client.get(self.ROLE_REFRESH_TAG_KEY(name=role))
        return ULID.from_str(raw.decode()) if raw else init_tag

    async def bump_refresh_tag(self, role: str) -> str:
        new_tag = str(ULID())
        await self._client.set(self.ROLE_REFRESH_TAG_KEY(name=role), new_tag)
        return new_tag

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        raw_roles: set[bytes] = await self._client.smembers(self.ROLES_INDEX)  # type: ignore[misc]
        role_names = [r.decode() for r in raw_roles]
        if not role_names:
            return []
        check_pipe = self._client.pipeline(transaction=False)
        for role in role_names:
            check_pipe.sismember(self.ROLE_QUEUES_KEY(name=role), queue_name)
        is_member_list: list[bool] = await check_pipe.execute()
        affected = [role for role, is_member in zip(role_names, is_member_list) if is_member]
        if affected:
            new_tag = str(ULID())
            pipe = self._client.pipeline(transaction=True)
            for role in affected:
                pipe.set(self.ROLE_REFRESH_TAG_KEY(name=role), new_tag)
            await pipe.execute()
        return affected

    # ── Queue limits (batch read) ─────────────────────────────────────────────

    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]:
        if not queues_set:
            return {}
        ordered = list(queues_set)
        pipe = self._client.pipeline(transaction=False)
        for name in ordered:
            pipe.get(self.QUEUE_KEY(name=name))
        raws: list[bytes | None] = await pipe.execute()
        result: dict[str, int | None] = {}
        for name, raw in zip(ordered, raws):
            if raw is None:
                result[name] = None
            else:
                cfg = QueueConfig.model_validate(deserialize(raw))
                result[name] = cfg.max_concurrent
        return result


# ---------------------------------------------------------------------------
# RedisTaskRoutingConfigAdapter  (plain Redis: string values)
# ---------------------------------------------------------------------------


class RedisTaskRoutingConfigAdapter:
    """
    TaskRoutingConfigProtocol backed by plain Redis keys. No SQL required.

    Key scheme:
      config:routing:{task_name}:{task_version}   — msgpack bytes (RoutingConfig fields)
    """

    ROUTING_KEY = "config:routing:{task_name}:{task_version}".format

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        raw = await self._client.get(self.ROUTING_KEY(task_name=task_name, task_version=task_version))
        if raw is None:
            return None
        return RoutingConfig.model_validate(deserialize(raw))

    async def save_routing_config(self, routing_config: RoutingConfig) -> None:
        payload = _pack(routing_config)
        await self._client.set(
            self.ROUTING_KEY(task_name=routing_config.task_name, task_version=routing_config.task_version),
            payload,
        )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        deleted: int = await self._client.delete(
            self.ROUTING_KEY(task_name=task_name, task_version=task_version)
        )
        return deleted > 0


# ---------------------------------------------------------------------------
# RedisRoutingBackend  (composes the two sub-adapters above)
# ---------------------------------------------------------------------------


class RedisRoutingBackend:
    """RoutingBackendProtocol backed by Redis. Delegates to sub-adapters."""

    def __init__(self, client: Redis) -> None:
        self._qca = RedisQueueConfigAdapter(client)
        self._rca = RedisTaskRoutingConfigAdapter(client)

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
# RedisTaskState  (plain Redis: binary strings — TaskStateProtocol + Atomic)
# ---------------------------------------------------------------------------

_MSGPACK_SUBMIT_SCRIPT = """
    local exists = redis.call('EXISTS', KEYS[2])
    if exists == 0 then
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
    end
    redis.call('SET', KEYS[2], ARGV[4])
    if ARGV[3] == '1' then
        redis.call('SADD', KEYS[3], ARGV[2])
    else
        redis.call('SREM', KEYS[3], ARGV[2])
    end
    if ARGV[5] ~= '' then
        redis.call('ZADD', KEYS[4], 'NX', ARGV[1], ARGV[5])
        redis.call('ZADD', KEYS[5], ARGV[1], ARGV[2])
    end
    return 1
"""

_MSGPACK_SUBMIT_RATE_LIMITED_SCRIPT = """
    local enqueued = 0
    local exists = redis.call('EXISTS', KEYS[3])
    local numerator = tonumber(ARGV[2])
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
    if exists == 0 then
        local count = redis.call('ZCARD', KEYS[1])
        if numerator ~= 0 and count < numerator then
            redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
            redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
            redis.call('SET', KEYS[3], ARGV[6])
            enqueued = 1
        end
    else
        redis.call('SET', KEYS[3], ARGV[6])
        enqueued = 1
    end
    if enqueued == 1 and ARGV[5] == '1' then
        redis.call('SADD', KEYS[4], ARGV[4])
    else
        redis.call('SREM', KEYS[4], ARGV[4])
    end
    if enqueued == 1 and ARGV[7] ~= '' then
        redis.call('ZADD', KEYS[5], 'NX', ARGV[3], ARGV[7])
        redis.call('ZADD', KEYS[6], ARGV[3], ARGV[4])
    end
    return enqueued
"""


class RedisTaskState(SharedTaskAdapterMixin):
    """
    TaskStateProtocol + AtomicTaskStateProtocol backed by plain Redis (msgpack encoding).

    Works with any standard Redis instance (no Redis Stack required).
    `get_all_tasks` applies `task_name`, `task_version`, and `status` filters in Python
    after fetching candidate task IDs from the queue sorted set.
    """

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

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Fetch tasks from the queue sorted set and filter in Python."""
        if pagination.order_by == PaginationOrder.SUBMITTED_AT:
            raw_ids = await self.data_store.zrangebyscore(
                self.TASKS_BY_QUEUE(queue=pagination.queue),
                "-inf",
                "+inf",
                start=pagination.offset,
                num=pagination.limit * 5,
            )
        else:
            raw_ids = await self.data_store.zrange(
                self.TASKS_BY_QUEUE(queue=pagination.queue),
                pagination.offset,
                pagination.offset + pagination.limit * 5 - 1,
            )

        results: list[Task] = []
        for raw_id in raw_ids:
            if len(results) >= pagination.limit:
                break
            task_id = ULID.from_bytes(raw_id)
            raw_data: bytes | None = await self.data_store.get(self.TASK_DETAILS(task_id=task_id))
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

    def stage_register_dag_run(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage DAG run index updates, including the per-run task set for the raw adapter."""
        super().stage_register_dag_run(pipe, task)
        if task.dag_run_id is None or task.submitted_at is None:
            return
        score = task.submitted_at.timestamp()
        p: Any = pipe
        p.zadd(self.DAG_RUN_TASKS(dag_run_id=task.dag_run_id), {bytes(task.id): score})

    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run using the per-run tasks sorted set."""
        score: float | None = await self.data_store.zscore(self.DAG_RUNS, bytes(dag_run_id))
        if score is None:
            return None
        submitted_at = dt.datetime.fromtimestamp(score, dt.UTC)
        raw_ids: list[bytes] = await self.data_store.zrange(self.DAG_RUN_TASKS(dag_run_id=dag_run_id), 0, -1)
        task_ids = [ULID.from_bytes(b) for b in raw_ids]
        return submitted_at, task_ids

    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Remove stale DAG run entries and their per-run task sets."""
        cutoff = (now - max_age).timestamp()
        stale: list[bytes] = await self.data_store.zrangebyscore(self.DAG_RUNS, "-inf", cutoff)
        if not stale:
            return
        pipe = self.data_store.pipeline(transaction=False)
        pipe.zrem(self.DAG_RUNS, *stale)
        for dag_id_bytes in stale:
            try:
                dag_run_id = ULID.from_bytes(dag_id_bytes)
            except ValueError:
                continue
            pipe.delete(self.DAG_RUN_TASKS(dag_run_id=dag_run_id))
        await pipe.execute()


# ---------------------------------------------------------------------------
# RedisTaskSubmit  (plain Redis: binary strings — TaskSubmitProtocol)
# ---------------------------------------------------------------------------


class RedisTaskSubmit(_SharedRedisTaskSubmitBase):
    """
    TaskSubmitProtocol backed by plain Redis (msgpack encoding).

    Lua scripts atomically enqueue the task blob and queue membership in one round-trip.
    Requires the same Redis client as the paired ``RedisTaskState``.
    """

    # Atomically enqueue a new task (no rate limiting).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # KEYS[4] = dag-runs
    # KEYS[5] = dag-run:{dag_run_id}:tasks (placeholder key when task has no DAG run)
    # ARGV[1] = submitted_at timestamp
    # ARGV[2] = task_id bytes
    # ARGV[3] = '1'/'0' for type index
    # ARGV[4] = msgpack-encoded task blob
    # ARGV[5] = dag_run_id bytes (empty string if task is not part of a DAG run)
    SUBMIT_SCRIPT = _MSGPACK_SUBMIT_SCRIPT

    # Atomically check rate limit and enqueue.
    # KEYS[1] = rate-limiter:{queue}
    # KEYS[2] = task-queues:{queue}
    # KEYS[3] = task:{task_id}
    # KEYS[4] = task-type-idx:{name}
    # KEYS[5] = dag-runs
    # KEYS[6] = dag-run:{dag_run_id}:tasks (placeholder key when task has no DAG run)
    # ARGV[1] = earliest_time
    # ARGV[2] = rate_numerator
    # ARGV[3] = submitted_at timestamp
    # ARGV[4] = task_id bytes
    # ARGV[5] = '1'/'0' for type index
    # ARGV[6] = msgpack-encoded task blob
    # ARGV[7] = dag_run_id bytes (empty string if task is not part of a DAG run)
    # Returns: 1 if enqueued, 0 if rate-limited
    SUBMIT_RATE_LIMITED_SCRIPT = _MSGPACK_SUBMIT_RATE_LIMITED_SCRIPT

    def __init__(self, data_store: Redis, state: RedisTaskState) -> None:
        super().__init__(data_store, state.pack, state.get_task)

    def _extra_submit_keys(self, task: Task) -> list[str]:
        return [
            self.DAG_RUN_TASKS(dag_run_id=task.dag_run_id)
            if task.dag_run_id is not None
            else self.DAG_RUN_TASKS(dag_run_id="")
        ]

    def _extra_rate_limited_keys(self, task: Task) -> list[str]:
        return [
            self.DAG_RUN_TASKS(dag_run_id=task.dag_run_id)
            if task.dag_run_id is not None
            else self.DAG_RUN_TASKS(dag_run_id="")
        ]


# ---------------------------------------------------------------------------
# RedisDeadQueue  (plain Redis: sorted sets, sets, hash indexes)
# ---------------------------------------------------------------------------


class RedisDeadQueue:
    r"""
    Dead letter queue backed by Redis, reusing task:<task_id> keys for task data.

    Keys:
    - `dlq` sorted set — member: task_id bytes, score: failed_at Unix timestamp.
    - `dlq-queue:{queue}` set — task_id bytes per queue name (queue filter index).
    - `dlq-name:{name}` set — task_id bytes per task name (name filter index).
    - `dlq-meta` hash — field: task_id bytes, value: `b"{queue}\0{name}\0{version}"`.
    """

    DLQ = "dlq"
    DLQ_QUEUE = "dlq-queue:{queue}".format
    DLQ_NAME = "dlq-name:{name}".format
    DLQ_META = "dlq-meta"

    def __init__(self, data_store: Redis, task_adapter: TaskStateProtocol) -> None:
        self.data_store = data_store
        self.ta = task_adapter

    @property
    def backend_key(self) -> str:
        return str(id(self.data_store))

    def pipeline(self, transaction: bool = True) -> Pipeline:
        return self.data_store.pipeline(transaction=transaction)

    async def ensure_index(self) -> None:
        """No-op: plain-Redis dead queue does not use a search index."""

    def stage_add(self, pipe: TransactionHandle, task: Task, failed_at: dt.datetime) -> None:
        """Queue all four DLQ index writes onto pipe (no execute)."""
        p: Any = pipe
        p.zadd(self.DLQ, {bytes(task.id): failed_at.timestamp()})
        p.sadd(self.DLQ_QUEUE(queue=task.queue), bytes(task.id))
        p.sadd(self.DLQ_NAME(name=task.name), bytes(task.id))
        p.hset(self.DLQ_META, str(task.id), f"{task.queue}\0{task.name}\0{task.version}")

    async def add_to_dlq(self, task: Task, failed_at: dt.datetime) -> None:
        """Add a task to the DLQ (non-pipeline version for saga path)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_add(pipe, task, failed_at)
        await pipe.execute()

    def stage_remove(self, pipe: TransactionHandle, task_id: ULID, queue: str, name: str) -> None:
        """Queue all four DLQ index deletes onto pipe (no execute)."""
        p: Any = pipe
        p.zrem(self.DLQ, bytes(task_id))
        p.srem(self.DLQ_QUEUE(queue=queue), bytes(task_id))
        p.srem(self.DLQ_NAME(name=name), bytes(task_id))
        p.hdel(self.DLQ_META, str(task_id))

    async def remove_from_dlq(self, task_id: ULID, queue: str, name: str) -> None:
        """Remove a task from the DLQ (non-pipeline version for saga path)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_remove(pipe, task_id, queue, name)
        await pipe.execute()

    async def get_history(self, task_id: str) -> list[dict[str, Any]]:
        """Return the per-attempt error history for a DLQ task from its stored task blob."""
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
            pipe.zscore(self.DLQ, bytes(u))
        scores = await pipe.execute()
        valid_ulids = [u for u, s in zip(ulids, scores) if s is not None]
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
        """Fetch DLQ entries matching the given filter criteria."""
        if queue is None and task_name is None:
            id_bytes: list[bytes] = await cast(
                "Awaitable[list[bytes]]",
                self.data_store.zrevrange(self.DLQ, 0, -1),
            )
        else:
            raw_ids: set[bytes]
            if queue is not None and task_name is not None:
                raw_ids = await cast(
                    "Awaitable[set[bytes]]",
                    self.data_store.sinter([self.DLQ_QUEUE(queue=queue), self.DLQ_NAME(name=task_name)]),
                )
            elif queue is not None:
                raw_ids = await cast(
                    "Awaitable[set[bytes]]",
                    self.data_store.smembers(self.DLQ_QUEUE(queue=queue)),
                )
            else:
                raw_ids = await cast(
                    "Awaitable[set[bytes]]",
                    self.data_store.smembers(self.DLQ_NAME(name=task_name)),
                )
            id_bytes = list(raw_ids)
        if not id_bytes:
            return []
        ulid_list = [ULID.from_bytes(b) for b in id_bytes]
        fetched: list[Task | None] = await self.ta.get_tasks_bulk(ulid_list)
        results: list[Task] = []
        for task in fetched:
            if len(results) >= limit:
                break
            if task is None:
                continue
            if task_version is not None and task.version != task_version:
                continue
            results.append(task)
        return results

    async def remove_many(self, task_ids: list[str]) -> None:
        """Remove multiple entries from the dead letter queue in a single transaction."""
        if not task_ids:
            return
        ulids = [ULID.from_str(tid) for tid in task_ids]
        read_pipe = self.data_store.pipeline(transaction=False)
        for u in ulids:
            read_pipe.hget(self.DLQ_META, str(u))
        meta_list = await read_pipe.execute()
        write_pipe = self.data_store.pipeline(transaction=True)
        for u, meta_bytes in zip(ulids, meta_list):
            if meta_bytes is None:
                continue
            queue, name, _ = meta_bytes.decode().split("\0")
            self.stage_remove(write_pipe, u, queue, name)
        await write_pipe.execute()

    async def clean(self, earlier_than: dt.datetime) -> None:
        """Remove failed tasks older than the specified datetime."""
        old_ids: list[bytes] = await cast(
            "Awaitable[list[bytes]]",
            self.data_store.zrangebyscore(self.DLQ, "-inf", earlier_than.timestamp()),
        )
        if not old_ids:
            return
        old_ulid_strs = [str(ULID.from_bytes(b)) for b in old_ids]
        pipe = self.data_store.pipeline(transaction=False)
        for uid_str in old_ulid_strs:
            pipe.hget(self.DLQ_META, uid_str)
        meta_list = await pipe.execute()
        pipe2 = self.data_store.pipeline(transaction=True)
        for task_id_bytes, uid_str, meta_bytes in zip(old_ids, old_ulid_strs, meta_list):
            pipe2.zrem(self.DLQ, task_id_bytes)
            if meta_bytes is not None:
                queue, name, _ = meta_bytes.decode().split("\0")
                pipe2.srem(self.DLQ_QUEUE(queue=queue), task_id_bytes)
                pipe2.srem(self.DLQ_NAME(name=name), task_id_bytes)
                pipe2.hdel(self.DLQ_META, uid_str)
        await pipe2.execute()


# ---------------------------------------------------------------------------
# RedisTaskScheduler — scheduled/delayed task queue in Redis sorted sets
# ---------------------------------------------------------------------------


class RedisTaskScheduler:
    """
    Manages scheduled tasks in Redis, reusing task:<task_id> keys for task data.

    Keys:
    - `schedule-queue:{queue}` sorted set — member: task_id bytes, score: run_at Unix timestamp.
    - `schedule-task-queue` hash — field: task_id bytes, value: queue name.
    """

    SCHEDULE_QUEUE = "schedule-queue:{queue}".format
    SCHEDULE_TASK_QUEUE = "schedule-task-queue"

    # Atomically acquire up to ARGV[2] tasks with score <= ARGV[1] (now) across all KEYS.
    # Each key is a "schedule-queue:{queue}" sorted set.
    # Returns a flat list: [task_id_bytes, score_str, task_id_bytes, score_str, ...]
    _ACQUIRE_SCRIPT = """
        local now = ARGV[1]
        local limit = tonumber(ARGV[2])
        local results = {}
        local collected = 0
        for _, key in ipairs(KEYS) do
            if collected >= limit then break end
            local remaining = limit - collected
            local items = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'WITHSCORES', 'LIMIT', '0', string.format('%d', remaining))
            for i = 1, #items, 2 do
                redis.call('ZREM', key, items[i])
                table.insert(results, items[i])
                table.insert(results, items[i + 1])
                collected = collected + 1
            end
        end
        return results
    """

    def __init__(
        self,
        data_store: Redis,
        task_adapter: TaskStateProtocol,
        get_all_queues: Callable[[], Awaitable[list[str]]],
    ) -> None:
        self.data_store = data_store
        self.ta = task_adapter
        self._get_all_queues = get_all_queues

    @property
    def backend_key(self) -> str:
        return str(id(self.data_store))

    def pipeline(self, transaction: bool = True) -> Pipeline:
        return self.data_store.pipeline(transaction=transaction)

    def stage_add(self, pipe: TransactionHandle, task: Task, run_at: dt.datetime) -> None:
        """Queue ZADD schedule-queue + HSET schedule-task-queue onto pipe (no execute)."""
        p: Any = pipe
        p.zadd(self.SCHEDULE_QUEUE(queue=task.queue), {bytes(task.id): run_at.timestamp()})
        p.hset(self.SCHEDULE_TASK_QUEUE, str(task.id), task.queue)

    async def add(self, task: Task, run_at: dt.datetime) -> None:
        """Add a task to the scheduler (non-pipeline version for saga path)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_add(pipe, task, run_at)
        await pipe.execute()

    def stage_remove(self, pipe: TransactionHandle, task_id: ULID, queue: str) -> None:
        """Queue ZREM schedule-queue + HDEL schedule-task-queue onto pipe (no execute)."""
        p: Any = pipe
        p.zrem(self.SCHEDULE_QUEUE(queue=queue), bytes(task_id))
        p.hdel(self.SCHEDULE_TASK_QUEUE, str(task_id))

    async def remove(self, task_id: ULID, queue: str) -> None:
        """Remove a task from the scheduler (non-pipeline version for saga path)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_remove(pipe, task_id, queue)
        await pipe.execute()

    async def get_run_at(self, task_id: ULID) -> dt.datetime | None:
        """Return the scheduled run_at for a single task, or None if not found."""
        queue_raw: bytes | None = await cast(
            "Awaitable[bytes | None]", self.data_store.hget(self.SCHEDULE_TASK_QUEUE, str(task_id))
        )
        if queue_raw is None:
            return None
        score: float | None = await self.data_store.zscore(
            self.SCHEDULE_QUEUE(queue=queue_raw.decode()), bytes(task_id)
        )
        return dt.datetime.fromtimestamp(score, dt.UTC) if score is not None else None

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
        start_after: str | None = None,
    ) -> list[tuple[Task, dt.datetime]]:
        """
        Fetch scheduled entries matching the given filter criteria.

        `start_after` is an exclusive ULID cursor for page-by-page iteration.
        Returns each task paired with its scheduled run_at timestamp.
        """
        if queue is not None:
            pairs: list[tuple[bytes, float]] = await cast(
                "Awaitable[list[tuple[bytes, float]]]",
                self.data_store.zrange(self.SCHEDULE_QUEUE(queue=queue), 0, -1, withscores=True),
            )
            score_map: dict[bytes, float] = dict(pairs)
        else:
            score_map = {}
            all_queues = await self._get_all_queues()
            for q in all_queues:
                pairs = await cast(
                    "Awaitable[list[tuple[bytes, float]]]",
                    self.data_store.zrange(self.SCHEDULE_QUEUE(queue=q), 0, -1, withscores=True),
                )
                score_map.update(pairs)

        raw_ids = sorted(score_map.keys())

        if start_after is not None:
            cursor = bytes(ULID.from_str(start_after))
            raw_ids = [r for r in raw_ids if r > cursor]

        ulid_list = [ULID.from_bytes(r) for r in raw_ids]
        fetched: list[Task | None] = await self.ta.get_tasks_bulk(ulid_list)
        results: list[tuple[Task, dt.datetime]] = []
        for task, id_bytes in zip(fetched, raw_ids, strict=True):
            if len(results) >= limit:
                break
            if task is None:
                continue
            if task_name is not None and task.name != task_name:
                continue
            if task_version is not None and task.version != task_version:
                continue
            run_at = dt.datetime.fromtimestamp(score_map[id_bytes], dt.UTC)
            results.append((task, run_at))

        return results

    async def next_due(self, queues: list[str] | None = None) -> Task | None:
        """
        Atomically acquire and return the earliest due task, or None.

        - `queues=None` — match any queue
        - `queues=[]` — return None immediately
        - `queues=[...]` — only match tasks in the given queues
        """
        results = await self.next_due_bulk(1, queues=queues)
        return results[0][0] if results else None

    async def next_due_bulk(self, n: int, queues: list[str] | None = None) -> list[tuple[Task, dt.datetime]]:
        """
        Atomically acquire and return up to n due tasks paired with their scheduled run_at.

        - `queues=None` — match any queue
        - `queues=[]` — return [] immediately
        - `queues=[...]` — only match tasks in the given queues
        """
        if queues is not None and not queues:
            return []

        if queues is None:
            queues = await self._get_all_queues()
            if not queues:
                return []

        now = dt.datetime.now(dt.UTC).timestamp()
        keys = [self.SCHEDULE_QUEUE(queue=q) for q in queues]
        raw: list[Any] = await cast(
            "Awaitable[list[Any]]",
            self.data_store.eval(self._ACQUIRE_SCRIPT, len(keys), *keys, str(now), n),
        )

        task_ids = [ULID.from_bytes(raw[i]) for i in range(0, len(raw), 2)]
        run_ats = [dt.datetime.fromtimestamp(float(raw[i + 1]), dt.UTC) for i in range(0, len(raw), 2)]
        tasks: list[Task | None] = await self.ta.get_tasks_bulk(task_ids)
        return [(task, run_at) for task, run_at in zip(tasks, run_ats) if task is not None]


# ---------------------------------------------------------------------------
# RedisCronDAGScheduler — Redis-backed recurring cron-scheduled DAG entries
# ---------------------------------------------------------------------------


class RedisCronDAGScheduler:
    """
    Manages recurring cron-scheduled DAG entries in Redis.

    Keys:
    - `cron-dag:{cron_id}` hash — serialised CronDAGEntry fields.
    - `cron-schedule` sorted set — member: cron_id bytes, score: next_run_at Unix timestamp.
    - `cron-active:{id}` string — active root task ID for skip_if_running entries (with TTL).

    The efficient "what's due?" query is a single ZRANGEBYSCORE on `cron-schedule`,
    giving O(log N + K) where N = total entries and K = entries due now.
    """

    CRON_DAG_KEY = "cron-dag:{cron_id}".format
    CRON_SCHEDULE = "cron-schedule"
    CRON_ACTIVE_KEY = "cron-active:{cron_id}".format

    # Atomically acquire up to ARGV[2] entries with score <= ARGV[1] (now).
    # Returns flat list: [cron_id_bytes, score_str, cron_id_bytes, score_str, ...]
    _ACQUIRE_SCRIPT = """
        local now = ARGV[1]
        local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'WITHSCORES', 'LIMIT', '0', ARGV[2])
        for i = 1, #items, 2 do
            redis.call('ZREM', KEYS[1], items[i])
        end
        return items
    """

    def __init__(self, data_store: Redis) -> None:
        self.data_store = data_store

    @property
    def backend_key(self) -> str:
        """Stable identifier matching other adapters that use the same Redis client."""
        return str(id(self.data_store))

    def pipeline(self, transaction: bool = True) -> Pipeline:
        """Return a Redis pipeline for atomic staging of cron ops."""
        return self.data_store.pipeline(transaction=transaction)

    def stage_add(self, pipe: Pipeline, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        """Queue HSET cron-dag:{id} + ZADD cron-schedule onto pipe (no execute)."""
        cron_id_str = str(entry.id)
        cron_id_bytes = bytes(entry.id)
        pipe.hset(
            self.CRON_DAG_KEY(cron_id=cron_id_str),
            mapping={
                "name": entry.name,
                "cron_expr": entry.cron_expr,
                "dag_spec": entry.dag_spec.model_dump_json(),
                "enabled": "1" if entry.enabled else "0",
                "concurrency_policy": entry.concurrency_policy.value,
                "created_at": entry.created_at.isoformat(),
            },
        )
        pipe.zadd(self.CRON_SCHEDULE, {cron_id_bytes: next_run_at.timestamp()})

    def stage_remove(self, pipe: Pipeline, cron_id: ULID) -> None:
        """Queue DEL cron-dag:{id} + ZREM cron-schedule onto pipe (no execute)."""
        pipe.delete(self.CRON_DAG_KEY(cron_id=str(cron_id)))
        pipe.zrem(self.CRON_SCHEDULE, bytes(cron_id))

    async def add(self, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        """Persist a cron entry and schedule it (direct, non-staged version)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_add(pipe, entry, next_run_at)
        await pipe.execute()

    async def remove(self, cron_id: ULID) -> None:
        """Delete a cron entry and remove it from the schedule (direct, non-staged version)."""
        pipe = self.data_store.pipeline(transaction=True)
        self.stage_remove(pipe, cron_id)
        await pipe.execute()

    async def get(self, cron_id: ULID) -> CronDAGEntry | None:
        """Fetch and deserialize a single CronDAGEntry from its hash, or None if missing."""
        raw: dict[bytes, bytes] = await cast(
            "Awaitable[dict[bytes, bytes]]",
            self.data_store.hgetall(self.CRON_DAG_KEY(cron_id=str(cron_id))),
        )
        if not raw:
            return None
        return CronDAGEntry(
            id=cron_id,
            name=raw[b"name"].decode(),
            cron_expr=raw[b"cron_expr"].decode(),
            dag_spec=DAGTaskSpec.model_validate(json.loads(raw[b"dag_spec"])),
            enabled=raw[b"enabled"] == b"1",
            concurrency_policy=ConcurrencyPolicy(raw[b"concurrency_policy"].decode()),
            created_at=dt.datetime.fromisoformat(raw[b"created_at"].decode()),
        )

    async def next_due_bulk(self, n: int) -> list[tuple[CronDAGEntry, dt.datetime]]:
        """
        Atomically acquire and return up to n due cron entries paired with their scheduled run_at.

        Entries are removed from `cron-schedule` atomically; caller is responsible for
        rescheduling via `stage_reschedule` after dispatching.
        """
        now = dt.datetime.now(dt.UTC).timestamp()
        raw: list[Any] = await cast(
            "Awaitable[list[Any]]",
            self.data_store.eval(self._ACQUIRE_SCRIPT, 1, self.CRON_SCHEDULE, str(now), n),
        )

        results: list[tuple[CronDAGEntry, dt.datetime]] = []
        for i in range(0, len(raw), 2):
            cron_id = ULID.from_bytes(raw[i])
            run_at = dt.datetime.fromtimestamp(float(raw[i + 1]), dt.UTC)
            entry = await self.get(cron_id)
            if entry is None:
                # The hash was deleted (e.g. by an admin) after the Lua script removed the
                # entry from cron-schedule.  Re-add it with a short retry delay so it is
                # not permanently lost; the caller will skip dispatch because there is no entry.
                retry_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=60)
                pipe = self.data_store.pipeline(transaction=True)
                pipe.zadd(self.CRON_SCHEDULE, {bytes(cron_id): retry_at.timestamp()})
                await pipe.execute()
                logger.error(
                    "Cron entry %s removed from schedule but hash is missing; "
                    "re-added with 60s retry delay. Check for concurrent deletion.",
                    cron_id,
                )
                continue
            results.append((entry, run_at))
        return results

    def stage_reschedule(self, pipe: TransactionHandle, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Queue ZADD cron-schedule with updated next_run_at score onto pipe (no execute)."""
        p: Any = pipe
        p.zadd(self.CRON_SCHEDULE, {bytes(cron_id): next_run_at.timestamp()})

    async def reschedule(self, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Update the cron entry's next scheduled run time (non-pipeline version for saga path)."""
        await self.data_store.zadd(self.CRON_SCHEDULE, {bytes(cron_id): next_run_at.timestamp()})

    async def get_active_run(self, cron_id: ULID) -> str | None:
        """Return the active root task ID string for a skip_if_running entry, or None."""
        raw: bytes | None = await cast(
            "Awaitable[bytes | None]",
            self.data_store.get(self.CRON_ACTIVE_KEY(cron_id=str(cron_id))),
        )
        return raw.decode() if raw is not None else None

    async def set_active_run(self, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False) -> bool:
        """Set the active-run marker for a cron entry. Returns False when nx=True and key exists."""
        result = await self.data_store.set(
            self.CRON_ACTIVE_KEY(cron_id=str(cron_id)), str(task_id), ex=ttl, nx=nx
        )
        return result is not None

    def stage_set_active_run(
        self, pipe: TransactionHandle, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False
    ) -> None:
        """
        Queue SET cron-active:{id} with TTL onto pipe (no execute).

        When *nx=True* the SET is conditional (SET NX): it only succeeds if the key
        does not already exist, providing an atomic guard against concurrent dispatches.
        """
        p: Any = pipe
        p.set(self.CRON_ACTIVE_KEY(cron_id=str(cron_id)), str(task_id), ex=ttl, nx=nx)

    def stage_clear_active_run(self, pipe: TransactionHandle, cron_id: ULID) -> None:
        """Queue DEL cron-active:{id} onto pipe (no execute)."""
        p: Any = pipe
        p.delete(self.CRON_ACTIVE_KEY(cron_id=str(cron_id)))

    async def clear_active_run(self, cron_id: ULID) -> None:
        """Delete the active-run marker for a cron entry (non-pipeline version for saga path)."""
        await self.data_store.delete(self.CRON_ACTIVE_KEY(cron_id=str(cron_id)))

    async def get_next_run_at(self, cron_id: ULID) -> dt.datetime | None:
        """Return the next scheduled run time for a cron entry, or None if not scheduled."""
        score: float | None = await cast(
            "Awaitable[float | None]",
            self.data_store.zscore(self.CRON_SCHEDULE, bytes(cron_id)),
        )
        return dt.datetime.fromtimestamp(score, dt.UTC) if score is not None else None

    async def list(
        self, offset: int = 0, limit: int = 50
    ) -> tuple[list[tuple[CronDAGEntry, dt.datetime]], int]:
        """
        Return a page of cron entries ordered by next_run_at ascending.

        Returns (entry, next_run_at) pairs plus the total count of all entries in the schedule.
        """
        total: int = await self.data_store.zcard(self.CRON_SCHEDULE)
        raw: list[tuple[bytes, float]] = await self.data_store.zrange(
            self.CRON_SCHEDULE, offset, offset + limit - 1, withscores=True
        )
        results: list[tuple[CronDAGEntry, dt.datetime]] = []
        for cron_id_bytes, score in raw:
            cron_id = ULID.from_bytes(cron_id_bytes)
            entry = await self.get(cron_id)
            if entry is not None:
                next_run_at = dt.datetime.fromtimestamp(score, dt.UTC)
                results.append((entry, next_run_at))
        return results, total
