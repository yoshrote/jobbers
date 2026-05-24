"""
Plain Redis backed implementations (no Redis Stack modules required).

Routing sub-adapters:
- `RedisQueueConfigAdapter` — queue/role config and refresh tags in plain Redis keys/sets.
- `RedisTaskRoutingConfigAdapter` — task routing config in plain Redis keys.
- `RedisRoutingBackend` — composes the two sub-adapters to satisfy RoutingBackendProtocol.

Task storage / dead-letter queue:
- `MsgpackTaskAdapter` — stores tasks as msgpack-encoded binary strings,
  queries via sorted-set range commands.
- `DeadQueue` — dead letter queue backed by Redis sorted sets, sets, and a
  hash for secondary indexes.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any, cast

from ulid import ULID

from jobbers.adapters._shared import SharedTaskAdapterMixin
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_routing import RoutingConfig
from jobbers.utils.serialization import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from pydantic import BaseModel
    from redis.asyncio.client import Pipeline, Redis

    from jobbers.protocols import TaskAdapterProtocol


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
            return cast("ULID", ULID.from_str(raw.decode()))
        init_tag = ULID()
        # SET NX so two concurrent callers don't clobber each other.
        await self._client.set(self.ROLE_REFRESH_TAG_KEY(name=role), str(init_tag), nx=True)
        raw = await self._client.get(self.ROLE_REFRESH_TAG_KEY(name=role))
        return cast("ULID", ULID.from_str(raw.decode())) if raw else init_tag

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
    """RoutingBackendProtocol backed by Redis. Delegates to RedisQueueConfigAdapter and RedisTaskRoutingConfigAdapter."""

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
# MsgpackTaskAdapter  (plain Redis: binary strings)
# ---------------------------------------------------------------------------


class MsgpackTaskAdapter(SharedTaskAdapterMixin):
    """
    Stores tasks as msgpack-encoded binary strings; queries via sorted-set range commands.

    Works with any standard Redis instance (no Redis Stack required).
    `get_all_tasks` applies `task_name`, `task_version`, and `status` filters in Python
    after fetching candidate task IDs from the queue sorted set.
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
    SUBMIT_SCRIPT = """
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

    def stage_register_dag_run(self, pipe: Pipeline, task: Task) -> None:
        """Stage DAG run index updates, including the per-run task set for the raw adapter."""
        super().stage_register_dag_run(pipe, task)
        if task.dag_run_id is None or task.submitted_at is None:
            return
        score = task.submitted_at.timestamp()
        pipe.zadd(self.DAG_RUN_TASKS(dag_run_id=task.dag_run_id), {bytes(task.id): score})

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
# DeadQueue  (plain Redis: sorted sets, sets, hash indexes)
# ---------------------------------------------------------------------------


class DeadQueue:
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

    def __init__(self, data_store: Redis, task_adapter: TaskAdapterProtocol) -> None:
        self.data_store = data_store
        self.ta = task_adapter

    async def ensure_index(self) -> None:
        """No-op: plain-Redis dead queue does not use a search index."""

    def stage_add(self, pipe: Pipeline, task: Task, failed_at: dt.datetime) -> None:
        """Queue all four DLQ index writes onto pipe (no execute)."""
        pipe.zadd(self.DLQ, {bytes(task.id): failed_at.timestamp()})
        pipe.sadd(self.DLQ_QUEUE(queue=task.queue), bytes(task.id))
        pipe.sadd(self.DLQ_NAME(name=task.name), bytes(task.id))
        pipe.hset(self.DLQ_META, str(task.id), f"{task.queue}\0{task.name}\0{task.version}")

    def stage_remove(self, pipe: Pipeline, task_id: ULID, queue: str, name: str) -> None:
        """Queue all four DLQ index deletes onto pipe (no execute)."""
        pipe.zrem(self.DLQ, bytes(task_id))
        pipe.srem(self.DLQ_QUEUE(queue=queue), bytes(task_id))
        pipe.srem(self.DLQ_NAME(name=name), bytes(task_id))
        pipe.hdel(self.DLQ_META, str(task_id))

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
