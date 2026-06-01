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
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.utils.serialization import deserialize

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline

    from jobbers.protocols import TransactionHandle


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
