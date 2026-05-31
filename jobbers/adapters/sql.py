"""
SQLAlchemy-backed routing sub-adapters, routing backend, task adapters, dead-letter queue, and cron DAG scheduler.

Routing sub-adapters:
- `SQLQueueConfigAdapter` — queue/role config and refresh tags in SQL tables.
- `SQLTaskRoutingConfigAdapter` — task routing config in the ``task_routing`` table.
- `SQLRoutingBackend` — composes the two sub-adapters to satisfy RoutingBackendProtocol.

Task storage:
- `SQLTaskState` — implements ``TaskStateProtocol``; does **not** implement
  ``AtomicTaskStateProtocol`` (missing ``stage_remove_heartbeat`` and
  ``optimistic_dispatch_scheduled`` — StateManager always uses saga mode with this adapter).
  Tables: ``tasks`` / ``task_queue`` / ``task_fan_in`` / ``dag_runs``.
- `SQLTaskSubmit` — implements ``TaskSubmitProtocol``; submit/pop operations backed by
  the ``tasks`` and ``task_queue`` tables.

Dead-letter queue:
- `SQLDeadQueue` — implements ``DeadQueueProtocol`` and ``AtomicDeadQueueProtocol``;
  backed by the ``dead_letter_queue`` table.

Task scheduler:
- `SQLTaskScheduler` — scheduled/delayed task queue in the ``task_schedule`` table;
  implements ``AtomicTaskSchedulerProtocol``.

Cron DAG scheduler:
- `SQLCronDAGScheduler` — recurring cron-scheduled DAG entries in the
  ``cron_dag_entries`` and ``cron_dag_active_runs`` tables.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError
from ulid import ULID

from jobbers.migrations.schema import (
    cron_dag_active_runs,
    cron_dag_entries,
    dag_runs,
    dead_letter_queue,
    queues,
    role_queues,
    roles,
    task_fan_in,
    task_queue,
    task_routing,
    task_schedule,
    tasks,
)
from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGTaskSpec
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_routing import RoutingConfig
from jobbers.models.task_status import TaskStatus
from jobbers.utils.sql_transaction import SQLTransactionBatch

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from jobbers.models.dag import DAGRunPagination
    from jobbers.protocols import TransactionHandle

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQLQueueConfigAdapter
# ---------------------------------------------------------------------------


class SQLQueueConfigAdapter:
    """
    QueueConfigProtocol backed by SQLAlchemy async sessions.

    Tables:
      roles       — named roles, each with a ``refresh_tag`` for change detection.
      queues      — queue configurations (concurrency limits and rate limiting).
      role_queues — many-to-many mapping of roles to queues.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    # ── Queue CRUD ────────────────────────────────────────────────────────────

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    queues.c.name,
                    queues.c.max_concurrent,
                    queues.c.rate_numerator,
                    queues.c.rate_denominator,
                    queues.c.rate_period,
                ).where(queues.c.name == queue)
            )
            row = result.fetchone()
        if row is None:
            return None
        return QueueConfig.from_row(row)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        async with self._session_factory.begin() as session:
            existing = await session.execute(select(queues.c.name).where(queues.c.name == queue_config.name))
            if existing.fetchone():
                await session.execute(
                    update(queues)
                    .where(queues.c.name == queue_config.name)
                    .values(
                        max_concurrent=queue_config.max_concurrent,
                        rate_numerator=queue_config.rate_numerator,
                        rate_denominator=queue_config.rate_denominator,
                        rate_period=queue_config.rate_period,
                    )
                )
            else:
                await session.execute(
                    insert(queues).values(
                        name=queue_config.name,
                        max_concurrent=queue_config.max_concurrent,
                        rate_numerator=queue_config.rate_numerator,
                        rate_denominator=queue_config.rate_denominator,
                        rate_period=queue_config.rate_period,
                    )
                )

    async def delete_queue(self, queue_name: str) -> None:
        """Delete a queue and cascade to role_queues; bump refresh_tag for affected roles."""
        new_tag = str(ULID())
        async with self._session_factory.begin() as session:
            result = await session.execute(
                select(role_queues.c.role).where(role_queues.c.queue == queue_name).distinct()
            )
            affected_roles = [row[0] for row in result.fetchall()]
            await session.execute(delete(queues).where(queues.c.name == queue_name))
            if affected_roles:
                await session.execute(
                    update(roles).where(roles.c.name.in_(affected_roles)).values(refresh_tag=new_tag)
                )

    async def get_all_queues(self) -> list[str]:
        async with self._session_factory() as session:
            result = await session.execute(select(queues.c.name).order_by(queues.c.name))
            return [row[0] for row in result.fetchall()]

    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]:
        """Return a map of queue name → max_concurrent for the requested queues."""
        if not queues_set:
            return {}
        async with self._session_factory() as session:
            result = await session.execute(
                select(queues.c.name, queues.c.max_concurrent).where(queues.c.name.in_(list(queues_set)))
            )
            found = {row[0]: row[1] for row in result.fetchall()}
        return {q: found.get(q) for q in queues_set}

    # ── Role CRUD ─────────────────────────────────────────────────────────────

    async def get_queues(self, role: str) -> set[str]:
        async with self._session_factory() as session:
            result = await session.execute(select(role_queues.c.queue).where(role_queues.c.role == role))
            return {row[0] for row in result.fetchall()}

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        new_tag = str(ULID())
        async with self._session_factory.begin() as session:
            existing = await session.execute(select(roles.c.name).where(roles.c.name == role))
            if existing.fetchone():
                await session.execute(update(roles).where(roles.c.name == role).values(refresh_tag=new_tag))
            else:
                await session.execute(insert(roles).values(name=role, refresh_tag=new_tag))
            await session.execute(delete(role_queues).where(role_queues.c.role == role))
            if queues_set:
                await session.execute(
                    insert(role_queues),
                    [{"role": role, "queue": q} for q in queues_set],
                )
        return new_tag

    async def get_all_roles(self) -> list[str]:
        async with self._session_factory() as session:
            result = await session.execute(select(roles.c.name).order_by(roles.c.name))
            return [row[0] for row in result.fetchall()]

    async def delete_role(self, role: str) -> None:
        """Delete a role (cascades to role_queues). Queue configs are preserved."""
        async with self._session_factory.begin() as session:
            await session.execute(delete(roles).where(roles.c.name == role))

    # ── Refresh tags ──────────────────────────────────────────────────────────

    async def get_refresh_tag(self, role: str) -> ULID:
        """Return the current refresh tag for a role, creating one if needed."""
        async with self._session_factory() as session:
            result = await session.execute(select(roles.c.refresh_tag).where(roles.c.name == role))
        tag_str: str | None = result.scalar()
        if tag_str:
            existing_tag: ULID = ULID.from_str(tag_str)
            return existing_tag

        init_tag = ULID()
        try:
            async with self._session_factory.begin() as session:
                await session.execute(insert(roles).values(name=role, refresh_tag=str(init_tag)))
        except IntegrityError:
            pass  # Another process inserted first
        # Re-read in case another process won the race
        async with self._session_factory() as session:
            result = await session.execute(select(roles.c.refresh_tag).where(roles.c.name == role))
        tag_str = result.scalar()
        return ULID.from_str(tag_str) if tag_str else init_tag

    async def bump_refresh_tag(self, role: str) -> str:
        """Generate a new ULID tag for *role* and write it to SQL. Returns the new tag string."""
        new_tag = str(ULID())
        async with self._session_factory.begin() as session:
            await session.execute(update(roles).where(roles.c.name == role).values(refresh_tag=new_tag))
        return new_tag

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        """Bump refresh_tag for every role that contains *queue_name*. Returns affected role names."""
        new_tag = str(ULID())
        async with self._session_factory.begin() as session:
            result = await session.execute(
                select(role_queues.c.role).where(role_queues.c.queue == queue_name).distinct()
            )
            affected_roles = [row[0] for row in result.fetchall()]
            if affected_roles:
                await session.execute(
                    update(roles).where(roles.c.name.in_(affected_roles)).values(refresh_tag=new_tag)
                )
        return affected_roles


# ---------------------------------------------------------------------------
# SQLTaskRoutingConfigAdapter
# ---------------------------------------------------------------------------


class SQLTaskRoutingConfigAdapter:
    """TaskRoutingConfigProtocol backed by SQLAlchemy (``task_routing`` table)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    task_routing.c.task_name,
                    task_routing.c.task_version,
                    task_routing.c.strategy,
                    task_routing.c.queues,
                    task_routing.c.weights,
                ).where(
                    task_routing.c.task_name == task_name,
                    task_routing.c.task_version == task_version,
                )
            )
            row = result.fetchone()
        if row is None:
            return None
        return RoutingConfig.from_row(row)

    async def save_routing_config(self, config: RoutingConfig) -> None:
        """Create or replace the routing config for a task type."""
        queues_json = json.dumps(config.queues)
        weights_json = json.dumps(config.weights) if config.weights is not None else None
        async with self._session_factory.begin() as session:
            existing = await session.execute(
                select(task_routing.c.task_name).where(
                    task_routing.c.task_name == config.task_name,
                    task_routing.c.task_version == config.task_version,
                )
            )
            if existing.fetchone():
                await session.execute(
                    update(task_routing)
                    .where(
                        task_routing.c.task_name == config.task_name,
                        task_routing.c.task_version == config.task_version,
                    )
                    .values(strategy=config.strategy, queues=queues_json, weights=weights_json)
                )
            else:
                await session.execute(
                    insert(task_routing).values(
                        task_name=config.task_name,
                        task_version=config.task_version,
                        strategy=config.strategy,
                        queues=queues_json,
                        weights=weights_json,
                    )
                )

    async def delete_routing_config(self, task_name: str, task_version: int) -> bool:
        """Remove the routing config for a task type. Returns False if it did not exist."""
        async with self._session_factory.begin() as session:
            existing = await session.execute(
                select(task_routing.c.task_name).where(
                    task_routing.c.task_name == task_name,
                    task_routing.c.task_version == task_version,
                )
            )
            if existing.fetchone() is None:
                return False
            await session.execute(
                delete(task_routing).where(
                    task_routing.c.task_name == task_name,
                    task_routing.c.task_version == task_version,
                )
            )
        return True


# ---------------------------------------------------------------------------
# SQLRoutingBackend  (composes the two sub-adapters above)
# ---------------------------------------------------------------------------


class SQLRoutingBackend:
    """RoutingBackendProtocol backed by SQLAlchemy. Delegates to sub-adapters."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._qca = SQLQueueConfigAdapter(session_factory)
        self._rca = SQLTaskRoutingConfigAdapter(session_factory)

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
# SQLTaskState  (SQLAlchemy: tasks / task_fan_in / dag_runs tables)
# ---------------------------------------------------------------------------

_TERMINAL_STATUSES = frozenset(
    [
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.STALLED,
        TaskStatus.DROPPED,
    ]
)


def _task_to_row(task: Task) -> dict[str, Any]:
    """Serialize a Task to a dict of column values."""
    data = task.model_dump(mode="json")
    return {
        "id": str(task.id),
        "name": task.name,
        "queue": task.queue,
        "version": task.version,
        "status": task.status.value,
        "retry_attempt": task.retry_attempt,
        "submitted_at": task.submitted_at,
        "retried_at": task.retried_at,
        "started_at": task.started_at,
        "heartbeat_at": task.heartbeat_at,
        "completed_at": task.completed_at,
        "inject_parent_results": task.inject_parent_results,
        "cron_id": str(task.cron_id) if task.cron_id is not None else None,
        "dag_run_id": str(task.dag_run_id) if task.dag_run_id is not None else None,
        "parameters": json.dumps(data.get("parameters", {})),
        "results": json.dumps(data.get("results", {})),
        "errors": json.dumps(data.get("errors", [])),
        "parent_ids": json.dumps(data.get("parent_ids", [])),
        "dag_callbacks": json.dumps(data.get("dag_callbacks", [])),
    }


def _ensure_utc(d: dt.datetime | None) -> dt.datetime | None:
    """SQLite returns naive datetimes even for DateTime(timezone=True) columns; re-attach UTC."""
    if d is None or d.tzinfo is not None:
        return d
    return d.replace(tzinfo=dt.UTC)


def _ensure_utc_nn(d: dt.datetime) -> dt.datetime:
    """Non-nullable variant of _ensure_utc."""
    return d if d.tzinfo is not None else d.replace(tzinfo=dt.UTC)


def _row_to_task(row: Any) -> Task:
    """Deserialize a tasks-table row to a Task."""
    data: dict[str, Any] = {
        "id": row.id,
        "name": row.name,
        "queue": row.queue,
        "version": row.version,
        "status": row.status,
        "retry_attempt": row.retry_attempt,
        "submitted_at": _ensure_utc(row.submitted_at),
        "retried_at": _ensure_utc(row.retried_at),
        "started_at": _ensure_utc(row.started_at),
        "heartbeat_at": _ensure_utc(row.heartbeat_at),
        "completed_at": _ensure_utc(row.completed_at),
        "inject_parent_results": row.inject_parent_results,
        "cron_id": row.cron_id,
        "dag_run_id": row.dag_run_id,
        "parameters": json.loads(row.parameters) if row.parameters else {},
        "results": json.loads(row.results) if row.results else {},
        "errors": json.loads(row.errors) if row.errors else [],
        "parent_ids": json.loads(row.parent_ids) if row.parent_ids else [],
        "dag_callbacks": json.loads(row.dag_callbacks) if row.dag_callbacks else [],
    }
    return Task.model_validate(data)


async def _upsert_task(session: AsyncSession, row: dict[str, Any]) -> None:
    """INSERT or UPDATE a task row by id."""
    task_id = row["id"]
    non_pk = {k: v for k, v in row.items() if k != "id"}
    result = await session.execute(update(tasks).where(tasks.c.id == task_id).values(**non_pk))
    if result.rowcount == 0:  # type: ignore[attr-defined]
        await session.execute(insert(tasks).values(**row))


class SQLTaskState:
    """
    TaskStateProtocol backed by SQLAlchemy.

    Tables: ``tasks`` / ``task_fan_in`` / ``dag_runs``.  Uses ``SQLTransactionBatch``
    for staged writes.  Does **not** implement ``AtomicTaskStateProtocol`` — missing
    ``stage_remove_heartbeat`` and ``optimistic_dispatch_scheduled``; ``StateManager``
    always uses saga mode with this adapter.
    """

    # Key-helper stubs: present for structural compatibility with AtomicTaskStateProtocol
    # (used by DummyTaskAdapter and shared test helpers that reference these names).
    # These string-format callables are only meaningful in the Redis adapters' Lua scripts
    # and the heartbeat sorted-set; SQL stores heartbeat in the tasks table itself.
    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format
    HEARTBEAT_SCORES = "task-heartbeats:{queue}".format
    TASK_BY_TYPE_IDX = "task-type-idx:{name}".format
    QUEUE_RATE_LIMITER = "rate-limiter:{queue}".format
    DLQ_MISSING_DATA = "dlq-missing-data"
    DAG_RUNS = "dag-runs"
    DAG_RUN_TASKS = "dag-run:{dag_run_id}:tasks".format

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], dsn: str = "") -> None:
        self._sf = session_factory
        self._dsn = dsn

    @property
    def backend_key(self) -> str:
        """Stable identifier shared by all SQL adapters pointing to the same database."""
        return self._dsn or str(id(self._sf))

    @property
    def _use_for_update(self) -> bool:
        return "sqlite" not in self._dsn

    def pipeline(self, transaction: bool = True) -> SQLTransactionBatch:
        """Return a new SQLTransactionBatch for staged atomic writes."""
        return SQLTransactionBatch(self._sf)

    # ── AtomicTaskStateProtocol: staged writes ─────────────────────────────

    def stage_save(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage an INSERT-or-UPDATE for this task."""
        row = _task_to_row(task)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _do_upsert(s: AsyncSession) -> None:
            await _upsert_task(s, row)

        pipe.add_op(_do_upsert)

    def _stage_enqueue(self, pipe: SQLTransactionBatch, task: Task) -> None:
        """Stage an INSERT-or-UPDATE in task_queue."""
        task_id_str = str(task.id)
        queue_name = task.queue
        submitted_at = task.submitted_at or dt.datetime.now(dt.UTC)

        async def _enqueue(s: AsyncSession) -> None:
            result = await s.execute(
                update(task_queue)
                .where(task_queue.c.task_id == task_id_str)
                .values(queue=queue_name, submitted_at=submitted_at)
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                await s.execute(
                    insert(task_queue).values(
                        task_id=task_id_str, queue=queue_name, submitted_at=submitted_at
                    )
                )

        pipe.add_op(_enqueue)

    def stage_requeue(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage a save + re-enqueue in task_queue."""
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101
        self.stage_save(pipe, task)
        self._stage_enqueue(pipe, task)

    def stage_submit_task(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage a save + enqueue in task_queue + DAG-run registration."""
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101
        self.stage_save(pipe, task)
        self._stage_enqueue(pipe, task)
        if task.dag_run_id is not None and task.submitted_at is not None:
            dag_run_id_str = str(task.dag_run_id)
            submitted_at = task.submitted_at

            async def _register_dag_run(s: AsyncSession) -> None:
                existing = await s.execute(select(dag_runs).where(dag_runs.c.dag_run_id == dag_run_id_str))
                if existing.first() is None:
                    await s.execute(
                        insert(dag_runs).values(dag_run_id=dag_run_id_str, submitted_at=submitted_at)
                    )

            pipe.add_op(_register_dag_run)

    def stage_remove_from_queue(self, pipe: TransactionHandle, task: Task) -> None:
        """Stage a DELETE from task_queue; task record in tasks is preserved."""
        task_id_str = str(task.id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _remove(s: AsyncSession) -> None:
            await s.execute(delete(task_queue).where(task_queue.c.task_id == task_id_str))

        pipe.add_op(_remove)

    def stage_init_fan_in(
        self, pipe: TransactionHandle, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None:
        """Stage INSERT of predecessor task-ids into task_fan_in."""
        now = dt.datetime.now(dt.UTC)
        rows = [{"fan_in_key": fan_in_key, "task_id": str(pid), "created_at": now} for pid in predecessor_ids]
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _insert_fan_in(s: AsyncSession) -> None:
            for row in rows:
                async with s.begin_nested() as sp:
                    try:
                        await s.execute(insert(task_fan_in).values(**row))
                    except IntegrityError:
                        await sp.rollback()

        pipe.add_op(_insert_fan_in)

    async def read_for_watch(self, pipe: TransactionHandle, task_id: ULID) -> Task | None:
        """Read a task inside the batch transaction, locking the row on backends that support FOR UPDATE."""
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101
        session = await pipe._get_session()
        stmt = select(tasks).where(tasks.c.id == str(task_id))
        if self._use_for_update:
            stmt = stmt.with_for_update()
        result = await session.execute(stmt)
        row = result.first()
        return _row_to_task(row) if row is not None else None

    # ── TaskStateProtocol: direct reads/writes ─────────────────────────────

    async def save_task(self, task: Task) -> None:
        """Persist a task directly (non-staged)."""
        async with self._sf() as session:
            async with session.begin():
                await _upsert_task(session, _task_to_row(task))

    async def get_task(self, task_id: ULID) -> Task | None:
        """Fetch a single task by id."""
        async with self._sf() as session:
            result = await session.execute(select(tasks).where(tasks.c.id == str(task_id)))
            row = result.first()
            return _row_to_task(row) if row is not None else None

    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]:
        """Fetch multiple tasks in a single query."""
        if not task_ids:
            return []
        id_strs = [str(t) for t in task_ids]
        async with self._sf() as session:
            result = await session.execute(select(tasks).where(tasks.c.id.in_(id_strs)))
            rows = {row.id: row for row in result.all()}
        return [_row_to_task(rows[s]) if s in rows else None for s in id_strs]

    async def task_exists(self, task_id: ULID) -> bool:
        async with self._sf() as session:
            result = await session.execute(
                select(func.count()).select_from(tasks).where(tasks.c.id == str(task_id))
            )
            return (result.scalar() or 0) > 0

    async def compare_and_set_status(self, task_id: ULID, expected: TaskStatus, new: TaskStatus) -> bool:
        """Atomically transition status only if it currently equals ``expected``."""
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    update(tasks)
                    .where(tasks.c.id == str(task_id), tasks.c.status == expected.value)
                    .values(status=new.value)
                )
                return bool(result.rowcount == 1)  # type: ignore[attr-defined]

    async def get_active_tasks(self, queues: set[str]) -> list[Task]:
        """Return tasks that have a non-null heartbeat_at for the given queues."""
        if not queues:
            return []
        async with self._sf() as session:
            result = await session.execute(
                select(tasks).where(
                    tasks.c.queue.in_(queues),
                    tasks.c.heartbeat_at.is_not(None),
                )
            )
            return [_row_to_task(row) for row in result.all()]

    async def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]:
        """Yield STARTED tasks whose heartbeat is older than ``stale_time``."""
        if not queues:
            return
        cutoff = dt.datetime.now(dt.UTC) - stale_time
        async with self._sf() as session:
            result = await session.execute(
                select(tasks).where(
                    tasks.c.queue.in_(queues),
                    tasks.c.status == TaskStatus.STARTED.value,
                    tasks.c.heartbeat_at < cutoff,
                )
            )
            for row in result.all():
                yield _row_to_task(row)

    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]:
        """Return a paginated list of tasks matching the filter criteria."""
        stmt = select(tasks).where(tasks.c.queue == pagination.queue)
        if pagination.task_name is not None:
            stmt = stmt.where(tasks.c.name == pagination.task_name)
        if pagination.task_version is not None:
            stmt = stmt.where(tasks.c.version == pagination.task_version)
        if pagination.status is not None:
            stmt = stmt.where(tasks.c.status == pagination.status.value)
        if pagination.start is not None:
            stmt = stmt.where(tasks.c.id > str(pagination.start))
        order_col = (
            tasks.c.submitted_at if pagination.order_by == PaginationOrder.SUBMITTED_AT else tasks.c.id
        )
        stmt = stmt.order_by(order_col).limit(pagination.limit).offset(pagination.offset)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [_row_to_task(row) for row in result.all()]

    async def update_task_heartbeat(self, task: Task) -> None:
        assert task.heartbeat_at  # noqa: S101
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    update(tasks).where(tasks.c.id == str(task.id)).values(heartbeat_at=task.heartbeat_at)
                )

    async def remove_task_heartbeat(self, task: Task) -> None:
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    update(tasks).where(tasks.c.id == str(task.id)).values(heartbeat_at=None)
                )

    # ── Fan-in ─────────────────────────────────────────────────────────────

    async def init_fan_in(self, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400) -> None:
        """Persist fan-in predecessor set (TTL is ignored for SQL)."""
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            async with session.begin():
                for pid in predecessor_ids:
                    async with session.begin_nested() as sp:
                        try:
                            await session.execute(
                                insert(task_fan_in).values(
                                    fan_in_key=fan_in_key, task_id=str(pid), created_at=now
                                )
                            )
                        except IntegrityError:
                            await sp.rollback()

    async def fan_in_complete(self, fan_in_key: str, task_id: ULID) -> int:
        """
        Mark task_id complete in the fan-in set and return remaining (incomplete) count.

        Returns -1 if task_id was not a member or was already completed.
        """
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    update(task_fan_in)
                    .where(
                        task_fan_in.c.fan_in_key == fan_in_key,
                        task_fan_in.c.task_id == str(task_id),
                        task_fan_in.c.completed == False,  # noqa: E712
                    )
                    .values(completed=True)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    return -1
                count_result = await session.execute(
                    select(func.count())
                    .select_from(task_fan_in)
                    .where(
                        task_fan_in.c.fan_in_key == fan_in_key,
                        task_fan_in.c.completed == False,  # noqa: E712
                    )
                )
                return count_result.scalar() or 0

    async def get_fan_in_members(self, fan_in_key: str) -> list[ULID]:
        """Return all predecessor IDs for a fan-in key."""
        async with self._sf() as session:
            result = await session.execute(
                select(task_fan_in.c.task_id).where(task_fan_in.c.fan_in_key == fan_in_key)
            )
            return [ULID.from_str(row.task_id) for row in result.all()]

    # ── DAG run index ───────────────────────────────────────────────────────

    async def get_dag_runs(self, pagination: DAGRunPagination) -> tuple[list[tuple[ULID, dt.datetime]], int]:
        """Return a paginated list of DAG runs ordered by submission time."""
        async with self._sf() as session:
            total_result = await session.execute(select(func.count()).select_from(dag_runs))
            total: int = total_result.scalar() or 0
            result = await session.execute(
                select(dag_runs)
                .order_by(dag_runs.c.submitted_at)
                .limit(pagination.limit)
                .offset(pagination.offset)
            )
            entries = [
                (ULID.from_str(row.dag_run_id), _ensure_utc_nn(row.submitted_at)) for row in result.all()
            ]
        return entries, total

    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None:
        """Return (submitted_at, task_ids) for a DAG run, or None."""
        dag_run_id_str = str(dag_run_id)
        async with self._sf() as session:
            run_result = await session.execute(
                select(dag_runs).where(dag_runs.c.dag_run_id == dag_run_id_str)
            )
            run_row = run_result.first()
            if run_row is None:
                return None
            task_result = await session.execute(
                select(tasks.c.id).where(tasks.c.dag_run_id == dag_run_id_str)
            )
            task_ids = [ULID.from_str(row.id) for row in task_result.all()]
        return _ensure_utc_nn(run_row.submitted_at), task_ids

    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Delete DAG run entries older than ``max_age``."""
        cutoff = now - max_age
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(dag_runs).where(dag_runs.c.submitted_at < cutoff))

    # ── Lifecycle ───────────────────────────────────────────────────────────

    async def ensure_index(self) -> None:
        """No-op: indexes are created by run_migrations."""

    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None:
        """Delete terminal tasks older than ``max_age``."""
        cutoff = now - max_age
        statuses = [s.value for s in _TERMINAL_STATUSES]
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(tasks).where(
                        tasks.c.status.in_(statuses),
                        tasks.c.completed_at < cutoff,
                    )
                )

    async def clean(
        self,
        queues: set[bytes],
        now: dt.datetime,
        min_queue_age: dt.datetime | None = None,
        max_queue_age: dt.datetime | None = None,
    ) -> None:
        """Remove queue entries within a time range."""
        if not (min_queue_age or max_queue_age):
            return
        earliest = min_queue_age or dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
        latest = max_queue_age or now
        queue_names = [q.decode() for q in queues]
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(tasks).where(
                        tasks.c.queue.in_(queue_names),
                        tasks.c.submitted_at >= earliest,
                        tasks.c.submitted_at <= latest,
                    )
                )


# ---------------------------------------------------------------------------
# SQLTaskSubmit  (SQLAlchemy: tasks + task_queue tables — TaskSubmitProtocol)
# ---------------------------------------------------------------------------


class SQLTaskSubmit:
    """
    TaskSubmitProtocol backed by SQLAlchemy.

    Tables: ``tasks`` and ``task_queue``.  Each submit/pop is a single transaction.
    Requires the same session factory as the paired ``SQLTaskState``.
    """

    # Key-helper stubs for structural compatibility with RedisTaskState.
    TASKS_BY_QUEUE = "task-queues:{queue}".format
    TASK_DETAILS = "task:{task_id}".format

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], dsn: str = "") -> None:
        self._sf = session_factory
        self._dsn = dsn

    @property
    def _use_for_update(self) -> bool:
        return "sqlite" not in self._dsn

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """
        Atomically pop and return the oldest queued task from any of the given queues.

        Non-blocking: ``pop_timeout`` is ignored.  Removes the entry from ``task_queue``
        so subsequent calls will not return the same task.
        """
        if not queues:
            return None
        async with self._sf() as session:
            async with session.begin():
                stmt = (
                    select(task_queue.c.task_id)
                    .where(task_queue.c.queue.in_(queues))
                    .order_by(task_queue.c.submitted_at)
                    .limit(1)
                )
                if self._use_for_update:
                    stmt = stmt.with_for_update(skip_locked=True)
                result = await session.execute(stmt)
                row = result.first()
                if row is None:
                    return None
                task_id_str = row.task_id
                await session.execute(delete(task_queue).where(task_queue.c.task_id == task_id_str))
                task_result = await session.execute(select(tasks).where(tasks.c.id == task_id_str))
                task_row = task_result.first()
                return _row_to_task(task_row) if task_row is not None else None

    async def submit_task(self, task: Task) -> bool:
        """Submit a task directly (non-staged)."""
        assert task.submitted_at  # noqa: S101
        row = _task_to_row(task)
        task_id_str = str(task.id)
        async with self._sf() as session:
            async with session.begin():
                await _upsert_task(session, row)
                result = await session.execute(
                    update(task_queue)
                    .where(task_queue.c.task_id == task_id_str)
                    .values(queue=task.queue, submitted_at=task.submitted_at)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await session.execute(
                        insert(task_queue).values(
                            task_id=task_id_str, queue=task.queue, submitted_at=task.submitted_at
                        )
                    )
        if task.dag_run_id is not None:
            dag_run_id_str = str(task.dag_run_id)
            async with self._sf() as session:
                async with session.begin():
                    existing = await session.execute(
                        select(dag_runs).where(dag_runs.c.dag_run_id == dag_run_id_str)
                    )
                    if existing.first() is None:
                        await session.execute(
                            insert(dag_runs).values(dag_run_id=dag_run_id_str, submitted_at=task.submitted_at)
                        )
        return True

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """Rate-limited submit is not implemented for the SQL adapter."""
        raise NotImplementedError("SQLTaskSubmit does not support rate-limited submission")

    async def clean_rate_limiter(
        self, queues: set[bytes], now: dt.datetime, rate_limit_age: dt.timedelta
    ) -> None:
        pass


# ---------------------------------------------------------------------------
# SQLDeadQueue  (SQLAlchemy: dead_letter_queue table)
# ---------------------------------------------------------------------------


def _task_to_dlq_row(task: Task, failed_at: dt.datetime) -> dict[str, Any]:
    return {
        "id": str(task.id),
        "queue": task.queue,
        "name": task.name,
        "version": task.version,
        "failed_at": failed_at,
        "task_data": task.model_dump_json(),
    }


def _dlq_row_to_task(row: Any) -> Task:
    data: dict[str, Any] = json.loads(row.task_data)
    return Task.model_validate(data)


class SQLDeadQueue:
    """DeadQueueProtocol and AtomicDeadQueueProtocol backed by SQLAlchemy (``dead_letter_queue`` table)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], dsn: str = "") -> None:
        self._sf = session_factory
        self._dsn = dsn

    @property
    def backend_key(self) -> str:
        """Stable identifier shared by all SQL adapters pointing to the same database."""
        return self._dsn or str(id(self._sf))

    def pipeline(self, transaction: bool = True) -> SQLTransactionBatch:
        """Return a new SQLTransactionBatch for staged atomic writes."""
        return SQLTransactionBatch(self._sf)

    async def ensure_index(self) -> None:
        """No-op: indexes are created by run_migrations."""

    # ── AtomicDeadQueueProtocol: staged writes ─────────────────────────────

    def stage_add(self, pipe: TransactionHandle, task: Task, failed_at: dt.datetime) -> None:
        """Stage an INSERT into dead_letter_queue."""
        row = _task_to_dlq_row(task, failed_at)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _insert(s: AsyncSession) -> None:
            exists = await s.execute(select(dead_letter_queue).where(dead_letter_queue.c.id == row["id"]))
            if exists.first() is None:
                await s.execute(insert(dead_letter_queue).values(**row))
            else:
                non_pk = {k: v for k, v in row.items() if k != "id"}
                await s.execute(
                    update(dead_letter_queue).where(dead_letter_queue.c.id == row["id"]).values(**non_pk)
                )

        pipe.add_op(_insert)

    def stage_remove(self, pipe: TransactionHandle, task_id: ULID, queue: str, name: str) -> None:
        """Stage a DELETE from dead_letter_queue."""
        task_id_str = str(task_id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _delete(s: AsyncSession) -> None:
            await s.execute(delete(dead_letter_queue).where(dead_letter_queue.c.id == task_id_str))

        pipe.add_op(_delete)

    # ── DeadQueueProtocol: direct writes ───────────────────────────────────

    async def add_to_dlq(self, task: Task, failed_at: dt.datetime) -> None:
        """Add a task to the DLQ directly (saga path)."""
        row = _task_to_dlq_row(task, failed_at)
        async with self._sf() as session:
            async with session.begin():
                exists = await session.execute(
                    select(dead_letter_queue).where(dead_letter_queue.c.id == row["id"])
                )
                if exists.first() is None:
                    await session.execute(insert(dead_letter_queue).values(**row))
                else:
                    non_pk = {k: v for k, v in row.items() if k != "id"}
                    await session.execute(
                        update(dead_letter_queue).where(dead_letter_queue.c.id == row["id"]).values(**non_pk)
                    )

    async def remove_from_dlq(self, task_id: ULID, queue: str, name: str) -> None:
        """Remove a task from the DLQ directly (saga path)."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(dead_letter_queue).where(dead_letter_queue.c.id == str(task_id)))

    # ── DeadQueueProtocol: reads ───────────────────────────────────────────

    async def get_history(self, task_id: str) -> list[dict[str, Any]]:
        """Return the per-attempt error history from the stored task blob."""
        async with self._sf() as session:
            result = await session.execute(select(dead_letter_queue).where(dead_letter_queue.c.id == task_id))
            row = result.first()
            if row is None:
                return []
            task = _dlq_row_to_task(row)
            return [{"attempt": i, "error": e} for i, e in enumerate(task.errors)]

    async def get_by_ids(self, task_ids: list[str]) -> list[Task]:
        """Fetch DLQ entries by explicit task ID list."""
        if not task_ids:
            return []
        async with self._sf() as session:
            result = await session.execute(
                select(dead_letter_queue).where(dead_letter_queue.c.id.in_(task_ids))
            )
            return [_dlq_row_to_task(row) for row in result.all()]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
    ) -> list[Task]:
        """Fetch DLQ entries matching the given filter criteria."""
        stmt = select(dead_letter_queue).order_by(dead_letter_queue.c.failed_at.desc())
        if queue is not None:
            stmt = stmt.where(dead_letter_queue.c.queue == queue)
        if task_name is not None:
            stmt = stmt.where(dead_letter_queue.c.name == task_name)
        if task_version is not None:
            stmt = stmt.where(dead_letter_queue.c.version == task_version)
        stmt = stmt.limit(limit)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [_dlq_row_to_task(row) for row in result.all()]

    async def remove_many(self, task_ids: list[str]) -> None:
        """Remove multiple DLQ entries in a single transaction."""
        if not task_ids:
            return
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(dead_letter_queue).where(dead_letter_queue.c.id.in_(task_ids)))

    async def clean(self, earlier_than: dt.datetime) -> None:
        """Delete DLQ entries older than ``earlier_than``."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(dead_letter_queue).where(dead_letter_queue.c.failed_at < earlier_than)
                )


# ---------------------------------------------------------------------------
# SQLTaskScheduler — scheduled-task queue in the task_schedule table
# ---------------------------------------------------------------------------


def _task_schedule_ensure_utc_nn(d: dt.datetime) -> dt.datetime:
    """SQLite returns naive datetimes; re-attach UTC (non-nullable variant)."""
    return d if d.tzinfo is not None else d.replace(tzinfo=dt.UTC)


def _task_to_schedule_row(task: Task, run_at: dt.datetime) -> dict[str, Any]:
    return {
        "task_id": str(task.id),
        "queue": task.queue,
        "task_data": task.model_dump_json(),
        "run_at": run_at,
    }


def _schedule_row_to_task(row: Any) -> Task:
    data: dict[str, Any] = json.loads(row.task_data)
    return Task.model_validate(data)


class SQLTaskScheduler:
    """
    Scheduled-task queue backed by SQLAlchemy (``task_schedule`` table).

    Implements ``AtomicTaskSchedulerProtocol`` using ``SQLTransactionBatch``.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        get_all_queues: Callable[[], Any],
        dsn: str = "",
    ) -> None:
        self._sf = session_factory
        self._get_all_queues = get_all_queues
        self._dsn = dsn

    @property
    def backend_key(self) -> str:
        """Stable identifier shared by all SQL adapters pointing to the same database."""
        return self._dsn or str(id(self._sf))

    def pipeline(self, transaction: bool = True) -> SQLTransactionBatch:
        """Return a new SQLTransactionBatch for staged atomic writes."""
        return SQLTransactionBatch(self._sf)

    # ── AtomicTaskSchedulerProtocol: staged writes ─────────────────────────

    def stage_add(self, pipe: TransactionHandle, task: Task, run_at: dt.datetime) -> None:
        """Stage an INSERT-or-UPDATE in task_schedule."""
        row = _task_to_schedule_row(task, run_at)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _upsert(s: AsyncSession) -> None:
            result = await s.execute(
                update(task_schedule)
                .where(task_schedule.c.task_id == row["task_id"])
                .values(queue=row["queue"], task_data=row["task_data"], run_at=row["run_at"])
            )
            if result.rowcount == 0:  # type: ignore[attr-defined]
                await s.execute(insert(task_schedule).values(**row))

        pipe.add_op(_upsert)

    def stage_remove(self, pipe: TransactionHandle, task_id: ULID, queue: str) -> None:
        """Stage a DELETE from task_schedule."""
        task_id_str = str(task_id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _delete(s: AsyncSession) -> None:
            await s.execute(delete(task_schedule).where(task_schedule.c.task_id == task_id_str))

        pipe.add_op(_delete)

    # ── TaskSchedulerProtocol: direct writes ───────────────────────────────

    async def add(self, task: Task, run_at: dt.datetime) -> None:
        """Add a task to the schedule directly (saga path)."""
        row = _task_to_schedule_row(task, run_at)
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    update(task_schedule)
                    .where(task_schedule.c.task_id == row["task_id"])
                    .values(queue=row["queue"], task_data=row["task_data"], run_at=row["run_at"])
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await session.execute(insert(task_schedule).values(**row))

    async def remove(self, task_id: ULID, queue: str) -> None:
        """Remove a task from the schedule directly (saga path)."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(task_schedule).where(task_schedule.c.task_id == str(task_id)))

    # ── TaskSchedulerProtocol: reads ───────────────────────────────────────

    async def get_run_at(self, task_id: ULID) -> dt.datetime | None:
        """Return the scheduled run_at for a task, or None if not scheduled."""
        async with self._sf() as session:
            result = await session.execute(
                select(task_schedule.c.run_at).where(task_schedule.c.task_id == str(task_id))
            )
            row = result.first()
            return _task_schedule_ensure_utc_nn(row.run_at) if row is not None else None

    async def next_due(self, queues: list[str] | None) -> Task | None:
        """Acquire and return the single earliest due task, or None."""
        results = await self.next_due_bulk(1, queues=queues)
        return results[0][0] if results else None

    async def next_due_bulk(self, n: int, queues: list[str] | None = None) -> list[tuple[Task, dt.datetime]]:
        """
        Atomically acquire and return up to n due tasks with their scheduled run_at.

        Tasks are removed from the schedule as they are acquired.
        """
        if queues is not None and not queues:
            return []
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            async with session.begin():
                stmt = select(task_schedule).where(task_schedule.c.run_at <= now)
                if queues is not None:
                    stmt = stmt.where(task_schedule.c.queue.in_(queues))
                elif queues is None:
                    all_q = await self._get_all_queues()
                    if all_q:
                        stmt = stmt.where(task_schedule.c.queue.in_(all_q))
                stmt = stmt.order_by(task_schedule.c.run_at).limit(n)
                result = await session.execute(stmt)
                rows = result.all()
                if not rows:
                    return []
                task_ids = [row.task_id for row in rows]
                await session.execute(delete(task_schedule).where(task_schedule.c.task_id.in_(task_ids)))
        return [(_schedule_row_to_task(row), _task_schedule_ensure_utc_nn(row.run_at)) for row in rows]

    async def get_by_filter(
        self,
        queue: str | None = None,
        task_name: str | None = None,
        task_version: int | None = None,
        limit: int = 100,
        start_after: str | None = None,
    ) -> list[tuple[Task, dt.datetime]]:
        """Fetch scheduled entries matching the given filter criteria."""
        stmt = select(task_schedule).order_by(task_schedule.c.run_at)
        if queue is not None:
            stmt = stmt.where(task_schedule.c.queue == queue)
        if start_after is not None:
            cursor = str(ULID.from_str(start_after))
            stmt = stmt.where(task_schedule.c.task_id > cursor)
        stmt = stmt.limit(limit * 10)  # over-fetch to allow name/version filtering
        async with self._sf() as session:
            result = await session.execute(stmt)
            rows = result.all()
        results: list[tuple[Task, dt.datetime]] = []
        for row in rows:
            if len(results) >= limit:
                break
            task = _schedule_row_to_task(row)
            if task_name is not None and task.name != task_name:
                continue
            if task_version is not None and task.version != task_version:
                continue
            results.append((task, _task_schedule_ensure_utc_nn(row.run_at)))
        return results


# ---------------------------------------------------------------------------
# SQLCronDAGScheduler — cron DAG entries in cron_dag_entries / cron_dag_active_runs
# ---------------------------------------------------------------------------


def _ensure_utc_cron(d: dt.datetime) -> dt.datetime:
    """SQLite returns naive datetimes; re-attach UTC (non-nullable variant)."""
    return d if d.tzinfo is not None else d.replace(tzinfo=dt.UTC)


def _ensure_utc_cron_opt(d: dt.datetime | None) -> dt.datetime | None:
    if d is None:
        return None
    return d if d.tzinfo is not None else d.replace(tzinfo=dt.UTC)


def _cron_row_to_entry(row: Any) -> CronDAGEntry:
    import json as _json

    return CronDAGEntry(
        id=ULID.from_str(row.id),
        name=row.name,
        cron_expr=row.cron_expr,
        dag_spec=DAGTaskSpec.model_validate(_json.loads(row.dag_spec)),
        enabled=bool(row.enabled),
        concurrency_policy=ConcurrencyPolicy(row.concurrency_policy),
        created_at=_ensure_utc_cron(row.created_at),
    )


def _cron_entry_values(entry: CronDAGEntry, next_run_at: dt.datetime) -> dict[str, Any]:
    return {
        "id": str(entry.id),
        "name": entry.name,
        "cron_expr": entry.cron_expr,
        "dag_spec": entry.dag_spec.model_dump_json(),
        "enabled": entry.enabled,
        "concurrency_policy": entry.concurrency_policy.value,
        "created_at": entry.created_at,
        "next_run_at": next_run_at,
    }


class SQLCronDAGScheduler:
    """
    Cron DAG scheduler backed by SQLAlchemy.

    Implements ``AtomicCronDAGSchedulerProtocol`` using ``SQLTransactionBatch``.
    When the cron scheduler and task-state adapter share the same ``session_factory``
    (same ``dsn``), ``StateManager`` can fold cron and task-state ops into one transaction.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        dsn: str = "",
    ) -> None:
        self._sf = session_factory
        self._dsn = dsn

    @property
    def backend_key(self) -> str:
        """Stable identifier shared by all SQL adapters pointing to the same database."""
        return self._dsn or str(id(self._sf))

    @property
    def _use_for_update(self) -> bool:
        return "sqlite" not in self._dsn

    def pipeline(self, transaction: bool = True) -> SQLTransactionBatch:
        """Return a new SQLTransactionBatch for staged atomic writes."""
        return SQLTransactionBatch(self._sf)

    # ── AtomicCronDAGSchedulerProtocol: staged writes ─────────────────────────

    def stage_reschedule(self, pipe: TransactionHandle, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Stage an UPDATE of next_run_at onto the batch."""
        cron_id_str = str(cron_id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _update(s: AsyncSession) -> None:
            await s.execute(
                update(cron_dag_entries)
                .where(cron_dag_entries.c.id == cron_id_str)
                .values(next_run_at=next_run_at)
            )

        pipe.add_op(_update)

    def stage_set_active_run(
        self,
        pipe: TransactionHandle,
        cron_id: ULID,
        task_id: ULID,
        ttl: int = 86400,
        nx: bool = False,
    ) -> None:
        """
        Stage an INSERT-or-UPSERT into cron_dag_active_runs onto the batch.

        When ``nx=True``, uses INSERT with IntegrityError suppression — the NX guard
        against duplicate dispatches.  The result is not checked after execute();
        callers read ``get_active_run()`` before building the pipeline to determine
        whether to proceed, mirroring the Redis NX pattern.
        """
        cron_id_str = str(cron_id)
        task_id_str = str(task_id)
        expires_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=ttl)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        if nx:

            async def _insert_nx(s: AsyncSession) -> None:
                async with s.begin_nested() as sp:
                    try:
                        await s.execute(
                            insert(cron_dag_active_runs).values(
                                cron_id=cron_id_str, task_id=task_id_str, expires_at=expires_at
                            )
                        )
                    except IntegrityError:
                        await sp.rollback()

            pipe.add_op(_insert_nx)
        else:

            async def _upsert(s: AsyncSession) -> None:
                result = await s.execute(
                    update(cron_dag_active_runs)
                    .where(cron_dag_active_runs.c.cron_id == cron_id_str)
                    .values(task_id=task_id_str, expires_at=expires_at)
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await s.execute(
                        insert(cron_dag_active_runs).values(
                            cron_id=cron_id_str, task_id=task_id_str, expires_at=expires_at
                        )
                    )

            pipe.add_op(_upsert)

    def stage_clear_active_run(self, pipe: TransactionHandle, cron_id: ULID) -> None:
        """Stage a DELETE from cron_dag_active_runs onto the batch."""
        cron_id_str = str(cron_id)
        assert isinstance(pipe, SQLTransactionBatch)  # noqa: S101

        async def _delete(s: AsyncSession) -> None:
            await s.execute(delete(cron_dag_active_runs).where(cron_dag_active_runs.c.cron_id == cron_id_str))

        pipe.add_op(_delete)

    # ── CronDAGSchedulerProtocol: direct writes ────────────────────────────────

    async def add(self, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        """Upsert a cron entry and set its next_run_at."""
        row = _cron_entry_values(entry, next_run_at)
        async with self._sf() as session:
            async with session.begin():
                result = await session.execute(
                    update(cron_dag_entries)
                    .where(cron_dag_entries.c.id == row["id"])
                    .values({k: v for k, v in row.items() if k != "id"})
                )
                if result.rowcount == 0:  # type: ignore[attr-defined]
                    await session.execute(insert(cron_dag_entries).values(**row))

    async def remove(self, cron_id: ULID) -> None:
        """Delete a cron entry (cascades to active runs)."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(delete(cron_dag_entries).where(cron_dag_entries.c.id == str(cron_id)))

    # ── CronDAGSchedulerProtocol: reads ───────────────────────────────────────

    async def get(self, cron_id: ULID) -> CronDAGEntry | None:
        """Fetch a single CronDAGEntry by id, or None if missing."""
        async with self._sf() as session:
            result = await session.execute(
                select(cron_dag_entries).where(cron_dag_entries.c.id == str(cron_id))
            )
            row = result.first()
            return _cron_row_to_entry(row) if row is not None else None

    async def next_due_bulk(self, n: int) -> list[tuple[CronDAGEntry, dt.datetime]]:
        """
        Atomically acquire and return up to n due cron entries.

        Marks acquired entries by setting ``next_run_at = NULL``; caller is responsible
        for rescheduling via ``reschedule()`` or ``stage_reschedule()`` after dispatching.
        """
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            async with session.begin():
                stmt = (
                    select(cron_dag_entries)
                    .where(
                        cron_dag_entries.c.next_run_at.is_not(None),
                        cron_dag_entries.c.next_run_at <= now,
                    )
                    .order_by(cron_dag_entries.c.next_run_at)
                    .limit(n)
                )
                if self._use_for_update:
                    stmt = stmt.with_for_update(skip_locked=True)
                result = await session.execute(stmt)
                rows = result.all()
                if not rows:
                    return []
                ids = [row.id for row in rows]
                await session.execute(
                    update(cron_dag_entries).where(cron_dag_entries.c.id.in_(ids)).values(next_run_at=None)
                )
        return [(_cron_row_to_entry(row), _ensure_utc_cron(row.next_run_at)) for row in rows]

    async def reschedule(self, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Update next_run_at for a cron entry (direct, non-staged version)."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    update(cron_dag_entries)
                    .where(cron_dag_entries.c.id == str(cron_id))
                    .values(next_run_at=next_run_at)
                )

    async def get_active_run(self, cron_id: ULID) -> str | None:
        """Return the active root task ID string, or None if no active (non-expired) run."""
        now = dt.datetime.now(dt.UTC)
        async with self._sf() as session:
            result = await session.execute(
                select(cron_dag_active_runs.c.task_id).where(
                    cron_dag_active_runs.c.cron_id == str(cron_id),
                    cron_dag_active_runs.c.expires_at > now,
                )
            )
            row = result.first()
            return row.task_id if row is not None else None

    async def set_active_run(self, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False) -> bool:
        """
        Set the active-run marker for a cron entry.

        When ``nx=True``, inserts only if no row exists (returns False on conflict).
        When ``nx=False``, upserts unconditionally (always returns True).
        """
        cron_id_str = str(cron_id)
        task_id_str = str(task_id)
        expires_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=ttl)
        async with self._sf() as session:
            async with session.begin():
                if nx:
                    async with session.begin_nested() as sp:
                        try:
                            await session.execute(
                                insert(cron_dag_active_runs).values(
                                    cron_id=cron_id_str,
                                    task_id=task_id_str,
                                    expires_at=expires_at,
                                )
                            )
                            return True
                        except IntegrityError:
                            await sp.rollback()
                            return False
                else:
                    result = await session.execute(
                        update(cron_dag_active_runs)
                        .where(cron_dag_active_runs.c.cron_id == cron_id_str)
                        .values(task_id=task_id_str, expires_at=expires_at)
                    )
                    if result.rowcount == 0:  # type: ignore[attr-defined]
                        await session.execute(
                            insert(cron_dag_active_runs).values(
                                cron_id=cron_id_str,
                                task_id=task_id_str,
                                expires_at=expires_at,
                            )
                        )
                    return True

    async def clear_active_run(self, cron_id: ULID) -> None:
        """Delete the active-run marker for a cron entry."""
        async with self._sf() as session:
            async with session.begin():
                await session.execute(
                    delete(cron_dag_active_runs).where(cron_dag_active_runs.c.cron_id == str(cron_id))
                )

    async def get_next_run_at(self, cron_id: ULID) -> dt.datetime | None:
        """Return the next scheduled run time, or None if not scheduled (acquired or missing)."""
        async with self._sf() as session:
            result = await session.execute(
                select(cron_dag_entries.c.next_run_at).where(cron_dag_entries.c.id == str(cron_id))
            )
            row = result.first()
            if row is None:
                return None
            return _ensure_utc_cron_opt(row.next_run_at)

    async def list(
        self, offset: int = 0, limit: int = 50
    ) -> tuple[list[tuple[CronDAGEntry, dt.datetime]], int]:
        """
        Return a page of scheduled cron entries ordered by next_run_at ascending.

        Excludes acquired entries (next_run_at IS NULL).  Returns (entries, total_scheduled).
        """
        async with self._sf() as session:
            count_result = await session.execute(
                select(cron_dag_entries).where(cron_dag_entries.c.next_run_at.is_not(None))
            )
            total = len(count_result.all())

            result = await session.execute(
                select(cron_dag_entries)
                .where(cron_dag_entries.c.next_run_at.is_not(None))
                .order_by(cron_dag_entries.c.next_run_at)
                .offset(offset)
                .limit(limit)
            )
            rows = result.all()

        return [(_cron_row_to_entry(row), _ensure_utc_cron(row.next_run_at)) for row in rows], total
