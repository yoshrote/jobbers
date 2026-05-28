"""SQLAlchemy-backed routing sub-adapters, routing backend, and cron DAG scheduler."""

from __future__ import annotations

import datetime as dt
import json
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError
from ulid import ULID

from jobbers.migrations.schema import (
    cron_dag_active_runs,
    cron_dag_entries,
    queues,
    role_queues,
    roles,
    task_routing,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from jobbers.models.cron_dag import CronDAGEntry
    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task_routing import RoutingConfig


class SQLQueueConfigAdapter:
    """
    Manages queue configuration in a data store via SQLAlchemy async sessions.

    - `roles`: table of named roles, each with a refresh_tag for change detection.
    - `queues`: table of queue configurations (concurrency and rate limiting).
    - `role_queues`: many-to-many mapping of roles to queues.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def get_queues(self, role: str) -> set[str]:
        async with self.session_factory() as session:
            result = await session.execute(select(role_queues.c.queue).where(role_queues.c.role == role))
            return {row[0] for row in result.fetchall()}

    async def save_role(self, role: str, queues_set: set[str]) -> str:
        new_tag = str(ULID())
        async with self.session_factory.begin() as session:
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

    async def get_all_queues(self) -> list[str]:
        async with self.session_factory() as session:
            result = await session.execute(select(queues.c.name).order_by(queues.c.name))
            return [row[0] for row in result.fetchall()]

    async def get_all_roles(self) -> list[str]:
        async with self.session_factory() as session:
            result = await session.execute(select(roles.c.name).order_by(roles.c.name))
            return [row[0] for row in result.fetchall()]

    async def get_queue_config(self, queue: str) -> QueueConfig | None:
        from jobbers.models.queue_config import QueueConfig as QC

        async with self.session_factory() as session:
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
        return QC.from_row(row)

    async def save_queue_config(self, queue_config: QueueConfig) -> None:
        async with self.session_factory.begin() as session:
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
        async with self.session_factory.begin() as session:
            result = await session.execute(
                select(role_queues.c.role).where(role_queues.c.queue == queue_name).distinct()
            )
            affected_roles = [row[0] for row in result.fetchall()]
            await session.execute(delete(queues).where(queues.c.name == queue_name))
            if affected_roles:
                await session.execute(
                    update(roles).where(roles.c.name.in_(affected_roles)).values(refresh_tag=new_tag)
                )

    async def delete_role(self, role: str) -> None:
        """Delete a role (cascades to role_queues). Queue configs are preserved."""
        async with self.session_factory.begin() as session:
            await session.execute(delete(roles).where(roles.c.name == role))

    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]:

        if not queues_set:
            return {}
        async with self.session_factory() as session:
            result = await session.execute(
                select(queues.c.name, queues.c.max_concurrent).where(queues.c.name.in_(list(queues_set)))
            )
            found = {row[0]: row[1] for row in result.fetchall()}
        return {q: found.get(q) for q in queues_set}

    async def get_refresh_tag(self, role: str) -> ULID:
        """Return the current refresh tag for a role, creating one if needed."""
        async with self.session_factory() as session:
            result = await session.execute(select(roles.c.refresh_tag).where(roles.c.name == role))
        tag_str: str | None = result.scalar()
        if tag_str:
            existing_tag: ULID = ULID.from_str(tag_str)
            return existing_tag

        init_tag = ULID()
        try:
            async with self.session_factory.begin() as session:
                await session.execute(insert(roles).values(name=role, refresh_tag=str(init_tag)))
        except IntegrityError:
            pass  # Another process inserted first
        # Re-read in case another process won the race
        async with self.session_factory() as session:
            result = await session.execute(select(roles.c.refresh_tag).where(roles.c.name == role))
        tag_str = result.scalar()
        return ULID.from_str(tag_str) if tag_str else init_tag

    async def bump_refresh_tag(self, role: str) -> str:
        """Generate a new ULID tag for *role* and write it to SQL. Returns the new tag string."""
        new_tag = str(ULID())
        async with self.session_factory.begin() as session:
            await session.execute(update(roles).where(roles.c.name == role).values(refresh_tag=new_tag))
        return new_tag

    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]:
        """Bump refresh_tag for every role that contains *queue_name*. Returns affected role names."""
        new_tag = str(ULID())
        async with self.session_factory.begin() as session:
            result = await session.execute(
                select(role_queues.c.role).where(role_queues.c.queue == queue_name).distinct()
            )
            affected_roles = [row[0] for row in result.fetchall()]
            if affected_roles:
                await session.execute(
                    update(roles).where(roles.c.name.in_(affected_roles)).values(refresh_tag=new_tag)
                )
        return affected_roles


class SQLTaskRoutingConfigAdapter:
    """Manages task routing configuration in the SQL data store."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None:
        from jobbers.models.task_routing import RoutingConfig as RC

        async with self.session_factory() as session:
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
        return RC.from_row(row)

    async def save_routing_config(self, config: RoutingConfig) -> None:
        """Create or replace the routing config for a task type."""
        queues_json = json.dumps(config.queues)
        weights_json = json.dumps(config.weights) if config.weights is not None else None
        async with self.session_factory.begin() as session:
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
        async with self.session_factory.begin() as session:
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


class SQLCronDAGScheduler:
    """CronDAGSchedulerProtocol backed by SQLAlchemy (shares the SQL routing session factory)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _row_to_entry(row: object) -> CronDAGEntry:
        from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
        from jobbers.models.dag import DAGTaskSpec

        return CronDAGEntry(
            id=ULID.from_str(row.id),  # type: ignore[attr-defined]
            name=row.name,  # type: ignore[attr-defined]
            cron_expr=row.cron_expr,  # type: ignore[attr-defined]
            dag_spec=DAGTaskSpec.model_validate(json.loads(row.dag_spec)),  # type: ignore[attr-defined]
            enabled=bool(row.enabled),  # type: ignore[attr-defined]
            concurrency_policy=ConcurrencyPolicy(row.concurrency_policy),  # type: ignore[attr-defined]
            created_at=dt.datetime.fromisoformat(row.created_at),  # type: ignore[attr-defined]
        )

    # ── protocol methods ──────────────────────────────────────────────────────

    async def add(self, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
        """Insert or replace a cron entry and set its next_run_at."""
        async with self.session_factory.begin() as session:
            existing = await session.execute(
                select(cron_dag_entries.c.id).where(cron_dag_entries.c.id == str(entry.id))
            )
            values = {
                "name": entry.name,
                "cron_expr": entry.cron_expr,
                "dag_spec": entry.dag_spec.model_dump_json(),
                "enabled": entry.enabled,
                "concurrency_policy": entry.concurrency_policy.value,
                "created_at": entry.created_at.isoformat(),
                "next_run_at": next_run_at.isoformat(),
            }
            if existing.fetchone():
                await session.execute(
                    update(cron_dag_entries).where(cron_dag_entries.c.id == str(entry.id)).values(**values)
                )
            else:
                await session.execute(insert(cron_dag_entries).values(id=str(entry.id), **values))

    async def remove(self, cron_id: ULID) -> None:
        """Delete a cron entry (cascades to active_runs)."""
        async with self.session_factory.begin() as session:
            await session.execute(
                delete(cron_dag_entries).where(cron_dag_entries.c.id == str(cron_id))
            )

    async def get(self, cron_id: ULID) -> CronDAGEntry | None:
        """Fetch a single CronDAGEntry by ID, or None if missing."""
        async with self.session_factory() as session:
            result = await session.execute(
                select(cron_dag_entries).where(cron_dag_entries.c.id == str(cron_id))
            )
            row = result.fetchone()
        return self._row_to_entry(row) if row is not None else None

    async def next_due_bulk(self, n: int) -> list[tuple[CronDAGEntry, dt.datetime]]:
        """
        Atomically acquire up to n due entries by clearing their next_run_at.

        Returns (entry, run_at) pairs.  Caller is responsible for rescheduling.
        """
        now_iso = dt.datetime.now(dt.UTC).isoformat()
        async with self.session_factory.begin() as session:
            result = await session.execute(
                select(cron_dag_entries)
                .where(cron_dag_entries.c.next_run_at.isnot(None))
                .where(cron_dag_entries.c.next_run_at <= now_iso)
                .order_by(cron_dag_entries.c.next_run_at)
                .limit(n)
            )
            rows = result.fetchall()
            if rows:
                ids = [r.id for r in rows]
                await session.execute(
                    update(cron_dag_entries)
                    .where(cron_dag_entries.c.id.in_(ids))
                    .values(next_run_at=None)
                )

        results: list[tuple[CronDAGEntry, dt.datetime]] = []
        for row in rows:
            entry = self._row_to_entry(row)
            run_at = dt.datetime.fromisoformat(row.next_run_at).replace(tzinfo=dt.UTC)
            results.append((entry, run_at))
        return results

    async def reschedule(self, cron_id: ULID, next_run_at: dt.datetime) -> None:
        """Update next_run_at for a cron entry."""
        async with self.session_factory.begin() as session:
            await session.execute(
                update(cron_dag_entries)
                .where(cron_dag_entries.c.id == str(cron_id))
                .values(next_run_at=next_run_at.isoformat())
            )

    async def get_active_run(self, cron_id: ULID) -> str | None:
        """Return the active root task ID string, or None if absent/expired."""
        now_iso = dt.datetime.now(dt.UTC).isoformat()
        async with self.session_factory() as session:
            result = await session.execute(
                select(cron_dag_active_runs.c.task_id)
                .where(cron_dag_active_runs.c.cron_id == str(cron_id))
                .where(cron_dag_active_runs.c.expires_at > now_iso)
            )
            row = result.fetchone()
        return row[0] if row is not None else None

    async def set_active_run(
        self, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False
    ) -> bool:
        """
        Persist the active root task ID with expiry.

        When nx=True, only inserts if no non-expired row exists; returns False on conflict.
        """
        now_iso = dt.datetime.now(dt.UTC).isoformat()
        expires_at = (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=ttl)).isoformat()
        cid = str(cron_id)
        async with self.session_factory.begin() as session:
            # Clean up any expired entry first
            await session.execute(
                delete(cron_dag_active_runs)
                .where(cron_dag_active_runs.c.cron_id == cid)
                .where(cron_dag_active_runs.c.expires_at <= now_iso)
            )
            if nx:
                existing = await session.execute(
                    select(cron_dag_active_runs.c.cron_id).where(
                        cron_dag_active_runs.c.cron_id == cid
                    )
                )
                if existing.fetchone() is not None:
                    return False
            try:
                await session.execute(
                    insert(cron_dag_active_runs).values(
                        cron_id=cid,
                        task_id=str(task_id),
                        expires_at=expires_at,
                    )
                )
            except IntegrityError:
                if nx:
                    return False
                await session.execute(
                    update(cron_dag_active_runs)
                    .where(cron_dag_active_runs.c.cron_id == cid)
                    .values(task_id=str(task_id), expires_at=expires_at)
                )
        return True

    async def clear_active_run(self, cron_id: ULID) -> None:
        """Delete the active run record for a cron entry."""
        async with self.session_factory.begin() as session:
            await session.execute(
                delete(cron_dag_active_runs).where(cron_dag_active_runs.c.cron_id == str(cron_id))
            )

    async def get_next_run_at(self, cron_id: ULID) -> dt.datetime | None:
        """Return the next scheduled run time, or None if not scheduled."""
        async with self.session_factory() as session:
            result = await session.execute(
                select(cron_dag_entries.c.next_run_at).where(cron_dag_entries.c.id == str(cron_id))
            )
            row = result.fetchone()
        if row is None or row[0] is None:
            return None
        return dt.datetime.fromisoformat(row[0]).replace(tzinfo=dt.UTC)

    async def list(
        self, offset: int = 0, limit: int = 50
    ) -> tuple[list[tuple[CronDAGEntry, dt.datetime]], int]:
        """Return a page of scheduled entries ordered by next_run_at ascending."""
        async with self.session_factory() as session:
            count_result = await session.execute(
                select(func.count()).select_from(cron_dag_entries).where(
                    cron_dag_entries.c.next_run_at.isnot(None)
                )
            )
            total: int = count_result.scalar() or 0

            result = await session.execute(
                select(cron_dag_entries)
                .where(cron_dag_entries.c.next_run_at.isnot(None))
                .order_by(cron_dag_entries.c.next_run_at)
                .offset(offset)
                .limit(limit)
            )
            rows = result.fetchall()

        entries: list[tuple[CronDAGEntry, dt.datetime]] = []
        for row in rows:
            entry = self._row_to_entry(row)
            next_run_at = dt.datetime.fromisoformat(row.next_run_at).replace(tzinfo=dt.UTC)
            entries.append((entry, next_run_at))
        return entries, total


class SQLRoutingBackend:
    """RoutingBackendProtocol backed by SQLAlchemy (the original SQL-based implementation)."""

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
