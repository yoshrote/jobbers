"""
Protocol contract tests for CronDAGSchedulerProtocol.

Parametrized over:
- RedisCronDAGScheduler (FakeAsyncRedis) via ``mutable_cron_dag_scheduler`` / ``cron_dag_scheduler``
- SQLCronDAGScheduler (in-memory SQLite) via ``mutable_cron_dag_scheduler`` / ``cron_dag_scheduler``
- StaticCronDAGScheduler (pre-seeded in-memory) via ``cron_dag_scheduler``

Tests that call ``add()`` / ``remove()`` use ``mutable_cron_dag_scheduler`` (redis + sql only).
Tests of ``get_active_run``, ``set_active_run``, ``clear_active_run``, ``get_next_run_at``,
and ``reschedule`` use ``cron_dag_scheduler`` (all three backends).
"""

from __future__ import annotations

import datetime as dt

import pytest
from ulid import ULID

from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGTaskSpec
from jobbers.protocols import RoutingBackendReadOnlyError

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
FUTURE = dt.datetime(2099, 1, 1, tzinfo=dt.UTC)


def make_entry(name: str = "test_cron", cron_expr: str = "0 * * * *", **kwargs) -> CronDAGEntry:
    return CronDAGEntry(
        name=name,
        cron_expr=cron_expr,
        dag_spec=DAGTaskSpec(name="root_task"),
        **kwargs,
    )


async def add_entry(scheduler: object, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
    await scheduler.add(entry, next_run_at)  # type: ignore[attr-defined]


# ── get ───────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_returns_none_for_missing(cron_dag_scheduler):
    """get() returns None for an unknown cron_id on all backends."""
    assert await cron_dag_scheduler.get(ULID()) is None


@pytest.mark.asyncio
async def test_add_and_get_round_trips(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, FUTURE)
    fetched = await mutable_cron_dag_scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.id == entry.id
    assert fetched.name == entry.name
    assert fetched.cron_expr == entry.cron_expr
    assert fetched.enabled is True
    assert fetched.concurrency_policy == ConcurrencyPolicy.ALWAYS


@pytest.mark.asyncio
async def test_add_preserves_concurrency_policy(mutable_cron_dag_scheduler):
    entry = make_entry(concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING)
    await add_entry(mutable_cron_dag_scheduler, entry, FUTURE)
    fetched = await mutable_cron_dag_scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.concurrency_policy == ConcurrencyPolicy.SKIP_IF_RUNNING


@pytest.mark.asyncio
async def test_add_preserves_disabled_flag(mutable_cron_dag_scheduler):
    entry = make_entry(enabled=False)
    await add_entry(mutable_cron_dag_scheduler, entry, FUTURE)
    fetched = await mutable_cron_dag_scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.enabled is False


@pytest.mark.asyncio
async def test_add_overwrites_existing_entry(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, FUTURE)
    updated = CronDAGEntry(
        id=entry.id,
        name="updated_name",
        cron_expr=entry.cron_expr,
        dag_spec=entry.dag_spec,
    )
    await add_entry(mutable_cron_dag_scheduler, updated, FUTURE)
    fetched = await mutable_cron_dag_scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.name == "updated_name"


# ── remove ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_deletes_entry(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, FUTURE)
    await mutable_cron_dag_scheduler.remove(entry.id)
    assert await mutable_cron_dag_scheduler.get(entry.id) is None


@pytest.mark.asyncio
async def test_remove_clears_schedule(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, PAST)
    await mutable_cron_dag_scheduler.remove(entry.id)
    assert await mutable_cron_dag_scheduler.next_due_bulk(10) == []


# ── static backend write restrictions ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_static_add_raises_readonly_error():
    from jobbers.adapters.static import StaticCronDAGScheduler

    scheduler = StaticCronDAGScheduler()
    with pytest.raises(RoutingBackendReadOnlyError):
        await scheduler.add(make_entry(), FUTURE)


@pytest.mark.asyncio
async def test_static_remove_raises_readonly_error():
    from jobbers.adapters.static import StaticCronDAGScheduler

    scheduler = StaticCronDAGScheduler()
    with pytest.raises(RoutingBackendReadOnlyError):
        await scheduler.remove(ULID())


# ── next_due_bulk ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_next_due_bulk_empty(mutable_cron_dag_scheduler):
    assert await mutable_cron_dag_scheduler.next_due_bulk(10) == []


@pytest.mark.asyncio
async def test_next_due_bulk_returns_due_entry(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, PAST)
    results = await mutable_cron_dag_scheduler.next_due_bulk(10)
    assert len(results) == 1
    fetched, run_at = results[0]
    assert fetched.id == entry.id
    assert run_at.tzinfo is not None


@pytest.mark.asyncio
async def test_next_due_bulk_future_not_returned(mutable_cron_dag_scheduler):
    await add_entry(mutable_cron_dag_scheduler, make_entry(), FUTURE)
    assert await mutable_cron_dag_scheduler.next_due_bulk(10) == []


@pytest.mark.asyncio
async def test_next_due_bulk_acquires_atomically(mutable_cron_dag_scheduler):
    """A second call returns nothing because the first already acquired the entry."""
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, PAST)
    first = await mutable_cron_dag_scheduler.next_due_bulk(10)
    second = await mutable_cron_dag_scheduler.next_due_bulk(10)
    assert len(first) == 1
    assert second == []


@pytest.mark.asyncio
async def test_next_due_bulk_respects_limit(mutable_cron_dag_scheduler):
    entries = [make_entry(name=f"cron_{i}") for i in range(3)]
    for e in entries:
        await add_entry(mutable_cron_dag_scheduler, e, PAST)
    assert len(await mutable_cron_dag_scheduler.next_due_bulk(2)) == 2


@pytest.mark.asyncio
async def test_next_due_bulk_run_at_matches_scheduled_time(mutable_cron_dag_scheduler):
    scheduled = dt.datetime(2020, 6, 15, 12, 0, 0, tzinfo=dt.UTC)
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, scheduled)
    ((_, run_at),) = await mutable_cron_dag_scheduler.next_due_bulk(10)
    assert abs((run_at - scheduled).total_seconds()) < 1.0


# ── reschedule ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reschedule_makes_entry_available_again(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, PAST)
    await mutable_cron_dag_scheduler.next_due_bulk(10)  # acquires

    await mutable_cron_dag_scheduler.reschedule(entry.id, PAST)

    results = await mutable_cron_dag_scheduler.next_due_bulk(10)
    assert len(results) == 1
    assert results[0][0].id == entry.id


@pytest.mark.asyncio
async def test_reschedule_to_future_not_immediately_due(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, PAST)
    await mutable_cron_dag_scheduler.next_due_bulk(10)

    await mutable_cron_dag_scheduler.reschedule(entry.id, FUTURE)
    assert await mutable_cron_dag_scheduler.next_due_bulk(10) == []


# ── active run tracking (all backends) ────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_run_returns_none_when_unset(cron_dag_scheduler):
    assert await cron_dag_scheduler.get_active_run(ULID()) is None


@pytest.mark.asyncio
async def test_set_and_get_active_run(cron_dag_scheduler):
    cron_id = ULID()
    task_id = ULID()
    result = await cron_dag_scheduler.set_active_run(cron_id, task_id, ttl=3600)
    assert result is True
    active = await cron_dag_scheduler.get_active_run(cron_id)
    assert active == str(task_id)


@pytest.mark.asyncio
async def test_set_active_run_nx_returns_false_when_exists(cron_dag_scheduler):
    cron_id = ULID()
    task_id_1 = ULID()
    task_id_2 = ULID()
    await cron_dag_scheduler.set_active_run(cron_id, task_id_1, ttl=3600)
    result = await cron_dag_scheduler.set_active_run(cron_id, task_id_2, ttl=3600, nx=True)
    assert result is False
    # Original task_id unchanged
    assert await cron_dag_scheduler.get_active_run(cron_id) == str(task_id_1)


@pytest.mark.asyncio
async def test_set_active_run_nx_succeeds_when_not_exists(cron_dag_scheduler):
    cron_id = ULID()
    task_id = ULID()
    result = await cron_dag_scheduler.set_active_run(cron_id, task_id, ttl=3600, nx=True)
    assert result is True
    assert await cron_dag_scheduler.get_active_run(cron_id) == str(task_id)


@pytest.mark.asyncio
async def test_clear_active_run(cron_dag_scheduler):
    cron_id = ULID()
    task_id = ULID()
    await cron_dag_scheduler.set_active_run(cron_id, task_id, ttl=3600)
    await cron_dag_scheduler.clear_active_run(cron_id)
    assert await cron_dag_scheduler.get_active_run(cron_id) is None


# ── get_next_run_at (all backends) ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_run_at_returns_none_when_not_scheduled(cron_dag_scheduler):
    assert await cron_dag_scheduler.get_next_run_at(ULID()) is None


@pytest.mark.asyncio
async def test_get_next_run_at_returns_time_after_add(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, FUTURE)
    result = await mutable_cron_dag_scheduler.get_next_run_at(entry.id)
    assert result is not None
    assert abs((result - FUTURE).total_seconds()) < 1.0


# ── list (mutable backends — static has pre-seeded data) ──────────────────────


@pytest.mark.asyncio
async def test_list_empty(mutable_cron_dag_scheduler):
    entries, total = await mutable_cron_dag_scheduler.list()
    assert entries == []
    assert total == 0


@pytest.mark.asyncio
async def test_list_returns_entry(mutable_cron_dag_scheduler):
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, FUTURE)
    entries, total = await mutable_cron_dag_scheduler.list()
    assert total == 1
    assert len(entries) == 1
    fetched, next_run_at = entries[0]
    assert fetched.id == entry.id
    assert abs((next_run_at - FUTURE).total_seconds()) < 1.0


@pytest.mark.asyncio
async def test_list_pagination_limit(mutable_cron_dag_scheduler):
    for i in range(3):
        await add_entry(mutable_cron_dag_scheduler, make_entry(name=f"cron_{i}"), FUTURE)
    entries, total = await mutable_cron_dag_scheduler.list(offset=0, limit=2)
    assert total == 3
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_list_pagination_offset(mutable_cron_dag_scheduler):
    for i in range(3):
        await add_entry(mutable_cron_dag_scheduler, make_entry(name=f"cron_{i}"), FUTURE)
    page1, _ = await mutable_cron_dag_scheduler.list(offset=0, limit=2)
    page2, _ = await mutable_cron_dag_scheduler.list(offset=2, limit=2)
    ids_p1 = {e.id for e, _ in page1}
    ids_p2 = {e.id for e, _ in page2}
    assert len(page1) == 2
    assert len(page2) == 1
    assert ids_p1.isdisjoint(ids_p2)


@pytest.mark.asyncio
async def test_list_excludes_acquired_entries(mutable_cron_dag_scheduler):
    """Entries that have been acquired (next_run_at cleared) should not appear in list()."""
    entry = make_entry()
    await add_entry(mutable_cron_dag_scheduler, entry, PAST)
    await mutable_cron_dag_scheduler.next_due_bulk(1)  # acquires, clears next_run_at
    entries, total = await mutable_cron_dag_scheduler.list()
    assert total == 0
    assert entries == []
