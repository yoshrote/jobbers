"""
CronDAGSchedulerProtocol contract tests — run against all backend implementations.

Fixtures:
- ``cron_dag_scheduler``: parametrized over [redis, sql, static]. Static is pre-seeded
  with one past-due entry; use for read/active-run/list tests.
- ``mutable_cron_dag_scheduler``: parametrized over [redis, sql] only. Use for tests
  that call ``add()`` or ``remove()`` since the static backend raises ReadOnly.
"""

import datetime as dt

import pytest
from ulid import ULID

from jobbers.adapters.static import StaticCronDAGScheduler
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


# ── get ───────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_returns_none_for_missing(mutable_cron_dag_scheduler):
    assert await mutable_cron_dag_scheduler.get(ULID()) is None


# ── add / get round-trip ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_add_and_get_round_trips(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
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
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    fetched = await mutable_cron_dag_scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.concurrency_policy == ConcurrencyPolicy.SKIP_IF_RUNNING


@pytest.mark.asyncio
async def test_add_overwrites_existing_entry(mutable_cron_dag_scheduler):
    entry = make_entry(name="original")
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    updated = CronDAGEntry(
        id=entry.id,
        name="updated",
        cron_expr=entry.cron_expr,
        dag_spec=entry.dag_spec,
    )
    await mutable_cron_dag_scheduler.add(updated, FUTURE)
    fetched = await mutable_cron_dag_scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.name == "updated"


# ── remove ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_deletes_entry(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, PAST)
    await mutable_cron_dag_scheduler.remove(entry.id)
    assert await mutable_cron_dag_scheduler.get(entry.id) is None
    assert await mutable_cron_dag_scheduler.next_due_bulk(10) == []


# ── next_due_bulk ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_next_due_bulk_empty(mutable_cron_dag_scheduler):
    assert await mutable_cron_dag_scheduler.next_due_bulk(10) == []


@pytest.mark.asyncio
async def test_next_due_bulk_returns_due_entry(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, PAST)
    results = await mutable_cron_dag_scheduler.next_due_bulk(10)
    assert len(results) == 1
    fetched, run_at = results[0]
    assert fetched.id == entry.id
    assert run_at.tzinfo is not None


@pytest.mark.asyncio
async def test_next_due_bulk_run_at_matches_scheduled_time(mutable_cron_dag_scheduler):
    scheduled = dt.datetime(2020, 6, 15, 12, 0, 0, tzinfo=dt.UTC)
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, scheduled)
    ((_, run_at),) = await mutable_cron_dag_scheduler.next_due_bulk(10)
    assert abs((run_at - scheduled).total_seconds()) < 0.001


@pytest.mark.asyncio
async def test_next_due_bulk_future_not_returned(mutable_cron_dag_scheduler):
    await mutable_cron_dag_scheduler.add(make_entry(), FUTURE)
    assert await mutable_cron_dag_scheduler.next_due_bulk(10) == []


@pytest.mark.asyncio
async def test_next_due_bulk_acquires_atomically(mutable_cron_dag_scheduler):
    """A second call returns nothing because the first call acquired the entry."""
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, PAST)
    first = await mutable_cron_dag_scheduler.next_due_bulk(10)
    second = await mutable_cron_dag_scheduler.next_due_bulk(10)
    assert len(first) == 1
    assert second == []


@pytest.mark.asyncio
async def test_next_due_bulk_respects_limit(mutable_cron_dag_scheduler):
    for i in range(3):
        await mutable_cron_dag_scheduler.add(make_entry(name=f"cron_{i}"), PAST)
    assert len(await mutable_cron_dag_scheduler.next_due_bulk(2)) == 2


# ── reschedule ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reschedule_makes_entry_available_again(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, PAST)
    await mutable_cron_dag_scheduler.next_due_bulk(10)  # acquires
    await mutable_cron_dag_scheduler.reschedule(entry.id, PAST)
    results = await mutable_cron_dag_scheduler.next_due_bulk(10)
    assert len(results) == 1
    assert results[0][0].id == entry.id


@pytest.mark.asyncio
async def test_reschedule_to_future_not_immediately_due(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, PAST)
    await mutable_cron_dag_scheduler.next_due_bulk(10)
    await mutable_cron_dag_scheduler.reschedule(entry.id, FUTURE)
    assert await mutable_cron_dag_scheduler.next_due_bulk(10) == []


# ── get_next_run_at ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_run_at_returns_scheduled_time(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    result = await mutable_cron_dag_scheduler.get_next_run_at(entry.id)
    assert result is not None
    assert abs((result - FUTURE).total_seconds()) < 0.001


@pytest.mark.asyncio
async def test_get_next_run_at_none_after_acquisition(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, PAST)
    await mutable_cron_dag_scheduler.next_due_bulk(10)
    result = await mutable_cron_dag_scheduler.get_next_run_at(entry.id)
    assert result is None


# ── active run tracking ───────────────────────────────────────────────────────
# These tests use mutable_cron_dag_scheduler so that an entry can be created first,
# satisfying the SQL foreign-key constraint on cron_dag_active_runs.cron_id.


@pytest.mark.asyncio
async def test_set_and_get_active_run(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    task_id = ULID()
    result = await mutable_cron_dag_scheduler.set_active_run(entry.id, task_id, ttl=3600)
    assert result is True
    active = await mutable_cron_dag_scheduler.get_active_run(entry.id)
    assert active == str(task_id)


@pytest.mark.asyncio
async def test_set_active_run_nx_returns_false_when_exists(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    task_id_1 = ULID()
    task_id_2 = ULID()
    await mutable_cron_dag_scheduler.set_active_run(entry.id, task_id_1, ttl=3600)
    result = await mutable_cron_dag_scheduler.set_active_run(entry.id, task_id_2, ttl=3600, nx=True)
    assert result is False
    # Original task_id is still stored
    assert await mutable_cron_dag_scheduler.get_active_run(entry.id) == str(task_id_1)


@pytest.mark.asyncio
async def test_set_active_run_nx_succeeds_when_not_exists(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    task_id = ULID()
    result = await mutable_cron_dag_scheduler.set_active_run(entry.id, task_id, ttl=3600, nx=True)
    assert result is True
    assert await mutable_cron_dag_scheduler.get_active_run(entry.id) == str(task_id)


@pytest.mark.asyncio
async def test_clear_active_run(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    task_id = ULID()
    await mutable_cron_dag_scheduler.set_active_run(entry.id, task_id, ttl=3600)
    await mutable_cron_dag_scheduler.clear_active_run(entry.id)
    assert await mutable_cron_dag_scheduler.get_active_run(entry.id) is None


@pytest.mark.asyncio
async def test_get_active_run_returns_none_when_unset(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    assert await mutable_cron_dag_scheduler.get_active_run(entry.id) is None


# ── list ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_returns_entry(mutable_cron_dag_scheduler):
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, FUTURE)
    entries, total = await mutable_cron_dag_scheduler.list()
    assert total == 1
    assert len(entries) == 1
    fetched, next_run_at = entries[0]
    assert fetched.id == entry.id
    assert abs((next_run_at - FUTURE).total_seconds()) < 0.001


@pytest.mark.asyncio
async def test_list_pagination_limit(mutable_cron_dag_scheduler):
    for i in range(3):
        await mutable_cron_dag_scheduler.add(make_entry(name=f"cron_{i}"), FUTURE)
    entries, total = await mutable_cron_dag_scheduler.list(offset=0, limit=2)
    assert total == 3
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_list_pagination_offset(mutable_cron_dag_scheduler):
    for i in range(3):
        await mutable_cron_dag_scheduler.add(make_entry(name=f"cron_{i}"), FUTURE)
    page1, _ = await mutable_cron_dag_scheduler.list(offset=0, limit=2)
    page2, _ = await mutable_cron_dag_scheduler.list(offset=2, limit=2)
    assert len(page1) == 2
    assert len(page2) == 1
    ids_p1 = {e.id for e, _ in page1}
    ids_p2 = {e.id for e, _ in page2}
    assert ids_p1.isdisjoint(ids_p2)


@pytest.mark.asyncio
async def test_list_excludes_acquired_entries(mutable_cron_dag_scheduler):
    """Entries whose next_run_at was cleared by next_due_bulk must not appear in list()."""
    entry = make_entry()
    await mutable_cron_dag_scheduler.add(entry, PAST)
    await mutable_cron_dag_scheduler.next_due_bulk(10)
    entries, total = await mutable_cron_dag_scheduler.list()
    assert total == 0
    assert entries == []


# ── static backend: read-only enforcement ────────────────────────────────────


@pytest.mark.asyncio
async def test_static_add_raises_readonly_error():
    scheduler = StaticCronDAGScheduler()
    with pytest.raises(RoutingBackendReadOnlyError):
        await scheduler.add(make_entry(), FUTURE)


@pytest.mark.asyncio
async def test_static_remove_raises_readonly_error():
    scheduler = StaticCronDAGScheduler()
    with pytest.raises(RoutingBackendReadOnlyError):
        await scheduler.remove(ULID())


@pytest.mark.asyncio
async def test_static_next_due_bulk_returns_preseeded_entry():
    entry = make_entry()
    scheduler = StaticCronDAGScheduler(entries=[entry], initial_next_run_at={entry.id: PAST})
    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1
    assert results[0][0].id == entry.id


@pytest.mark.asyncio
async def test_static_reschedule_and_next_due_bulk():
    """Reschedule makes a static entry available again after acquisition."""
    entry = make_entry()
    scheduler = StaticCronDAGScheduler(entries=[entry], initial_next_run_at={entry.id: PAST})
    await scheduler.next_due_bulk(10)  # acquire
    await scheduler.reschedule(entry.id, PAST)
    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1


@pytest.mark.asyncio
async def test_static_set_and_get_active_run():
    """Static active-run tracking works without an FK constraint."""
    entry = make_entry()
    scheduler = StaticCronDAGScheduler(entries=[entry])
    task_id = ULID()
    result = await scheduler.set_active_run(entry.id, task_id, ttl=3600)
    assert result is True
    assert await scheduler.get_active_run(entry.id) == str(task_id)


@pytest.mark.asyncio
async def test_static_set_active_run_nx():
    entry = make_entry()
    scheduler = StaticCronDAGScheduler(entries=[entry])
    task_id_1 = ULID()
    task_id_2 = ULID()
    await scheduler.set_active_run(entry.id, task_id_1)
    result = await scheduler.set_active_run(entry.id, task_id_2, nx=True)
    assert result is False
    assert await scheduler.get_active_run(entry.id) == str(task_id_1)
