"""
Redis-specific tests for RedisCronDAGScheduler.

Protocol-level contract tests (covering all backends) live in
tests/adapters/test_cron_dag_scheduler_common.py.  This file covers
Redis-specific edge cases that cannot be expressed as protocol contracts:
- Direct sorted-set and hash key inspection
- Missing-hash recovery in next_due_bulk
- ConcurrencyStager internal wiring
"""

import datetime as dt

import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGTaskSpec
from jobbers.schedulers.cron_dag_scheduler import ConcurrencyStager, RedisCronDAGScheduler

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
FUTURE = dt.datetime(2099, 1, 1, tzinfo=dt.UTC)


def make_entry(name: str = "test_cron", cron_expr: str = "0 * * * *", **kwargs) -> CronDAGEntry:
    return CronDAGEntry(
        name=name,
        cron_expr=cron_expr,
        dag_spec=DAGTaskSpec(name="root_task"),
        **kwargs,
    )


@pytest_asyncio.fixture
async def scheduler(redis):
    yield RedisCronDAGScheduler(redis)


# ── ConcurrencyStager ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_concurrency_stager_calls_set_fn():
    """set_active_run delegates to the injected async _set_fn."""
    calls: list[ULID] = []
    task_id = ULID()

    async def capture(t: ULID) -> None:
        calls.append(t)

    stager = ConcurrencyStager(skipped=False, _set_fn=capture)
    await stager.set_active_run(task_id)
    assert calls == [task_id]


# ── add / get round-trips ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_returns_none_for_missing_entry(scheduler):
    assert await scheduler.get(ULID()) is None


@pytest.mark.asyncio
async def test_add_and_get_round_trips(scheduler):
    entry = make_entry()
    await scheduler.add(entry, FUTURE)
    fetched = await scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.id == entry.id
    assert fetched.name == entry.name
    assert fetched.cron_expr == entry.cron_expr
    assert fetched.enabled is True
    assert fetched.concurrency_policy == ConcurrencyPolicy.ALWAYS


@pytest.mark.asyncio
async def test_add_and_get_preserves_concurrency_policy(scheduler):
    entry = make_entry(concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING)
    await scheduler.add(entry, FUTURE)
    fetched = await scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.concurrency_policy == ConcurrencyPolicy.SKIP_IF_RUNNING


@pytest.mark.asyncio
async def test_add_and_get_preserves_disabled(scheduler):
    entry = make_entry(enabled=False)
    await scheduler.add(entry, FUTURE)
    fetched = await scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.enabled is False


@pytest.mark.asyncio
async def test_add_and_get_preserves_dag_spec(scheduler):
    spec = DAGTaskSpec(name="special_task", queue="myqueue", version=2)
    entry = make_entry()
    entry = CronDAGEntry(**{**entry.model_dump(), "dag_spec": spec})
    await scheduler.add(entry, FUTURE)
    fetched = await scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.dag_spec.name == "special_task"
    assert fetched.dag_spec.queue == "myqueue"
    assert fetched.dag_spec.version == 2


# ── remove ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_clears_hash_and_sorted_set(scheduler):
    entry = make_entry()
    await scheduler.add(entry, PAST)
    await scheduler.remove(entry.id)
    assert await scheduler.get(entry.id) is None
    assert await scheduler.next_due_bulk(10) == []


# ── next_due_bulk ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_next_due_bulk_empty(scheduler):
    assert await scheduler.next_due_bulk(10) == []


@pytest.mark.asyncio
async def test_next_due_bulk_returns_due_entry(scheduler):
    entry = make_entry()
    await scheduler.add(entry, PAST)
    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1
    fetched, run_at = results[0]
    assert fetched.id == entry.id
    assert run_at.tzinfo is not None


@pytest.mark.asyncio
async def test_next_due_bulk_run_at_matches_scheduled_time(scheduler):
    scheduled = dt.datetime(2020, 6, 15, 12, 0, 0, tzinfo=dt.UTC)
    entry = make_entry()
    await scheduler.add(entry, scheduled)
    ((_, run_at),) = await scheduler.next_due_bulk(10)
    assert abs((run_at - scheduled).total_seconds()) < 0.001


@pytest.mark.asyncio
async def test_next_due_bulk_future_not_returned(scheduler):
    await scheduler.add(make_entry(), FUTURE)
    assert await scheduler.next_due_bulk(10) == []


@pytest.mark.asyncio
async def test_next_due_bulk_acquires_atomically(scheduler):
    """A second call returns nothing because the first call removed the entry."""
    entry = make_entry()
    await scheduler.add(entry, PAST)
    first = await scheduler.next_due_bulk(10)
    second = await scheduler.next_due_bulk(10)
    assert len(first) == 1
    assert second == []


@pytest.mark.asyncio
async def test_next_due_bulk_respects_limit(scheduler):
    entries = [make_entry(name=f"cron_{i}") for i in range(3)]
    for e in entries:
        await scheduler.add(e, PAST)
    assert len(await scheduler.next_due_bulk(2)) == 2


@pytest.mark.asyncio
async def test_next_due_bulk_mixed_past_and_future(scheduler):
    past_entry = make_entry(name="past")
    future_entry = make_entry(name="future")
    await scheduler.add(past_entry, PAST)
    await scheduler.add(future_entry, FUTURE)
    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1
    assert results[0][0].id == past_entry.id


# ── reschedule ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reschedule_makes_entry_available_again(scheduler):
    entry = make_entry()
    await scheduler.add(entry, PAST)
    await scheduler.next_due_bulk(10)  # acquires

    await scheduler.reschedule(entry.id, PAST)

    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1
    assert results[0][0].id == entry.id


@pytest.mark.asyncio
async def test_reschedule_to_future_not_immediately_due(scheduler):
    entry = make_entry()
    await scheduler.add(entry, PAST)
    await scheduler.next_due_bulk(10)

    await scheduler.reschedule(entry.id, FUTURE)
    assert await scheduler.next_due_bulk(10) == []


# ── active run tracking ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_run_returns_none_when_unset(scheduler):
    assert await scheduler.get_active_run(ULID()) is None


@pytest.mark.asyncio
async def test_set_and_get_active_run(scheduler):
    cron_id = ULID()
    task_id = ULID()
    await scheduler.set_active_run(cron_id, task_id, ttl=3600)
    result = await scheduler.get_active_run(cron_id)
    assert result == str(task_id)


@pytest.mark.asyncio
async def test_clear_active_run(scheduler):
    cron_id = ULID()
    task_id = ULID()
    await scheduler.set_active_run(cron_id, task_id)
    await scheduler.clear_active_run(cron_id)
    assert await scheduler.get_active_run(cron_id) is None


# ── get_next_run_at ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_run_at_returns_none_when_not_scheduled(scheduler):
    assert await scheduler.get_next_run_at(ULID()) is None


@pytest.mark.asyncio
async def test_get_next_run_at_returns_scheduled_time(scheduler):
    entry = make_entry()
    await scheduler.add(entry, FUTURE)
    result = await scheduler.get_next_run_at(entry.id)
    assert result is not None
    assert abs((result - FUTURE).total_seconds()) < 0.001


# ── list ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_empty(scheduler):
    entries, total = await scheduler.list()
    assert entries == []
    assert total == 0


@pytest.mark.asyncio
async def test_list_returns_entry_with_next_run_at(scheduler):
    entry = make_entry()
    await scheduler.add(entry, FUTURE)
    entries, total = await scheduler.list()
    assert total == 1
    assert len(entries) == 1
    fetched, next_run_at = entries[0]
    assert fetched.id == entry.id
    assert abs((next_run_at - FUTURE).total_seconds()) < 0.001


@pytest.mark.asyncio
async def test_list_pagination_limit(scheduler):
    for i in range(3):
        await scheduler.add(make_entry(name=f"cron_{i}"), FUTURE)
    entries, total = await scheduler.list(offset=0, limit=2)
    assert total == 3
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_list_pagination_offset(scheduler):
    cron_entries = [make_entry(name=f"cron_{i}") for i in range(3)]
    for e in cron_entries:
        await scheduler.add(e, FUTURE)
    page1, _ = await scheduler.list(offset=0, limit=2)
    page2, _ = await scheduler.list(offset=2, limit=2)
    assert len(page1) == 2
    assert len(page2) == 1
    ids_p1 = {e.id for e, _ in page1}
    ids_p2 = {e.id for e, _ in page2}
    assert ids_p1.isdisjoint(ids_p2)


@pytest.mark.asyncio
async def test_list_skips_entry_with_missing_hash(scheduler):
    """list() silently skips sorted-set entries whose hash has been deleted — Redis-specific."""
    cron_id = ULID()
    await scheduler.data_store.zadd(scheduler.CRON_SCHEDULE, {bytes(cron_id): FUTURE.timestamp()})
    # Hash deliberately not written — simulates a race with concurrent deletion.
    entries, total = await scheduler.list()
    assert total == 1
    assert entries == []


# ── next_due_bulk: missing hash (Redis-specific) ───────────────────────────────


@pytest.mark.asyncio
async def test_next_due_bulk_missing_hash_is_skipped_and_rescheduled(scheduler):
    """Entry acquired from schedule but with no hash is skipped and re-added with retry delay."""
    cron_id = ULID()
    await scheduler.data_store.zadd(scheduler.CRON_SCHEDULE, {bytes(cron_id): PAST.timestamp()})
    # Hash deliberately not written.

    results = await scheduler.next_due_bulk(10)
    assert results == []

    # Entry should have been re-added to the schedule with a ~60s retry delay.
    score = await scheduler.data_store.zscore(scheduler.CRON_SCHEDULE, bytes(cron_id))
    assert score is not None
