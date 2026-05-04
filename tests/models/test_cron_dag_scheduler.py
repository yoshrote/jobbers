"""Tests for CronDAGScheduler."""

import datetime as dt

import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.cron_dag_scheduler import ConcurrencyStager, CronDAGScheduler
from jobbers.models.dag import DAGTaskSpec

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
    yield CronDAGScheduler(redis)


async def add_entry(s: CronDAGScheduler, entry: CronDAGEntry, next_run_at: dt.datetime) -> None:
    pipe = s.data_store.pipeline(transaction=True)
    s.stage_add(pipe, entry, next_run_at)
    await pipe.execute()


# ── ConcurrencyStager ─────────────────────────────────────────────────────────


def test_concurrency_stager_calls_stage_fn():
    """stage_active_run delegates to the injected _stage_fn."""
    calls: list[tuple] = []
    pipe_sentinel = object()
    task_id = ULID()
    stager = ConcurrencyStager(skipped=False, _stage_fn=lambda p, t: calls.append((p, t)))
    stager.stage_active_run(pipe_sentinel, task_id)
    assert calls == [(pipe_sentinel, task_id)]


# ── stage_add / get ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_returns_none_for_missing_entry(scheduler):
    assert await scheduler.get(ULID()) is None


@pytest.mark.asyncio
async def test_add_and_get_round_trips(scheduler):
    entry = make_entry()
    await add_entry(scheduler, entry, FUTURE)
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
    await add_entry(scheduler, entry, FUTURE)
    fetched = await scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.concurrency_policy == ConcurrencyPolicy.SKIP_IF_RUNNING


@pytest.mark.asyncio
async def test_add_and_get_preserves_disabled(scheduler):
    entry = make_entry(enabled=False)
    await add_entry(scheduler, entry, FUTURE)
    fetched = await scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.enabled is False


@pytest.mark.asyncio
async def test_add_and_get_preserves_dag_spec(scheduler):
    spec = DAGTaskSpec(name="special_task", queue="myqueue", version=2)
    entry = make_entry()
    entry = CronDAGEntry(**{**entry.model_dump(), "dag_spec": spec})
    await add_entry(scheduler, entry, FUTURE)
    fetched = await scheduler.get(entry.id)
    assert fetched is not None
    assert fetched.dag_spec.name == "special_task"
    assert fetched.dag_spec.queue == "myqueue"
    assert fetched.dag_spec.version == 2


# ── stage_remove ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_clears_hash_and_sorted_set(scheduler):
    entry = make_entry()
    await add_entry(scheduler, entry, PAST)
    pipe = scheduler.data_store.pipeline(transaction=True)
    scheduler.stage_remove(pipe, entry.id)
    await pipe.execute()
    assert await scheduler.get(entry.id) is None
    assert await scheduler.next_due_bulk(10) == []


# ── next_due_bulk ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_next_due_bulk_empty(scheduler):
    assert await scheduler.next_due_bulk(10) == []


@pytest.mark.asyncio
async def test_next_due_bulk_returns_due_entry(scheduler):
    entry = make_entry()
    await add_entry(scheduler, entry, PAST)
    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1
    fetched, run_at = results[0]
    assert fetched.id == entry.id
    assert run_at.tzinfo is not None


@pytest.mark.asyncio
async def test_next_due_bulk_run_at_matches_scheduled_time(scheduler):
    scheduled = dt.datetime(2020, 6, 15, 12, 0, 0, tzinfo=dt.UTC)
    entry = make_entry()
    await add_entry(scheduler, entry, scheduled)
    ((_, run_at),) = await scheduler.next_due_bulk(10)
    assert abs((run_at - scheduled).total_seconds()) < 0.001


@pytest.mark.asyncio
async def test_next_due_bulk_future_not_returned(scheduler):
    await add_entry(scheduler, make_entry(), FUTURE)
    assert await scheduler.next_due_bulk(10) == []


@pytest.mark.asyncio
async def test_next_due_bulk_acquires_atomically(scheduler):
    """A second call returns nothing because the first call removed the entry."""
    entry = make_entry()
    await add_entry(scheduler, entry, PAST)
    first = await scheduler.next_due_bulk(10)
    second = await scheduler.next_due_bulk(10)
    assert len(first) == 1
    assert second == []


@pytest.mark.asyncio
async def test_next_due_bulk_respects_limit(scheduler):
    entries = [make_entry(name=f"cron_{i}") for i in range(3)]
    for e in entries:
        await add_entry(scheduler, e, PAST)
    assert len(await scheduler.next_due_bulk(2)) == 2


@pytest.mark.asyncio
async def test_next_due_bulk_mixed_past_and_future(scheduler):
    past_entry = make_entry(name="past")
    future_entry = make_entry(name="future")
    await add_entry(scheduler, past_entry, PAST)
    await add_entry(scheduler, future_entry, FUTURE)
    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1
    assert results[0][0].id == past_entry.id


# ── stage_reschedule ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reschedule_makes_entry_available_again(scheduler):
    entry = make_entry()
    await add_entry(scheduler, entry, PAST)
    await scheduler.next_due_bulk(10)  # acquires (removes from sorted set)

    pipe = scheduler.data_store.pipeline(transaction=True)
    scheduler.stage_reschedule(pipe, entry.id, PAST)
    await pipe.execute()

    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1
    assert results[0][0].id == entry.id


@pytest.mark.asyncio
async def test_reschedule_to_future_not_immediately_due(scheduler):
    entry = make_entry()
    await add_entry(scheduler, entry, PAST)
    await scheduler.next_due_bulk(10)

    pipe = scheduler.data_store.pipeline(transaction=True)
    scheduler.stage_reschedule(pipe, entry.id, FUTURE)
    await pipe.execute()

    assert await scheduler.next_due_bulk(10) == []


# ── active run tracking ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_run_returns_none_when_unset(scheduler):
    assert await scheduler.get_active_run(ULID()) is None


@pytest.mark.asyncio
async def test_set_and_get_active_run(scheduler):
    cron_id = ULID()
    task_id = ULID()
    pipe = scheduler.data_store.pipeline(transaction=True)
    scheduler.stage_set_active_run(pipe, cron_id, task_id, ttl=3600)
    await pipe.execute()
    result = await scheduler.get_active_run(cron_id)
    assert result == str(task_id)


@pytest.mark.asyncio
async def test_clear_active_run(scheduler):
    cron_id = ULID()
    task_id = ULID()
    pipe = scheduler.data_store.pipeline(transaction=True)
    scheduler.stage_set_active_run(pipe, cron_id, task_id)
    await pipe.execute()

    pipe = scheduler.data_store.pipeline(transaction=True)
    scheduler.stage_clear_active_run(pipe, cron_id)
    await pipe.execute()

    assert await scheduler.get_active_run(cron_id) is None


# ── get_next_run_at ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_run_at_returns_none_when_not_scheduled(scheduler):
    """Returns None for a cron_id not present in the schedule."""
    assert await scheduler.get_next_run_at(ULID()) is None


@pytest.mark.asyncio
async def test_get_next_run_at_returns_scheduled_time(scheduler):
    """Returns the next_run_at timestamp for a scheduled entry."""
    entry = make_entry()
    await add_entry(scheduler, entry, FUTURE)
    result = await scheduler.get_next_run_at(entry.id)
    assert result is not None
    assert abs((result - FUTURE).total_seconds()) < 0.001


# ── list ───────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_empty(scheduler):
    """list() returns empty results and total=0 when no entries exist."""
    entries, total = await scheduler.list()
    assert entries == []
    assert total == 0


@pytest.mark.asyncio
async def test_list_returns_entry_with_next_run_at(scheduler):
    """list() returns (entry, next_run_at) pairs for scheduled entries."""
    entry = make_entry()
    await add_entry(scheduler, entry, FUTURE)
    entries, total = await scheduler.list()
    assert total == 1
    assert len(entries) == 1
    fetched, next_run_at = entries[0]
    assert fetched.id == entry.id
    assert abs((next_run_at - FUTURE).total_seconds()) < 0.001


@pytest.mark.asyncio
async def test_list_pagination_limit(scheduler):
    """list() respects the limit parameter."""
    for i in range(3):
        await add_entry(scheduler, make_entry(name=f"cron_{i}"), FUTURE)
    entries, total = await scheduler.list(offset=0, limit=2)
    assert total == 3
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_list_pagination_offset(scheduler):
    """list() respects the offset parameter for successive pages."""
    cron_entries = [make_entry(name=f"cron_{i}") for i in range(3)]
    for e in cron_entries:
        await add_entry(scheduler, e, FUTURE)
    page1, _ = await scheduler.list(offset=0, limit=2)
    page2, _ = await scheduler.list(offset=2, limit=2)
    assert len(page1) == 2
    assert len(page2) == 1
    ids_p1 = {e.id for e, _ in page1}
    ids_p2 = {e.id for e, _ in page2}
    assert ids_p1.isdisjoint(ids_p2)


@pytest.mark.asyncio
async def test_list_skips_entry_with_missing_hash(scheduler):
    """list() silently skips schedule entries whose hash has been deleted."""
    cron_id = ULID()
    await scheduler.data_store.zadd(scheduler.CRON_SCHEDULE, {bytes(cron_id): FUTURE.timestamp()})
    # Hash deliberately not written — simulates a race with concurrent deletion.
    entries, total = await scheduler.list()
    assert total == 1
    assert entries == []


# ── next_due_bulk: missing hash ────────────────────────────────────────────────


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
