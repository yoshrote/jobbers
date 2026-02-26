import datetime as dt

import pytest

from jobbers.models.task import Task
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_status import TaskStatus

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
FUTURE = dt.datetime(2099, 1, 1, tzinfo=dt.UTC)


def make_task(task_id: str = "01JQC31AJP7TSA9X8AEP64XG08", queue: str = "default") -> Task:
    return Task(id=task_id, name="test_task", version=1, queue=queue, status=TaskStatus.SCHEDULED)


@pytest.fixture
def scheduler(tmp_path):
    with TaskScheduler(tmp_path / "schedule.db") as s:
        yield s


# ── basic CRUD ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_empty_db(scheduler):
    assert await scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_add_and_next_due(scheduler):
    task = make_task()
    await scheduler.add(task, PAST)
    result = await scheduler.next_due(["default"])
    assert result is not None
    assert result.id == task.id


@pytest.mark.asyncio
async def test_remove_prevents_next_due(scheduler):
    task = make_task()
    await scheduler.add(task, PAST)
    await scheduler.remove(task.id)
    assert await scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_add_replaces_existing_row(scheduler):
    """INSERT OR REPLACE lets us re-schedule an already-acquired row."""
    task = make_task()
    await scheduler.add(task, PAST)
    await scheduler.next_due(["default"])  # marks acquired = 1
    await scheduler.add(task, PAST)        # re-insert resets acquired to 0
    assert await scheduler.next_due(["default"]) is not None


# ── run_at filtering ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_future_task_not_returned(scheduler):
    await scheduler.add(make_task(), FUTURE)
    assert await scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_past_task_is_returned(scheduler):
    await scheduler.add(make_task(), PAST)
    assert await scheduler.next_due(["default"]) is not None


# ── queue filtering ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_empty_queue_list(scheduler):
    await scheduler.add(make_task(), PAST)
    assert await scheduler.next_due([]) is None


@pytest.mark.asyncio
async def test_next_due_wrong_queue_returns_none(scheduler):
    await scheduler.add(make_task(queue="other"), PAST)
    assert await scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_next_due_correct_queue_returned(scheduler):
    await scheduler.add(make_task(queue="other"), PAST)
    result = await scheduler.next_due(["other"])
    assert result is not None


@pytest.mark.asyncio
async def test_next_due_multi_queue_filter(scheduler):
    await scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", queue="alpha"), PAST)
    await scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", queue="beta"), PAST)
    result = await scheduler.next_due(["alpha", "beta"])
    assert result is not None


# ── acquire semantics ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_acquires_once(scheduler):
    """A second call should not return the same task."""
    await scheduler.add(make_task(), PAST)
    first = await scheduler.next_due(["default"])
    second = await scheduler.next_due(["default"])
    assert first is not None
    assert second is None


@pytest.mark.asyncio
async def test_same_run_at_ordered_by_task_id(scheduler):
    """When run_at is equal the tie is broken by task_id ascending."""
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    # Insert in reverse order to prove it's task_id, not insertion order
    await scheduler.add(t2, PAST)
    await scheduler.add(t1, PAST)
    first = await scheduler.next_due(["default"])
    second = await scheduler.next_due(["default"])
    third = await scheduler.next_due(["default"])
    assert first is not None
    assert second is not None
    assert first.id == t1.id   # smaller task_id wins
    assert second.id == t2.id
    assert third is None


# ── ordering ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_returns_earliest_run_at(scheduler):
    earlier = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    later = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    earlier_time = dt.datetime(2020, 3, 1, tzinfo=dt.UTC)
    later_time = dt.datetime(2020, 6, 1, tzinfo=dt.UTC)
    # Insert in reverse order to prove sorting is by run_at, not insertion order
    await scheduler.add(later, later_time)
    await scheduler.add(earlier, earlier_time)
    result = await scheduler.next_due(["default"])
    assert result is not None
    assert result.id == earlier.id


# ── queues=None (all-queues mode) ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_no_args_returns_due_task(scheduler):
    """next_due() with no arguments matches any queue."""
    await scheduler.add(make_task(queue="alpha"), PAST)
    assert await scheduler.next_due() is not None


@pytest.mark.asyncio
async def test_next_due_none_returns_due_task(scheduler):
    """next_due(None) matches any queue."""
    await scheduler.add(make_task(queue="beta"), PAST)
    assert await scheduler.next_due(None) is not None


@pytest.mark.asyncio
async def test_next_due_none_skips_future_task(scheduler):
    """next_due(None) does not return a task whose run_at is in the future."""
    await scheduler.add(make_task(), FUTURE)
    assert await scheduler.next_due(None) is None


@pytest.mark.asyncio
async def test_next_due_none_acquires_once(scheduler):
    """next_due(None) will not return the same task twice."""
    await scheduler.add(make_task(), PAST)
    assert await scheduler.next_due(None) is not None
    assert await scheduler.next_due(None) is None


@pytest.mark.asyncio
async def test_next_due_none_returns_across_multiple_queues(scheduler):
    """next_due(None) returns tasks from all queues."""
    await scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", queue="q1"), PAST)
    await scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", queue="q2"), PAST)
    first = await scheduler.next_due(None)
    second = await scheduler.next_due(None)
    assert first is not None
    assert second is not None
    assert {first.id, second.id} == {
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01").id,
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02").id,
    }
    assert await scheduler.next_due(None) is None


# ── next_due_bulk ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_bulk_returns_task_run_at_tuples(scheduler):
    """next_due_bulk returns (Task, datetime) pairs."""
    await scheduler.add(make_task(), PAST)
    results = await scheduler.next_due_bulk(10)
    assert len(results) == 1
    task, run_at = results[0]
    assert task.id == make_task().id
    assert isinstance(run_at, dt.datetime)
    assert run_at.tzinfo is not None


@pytest.mark.asyncio
async def test_next_due_bulk_run_at_matches_scheduled_time(scheduler):
    """The run_at in each tuple equals the datetime passed to add()."""
    scheduled_time = dt.datetime(2020, 6, 15, 12, 0, 0, tzinfo=dt.UTC)
    await scheduler.add(make_task(), scheduled_time)
    (_, run_at), = await scheduler.next_due_bulk(10)
    assert run_at == scheduled_time


@pytest.mark.asyncio
async def test_next_due_bulk_respects_limit(scheduler):
    """next_due_bulk returns at most n tasks."""
    await scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01"), PAST)
    await scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02"), PAST)
    await scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG03"), PAST)
    assert len(await scheduler.next_due_bulk(2)) == 2


@pytest.mark.asyncio
async def test_next_due_bulk_empty_queue_list_returns_empty(scheduler):
    """next_due_bulk(queues=[]) returns [] without touching the DB."""
    await scheduler.add(make_task(), PAST)
    assert await scheduler.next_due_bulk(10, queues=[]) == []


@pytest.mark.asyncio
async def test_next_due_bulk_acquires_so_second_call_returns_empty(scheduler):
    """Tasks returned by next_due_bulk are marked acquired and not returned again."""
    await scheduler.add(make_task(), PAST)
    assert len(await scheduler.next_due_bulk(10)) == 1
    assert await scheduler.next_due_bulk(10) == []
