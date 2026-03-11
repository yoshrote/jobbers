"""Tests for TaskScheduler."""
import datetime as dt

import pytest
import pytest_asyncio

from jobbers.adapters import JsonTaskAdapter
from jobbers.models.task import Task
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_status import TaskStatus

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
FUTURE = dt.datetime(2099, 1, 1, tzinfo=dt.UTC)


def make_task(task_id: str = "01JQC31AJP7TSA9X8AEP64XG08", queue: str = "default") -> Task:
    return Task(id=task_id, name="test_task", version=1, queue=queue, status=TaskStatus.SCHEDULED)


@pytest_asyncio.fixture
async def scheduler(redis):
    yield TaskScheduler(redis, JsonTaskAdapter(redis))


# ── basic CRUD ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_empty(scheduler):
    assert await scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_add_and_next_due(scheduler, redis):
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    result = await scheduler.next_due(["default"])
    assert result is not None
    assert result.id == task.id


@pytest.mark.asyncio
async def test_remove_prevents_next_due(scheduler, redis):
    task = make_task()
    # Store task data so next_due can load it
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    await scheduler.remove(task.id)
    assert await scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_add_replaces_existing(scheduler, redis):
    """Re-adding a task that was already acquired makes it available again."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    await scheduler.next_due(["default"])    # acquires (removes from sorted set)
    await scheduler.add(task, PAST)          # re-adds to sorted set
    assert await scheduler.next_due(["default"]) is not None


# ── run_at filtering ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_future_task_not_returned(scheduler):
    await scheduler.add(make_task(), FUTURE)
    assert await scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_past_task_is_returned(scheduler, redis):
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
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
async def test_next_due_correct_queue_returned(scheduler, redis):
    task = make_task(queue="other")
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    result = await scheduler.next_due(["other"])
    assert result is not None


@pytest.mark.asyncio
async def test_next_due_multi_queue_filter(scheduler, redis):
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", queue="alpha")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", queue="beta")
    for t in (t1, t2):
        await redis.json().set(f"task:{t.id}", "$", t.to_dict())
        await scheduler.add(t, PAST)
    result = await scheduler.next_due(["alpha", "beta"])
    assert result is not None


# ── acquire semantics ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_acquires_once(scheduler, redis):
    """A second call should not return the same task."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    first = await scheduler.next_due(["default"])
    second = await scheduler.next_due(["default"])
    assert first is not None
    assert second is None


# ── ordering ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_returns_earliest_run_at(scheduler, redis):
    earlier = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    later = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    earlier_time = dt.datetime(2020, 3, 1, tzinfo=dt.UTC)
    later_time = dt.datetime(2020, 6, 1, tzinfo=dt.UTC)
    for t in (earlier, later):
        await redis.json().set(f"task:{t.id}", "$", t.to_dict())
    # Insert in reverse order to prove sorting is by run_at, not insertion order
    await scheduler.add(later, later_time)
    await scheduler.add(earlier, earlier_time)
    result = await scheduler.next_due(["default"])
    assert result is not None
    assert result.id == earlier.id


# ── queues=None (all-queues mode) ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_none_returns_due_task(scheduler, redis):
    """next_due(None) matches any queue when all-queues set is populated."""
    task = make_task(queue="alpha")
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await redis.sadd("all-queues", "alpha")
    await scheduler.add(task, PAST)
    assert await scheduler.next_due(None) is not None


@pytest.mark.asyncio
async def test_next_due_none_skips_future_task(scheduler, redis):
    """next_due(None) does not return a task whose run_at is in the future."""
    await redis.sadd("all-queues", "default")
    await scheduler.add(make_task(), FUTURE)
    assert await scheduler.next_due(None) is None


@pytest.mark.asyncio
async def test_next_due_none_acquires_once(scheduler, redis):
    """next_due(None) will not return the same task twice."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await redis.sadd("all-queues", "default")
    await scheduler.add(task, PAST)
    assert await scheduler.next_due(None) is not None
    assert await scheduler.next_due(None) is None


# ── next_due_bulk ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_next_due_bulk_returns_task_run_at_tuples(scheduler, redis):
    """next_due_bulk returns (Task, datetime) pairs."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    results = await scheduler.next_due_bulk(10, queues=["default"])
    assert len(results) == 1
    t, run_at = results[0]
    assert t.id == task.id
    assert isinstance(run_at, dt.datetime)
    assert run_at.tzinfo is not None


@pytest.mark.asyncio
async def test_next_due_bulk_run_at_matches_scheduled_time(scheduler, redis):
    """The run_at in each tuple equals the datetime passed to add()."""
    scheduled_time = dt.datetime(2020, 6, 15, 12, 0, 0, tzinfo=dt.UTC)
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, scheduled_time)
    (_, run_at), = await scheduler.next_due_bulk(10, queues=["default"])
    # Compare at second precision (timestamps are floats)
    assert abs((run_at - scheduled_time).total_seconds()) < 0.001


@pytest.mark.asyncio
async def test_next_due_bulk_respects_limit(scheduler, redis):
    """next_due_bulk returns at most n tasks."""
    tasks = [
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01"),
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02"),
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG03"),
    ]
    for t in tasks:
        await redis.json().set(f"task:{t.id}", "$", t.to_dict())
        await scheduler.add(t, PAST)
    assert len(await scheduler.next_due_bulk(2, queues=["default"])) == 2


@pytest.mark.asyncio
async def test_next_due_bulk_empty_queue_list_returns_empty(scheduler, redis):
    """next_due_bulk(queues=[]) returns [] without touching Redis."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    assert await scheduler.next_due_bulk(10, queues=[]) == []


@pytest.mark.asyncio
async def test_next_due_bulk_acquires_so_second_call_returns_empty(scheduler, redis):
    """Tasks returned by next_due_bulk are removed and not returned again."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    assert len(await scheduler.next_due_bulk(10, queues=["default"])) == 1
    assert await scheduler.next_due_bulk(10, queues=["default"]) == []


# ── get_by_filter ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_by_filter_returns_scheduled_tasks(scheduler, redis):
    """get_by_filter returns tasks currently in the schedule for a queue."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    results = await scheduler.get_by_filter(queue="default")
    assert len(results) == 1
    assert results[0].id == task.id


@pytest.mark.asyncio
async def test_get_by_filter_by_task_name(scheduler, redis):
    """get_by_filter filters by task_name."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    assert len(await scheduler.get_by_filter(queue="default", task_name="test_task")) == 1
    assert len(await scheduler.get_by_filter(queue="default", task_name="other")) == 0


@pytest.mark.asyncio
async def test_get_by_filter_by_task_version(scheduler, redis):
    """get_by_filter filters by task_version."""
    task = make_task()
    await redis.json().set(f"task:{task.id}", "$", task.to_dict())
    await scheduler.add(task, PAST)
    assert len(await scheduler.get_by_filter(queue="default", task_version=1)) == 1
    assert len(await scheduler.get_by_filter(queue="default", task_version=99)) == 0


@pytest.mark.asyncio
async def test_get_by_filter_cursor_pagination(scheduler, redis):
    """start_after excludes tasks whose id <= cursor."""
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    for t in (t1, t2):
        await redis.json().set(f"task:{t.id}", "$", t.to_dict())
        await scheduler.add(t, PAST)
    results = await scheduler.get_by_filter(queue="default", start_after=str(t1.id))
    assert len(results) == 1
    assert results[0].id == t2.id
