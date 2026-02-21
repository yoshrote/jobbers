import datetime as dt

import pytest

from jobbers.models.task import Task
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.models.task_status import TaskStatus

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
FUTURE = dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc)


def make_task(task_id: str = "01JQC31AJP7TSA9X8AEP64XG08", queue: str = "default") -> Task:
    return Task(id=task_id, name="test_task", version=1, queue=queue, status=TaskStatus.SCHEDULED)


@pytest.fixture
def scheduler(tmp_path):
    with TaskScheduler(tmp_path / "schedule.db") as s:
        yield s


# ── basic CRUD ────────────────────────────────────────────────────────────────

def test_next_due_empty_db(scheduler):
    assert scheduler.next_due(["default"]) is None


def test_add_and_next_due(scheduler):
    task = make_task()
    scheduler.add(task, PAST)
    result = scheduler.next_due(["default"])
    assert result is not None
    assert result.id == task.id


def test_remove_prevents_next_due(scheduler):
    task = make_task()
    scheduler.add(task, PAST)
    scheduler.remove(task.id)
    assert scheduler.next_due(["default"]) is None


def test_add_replaces_existing_row(scheduler):
    """INSERT OR REPLACE lets us re-schedule an already-acquired row."""
    task = make_task()
    scheduler.add(task, PAST)
    scheduler.next_due(["default"])  # marks acquired = 1
    scheduler.add(task, PAST)        # re-insert resets acquired to 0
    assert scheduler.next_due(["default"]) is not None


# ── run_at filtering ──────────────────────────────────────────────────────────

def test_future_task_not_returned(scheduler):
    scheduler.add(make_task(), FUTURE)
    assert scheduler.next_due(["default"]) is None


def test_past_task_is_returned(scheduler):
    scheduler.add(make_task(), PAST)
    assert scheduler.next_due(["default"]) is not None


# ── queue filtering ───────────────────────────────────────────────────────────

def test_next_due_empty_queue_list(scheduler):
    scheduler.add(make_task(), PAST)
    assert scheduler.next_due([]) is None


def test_next_due_wrong_queue_returns_none(scheduler):
    scheduler.add(make_task(queue="other"), PAST)
    assert scheduler.next_due(["default"]) is None


def test_next_due_correct_queue_returned(scheduler):
    scheduler.add(make_task(queue="other"), PAST)
    result = scheduler.next_due(["other"])
    assert result is not None


def test_next_due_multi_queue_filter(scheduler):
    scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", queue="alpha"), PAST)
    scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", queue="beta"), PAST)
    result = scheduler.next_due(["alpha", "beta"])
    assert result is not None


# ── acquire semantics ─────────────────────────────────────────────────────────

def test_next_due_acquires_once(scheduler):
    """A second call should not return the same task."""
    scheduler.add(make_task(), PAST)
    first = scheduler.next_due(["default"])
    second = scheduler.next_due(["default"])
    assert first is not None
    assert second is None


def test_same_run_at_ordered_by_task_id(scheduler):
    """When run_at is equal the tie is broken by task_id ascending."""
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    # Insert in reverse order to prove it's task_id, not insertion order
    scheduler.add(t2, PAST)
    scheduler.add(t1, PAST)
    first = scheduler.next_due(["default"])
    second = scheduler.next_due(["default"])
    third = scheduler.next_due(["default"])
    assert first is not None
    assert second is not None
    assert first.id == t1.id   # smaller task_id wins
    assert second.id == t2.id
    assert third is None


# ── ordering ──────────────────────────────────────────────────────────────────

def test_next_due_returns_earliest_run_at(scheduler):
    earlier = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    later = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    earlier_time = dt.datetime(2020, 3, 1, tzinfo=dt.timezone.utc)
    later_time = dt.datetime(2020, 6, 1, tzinfo=dt.timezone.utc)
    # Insert in reverse order to prove sorting is by run_at, not insertion order
    scheduler.add(later, later_time)
    scheduler.add(earlier, earlier_time)
    result = scheduler.next_due(["default"])
    assert result is not None
    assert result.id == earlier.id


# ── queues=None (all-queues mode) ─────────────────────────────────────────────

def test_next_due_no_args_returns_due_task(scheduler):
    """next_due() with no arguments matches any queue."""
    scheduler.add(make_task(queue="alpha"), PAST)
    assert scheduler.next_due() is not None


def test_next_due_none_returns_due_task(scheduler):
    """next_due(None) matches any queue."""
    scheduler.add(make_task(queue="beta"), PAST)
    assert scheduler.next_due(None) is not None


def test_next_due_none_skips_future_task(scheduler):
    """next_due(None) does not return a task whose run_at is in the future."""
    scheduler.add(make_task(), FUTURE)
    assert scheduler.next_due(None) is None


def test_next_due_none_acquires_once(scheduler):
    """next_due(None) will not return the same task twice."""
    scheduler.add(make_task(), PAST)
    assert scheduler.next_due(None) is not None
    assert scheduler.next_due(None) is None


def test_next_due_none_returns_across_multiple_queues(scheduler):
    """next_due(None) returns tasks from all queues."""
    scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", queue="q1"), PAST)
    scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", queue="q2"), PAST)
    first = scheduler.next_due(None)
    second = scheduler.next_due(None)
    assert first is not None
    assert second is not None
    assert {first.id, second.id} == {
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01").id,
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02").id,
    }
    assert scheduler.next_due(None) is None


# ── next_due_bulk ──────────────────────────────────────────────────────────────

def test_next_due_bulk_returns_task_run_at_tuples(scheduler):
    """next_due_bulk returns (Task, datetime) pairs."""
    scheduler.add(make_task(), PAST)
    results = scheduler.next_due_bulk(10)
    assert len(results) == 1
    task, run_at = results[0]
    assert task.id == make_task().id
    assert isinstance(run_at, dt.datetime)
    assert run_at.tzinfo is not None


def test_next_due_bulk_run_at_matches_scheduled_time(scheduler):
    """The run_at in each tuple equals the datetime passed to add()."""
    scheduled_time = dt.datetime(2020, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
    scheduler.add(make_task(), scheduled_time)
    (_, run_at), = scheduler.next_due_bulk(10)
    assert run_at == scheduled_time


def test_next_due_bulk_respects_limit(scheduler):
    """next_due_bulk returns at most n tasks."""
    scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01"), PAST)
    scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02"), PAST)
    scheduler.add(make_task(task_id="01JQC31AJP7TSA9X8AEP64XG03"), PAST)
    assert len(scheduler.next_due_bulk(2)) == 2


def test_next_due_bulk_empty_queue_list_returns_empty(scheduler):
    """next_due_bulk(queues=[]) returns [] without touching the DB."""
    scheduler.add(make_task(), PAST)
    assert scheduler.next_due_bulk(10, queues=[]) == []


def test_next_due_bulk_acquires_so_second_call_returns_empty(scheduler):
    """Tasks returned by next_due_bulk are marked acquired and not returned again."""
    scheduler.add(make_task(), PAST)
    assert len(scheduler.next_due_bulk(10)) == 1
    assert scheduler.next_due_bulk(10) == []
