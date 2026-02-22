import datetime as dt

import pytest
from ulid import ULID

from jobbers.models.dead_queue import DeadQueue
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus

FAILED_AT = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
ULID1 = ULID()
ULID2 = ULID()
ULID3 = ULID()


def make_task(
    task_id: ULID = ULID1,
    name: str = "my_task",
    version: int = 1,
    queue: str = "default",
    retry_attempt: int = 0,
) -> Task:
    return Task(
        id=task_id,
        name=name,
        version=version,
        queue=queue,
        status=TaskStatus.FAILED,
        retry_attempt=retry_attempt,
        errors=["something went wrong"],
    )


@pytest.fixture
def dq(tmp_path):
    with DeadQueue(tmp_path / "dead_queue.db") as dead_queue:
        yield dead_queue


# ── add / get_by_ids ──────────────────────────────────────────────────────────

def test_add_and_get_by_id(dq):
    task = make_task()
    dq.add(task, FAILED_AT)

    results = dq.get_by_ids([str(ULID1)])
    assert len(results) == 1
    assert results[0].id == task.id
    assert results[0].name == task.name
    assert results[0].queue == task.queue


def test_get_by_ids_multiple(dq):
    task1 = make_task(task_id=ULID1)
    task2 = make_task(task_id=ULID2)
    dq.add(task1, FAILED_AT)
    dq.add(task2, FAILED_AT)

    results = dq.get_by_ids([str(ULID1), str(ULID2)])
    result_ids = {r.id for r in results}
    assert result_ids == {ULID1, ULID2}


def test_get_by_ids_returns_only_matching(dq):
    task1 = make_task(task_id=ULID1)
    task2 = make_task(task_id=ULID2)
    dq.add(task1, FAILED_AT)
    dq.add(task2, FAILED_AT)

    results = dq.get_by_ids([str(ULID1)])
    assert len(results) == 1
    assert results[0].id == ULID1


def test_get_by_ids_nonexistent_returns_empty(dq):
    results = dq.get_by_ids([str(ULID3)])
    assert results == []


def test_get_by_ids_empty_list_returns_empty(dq):
    dq.add(make_task(), FAILED_AT)
    results = dq.get_by_ids([])
    assert results == []


# ── get_by_filter ─────────────────────────────────────────────────────────────

def test_get_by_filter_queue(dq):
    dq.add(make_task(task_id=ULID1, queue="q1"), FAILED_AT)
    dq.add(make_task(task_id=ULID2, queue="q2"), FAILED_AT)

    results = dq.get_by_filter(queue="q1")
    assert len(results) == 1
    assert results[0].queue == "q1"


def test_get_by_filter_task_name(dq):
    dq.add(make_task(task_id=ULID1, name="task_a"), FAILED_AT)
    dq.add(make_task(task_id=ULID2, name="task_b"), FAILED_AT)

    results = dq.get_by_filter(task_name="task_a")
    assert len(results) == 1
    assert results[0].name == "task_a"


def test_get_by_filter_task_version(dq):
    dq.add(make_task(task_id=ULID1, version=1), FAILED_AT)
    dq.add(make_task(task_id=ULID2, version=2), FAILED_AT)

    results = dq.get_by_filter(task_version=2)
    assert len(results) == 1
    assert results[0].version == 2


def test_get_by_filter_version_zero(dq):
    """task_version=0 should be treated as a real filter value, not falsy."""
    dq.add(make_task(task_id=ULID1, version=0), FAILED_AT)
    dq.add(make_task(task_id=ULID2, version=1), FAILED_AT)

    results = dq.get_by_filter(task_version=0)
    assert len(results) == 1
    assert results[0].version == 0


def test_get_by_filter_combined(dq):
    dq.add(make_task(task_id=ULID1, name="task_a", version=1, queue="q1"), FAILED_AT)
    dq.add(make_task(task_id=ULID2, name="task_a", version=2, queue="q1"), FAILED_AT)
    dq.add(make_task(task_id=ULID3, name="task_b", version=1, queue="q1"), FAILED_AT)

    results = dq.get_by_filter(queue="q1", task_name="task_a", task_version=1)
    assert len(results) == 1
    assert results[0].id == ULID1


def test_get_by_filter_no_criteria_returns_all(dq):
    dq.add(make_task(task_id=ULID1), FAILED_AT)
    dq.add(make_task(task_id=ULID2), FAILED_AT)

    results = dq.get_by_filter()
    assert len(results) == 2


def test_get_by_filter_limit_respected(dq):
    dq.add(make_task(task_id=ULID1), FAILED_AT)
    dq.add(make_task(task_id=ULID2), FAILED_AT)
    dq.add(make_task(task_id=ULID3), FAILED_AT)

    results = dq.get_by_filter(limit=2)
    assert len(results) == 2


def test_get_by_filter_no_match_returns_empty(dq):
    dq.add(make_task(task_id=ULID1, queue="q1"), FAILED_AT)

    results = dq.get_by_filter(queue="q_nonexistent")
    assert results == []


# ── remove ────────────────────────────────────────────────────────────────────

def test_remove_deletes_entry(dq):
    dq.add(make_task(), FAILED_AT)
    dq.remove(str(ULID1))

    assert dq.get_by_ids([str(ULID1)]) == []


def test_remove_leaves_other_entries_intact(dq):
    dq.add(make_task(task_id=ULID1), FAILED_AT)
    dq.add(make_task(task_id=ULID2), FAILED_AT)
    dq.remove(str(ULID1))

    assert dq.get_by_ids([str(ULID1)]) == []
    assert len(dq.get_by_ids([str(ULID2)])) == 1


def test_remove_nonexistent_is_silent(dq):
    """Removing a task that was never added should not raise."""
    dq.remove(str(ULID3))  # no exception


# ── get_history ───────────────────────────────────────────────────────────────

def test_get_history_returns_errors_from_task_blob(dq):
    task = make_task()
    task.errors = ["first error", "second error", "third error"]
    dq.add(task, FAILED_AT)

    history = dq.get_history(str(ULID1))
    assert len(history) == 3
    assert history[0] == {"attempt": 0, "error": "first error"}
    assert history[1] == {"attempt": 1, "error": "second error"}
    assert history[2] == {"attempt": 2, "error": "third error"}


def test_get_history_empty_when_no_errors(dq):
    task = make_task()
    task.errors = []
    dq.add(task, FAILED_AT)

    history = dq.get_history(str(ULID1))
    assert history == []


def test_get_history_empty_for_unknown_task(dq):
    assert dq.get_history(str(ULID3)) == []


def test_get_history_isolated_by_task_id(dq):
    task_a = make_task(task_id=ULID1)
    task_a.errors = ["error a"]
    task_b = make_task(task_id=ULID2)
    task_b.errors = ["error b1", "error b2"]
    dq.add(task_a, FAILED_AT)
    dq.add(task_b, FAILED_AT)

    assert len(dq.get_history(str(ULID1))) == 1
    assert len(dq.get_history(str(ULID2))) == 2
