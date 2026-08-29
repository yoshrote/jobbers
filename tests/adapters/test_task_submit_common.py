"""
Contract tests for TaskSubmitProtocol implementations.

Runs against all three backends via the ``task_adapter`` fixture (yields a
``(state, submit)`` pair).  Assertions use ``state`` only to verify that the
submit operation left the expected data; the operations under test are always
called on ``submit``.
"""

import datetime as dt

import pytest
from ulid import ULID

from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")


def make_task(
    task_id: ULID = ULID1,
    name: str = "my_task",
    version: int = 1,
    queue: str = "default",
    status: TaskStatus = TaskStatus.SUBMITTED,
    submitted_at: dt.datetime = FROZEN_TIME,
) -> Task:
    task = Task(id=task_id, name=name, version=version, queue=queue, status=status)
    task.submitted_at = submitted_at
    return task


# ── submit_task ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_task(task_adapter):
    """submit_task enqueues the task and persists its data."""
    state, submit = task_adapter
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    task.set_status(TaskStatus.SUBMITTED)
    await submit.submit_task(task)

    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.name == "Test Task"
    assert saved.status == TaskStatus.SUBMITTED
    assert saved.submitted_at == task.submitted_at
    assert await state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_submit_task_twice_updates_only(task_adapter):
    """Submitting the same task ID twice updates the data without duplicating the queue entry."""
    state, submit = task_adapter
    task = Task(id=ULID1, name="Initial Task", status="unsubmitted")
    task.set_status(TaskStatus.SUBMITTED)
    await submit.submit_task(task)

    updated = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await submit.submit_task(updated)

    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.name == "Updated Task"
    assert saved.status == TaskStatus.COMPLETED
    assert saved.submitted_at == task.submitted_at
    assert len(await state.get_all_tasks(TaskPagination(queue=task.queue))) == 1


# ── enqueue ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enqueue_adds_task_to_queue_after_blob_already_saved(task_adapter):
    """
    enqueue() must add the task to its queue even though the blob already exists.

    This is the saga two-step: task_state.save_task() persists the blob first, then
    task_submit.enqueue() adds queue membership. submit_task() is unsuitable for this
    second step on Redis backends — its Lua script only ZADDs when the blob does NOT
    already exist, so calling it after save_task() would silently skip the enqueue.
    """
    state, submit = task_adapter
    task = make_task()

    # Blob already persisted, exactly as it would be after task_state.save_task().
    await state.save_task(task)

    await submit.enqueue(task)

    popped = await submit.get_next_task(queues={"default"}, pop_timeout=1)
    assert popped is not None
    assert popped.id == ULID1


@pytest.mark.asyncio
async def test_enqueue_does_not_duplicate_task_blob_write(task_adapter):
    """enqueue() only touches queue membership — it must not revert fields set after save_task()."""
    state, submit = task_adapter
    task = make_task()
    await state.save_task(task)

    # Mutate the in-memory task after the blob was saved; enqueue() must not re-persist
    # this (stale, in-memory-only) snapshot over the already-saved blob.
    task.results = {"should": "not be persisted by enqueue"}
    await submit.enqueue(task)

    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.results == {}


@pytest.mark.asyncio
async def test_enqueue_registers_dag_run(task_adapter):
    """enqueue() registers the task's DAG run, matching submit_task()'s behavior."""
    state, submit = task_adapter
    dag_run_id = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
    task = make_task()
    task.dag_run_id = dag_run_id
    await state.save_task(task)

    await submit.enqueue(task)

    run = await state.get_dag_run(dag_run_id)
    assert run is not None
    assert ULID1 in run.task_ids


@pytest.mark.asyncio
async def test_submit_task_dag_run_shared_by_multiple_tasks(task_adapter):
    """Two tasks submitted into the same dag_run_id both register as pending without conflict."""
    state, submit = task_adapter
    dag_run_id = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
    task_a = make_task(ULID1)
    task_b = make_task(ULID2)
    task_a.dag_run_id = dag_run_id
    task_b.dag_run_id = dag_run_id

    await submit.submit_task(task_a)
    # Second task registers against the already-existing dag_runs row.
    await submit.submit_task(task_b)

    run = await state.get_dag_run(dag_run_id)
    assert run is not None
    assert set(run.task_ids) == {ULID1, ULID2}


@pytest.mark.asyncio
async def test_enqueue_dag_run_reregistration_is_idempotent(task_adapter):
    """
    Re-enqueuing the same task into the same dag_run_id (e.g. a retry re-dispatch) does not error.

    Exercises the IntegrityError-guarded nested-transaction insert in _register_dag_run's
    dag_run_pending upsert -- the same (dag_run_id, task_id) pair is registered twice.
    """
    state, submit = task_adapter
    dag_run_id = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
    task = make_task()
    task.dag_run_id = dag_run_id
    await state.save_task(task)

    for _ in range(2):
        await submit.enqueue(task)

    run = await state.get_dag_run(dag_run_id)
    assert run is not None
    assert run.task_ids == [ULID1]


# ── get_next_task ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_task_returns_submitted_task(task_adapter):
    """get_next_task pops and returns the next available task, leaving the queue empty."""
    state, submit = task_adapter
    await submit.submit_task(make_task())

    result = await submit.get_next_task(queues={"default"}, pop_timeout=1)
    assert result is not None
    assert result.id == ULID1

    result2 = await submit.get_next_task(queues={"default"}, pop_timeout=1)
    assert result2 is None


# ── submit_rate_limited_task ──────────────────────────────────────────────────


def _rate_limited_queue(name: str = "default") -> QueueConfig:
    return QueueConfig(name=name, rate_numerator=5, rate_denominator=1, rate_period=RatePeriod.MINUTE)


@pytest.mark.asyncio
async def test_submit_rate_limited_task_enqueues_and_stores(task_adapter):
    """submit_rate_limited_task enqueues the task and stores it when the rate limit is not exceeded."""
    state, submit = task_adapter
    task = make_task()
    task.set_status(TaskStatus.SUBMITTED)
    queue_config = _rate_limited_queue()

    result = await submit.submit_rate_limited_task(task, queue_config)

    assert result is True
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_submit_rate_limited_task_rejects_when_limit_reached(task_adapter):
    """submit_rate_limited_task returns False when the per-period rate limit is exhausted."""
    state, submit = task_adapter
    queue_config = QueueConfig(
        name="default", rate_numerator=1, rate_denominator=1, rate_period=RatePeriod.MINUTE
    )
    first = make_task(task_id=ULID1)
    first.set_status(TaskStatus.SUBMITTED)
    await submit.submit_rate_limited_task(first, queue_config)

    second = make_task(task_id=ULID2, submitted_at=FROZEN_TIME)
    second.set_status(TaskStatus.SUBMITTED)
    result = await submit.submit_rate_limited_task(second, queue_config)

    assert result is False


@pytest.mark.asyncio
async def test_submit_rate_limited_task_resubmit_same_id_is_idempotent(task_adapter):
    """Resubmitting the same task ID inside the window doesn't consume a second capacity slot."""
    state, submit = task_adapter
    queue_config = QueueConfig(
        name="default", rate_numerator=1, rate_denominator=1, rate_period=RatePeriod.MINUTE
    )
    # make_task() already builds a SUBMITTED task with the given submitted_at directly
    # via the constructor -- do not call set_status() afterward, since set_status()
    # unconditionally resets submitted_at to real "now" for the SUBMITTED case.
    now = dt.datetime.now(dt.UTC)
    task = make_task(task_id=ULID1, submitted_at=now)
    assert await submit.submit_rate_limited_task(task, queue_config) is True

    resubmit = make_task(task_id=ULID1, submitted_at=now)
    assert await submit.submit_rate_limited_task(resubmit, queue_config) is True

    # The single slot is still held by ULID1 -- a different ID must be rejected.
    other = make_task(task_id=ULID2, submitted_at=now)
    assert await submit.submit_rate_limited_task(other, queue_config) is False


@pytest.mark.asyncio
async def test_submit_rate_limited_task_reused_id_after_window_expiry_respects_current_capacity(
    task_adapter,
):
    """
    A reused task ID whose rate-limit window has expired must respect current capacity.

    A task ID whose rate-limit window entry has expired must go through the full
    capacity check again, even though its blob still exists in storage from a much
    earlier submission (e.g. it ran to completion long ago and is now being
    resubmitted with the same ID).

    Before the fix, the Redis Lua script used blob existence -- which persists
    indefinitely -- as its "already tracked, skip the check" signal instead of the
    rate limiter's own sliding window, letting a stale/reused ID bypass the
    capacity check forever.
    """
    state, submit = task_adapter
    queue_config = QueueConfig(
        name="default", rate_numerator=1, rate_denominator=1, rate_period=RatePeriod.MINUTE
    )

    # Accepted "long ago" -- FROZEN_TIME is far enough in the past that its rate-limit
    # window entry (scored at FROZEN_TIME) will have aged out by the time real
    # wall-clock "now" is used to prune the window on the next call. make_task()
    # already builds a SUBMITTED task with this submitted_at directly via the
    # constructor -- do not call set_status() afterward, since set_status()
    # unconditionally resets submitted_at to real "now" for the SUBMITTED case.
    old_task = make_task(task_id=ULID1, submitted_at=FROZEN_TIME)
    assert await submit.submit_rate_limited_task(old_task, queue_config) is True

    # A different task now consumes the queue's only current capacity slot.
    now = dt.datetime.now(dt.UTC)
    occupier = make_task(task_id=ULID2, submitted_at=now)
    assert await submit.submit_rate_limited_task(occupier, queue_config) is True

    # Resubmitting ULID1 now must go through the full capacity check and be
    # rejected, since ULID2 already holds the queue's only slot.
    reused = make_task(task_id=ULID1, submitted_at=now)
    result = await submit.submit_rate_limited_task(reused, queue_config)

    assert result is False


# ── clean_rate_limiter ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_rate_limiter_removes_expired_entries(task_adapter):
    """clean_rate_limiter removes rate-limit entries older than the configured age."""
    state, submit = task_adapter
    queue_config = _rate_limited_queue()
    task = make_task()
    task.set_status(TaskStatus.SUBMITTED)
    await submit.submit_rate_limited_task(task, queue_config)

    far_future = FROZEN_TIME + dt.timedelta(hours=2)
    await submit.clean_rate_limiter({"default"}, now=far_future, rate_limit_age=dt.timedelta(hours=1))

    tasks_in_queue = await state.get_all_tasks(TaskPagination(queue="default"))
    assert len(tasks_in_queue) == 1
