"""
Implementation-specific tests for SQLTaskState.

Tests the atomic pipeline path (stage_* methods via SQLTransactionBatch) and
the ``clean()`` method, verified through the public saga API rather than raw DB
queries.  ``TaskSubmitProtocol`` coverage is provided by the contract tests in
test_task_submit_common.py; SQLTaskSubmit-specific behaviour (submit_task,
get_next_task) is exercised here only as test setup for state operations.
"""

import datetime as dt
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from ulid import ULID

from jobbers.adapters.sql import SQLTaskState, SQLTaskSubmit
from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")
ULID3 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG03")


def make_task(
    task_id: ULID = ULID1,
    name: str = "my_task",
    queue: str = "default",
    status: TaskStatus = TaskStatus.SUBMITTED,
    submitted_at: dt.datetime = FROZEN_TIME,
) -> Task:
    task = Task(id=task_id, name=name, queue=queue, status=status)
    task.submitted_at = submitted_at
    return task


# ── stage_submit_task ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_submit_task_persists_and_retrieves(session_factory):
    """stage_submit_task inside a SQLTransactionBatch persists the task."""
    state = SQLTaskState(session_factory)
    task = make_task()
    pipe = state.pipeline(transaction=True)
    state.stage_submit_task(pipe, task)
    await pipe.execute()
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.id == ULID1
    assert saved.name == "my_task"


@pytest.mark.asyncio
async def test_stage_submit_task_appears_in_queue(session_factory):
    """A task staged via stage_submit_task is returned by get_all_tasks."""
    state = SQLTaskState(session_factory)
    task = make_task()
    pipe = state.pipeline(transaction=True)
    state.stage_submit_task(pipe, task)
    await pipe.execute()
    results = await state.get_all_tasks(TaskPagination(queue="default"))
    assert any(t.id == ULID1 for t in results)


# ── stage_requeue ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_requeue_persists_and_retrieves(session_factory):
    """stage_requeue inside a SQLTransactionBatch persists the task."""
    state = SQLTaskState(session_factory)
    task = make_task()
    pipe = state.pipeline(transaction=True)
    state.stage_requeue(pipe, task)
    await pipe.execute()
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.id == ULID1


# ── stage_remove_from_queue ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_remove_from_queue_removes_from_queue(session_factory):
    """stage_remove_from_queue removes the task from task_queue; tasks record is preserved."""
    state = SQLTaskState(session_factory)
    submit = SQLTaskSubmit(session_factory)
    task = make_task()
    await submit.submit_task(task)
    pipe = state.pipeline(transaction=True)
    state.stage_remove_from_queue(pipe, task)
    await pipe.execute()
    assert await state.task_exists(ULID1)
    assert await submit.get_next_task(queues={"default"}, pop_timeout=1) is None


# ── stage_save ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_save_upserts(session_factory):
    """stage_save called twice with an updated name reflects the latest version."""
    state = SQLTaskState(session_factory)
    task = make_task()
    pipe = state.pipeline(transaction=True)
    state.stage_save(pipe, task)
    await pipe.execute()
    task.name = "updated_name"
    pipe = state.pipeline(transaction=True)
    state.stage_save(pipe, task)
    await pipe.execute()
    saved = await state.get_task(ULID1)
    assert saved is not None
    assert saved.name == "updated_name"


# ── stage_init_fan_in ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage_init_fan_in_creates_members(session_factory):
    """stage_init_fan_in via SQLTransactionBatch → get_fan_in_members returns the set."""
    state = SQLTaskState(session_factory)
    dag_run_id = ULID()
    fan_in_key = "fan-in:sql-test"
    predecessor_ids = {ULID1, ULID2, ULID3}
    pipe = state.pipeline(transaction=True)
    state.stage_init_fan_in(pipe, dag_run_id, fan_in_key, predecessor_ids)
    await pipe.execute()
    assert set(await state.get_fan_in_members(dag_run_id, fan_in_key)) == predecessor_ids


# ── clean ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_max_queue_age_removes_old_tasks(session_factory):
    """clean(max_queue_age) removes tasks submitted at or before the cutoff."""
    state = SQLTaskState(session_factory)
    submit = SQLTaskSubmit(session_factory)
    await submit.submit_task(make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=2)))
    await submit.submit_task(make_task(ULID2, submitted_at=FROZEN_TIME))
    await state.clean(queues={"default"}, now=FROZEN_TIME, max_queue_age=FROZEN_TIME - dt.timedelta(days=1))
    ids = {t.id for t in await state.get_all_tasks(TaskPagination(queue="default"))}
    assert ULID1 not in ids
    assert ULID2 in ids


@pytest.mark.asyncio
async def test_clean_min_queue_age_removes_recent_tasks(session_factory):
    """clean(min_queue_age) removes tasks submitted at or after the floor."""
    state = SQLTaskState(session_factory)
    submit = SQLTaskSubmit(session_factory)
    await submit.submit_task(make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=10)))
    await submit.submit_task(make_task(ULID2, submitted_at=FROZEN_TIME))
    await state.clean(queues={"default"}, now=FROZEN_TIME, min_queue_age=FROZEN_TIME - dt.timedelta(days=1))
    ids = {t.id for t in await state.get_all_tasks(TaskPagination(queue="default"))}
    assert ULID1 in ids
    assert ULID2 not in ids


@pytest.mark.asyncio
async def test_clean_noop_when_no_age_params(session_factory):
    """clean() with no age params is a no-op; all tasks remain."""
    state = SQLTaskState(session_factory)
    submit = SQLTaskSubmit(session_factory)
    await submit.submit_task(make_task())
    await state.clean(queues={"default"}, now=dt.datetime.now(dt.UTC))
    assert await state.task_exists(ULID1)


# ── close_dag_run_task ────────────────────────────────────────────────────────
#
# Decreasing-count/idempotency protocol behaviour is covered once, against all
# three backends, by test_task_state_common.py's test_close_dag_run_task_*
# tests. The test below only pins the SQL-specific locking implementation
# detail (which table the serializing FOR UPDATE targets) that those common
# tests can't see and don't need to.


@pytest.mark.asyncio
async def test_close_dag_run_task_locks_dag_runs_anchor_not_every_pending_row(session_factory):
    """
    close_dag_run_task's serializing lock targets the run's single dag_runs row.

    Regression test: an earlier version of this lock selected FOR UPDATE over every
    dag_run_pending row in the run (unfiltered by task_id), which serializes every
    concurrent sibling completion in the run instead of just the read-then-write for
    one task -- a severe throughput cost for large fan-outs on Postgres. Locking the
    run's one dag_runs row gives the same serialization guarantee without a lock
    that grows with the run's size. This can't be observed behaviourally under
    SQLite's single-connection test fixture, so it's pinned by inspecting the
    compiled lock statement instead.
    """
    state = SQLTaskState(session_factory)
    submit = SQLTaskSubmit(session_factory)
    dag_run_id = ULID()
    task = make_task(ULID1, submitted_at=FROZEN_TIME)
    task.dag_run_id = dag_run_id
    await submit.submit_task(task)

    original_execute = AsyncSession.execute
    captured_statements: list[str] = []

    async def _capturing_execute(self, statement, *args, **kwargs):
        captured_statements.append(str(statement))
        return await original_execute(self, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", _capturing_execute):
        await state.close_dag_run_task(dag_run_id, ULID1)

    lock_statement = captured_statements[0]
    assert "FROM dag_runs" in lock_statement
    assert "dag_run_pending" not in lock_statement


# ── transaction atomicity ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_multiple_stages_commit_atomically(session_factory):
    """All ops staged in one pipeline are committed together."""
    state = SQLTaskState(session_factory)
    t1 = make_task(ULID1, name="task_a")
    t2 = make_task(ULID2, name="task_b")
    pipe = state.pipeline(transaction=True)
    state.stage_submit_task(pipe, t1)
    state.stage_submit_task(pipe, t2)
    await pipe.execute()
    ids = {t.id for t in await state.get_all_tasks(TaskPagination(queue="default"))}
    assert ULID1 in ids
    assert ULID2 in ids
