"""
Tests for SQLTransactionBatch — the TransactionHandle backing every SQL atomic pipeline.

SQLDeadQueue.stage_add/stage_remove, SQLTaskState.stage_save/stage_submit_task/
atomic_save_if_status, SQLTaskScheduler.stage_add/stage_remove, and
SQLCronDAGScheduler's staged writes all funnel through this class's execute().
These tests verify the "all or nothing" guarantee those callers rely on directly,
independent of any specific adapter.
"""

import pytest
from ulid import ULID

from jobbers.adapters.sql import SQLTaskState
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus
from jobbers.utils.sql_transaction import SQLTransactionBatch

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")


@pytest.mark.asyncio
async def test_execute_commits_all_staged_ops(session_factory):
    """A batch with only successful ops commits every one of them."""
    state = SQLTaskState(session_factory)
    task_a = Task(id=ULID1, name="a", version=1, queue="default", status=TaskStatus.SUBMITTED)
    task_b = Task(id=ULID2, name="b", version=1, queue="default", status=TaskStatus.SUBMITTED)
    batch = SQLTransactionBatch(session_factory)
    state.stage_save(batch, task_a)
    state.stage_save(batch, task_b)

    await batch.execute()

    assert await state.get_task(ULID1) is not None
    assert await state.get_task(ULID2) is not None


@pytest.mark.asyncio
async def test_execute_rolls_back_earlier_ops_when_a_later_op_raises(session_factory):
    """If a later staged op raises, earlier ops already run in the same batch are rolled back too."""
    state = SQLTaskState(session_factory)
    task = Task(id=ULID1, name="a", version=1, queue="default", status=TaskStatus.SUBMITTED)
    batch = SQLTransactionBatch(session_factory)
    state.stage_save(batch, task)

    async def _boom(session: object) -> None:
        raise RuntimeError("boom")

    batch.add_op(_boom)

    with pytest.raises(RuntimeError, match="boom"):
        await batch.execute()

    # The whole transaction rolled back -- stage_save's insert must not have persisted.
    assert await state.get_task(ULID1) is None


@pytest.mark.asyncio
async def test_execute_propagates_the_original_exception(session_factory):
    """execute() re-raises the staged op's own exception, not a rollback-related one."""
    batch = SQLTransactionBatch(session_factory)

    async def _boom(session: object) -> None:
        raise ValueError("specific failure")

    batch.add_op(_boom)

    with pytest.raises(ValueError, match="specific failure"):
        await batch.execute()


@pytest.mark.asyncio
async def test_execute_closes_the_session_after_failure(session_factory):
    """After a rollback, the batch's session is discarded so a retry starts a fresh one."""
    batch = SQLTransactionBatch(session_factory)

    async def _boom(session: object) -> None:
        raise RuntimeError("boom")

    batch.add_op(_boom)

    with pytest.raises(RuntimeError):
        await batch.execute()

    assert batch._session is None


@pytest.mark.asyncio
async def test_execute_with_no_staged_ops_is_a_noop(session_factory):
    """execute() on an empty batch commits an empty transaction without error."""
    batch = SQLTransactionBatch(session_factory)
    result = await batch.execute()
    assert result == []
