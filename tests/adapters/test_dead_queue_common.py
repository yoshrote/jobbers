"""
Contract tests for DeadQueueProtocol implementations.

Each test runs against all registered implementations via the ``dead_queue``
fixture defined in ``tests/adapters/conftest.py``.
"""

import datetime as dt

import pytest

from jobbers.adapters.raw_redis import DeadQueue
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus

FAILED_AT = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
EARLIER = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
LATER = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)


def make_task(
    task_id: str = "01JQC31AJP7TSA9X8AEP64XG08",
    name: str = "my_task",
    version: int = 1,
    queue: str = "default",
    errors: list[str] | None = None,
) -> Task:
    from ulid import ULID

    return Task(
        id=ULID.from_str(task_id),
        name=name,
        version=version,
        queue=queue,
        status=TaskStatus.FAILED,
        errors=errors if errors is not None else ["something went wrong"],
    )


async def add_to_dlq(dq, task: Task, failed_at: dt.datetime) -> None:
    pipe = dq.data_store.pipeline(transaction=True)
    dq.stage_add(pipe, task, failed_at)
    await pipe.execute()


# ── add / get_by_ids ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_add_and_get_by_id(dead_queue):
    dq, adapter = dead_queue
    task = make_task()
    await adapter.save_task(task)
    await add_to_dlq(dq, task, FAILED_AT)

    results = await dq.get_by_ids([str(task.id)])
    assert len(results) == 1
    assert results[0].id == task.id
    assert results[0].name == task.name
    assert results[0].queue == task.queue


@pytest.mark.asyncio
async def test_get_by_ids_multiple(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_ids([str(t1.id), str(t2.id)])
    assert {r.id for r in results} == {t1.id, t2.id}


@pytest.mark.asyncio
async def test_get_by_ids_returns_only_matching(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_ids([str(t1.id)])
    assert len(results) == 1
    assert results[0].id == t1.id


@pytest.mark.asyncio
async def test_get_by_ids_nonexistent_returns_empty(dead_queue):
    dq, _ = dead_queue
    assert await dq.get_by_ids(["01JQC31AJP7TSA9X8AEP64XG99"]) == []


@pytest.mark.asyncio
async def test_get_by_ids_empty_list_returns_empty(dead_queue):
    dq, adapter = dead_queue
    task = make_task()
    await adapter.save_task(task)
    await add_to_dlq(dq, task, FAILED_AT)
    assert await dq.get_by_ids([]) == []


# ── get_by_filter ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_by_filter_queue(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", queue="q1")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", queue="q2")
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_filter(queue="q1")
    assert len(results) == 1
    assert results[0].queue == "q1"


@pytest.mark.asyncio
async def test_get_by_filter_task_name(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", name="task_a")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", name="task_b")
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_filter(task_name="task_a")
    assert len(results) == 1
    assert results[0].name == "task_a"


@pytest.mark.asyncio
async def test_get_by_filter_task_version(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", version=1)
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", version=2)
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_filter(task_version=2)
    assert len(results) == 1
    assert results[0].version == 2


@pytest.mark.asyncio
async def test_get_by_filter_version_zero(dead_queue):
    """task_version=0 should be treated as a real filter value, not falsy."""
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", version=0)
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", version=1)
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_filter(task_version=0)
    assert len(results) == 1
    assert results[0].version == 0


@pytest.mark.asyncio
async def test_get_by_filter_combined(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", name="task_a", version=1, queue="q1")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", name="task_a", version=2, queue="q1")
    t3 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG03", name="task_b", version=1, queue="q1")
    for t in (t1, t2, t3):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_filter(queue="q1", task_name="task_a", task_version=1)
    assert len(results) == 1
    assert results[0].id == t1.id


@pytest.mark.asyncio
async def test_get_by_filter_no_criteria_returns_all(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_filter()
    assert len(results) == 2


@pytest.mark.asyncio
async def test_get_by_filter_limit_respected(dead_queue):
    dq, adapter = dead_queue
    tasks = [
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01"),
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02"),
        make_task(task_id="01JQC31AJP7TSA9X8AEP64XG03"),
    ]
    for t in tasks:
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    results = await dq.get_by_filter(limit=2)
    assert len(results) == 2


@pytest.mark.asyncio
async def test_get_by_filter_skips_missing_task_data(dead_queue):
    """If a task is in the DLQ sorted set but its blob is gone, it is skipped."""
    dq, _ = dead_queue
    if not isinstance(dq, DeadQueue):
        pytest.skip("raw DeadQueue only: tests blob-missing resilience")
    task = make_task()
    pipe = dq.data_store.pipeline(transaction=True)
    dq.stage_add(pipe, task, FAILED_AT)
    await pipe.execute()

    assert await dq.get_by_filter() == []


@pytest.mark.asyncio
async def test_get_by_filter_no_match_returns_empty(dead_queue):
    dq, adapter = dead_queue
    task = make_task(queue="q1")
    await adapter.save_task(task)
    await add_to_dlq(dq, task, FAILED_AT)

    assert await dq.get_by_filter(queue="q_nonexistent") == []


# ── remove ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_deletes_entry(dead_queue):
    dq, adapter = dead_queue
    task = make_task()
    await adapter.save_task(task)
    await add_to_dlq(dq, task, FAILED_AT)
    await dq.remove_many([str(task.id)])

    assert await dq.get_by_ids([str(task.id)]) == []


@pytest.mark.asyncio
async def test_remove_leaves_other_entries_intact(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)
    await dq.remove_many([str(t1.id)])

    assert await dq.get_by_ids([str(t1.id)]) == []
    assert len(await dq.get_by_ids([str(t2.id)])) == 1


@pytest.mark.asyncio
async def test_remove_nonexistent_is_silent(dead_queue):
    """Removing a task that was never added should not raise."""
    dq, _ = dead_queue
    await dq.remove_many(["01JQC31AJP7TSA9X8AEP64XG99"])


@pytest.mark.asyncio
async def test_remove_cleans_up_secondary_indexes(dead_queue):
    """After remove, the queue and name indexes no longer contain the task."""
    dq, adapter = dead_queue
    task = make_task(queue="myqueue", name="mytask")
    await adapter.save_task(task)
    await add_to_dlq(dq, task, FAILED_AT)
    await dq.remove_many([str(task.id)])

    assert await dq.get_by_filter(queue="myqueue") == []
    assert await dq.get_by_filter(task_name="mytask") == []


# ── remove_many ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remove_many_deletes_all(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    for t in (t1, t2):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)
    await dq.remove_many([str(t1.id), str(t2.id)])

    assert await dq.get_by_ids([str(t1.id), str(t2.id)]) == []


@pytest.mark.asyncio
async def test_remove_many_leaves_unmentioned_entries_intact(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    t3 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG03")
    for t in (t1, t2, t3):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)
    await dq.remove_many([str(t1.id), str(t2.id)])

    assert await dq.get_by_ids([str(t1.id), str(t2.id)]) == []
    assert len(await dq.get_by_ids([str(t3.id)])) == 1


@pytest.mark.asyncio
async def test_remove_many_empty_list_is_silent(dead_queue):
    dq, adapter = dead_queue
    task = make_task()
    await adapter.save_task(task)
    await add_to_dlq(dq, task, FAILED_AT)
    await dq.remove_many([])
    assert len(await dq.get_by_ids([str(task.id)])) == 1


@pytest.mark.asyncio
async def test_remove_many_nonexistent_ids_are_silent(dead_queue):
    """IDs that don't exist should not raise."""
    dq, _ = dead_queue
    await dq.remove_many(["01JQC31AJP7TSA9X8AEP64XG01", "01JQC31AJP7TSA9X8AEP64XG02"])


@pytest.mark.asyncio
async def test_remove_many_partial_match(dead_queue):
    dq, adapter = dead_queue
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    await adapter.save_task(t1)
    await add_to_dlq(dq, t1, FAILED_AT)
    await dq.remove_many([str(t1.id), "01JQC31AJP7TSA9X8AEP64XG99"])

    assert await dq.get_by_ids([str(t1.id)]) == []


# ── get_history ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_history_returns_errors_from_task_blob(dead_queue):
    dq, adapter = dead_queue
    task = make_task(errors=["first error", "second error", "third error"])
    await adapter.save_task(task)
    await add_to_dlq(dq, task, FAILED_AT)

    history = await dq.get_history(str(task.id))
    assert len(history) == 3
    assert history[0] == {"attempt": 0, "error": "first error"}
    assert history[1] == {"attempt": 1, "error": "second error"}
    assert history[2] == {"attempt": 2, "error": "third error"}


@pytest.mark.asyncio
async def test_get_history_empty_when_no_errors(dead_queue):
    dq, adapter = dead_queue
    task = make_task(errors=[])
    await adapter.save_task(task)
    await add_to_dlq(dq, task, FAILED_AT)

    assert await dq.get_history(str(task.id)) == []


@pytest.mark.asyncio
async def test_get_history_empty_for_unknown_task(dead_queue):
    dq, _ = dead_queue
    assert await dq.get_history("01JQC31AJP7TSA9X8AEP64XG99") == []


@pytest.mark.asyncio
async def test_get_history_isolated_by_task_id(dead_queue):
    dq, adapter = dead_queue
    ta = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01", errors=["error a"])
    tb = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02", errors=["error b1", "error b2"])
    for t in (ta, tb):
        await adapter.save_task(t)
        await add_to_dlq(dq, t, FAILED_AT)

    assert len(await dq.get_history(str(ta.id))) == 1
    assert len(await dq.get_history(str(tb.id))) == 2


# ── clean ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_removes_old_entries(dead_queue):
    """Tasks added with EARLIER failed_at are removed; LATER ones remain."""
    dq, adapter = dead_queue
    t_old = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t_new = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    for t in (t_old, t_new):
        await adapter.save_task(t)
    await add_to_dlq(dq, t_old, EARLIER)
    await add_to_dlq(dq, t_new, LATER)

    cutoff = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)
    await dq.clean(cutoff)

    assert await dq.get_by_ids([str(t_old.id)]) == []
    assert len(await dq.get_by_ids([str(t_new.id)])) == 1


@pytest.mark.asyncio
async def test_clean_removes_secondary_indexes(dead_queue):
    """After clean, old tasks are gone from queue and name indexes."""
    dq, adapter = dead_queue
    task = make_task(queue="myqueue", name="mytask")
    await adapter.save_task(task)
    await add_to_dlq(dq, task, EARLIER)

    cutoff = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)
    await dq.clean(cutoff)

    assert await dq.get_by_filter(queue="myqueue") == []
    assert await dq.get_by_filter(task_name="mytask") == []


@pytest.mark.asyncio
async def test_clean_handles_missing_meta(dead_queue):
    """DeadQueue: clean() removes the sorted-set entry even when the meta hash entry is absent."""
    dq, _ = dead_queue
    if not isinstance(dq, DeadQueue):
        pytest.skip("raw DeadQueue only: tests meta-hash resilience")
    task = make_task()
    old_time = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)

    await dq.data_store.zadd(dq.DLQ, {bytes(task.id): old_time.timestamp()})

    cutoff = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)
    await dq.clean(cutoff)

    score = await dq.data_store.zscore(dq.DLQ, bytes(task.id))
    assert score is None


@pytest.mark.asyncio
async def test_clean_empty_queue_is_silent(dead_queue):
    """Clean on an empty DLQ should not raise."""
    dq, _ = dead_queue
    await dq.clean(dt.datetime(2025, 1, 1, tzinfo=dt.UTC))


# ── ordering ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_by_filter_sorted_by_failed_at_desc(dead_queue):
    """Results should be sorted by failed_at descending (most recent first)."""
    dq, adapter = dead_queue
    if isinstance(dq, DeadQueue):
        pytest.xfail("DeadQueue (raw) does not support server-side sorting by failed_at")

    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    await adapter.save_task(t1)
    await adapter.save_task(t2)
    await add_to_dlq(dq, t1, EARLIER)
    await add_to_dlq(dq, t2, LATER)

    results = await dq.get_by_filter()
    assert len(results) == 2
    assert results[0].id == t2.id  # LATER first
