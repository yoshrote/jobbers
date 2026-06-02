"""
Implementation-specific tests for RedisTaskState and RedisJSONTaskState.

Section 1 — parametrized over both Redis backends via the ``redis_task_adapter``
fixture (yields a ``(state, submit)`` pair for ``["redis", "redis_json"]``).
Tests verify Redis-internal state (sorted sets, hashes, sets, blobs) that the
protocol contract tests do not cover.

Section 2 — plain-Redis-only edge cases (``msgpack_adapter`` fixture): known
RedisTaskState sorting limitations and scan-path guards.

Section 3 — RedisDeadQueue edge case (``msgpack_dead_queue`` fixture).
"""

import datetime as dt

import pytest
from ulid import ULID

from jobbers.models.task import PaginationOrder, Task, TaskPagination
from jobbers.models.task_status import TaskStatus

FROZEN_TIME = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG01")
ULID2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG02")
ULID3 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG03")
ULID4 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG04")


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


# ── Section 1: both Redis backends (parametrized) ─────────────────────────────


@pytest.mark.asyncio
async def test_stage_requeue_adds_task_to_queue(redis_task_adapter):
    """stage_requeue enqueues the task back into its sorted-set queue."""
    state, submit = redis_task_adapter
    task = make_task()
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_requeue(pipe, task)
    await pipe.execute()
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_stage_requeue_uses_submitted_at_as_score(redis_task_adapter):
    """The queue sorted-set score matches the task's submitted_at timestamp."""
    state, submit = redis_task_adapter
    task = make_task()
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_requeue(pipe, task)
    await pipe.execute()
    score = await state.data_store.zscore(state.TASKS_BY_QUEUE(queue="default"), bytes(ULID1))
    assert score == pytest.approx(FROZEN_TIME.timestamp())


@pytest.mark.asyncio
async def test_stage_requeue_does_not_duplicate_queue_entry(redis_task_adapter):
    """Re-queuing the same task ID does not create a duplicate entry."""
    state, submit = redis_task_adapter
    task = make_task()
    for _ in range(3):
        pipe = state.data_store.pipeline(transaction=True)
        state.stage_requeue(pipe, task)
        await pipe.execute()
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert members.count(bytes(ULID1)) == 1


@pytest.mark.asyncio
async def test_stage_submit_task_adds_task_to_queue(redis_task_adapter):
    """stage_submit_task enqueues the task into its sorted-set queue."""
    state, submit = redis_task_adapter
    task = make_task()
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_submit_task(pipe, task)
    await pipe.execute()
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_stage_submit_task_uses_submitted_at_as_score(redis_task_adapter):
    """The queue sorted-set score matches the task's submitted_at timestamp."""
    state, submit = redis_task_adapter
    task = make_task()
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_submit_task(pipe, task)
    await pipe.execute()
    score = await state.data_store.zscore(state.TASKS_BY_QUEUE(queue="default"), bytes(ULID1))
    assert score == pytest.approx(FROZEN_TIME.timestamp())


@pytest.mark.asyncio
async def test_stage_submit_task_does_not_duplicate_queue_entry(redis_task_adapter):
    """Staging the same task ID twice does not create a duplicate queue entry."""
    state, submit = redis_task_adapter
    task = make_task()
    for _ in range(3):
        pipe = state.data_store.pipeline(transaction=True)
        state.stage_submit_task(pipe, task)
        await pipe.execute()
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert members.count(bytes(ULID1)) == 1


@pytest.mark.asyncio
async def test_stage_remove_from_queue_removes_from_sorted_set(redis_task_adapter):
    """stage_remove_from_queue removes the task ID from the queue sorted set."""
    state, submit = redis_task_adapter
    task = make_task()
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_requeue(pipe, task)
    await pipe.execute()
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_remove_from_queue(pipe, task)
    await pipe.execute()
    score = await state.data_store.zscore(state.TASKS_BY_QUEUE(queue="default"), bytes(ULID1))
    assert score is None


@pytest.mark.asyncio
async def test_stage_remove_from_queue_removes_from_type_index(redis_task_adapter):
    """stage_remove_from_queue removes the task ID from the type index set."""
    state, submit = redis_task_adapter
    task = make_task()
    await state.data_store.sadd(state.TASK_BY_TYPE_IDX(name="my_task"), bytes(ULID1))
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_remove_from_queue(pipe, task)
    await pipe.execute()
    members = await state.data_store.smembers(state.TASK_BY_TYPE_IDX(name="my_task"))
    assert bytes(ULID1) not in members


@pytest.mark.asyncio
async def test_stage_remove_from_queue_leaves_other_tasks_untouched(redis_task_adapter):
    """stage_remove_from_queue only removes the target task, not its queue-mates."""
    state, submit = redis_task_adapter
    t1 = make_task(ULID1)
    t2 = make_task(ULID2, submitted_at=FROZEN_TIME + dt.timedelta(seconds=1))
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_requeue(pipe, t1)
    state.stage_requeue(pipe, t2)
    await pipe.execute()
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_remove_from_queue(pipe, t1)
    await pipe.execute()
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) not in members
    assert bytes(ULID2) in members


@pytest.mark.asyncio
async def test_remove_task_heartbeat_removes_from_sorted_set(redis_task_adapter):
    """remove_task_heartbeat removes the task from the heartbeat sorted set."""
    state, submit = redis_task_adapter
    task = make_task()
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    assert await state.data_store.zscore(state.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)) is not None
    await state.remove_task_heartbeat(task)
    assert await state.data_store.zscore(state.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)) is None


@pytest.mark.asyncio
async def test_update_task_heartbeat_adds_to_sorted_set(redis_task_adapter):
    """update_task_heartbeat adds the task to the heartbeat sorted set."""
    state, submit = redis_task_adapter
    task = make_task(status=TaskStatus.STARTED)
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    scores = await state.data_store.zrange(state.HEARTBEAT_SCORES(queue=task.queue), 0, -1, withscores=True)
    assert any(bytes(task.id) == tid for tid, _ in scores)


@pytest.mark.asyncio
async def test_update_task_heartbeat_updates_existing_score(redis_task_adapter):
    """A second update_task_heartbeat call overwrites the previous heartbeat score."""
    state, submit = redis_task_adapter
    task = make_task(status=TaskStatus.STARTED)
    await state.save_task(task)
    task.heartbeat_at = FROZEN_TIME
    await state.update_task_heartbeat(task)
    second_time = FROZEN_TIME + dt.timedelta(seconds=10)
    task.heartbeat_at = second_time
    await state.update_task_heartbeat(task)
    scores = await state.data_store.zrange(state.HEARTBEAT_SCORES(queue=task.queue), 0, -1, withscores=True)
    score = next(s for tid, s in scores if bytes(task.id) == tid)
    assert score == second_time.timestamp()


@pytest.mark.asyncio
async def test_get_all_tasks_skips_missing_data(redis_task_adapter):
    """Task registered in the queue but blob deleted → not returned by get_all_tasks."""
    state, submit = redis_task_adapter
    task = make_task()
    await submit.submit_task(task)
    await state.data_store.delete(state.TASK_DETAILS(task_id=ULID1))
    assert await state.get_all_tasks(TaskPagination(queue="default")) == []


@pytest.mark.asyncio
async def test_get_stale_tasks_filters_out_missing_blobs(redis_task_adapter):
    """Tasks whose blob has been deleted are silently excluded from stale results."""
    state, submit = redis_task_adapter
    now = dt.datetime.now(dt.UTC)
    stale = Task(
        id=ULID1,
        name="s",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=now,
        heartbeat_at=now - dt.timedelta(minutes=10),
    )
    await state.save_task(stale)
    await state.update_task_heartbeat(stale)
    await state.data_store.delete(state.TASK_DETAILS(task_id=stale.id))
    result = [t async for t in state.get_stale_tasks({"default"}, dt.timedelta(minutes=5))]
    assert len(result) == 0


@pytest.mark.asyncio
async def test_clean_noop_when_no_age_params_leaves_sorted_set_intact(redis_task_adapter):
    """clean() with neither min_queue_age nor max_queue_age does nothing to the sorted set."""
    state, submit = redis_task_adapter
    await submit.submit_task(make_task())
    await state.clean(queues={b"default"}, now=dt.datetime.now(dt.UTC))
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_clean_inverted_time_range(redis_task_adapter):
    """`clean()` removes everything outside the (max_queue_age, min_queue_age) window."""
    state, submit = redis_task_adapter
    await submit.submit_task(make_task(ULID1, submitted_at=FROZEN_TIME))
    await state.clean(
        queues={b"default"},
        now=FROZEN_TIME + dt.timedelta(hours=1),
        min_queue_age=FROZEN_TIME + dt.timedelta(seconds=30),
        max_queue_age=FROZEN_TIME - dt.timedelta(seconds=30),
    )
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) not in members


@pytest.mark.asyncio
async def test_clean_max_queue_age_removes_old_entries_from_sorted_set(redis_task_adapter):
    """max_queue_age removes entries at or before that timestamp from the sorted set."""
    state, submit = redis_task_adapter
    await submit.submit_task(make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=2)))
    await submit.submit_task(make_task(ULID2, submitted_at=FROZEN_TIME))
    await state.clean(queues={b"default"}, now=FROZEN_TIME, max_queue_age=FROZEN_TIME - dt.timedelta(days=1))
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) not in members
    assert bytes(ULID2) in members


@pytest.mark.asyncio
async def test_clean_min_queue_age_removes_entries_from_sorted_set(redis_task_adapter):
    """min_queue_age removes entries at or after that timestamp from the sorted set."""
    state, submit = redis_task_adapter
    await submit.submit_task(make_task(ULID1, submitted_at=FROZEN_TIME - dt.timedelta(days=10)))
    await submit.submit_task(make_task(ULID2, submitted_at=FROZEN_TIME))
    await state.clean(queues={b"default"}, now=FROZEN_TIME, min_queue_age=FROZEN_TIME - dt.timedelta(days=1))
    members = await state.data_store.zrange(state.TASKS_BY_QUEUE(queue="default"), 0, -1)
    assert bytes(ULID1) in members
    assert bytes(ULID2) not in members


@pytest.mark.asyncio
async def test_clean_terminal_tasks_deletes_blob(redis_task_adapter):
    """A completed task blob older than max_age is deleted from the key-value store."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await state.data_store.exists(state.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_removes_heartbeat_entry(redis_task_adapter):
    """The heartbeat sorted-set entry is removed along with the task blob."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.data_store.zadd(
        state.HEARTBEAT_SCORES(queue="default"),
        {ULID1.bytes: (FROZEN_TIME - dt.timedelta(days=8)).timestamp()},
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert await state.data_store.zscore(state.HEARTBEAT_SCORES(queue="default"), ULID1.bytes) is None


@pytest.mark.asyncio
async def test_clean_terminal_tasks_removes_type_index_entry(redis_task_adapter):
    """Orphaned task-type-idx entries are removed alongside the task blob."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="test_task",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.data_store.sadd(state.TASK_BY_TYPE_IDX(name="test_task"), ULID1.bytes)
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    members = await state.data_store.smembers(state.TASK_BY_TYPE_IDX(name="test_task"))
    assert ULID1.bytes not in members


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_active_task_blob(redis_task_adapter):
    """Tasks in active statuses are never deleted regardless of age."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=FROZEN_TIME - dt.timedelta(days=100),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))
    assert await state.data_store.exists(state.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_task_within_age_blob(redis_task_adapter):
    """A completed task whose completed_at is within max_age is not deleted."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(hours=1),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert await state.data_store.exists(state.TASK_DETAILS(task_id=ULID1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_task_without_completed_at_blob(redis_task_adapter):
    """A terminal task with no completed_at is not deleted."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(id=ULID1, name="t", queue="default", status=TaskStatus.FAILED, completed_at=None)
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))
    assert await state.data_store.exists(state.TASK_DETAILS(task_id=ULID1))


@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.STALLED,
        TaskStatus.DROPPED,
    ],
)
@pytest.mark.asyncio
async def test_clean_terminal_tasks_cleans_all_terminal_statuses_blob(redis_task_adapter, status):
    """All five terminal statuses are eligible for cleanup when old enough."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=status,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await state.data_store.exists(state.TASK_DETAILS(task_id=ULID1))


@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.STALLED,
        TaskStatus.DROPPED,
    ],
)
@pytest.mark.asyncio
async def test_clean_terminal_tasks_removes_heartbeat_for_all_terminal_statuses(redis_task_adapter, status):
    """Heartbeat entries are removed for every terminal status, not just COMPLETED."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=status,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.data_store.zadd(
        state.HEARTBEAT_SCORES(queue="default"),
        {ULID1.bytes: (FROZEN_TIME - dt.timedelta(days=8)).timestamp()},
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert await state.data_store.zscore(state.HEARTBEAT_SCORES(queue="default"), ULID1.bytes) is None


@pytest.mark.asyncio
async def test_clean_terminal_tasks_leaves_other_tasks_blob_untouched(redis_task_adapter):
    """Only old terminal tasks are deleted; recent task blobs remain."""
    state, submit = redis_task_adapter
    await state.save_task(
        Task(
            id=ULID1,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(days=8),
        )
    )
    await state.save_task(
        Task(
            id=ULID2,
            name="t",
            queue="default",
            status=TaskStatus.COMPLETED,
            completed_at=FROZEN_TIME - dt.timedelta(hours=1),
        )
    )
    await state.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))
    assert not await state.data_store.exists(state.TASK_DETAILS(task_id=ULID1))
    assert await state.data_store.exists(state.TASK_DETAILS(task_id=ULID2))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_non_ulid_keys(redis_task_adapter):
    """A task:* key whose suffix is not a valid ULID is silently skipped."""
    state, submit = redis_task_adapter
    await state.data_store.set("task:not_a_valid_ulid", b"some data")
    await state.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))
    assert await state.data_store.exists("task:not_a_valid_ulid")


@pytest.mark.asyncio
async def test_clean_dag_runs_removes_stale_entries_from_sorted_set(redis_task_adapter):
    """clean_dag_runs removes DAG run entries older than max_age from the sorted set."""
    state, submit = redis_task_adapter
    old_score = (FROZEN_TIME - dt.timedelta(days=10)).timestamp()
    await state.data_store.zadd(state.DAG_RUNS, {bytes(ULID1): old_score})
    await state.clean_dag_runs(FROZEN_TIME, dt.timedelta(days=7))
    assert await state.data_store.zscore(state.DAG_RUNS, bytes(ULID1)) is None


@pytest.mark.asyncio
async def test_clean_dag_runs_noop_when_nothing_stale(redis_task_adapter):
    """clean_dag_runs leaves recent DAG run entries untouched."""
    state, submit = redis_task_adapter
    recent_score = (FROZEN_TIME - dt.timedelta(hours=1)).timestamp()
    await state.data_store.zadd(state.DAG_RUNS, {bytes(ULID1): recent_score})
    await state.clean_dag_runs(FROZEN_TIME, dt.timedelta(days=7))
    assert await state.data_store.zscore(state.DAG_RUNS, bytes(ULID1)) is not None


@pytest.mark.asyncio
async def test_stage_register_dag_run_adds_to_dag_runs_set(redis_task_adapter):
    """stage_submit_task with a dag_run_id registers the run in the DAG_RUNS sorted set."""
    state, submit = redis_task_adapter
    dag_run_id = ULID()
    task = make_task()
    task.dag_run_id = dag_run_id
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_submit_task(pipe, task)
    await pipe.execute()
    score = await state.data_store.zscore(state.DAG_RUNS, bytes(dag_run_id))
    assert score is not None
    assert score == pytest.approx(FROZEN_TIME.timestamp())


@pytest.mark.asyncio
async def test_stage_register_dag_run_noop_when_no_dag_run_id(redis_task_adapter):
    """stage_submit_task without a dag_run_id does not add anything to DAG_RUNS."""
    state, submit = redis_task_adapter
    task = make_task()
    assert task.dag_run_id is None
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_submit_task(pipe, task)
    await pipe.execute()
    assert await state.data_store.zcard(state.DAG_RUNS) == 0


@pytest.mark.asyncio
async def test_stage_init_fan_in_creates_tracking_and_members_sets(redis_task_adapter):
    """stage_init_fan_in writes a tracking set and a permanent members set."""
    state, submit = redis_task_adapter
    fan_in_key = "fan-in:test"
    predecessor_ids = {ULID1, ULID2}
    pipe = state.data_store.pipeline(transaction=True)
    state.stage_init_fan_in(pipe, fan_in_key, predecessor_ids)
    await pipe.execute()
    tracking = await state.data_store.smembers(fan_in_key)
    members = await state.data_store.smembers(f"{fan_in_key}:members")
    expected = {str(ULID1).encode(), str(ULID2).encode()}
    assert tracking == expected
    assert members == expected


@pytest.mark.asyncio
async def test_init_fan_in_creates_tracking_and_members_sets(redis_task_adapter):
    """init_fan_in writes the same sets as stage_init_fan_in but executes immediately."""
    state, submit = redis_task_adapter
    fan_in_key = "fan-in:direct"
    predecessor_ids = {ULID1, ULID2, ULID3}
    await state.init_fan_in(fan_in_key, predecessor_ids)
    tracking = await state.data_store.smembers(fan_in_key)
    members = await state.data_store.smembers(f"{fan_in_key}:members")
    expected = {str(uid).encode() for uid in predecessor_ids}
    assert tracking == expected
    assert members == expected


@pytest.mark.asyncio
async def test_read_for_watch_returns_task(redis_task_adapter):
    """read_for_watch returns the task when it exists."""
    state, submit = redis_task_adapter
    task = make_task()
    await state.save_task(task)
    task_key = state.TASK_DETAILS(task_id=ULID1)
    pipe = state.data_store.pipeline()
    await pipe.watch(task_key)
    result = await state.read_for_watch(pipe, ULID1)
    await pipe.unwatch()
    assert result is not None
    assert result.id == ULID1
    assert result.name == "my_task"


@pytest.mark.asyncio
async def test_read_for_watch_returns_none_when_missing(redis_task_adapter):
    """read_for_watch returns None when the task does not exist."""
    state, submit = redis_task_adapter
    task_key = state.TASK_DETAILS(task_id=ULID1)
    pipe = state.data_store.pipeline()
    await pipe.watch(task_key)
    result = await state.read_for_watch(pipe, ULID1)
    await pipe.unwatch()
    assert result is None


@pytest.mark.asyncio
async def test_read_for_watch_preserves_task_fields(redis_task_adapter):
    """read_for_watch returns a task with all fields intact."""
    state, submit = redis_task_adapter
    task = make_task(status=TaskStatus.STARTED)
    task.errors = ["oops"]
    task.retry_attempt = 2
    await state.save_task(task)
    task_key = state.TASK_DETAILS(task_id=ULID1)
    pipe = state.data_store.pipeline()
    await pipe.watch(task_key)
    result = await state.read_for_watch(pipe, ULID1)
    await pipe.unwatch()
    assert result is not None
    assert result.status == TaskStatus.STARTED
    assert result.errors == ["oops"]
    assert result.retry_attempt == 2


@pytest.mark.asyncio
async def test_add_task_to_results_skips_missing_task(redis_task_adapter):
    """_add_task_to_results returns the list unchanged when the task blob is absent."""
    state, submit = redis_task_adapter
    results: list[Task] = []
    returned = await state._add_task_to_results(ULID1, results)
    assert returned == []
    assert results == []


@pytest.mark.asyncio
async def test_add_task_to_results_appends_found_task(redis_task_adapter):
    """_add_task_to_results appends the task when it exists."""
    state, submit = redis_task_adapter
    await state.save_task(make_task())
    results: list[Task] = []
    returned = await state._add_task_to_results(ULID1, results)
    assert len(returned) == 1
    assert returned[0].id == ULID1


# ── Section 2: plain-Redis-only edge cases (msgpack_adapter) ──────────────────


@pytest.mark.xfail(
    reason=(
        "RedisTaskState applies offset to queue positions before Python-side filtering, "
        "so offset=1 skips one *queue entry* (task_b), not one *filtered result* (task_a). "
        "Page 2 therefore returns the same task as page 1 — pages overlap, making "
        "pagination over a filtered result set unpredictable."
    ),
    strict=True,
)
@pytest.mark.asyncio
async def test_get_all_tasks_filter_pagination_page_size_is_unpredictable(msgpack_adapter):
    """Tripwire: documents the known offset-before-filter limitation in RedisTaskState."""
    state, submit = msgpack_adapter
    for t in (
        make_task(ULID1, name="task_b", submitted_at=FROZEN_TIME),
        make_task(ULID2, name="task_b", submitted_at=FROZEN_TIME + dt.timedelta(seconds=1)),
        make_task(ULID3, name="task_a", submitted_at=FROZEN_TIME + dt.timedelta(seconds=2)),
        make_task(ULID4, name="task_a", submitted_at=FROZEN_TIME + dt.timedelta(seconds=3)),
    ):
        await submit.submit_task(t)
    page1 = await state.get_all_tasks(
        TaskPagination(
            queue="default", task_name="task_a", limit=1, offset=0, order_by=PaginationOrder.SUBMITTED_AT
        )
    )
    page2 = await state.get_all_tasks(
        TaskPagination(
            queue="default", task_name="task_a", limit=1, offset=1, order_by=PaginationOrder.SUBMITTED_AT
        )
    )
    assert [t.id for t in page1] == [ULID3]
    assert [t.id for t in page2] == [ULID4]  # A2 — expected but not what happens


@pytest.mark.xfail(
    reason=(
        "RedisTaskState does not sort by task ID. "
        "order_by=TASK_ID uses zrange, which sorts by the queue sorted-set score "
        "(submitted_at timestamp), not by ULID."
    ),
    strict=True,
)
@pytest.mark.asyncio
async def test_get_all_tasks_task_id_order(msgpack_adapter):
    """Tripwire: RedisTaskState ignores order_by=TASK_ID and sorts by submitted_at instead."""
    state, submit = msgpack_adapter
    # ULID order: ULID1 < ULID2 < ULID3, submitted_at order: ULID3 < ULID2 < ULID1
    for t in (
        make_task(ULID1, submitted_at=FROZEN_TIME + dt.timedelta(seconds=2)),
        make_task(ULID2, submitted_at=FROZEN_TIME + dt.timedelta(seconds=1)),
        make_task(ULID3, submitted_at=FROZEN_TIME),
    ):
        await submit.submit_task(t)
    results = await state.get_all_tasks(
        TaskPagination(queue="default", order_by=PaginationOrder.SUBMITTED_AT)
    )
    assert [r.id for r in results] == [ULID3, ULID2, ULID1]
    results = await state.get_all_tasks(TaskPagination(queue="default", order_by=PaginationOrder.TASK_ID))
    assert [r.id for r in results] == [ULID1, ULID2, ULID3]


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_none_blob(msgpack_adapter, redis):
    """A task:* key that returns no data is silently skipped."""
    state, submit = msgpack_adapter
    await redis.set("task:ghost", b"")
    await redis.delete("task:ghost")
    await redis.set("task:placeholder", b"data")
    await redis.delete("task:placeholder")
    await state.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))


@pytest.mark.asyncio
async def test_clean_terminal_tasks_skips_non_ulid_keys_msgpack(msgpack_adapter, redis):
    """A task:* key whose suffix is not a valid ULID is skipped without error (msgpack path)."""
    state, submit = msgpack_adapter
    await redis.set("task:not_a_valid_ulid_at_all", b"some data")
    await state.clean_terminal_tasks(dt.datetime.now(dt.UTC), dt.timedelta(days=1))
    assert await redis.exists("task:not_a_valid_ulid_at_all")


@pytest.mark.asyncio
async def test_pack_includes_cron_id(msgpack_adapter):
    """pack() includes cron_id when set; unpack() round-trips it correctly."""
    state, submit = msgpack_adapter
    task = make_task(task_id=ULID1)
    cron_id = ULID()
    task.cron_id = cron_id
    packed = state.pack(task)
    unpacked = state.unpack(task.id, packed)
    assert unpacked.cron_id == cron_id


@pytest.mark.asyncio
async def test_clean_dag_runs_skips_invalid_ulid_bytes(msgpack_adapter):
    """clean_dag_runs silently skips sorted-set entries whose bytes cannot be parsed as a ULID."""
    state, submit = msgpack_adapter
    invalid_bytes = b"bad"
    old_score = (FROZEN_TIME - dt.timedelta(days=10)).timestamp()
    await state.data_store.zadd(state.DAG_RUNS, {invalid_bytes: old_score})
    await state.clean_dag_runs(FROZEN_TIME, dt.timedelta(seconds=1))
    assert await state.data_store.zscore(state.DAG_RUNS, invalid_bytes) is None


# ── Section 3: RedisDeadQueue edge case (msgpack_dead_queue) ──────────────────


async def _add_to_dlq(dq, task: Task, failed_at: dt.datetime) -> None:
    pipe = dq.data_store.pipeline(transaction=True)
    dq.stage_add(pipe, task, failed_at)
    await pipe.execute()


@pytest.mark.asyncio
async def test_clean_handles_missing_meta(msgpack_dead_queue):
    """
    clean() removes the sorted-set entry even when the meta hash entry is absent.

    RedisDeadQueue stores DLQ membership in a sorted set (``dlq``) and queue/name
    metadata in a separate hash (``dlq-meta``).  If the meta entry is missing
    (e.g. written by an older version), clean() must still remove the sorted-set
    entry without error.
    """
    task = make_task(task_id=ULID1, status=TaskStatus.FAILED)
    old_time = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
    await _add_to_dlq(msgpack_dead_queue, task, old_time)
    await msgpack_dead_queue.data_store.hdel(msgpack_dead_queue.DLQ_META, str(task.id))
    await msgpack_dead_queue.clean(dt.datetime(2025, 1, 1, tzinfo=dt.UTC))
    assert await msgpack_dead_queue.data_store.zscore(msgpack_dead_queue.DLQ, bytes(task.id)) is None
