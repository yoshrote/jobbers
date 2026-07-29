import asyncio
import contextlib
import datetime as dt
import logging
from collections import defaultdict
from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers import registry
from jobbers.adapters.redis import (
    RedisCancellationBus,
    RedisCronDAGScheduler,
    RedisDeadQueue,
    RedisRoutingNotifications,
    RedisTaskScheduler,
)
from jobbers.adapters.sql import SQLQueueConfigAdapter, SQLRoutingBackend
from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGNode, DagRunStatus, DAGTaskSpec
from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task
from jobbers.models.task_config import DeadLetterPolicy, TaskConfig
from jobbers.models.task_routing import RoutingConfig, RoutingStrategy
from jobbers.models.task_status import TaskStatus
from jobbers.state_manager import StateManager, TaskException, TaskRateLimitedError, UserCancellationError
from tests.conftest import DummyCronDAGScheduler, DummyTaskSubmit

FROZEN_TIME = dt.datetime.fromisoformat("2021-01-01T00:00:00+00:00")
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")


async def schedule(sm: StateManager, task: Task, run_at: dt.datetime) -> None:
    """Stage a task into the task scheduler sorted set at the given run_at time."""
    pipe = sm.task_scheduler.pipeline(transaction=True)
    sm.task_scheduler.stage_add(pipe, task, run_at)
    await pipe.execute()


async def add_to_dlq(sm: StateManager, task: Task, failed_at: dt.datetime) -> None:
    """Stage a task into the dead-letter queue at the given failed_at timestamp."""
    pipe = sm.dead_queue.pipeline(transaction=True)
    sm.dead_queue.stage_add(pipe, task, failed_at)
    await pipe.execute()


@pytest.mark.asyncio
async def test_bump_refresh_tag_publishes_pubsub(redis, state_manager):
    """bump_refresh_tag publishes the new tag value to queue-config-refresh:{role}."""
    with patch.object(redis, "publish", new_callable=AsyncMock) as mock_publish:
        new_tag = await state_manager.bump_refresh_tag("myrole")

    mock_publish.assert_called_once_with("queue-config-refresh:myrole", new_tag)


@pytest.mark.asyncio
async def test_save_role_publishes_pubsub(redis, state_manager):
    """save_role publishes the new refresh tag to queue-config-refresh:{role}."""
    with patch.object(redis, "publish", new_callable=AsyncMock) as mock_publish:
        await state_manager.save_role("newrole", set())

    channel, tag = mock_publish.call_args[0]
    assert channel == "queue-config-refresh:newrole"
    assert tag  # non-empty tag string


# ── get_next_task ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_next_task_no_task_found(state_manager_real_ta):
    """Test that get_next_task returns None if no task is found."""
    task = await state_manager_real_ta.get_next_task(["queue1", "queue2"], pop_timeout=1)
    assert task is None


@pytest.mark.asyncio
async def test_get_next_task_skips_missing_data_and_returns_valid(redis, state_manager_real_ta):
    """When the first queued task has missing data it is skipped and the next valid task is returned."""
    missing_id = ULID()
    valid_id = ULID()
    valid_task = Task(
        id=valid_id,
        name="Test Task",
        queue="queue1",
        version=1,
        status=TaskStatus.SUBMITTED,
        submitted_at=FROZEN_TIME,
    )

    # missing_id has score=1 so it is popped first; FROZEN_TIME >> 1 so valid_task is popped second
    await redis.zadd("task-queues:queue1", {missing_id.bytes: 1})
    await state_manager_real_ta.task_submit.submit_task(valid_task)

    task = await state_manager_real_ta.get_next_task(["queue1"], pop_timeout=1)

    assert task is not None
    assert task.id == valid_id
    dlq_members = await redis.zrange("dlq-missing-data", 0, -1)
    assert missing_id.bytes in dlq_members


# ── concurrency limits ────────────────────────────────────────────────────────


@pytest.fixture
def rate_limiter(state_manager):
    return state_manager.submission_limiter


@pytest.mark.asyncio
async def test_concurrency_limits_no_limits(rate_limiter):
    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = {
        "queue1": {ULID()},
        "queue2": {ULID()},
    }

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue1", "queue2"}


@pytest.mark.asyncio
async def test_concurrency_limits_with_limits(state_manager, rate_limiter):
    await state_manager.routing.save_queue_config(QueueConfig(name="queue1", max_concurrent=1))
    await state_manager.routing.save_queue_config(QueueConfig(name="queue2", max_concurrent=2))

    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = {
        "queue1": {ULID()},
        "queue2": {ULID()},
    }

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue2"}


@pytest.mark.asyncio
async def test_concurrency_limits_max_concurrent_zero_is_unlimited(state_manager, rate_limiter):
    """max_concurrent=0 means unlimited, same as None -- not "block this queue"."""
    await state_manager.routing.save_queue_config(QueueConfig(name="queue1", max_concurrent=0))

    task_queues = ["queue1"]
    current_tasks_by_queue = {"queue1": {ULID(), ULID(), ULID()}}

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue1"}


@pytest.mark.asyncio
async def test_concurrency_limits_empty_queues(state_manager, rate_limiter):
    await state_manager.routing.save_queue_config(QueueConfig(name="queue1", max_concurrent=1))
    await state_manager.routing.save_queue_config(QueueConfig(name="queue2", max_concurrent=1))

    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = defaultdict(set)

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue1", "queue2"}


# ── clean ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_rate_limit_age(redis, state_manager_real_ta):
    """Test cleaning tasks from the rate limiter based on rate_limit_age."""
    state_manager = state_manager_real_ta
    await state_manager.routing.save_queue_config(QueueConfig(name="queue1"))
    await state_manager.routing.save_queue_config(QueueConfig(name="queue2"))
    await redis.zadd("rate-limiter:queue1", {ULID1.bytes: FROZEN_TIME.timestamp() - 3600})
    await redis.zadd("rate-limiter:queue2", {ULID2.bytes: FROZEN_TIME.timestamp() - 1800})

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(rate_limit_age=dt.timedelta(hours=1))

    queue1_tasks = await redis.zrange("rate-limiter:queue1", 0, -1)
    queue2_tasks = await redis.zrange("rate-limiter:queue2", 0, -1)
    assert queue1_tasks == []
    assert queue2_tasks == [ULID2.bytes]


@pytest.mark.asyncio
async def test_clean_dlq_age_removes_old_entries(redis, state_manager):
    """dlq_age removes DLQ index entries older than the cutoff."""
    await state_manager.routing.save_queue_config(QueueConfig(name="default"))
    old_task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED)
    recent_task = Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.FAILED)
    await add_to_dlq(state_manager, old_task, FROZEN_TIME - dt.timedelta(days=8))
    await add_to_dlq(state_manager, recent_task, FROZEN_TIME - dt.timedelta(days=6))

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(dlq_age=dt.timedelta(days=7))

    assert await redis.zscore("dlq", ULID1.bytes) is None
    assert ULID1.bytes not in await redis.smembers("dlq-queue:default")
    assert await redis.zscore("dlq", ULID2.bytes) is not None


@pytest.mark.asyncio
async def test_clean_dlq_age_keeps_recent_entries(redis, state_manager):
    """dlq_age does not remove DLQ entries within the cutoff window."""
    await state_manager.routing.save_queue_config(QueueConfig(name="default"))
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED)
    await add_to_dlq(state_manager, task, FROZEN_TIME - dt.timedelta(days=6))

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FROZEN_TIME
        await state_manager.clean(dlq_age=dt.timedelta(days=7))

    assert await redis.zscore("dlq", ULID1.bytes) is not None


@pytest.mark.asyncio
async def test_clean_stale_time_skips_terminal_tasks(redis, state_manager_real_ta):
    """A COMPLETED task with a stale heartbeat entry is NOT re-marked STALLED."""
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    completed = Task(
        id=ULID1,
        name="my_task",
        queue="default",
        status=TaskStatus.COMPLETED,
        completed_at=two_hours_ago + dt.timedelta(minutes=30),
        heartbeat_at=two_hours_ago,
    )
    await state_manager_real_ta.task_state.save_task(completed)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager_real_ta.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task", function=dummy_fn, max_heartbeat_interval=dt.timedelta(minutes=5)
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager_real_ta.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager_real_ta.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_clean_stale_time_removes_heartbeat_on_stall(redis, state_manager_real_ta):
    """When a STARTED task is marked STALLED, its heartbeat sorted-set entry is removed."""
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    started = Task(
        id=ULID1,
        name="my_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=two_hours_ago,
        heartbeat_at=two_hours_ago,
    )
    await state_manager_real_ta.task_state.save_task(started)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager_real_ta.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task", function=dummy_fn, max_heartbeat_interval=dt.timedelta(minutes=5)
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager_real_ta.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager_real_ta.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STALLED
    assert await redis.zscore("task-heartbeats:default", ULID1.bytes) is None


@pytest.mark.asyncio
async def test_clean_stale_time_stalled_dag_task_leaves_run_open(redis, state_manager_real_ta):
    """
    A stale-heartbeat DAG task is marked STALLED but never closes out of DAG_RUN_PENDING.

    Regression test: an earlier version of this Cleaner path called
    close_dag_run_task_and_sweep for newly-stalled tasks, which would delete this
    task (its own cleanup_on matches STALLED) once it looked like the run's last
    pending task. A STALLED task never calls generate_callbacks(), so its
    FanInCallback/DynamicFanOutCallback never fires and the DAG can't complete on
    its own — the run must stay open (fan-in tracking and sibling records
    preserved within their TTLs) instead of being swept away as if it had
    completed normally.
    """
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    dag_run_id = ULID()
    started = Task(
        id=ULID1,
        name="my_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=two_hours_ago,
        heartbeat_at=two_hours_ago,
        dag_run_id=dag_run_id,
        submitted_at=two_hours_ago,
    )
    await state_manager_real_ta.task_submit.submit_task(task=started)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager_real_ta.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task",
        function=dummy_fn,
        max_heartbeat_interval=dt.timedelta(minutes=5),
        cleanup_on=frozenset({TaskStatus.STALLED}),
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager_real_ta.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager_real_ta.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STALLED
    # Never closed out of DAG_RUN_PENDING: close_dag_run_task still finds it
    # pending and decrements successfully, instead of reporting -1 (already closed).
    assert await state_manager_real_ta.close_dag_run_task(dag_run_id, ULID1) == 0


@pytest.mark.asyncio
async def test_clean_stale_time_stalled_task_with_dlq_policy_moves_to_dlq(redis, state_manager_real_ta):
    """A stale-heartbeat task with dead_letter_policy=SAVE is marked STALLED and sent to the DLQ."""
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    started = Task(
        id=ULID1,
        name="my_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=two_hours_ago,
        heartbeat_at=two_hours_ago,
        submitted_at=two_hours_ago,
    )
    await state_manager_real_ta.task_submit.submit_task(task=started)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager_real_ta.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task",
        function=dummy_fn,
        max_heartbeat_interval=dt.timedelta(minutes=5),
        dead_letter_policy=DeadLetterPolicy.SAVE,
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager_real_ta.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager_real_ta.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STALLED
    dlq = await state_manager_real_ta.dead_queue.get_by_ids([str(ULID1)])
    assert len(dlq) == 1
    assert dlq[0].id == ULID1


@pytest.mark.asyncio
async def test_clean_stale_time_stalled_task_without_dlq_policy_skips_dlq(redis, state_manager_real_ta):
    """A stale-heartbeat task with the default (NONE) dead_letter_policy is not sent to the DLQ."""
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    started = Task(
        id=ULID1,
        name="my_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=two_hours_ago,
        heartbeat_at=two_hours_ago,
        submitted_at=two_hours_ago,
    )
    await state_manager_real_ta.task_submit.submit_task(task=started)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager_real_ta.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task", function=dummy_fn, max_heartbeat_interval=dt.timedelta(minutes=5)
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager_real_ta.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager_real_ta.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STALLED
    assert await state_manager_real_ta.dead_queue.get_by_ids([str(ULID1)]) == []


@pytest.mark.asyncio
async def test_clean_stale_time_stalled_task_with_dlq_policy_saga_mode(saga_state_manager):
    """Same DLQ-on-stall behavior holds in saga mode (non-atomic task-state backend)."""
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    started = Task(
        id=ULID1,
        name="my_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=two_hours_ago,
        heartbeat_at=two_hours_ago,
    )
    await saga_state_manager.task_state.save_task(started)
    await saga_state_manager.task_state.update_task_heartbeat(started)
    await saga_state_manager.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task",
        function=dummy_fn,
        max_heartbeat_interval=dt.timedelta(minutes=5),
        dead_letter_policy=DeadLetterPolicy.SAVE,
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await saga_state_manager.clean(stale_time=dt.timedelta(minutes=30))

    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STALLED
    dlq = await saga_state_manager.dead_queue.get_by_ids([str(ULID1)])
    assert len(dlq) == 1
    assert dlq[0].id == ULID1


# ── close_dag_run_task_and_sweep / sweep_dag_run ──────────────────────────────


@pytest.mark.asyncio
async def test_close_dag_run_task_and_sweep_waits_when_siblings_pending(state_manager_real_ta):
    """Remaining > 0 must not trigger a sweep — the still-pending sibling is untouched."""
    dag_run_id = ULID()
    task_a = Task(
        id=ULID1,
        name="my_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    task_b = Task(
        id=ULID2,
        name="my_task",
        version=1,
        status=TaskStatus.SUBMITTED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    await state_manager_real_ta.task_submit.submit_task(task=task_a)
    await state_manager_real_ta.task_submit.submit_task(task=task_b)

    cleanup_config = TaskConfig(
        name="my_task", function=dummy_fn, cleanup_on=frozenset({TaskStatus.COMPLETED})
    )
    with patch.object(registry, "get_task_config", return_value=cleanup_config):
        await state_manager_real_ta.close_dag_run_task_and_sweep(task_a)

    assert await state_manager_real_ta.task_state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_close_dag_run_task_and_sweep_deletes_terminal_siblings_when_run_closes(state_manager_real_ta):
    """When the last sibling closes, the sweep deletes every sibling whose status matches its cleanup_on."""
    dag_run_id = ULID()
    task_a = Task(
        id=ULID1,
        name="my_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    task_b = Task(
        id=ULID2,
        name="my_task",
        version=1,
        status=TaskStatus.FAILED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    await state_manager_real_ta.task_submit.submit_task(task=task_a)
    await state_manager_real_ta.task_submit.submit_task(task=task_b)

    cleanup_config = TaskConfig(
        name="my_task", function=dummy_fn, cleanup_on=frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED})
    )
    with patch.object(registry, "get_task_config", return_value=cleanup_config):
        await state_manager_real_ta.close_dag_run_task_and_sweep(task_a)
        await state_manager_real_ta.close_dag_run_task_and_sweep(task_b)

    assert not await state_manager_real_ta.task_state.task_exists(ULID1)
    assert not await state_manager_real_ta.task_state.task_exists(ULID2)


@pytest.mark.asyncio
async def test_sweep_dag_run_orphaned_index_falls_back_to_fallback_task(state_manager_real_ta):
    """
    An orphaned run index still cleans up the task that triggered the call, per its own cleanup_on.

    Regression test: _sweep_dag_run used to just return when get_dag_run() found
    nothing (e.g. clean_dag_runs concurrently swept the run's index), leaving the
    completing task stuck instead of being reclaimed immediately.
    """
    dag_run_id = ULID()  # never registered via submit_task, so get_dag_run() -> None
    task = Task(
        id=ULID1,
        name="my_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    await state_manager_real_ta.task_state.save_task(task)

    cleanup_config = TaskConfig(
        name="my_task", function=dummy_fn, cleanup_on=frozenset({TaskStatus.COMPLETED})
    )
    with patch.object(registry, "get_task_config", return_value=cleanup_config):
        await state_manager_real_ta.sweep_dag_run(dag_run_id, fallback_task=task)

    assert not await state_manager_real_ta.task_state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_sweep_dag_run_orphaned_index_skips_fallback_when_status_not_in_cleanup_on(
    state_manager_real_ta,
):
    """Orphaned run index + fallback_task whose status doesn't match cleanup_on -> no deletion."""
    dag_run_id = ULID()
    task = Task(
        id=ULID1,
        name="my_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    await state_manager_real_ta.task_state.save_task(task)

    cleanup_config = TaskConfig(name="my_task", function=dummy_fn, cleanup_on=frozenset({TaskStatus.FAILED}))
    with patch.object(registry, "get_task_config", return_value=cleanup_config):
        await state_manager_real_ta.sweep_dag_run(dag_run_id, fallback_task=task)

    assert await state_manager_real_ta.task_state.task_exists(ULID1)


@pytest.mark.asyncio
async def test_sweep_dag_run_orphaned_index_no_fallback_task_is_noop(state_manager_real_ta):
    """Orphaned run index with no fallback_task is a plain no-op (no error)."""
    dag_run_id = ULID()
    await state_manager_real_ta.sweep_dag_run(dag_run_id)


# ── finalize_dag_run_task ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_finalize_dag_run_task_uses_pipelined_path_when_atomic_dag_run_available(state_manager_real_ta):
    """
    Sanity check for the fixture used by the tests below.

    state_manager_real_ta's RedisTaskState implements AtomicDagRunProtocol, so
    finalize_dag_run_task takes the pipelined record+close round trip.
    """
    assert state_manager_real_ta._atomic_dag_run is not None


@pytest.mark.asyncio
async def test_finalize_dag_run_task_completes_run_only_after_last_task(state_manager_real_ta):
    """Pipelined path: a 2-task run reaches 'complete' only once both tasks have finalized."""
    dag_run_id = ULID()
    task_a = Task(
        id=ULID1,
        name="my_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    task_b = Task(
        id=ULID2,
        name="my_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    await state_manager_real_ta.task_submit.submit_task(task=task_a)
    await state_manager_real_ta.task_submit.submit_task(task=task_b)

    cleanup_config = TaskConfig(name="my_task", function=dummy_fn)
    with patch.object(registry, "get_task_config", return_value=cleanup_config):
        await state_manager_real_ta.finalize_dag_run_task(task_a)
        run = await state_manager_real_ta.get_dag_run(dag_run_id)
        assert run is not None
        assert run.status.value == "running"

        await state_manager_real_ta.finalize_dag_run_task(task_b)
        run = await state_manager_real_ta.get_dag_run(dag_run_id)
        assert run is not None
        assert run.status.value == "complete"


@pytest.mark.asyncio
async def test_finalize_dag_run_task_stuck_status_records_but_never_closes(state_manager_real_ta):
    """A FAILED task's outcome is recorded, but it never closes out of the pending set."""
    dag_run_id = ULID()
    task = Task(
        id=ULID1,
        name="my_task",
        version=1,
        status=TaskStatus.FAILED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    await state_manager_real_ta.task_submit.submit_task(task=task)

    await state_manager_real_ta.finalize_dag_run_task(task)

    run = await state_manager_real_ta.get_dag_run(dag_run_id)
    assert run is not None
    assert run.status.value == "failed"
    # Never closed out of DAG_RUN_PENDING: close_dag_run_task still finds it pending.
    assert await state_manager_real_ta.close_dag_run_task(dag_run_id, ULID1) == 0


@pytest.mark.asyncio
async def test_finalize_dag_run_task_sequential_fallback_matches_pipelined_result(state_manager_real_ta):
    """
    Sequential fallback (no AtomicDagRunProtocol) reaches the same end state as the pipelined path.

    With _atomic_dag_run forced off, finalize_dag_run_task falls back to two
    sequential calls: record_dag_run_task_terminal then close_dag_run_task_and_sweep.
    """
    state_manager_real_ta._atomic_dag_run = None

    dag_run_id = ULID()
    task_a = Task(
        id=ULID1,
        name="my_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    task_b = Task(
        id=ULID2,
        name="my_task",
        version=1,
        status=TaskStatus.COMPLETED,
        queue="default",
        dag_run_id=dag_run_id,
        submitted_at=FROZEN_TIME,
    )
    await state_manager_real_ta.task_submit.submit_task(task=task_a)
    await state_manager_real_ta.task_submit.submit_task(task=task_b)

    cleanup_config = TaskConfig(name="my_task", function=dummy_fn)
    with patch.object(registry, "get_task_config", return_value=cleanup_config):
        await state_manager_real_ta.finalize_dag_run_task(task_a)
        run = await state_manager_real_ta.get_dag_run(dag_run_id)
        assert run is not None
        assert run.status.value == "running"

        await state_manager_real_ta.finalize_dag_run_task(task_b)
        run = await state_manager_real_ta.get_dag_run(dag_run_id)
        assert run is not None
        assert run.status.value == "complete"


# ── fail_task ─────────────────────────────────────────────────────────────────


async def dummy_fn():  # pragma: no cover
    pass


def make_task_config(dead_letter_policy: DeadLetterPolicy = DeadLetterPolicy.NONE) -> TaskConfig:
    return TaskConfig(name="my_task", function=dummy_fn, dead_letter_policy=dead_letter_policy)


@pytest.mark.asyncio
async def test_fail_task_no_dlq_writes_redis_only(redis, state_manager):
    """fail_task with NONE policy updates Redis but does not touch the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED)
    task.task_config = make_task_config(DeadLetterPolicy.NONE)

    await state_manager.fail_task(task)

    saved = await state_manager.task_state.get_task(ULID1)
    assert saved.status == TaskStatus.FAILED
    assert await state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


@pytest.mark.asyncio
async def test_fail_task_with_dlq_writes_both_stores(redis, state_manager):
    """fail_task with SAVE policy updates Redis and writes to the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["oops"])
    task.task_config = make_task_config(DeadLetterPolicy.SAVE)

    await state_manager.fail_task(task)

    saved = await state_manager.task_state.get_task(ULID1)
    assert saved.status == TaskStatus.FAILED
    dlq = await state_manager.dead_queue.get_by_ids([str(ULID1)])
    assert len(dlq) == 1
    assert dlq[0].id == ULID1


# ── resubmit_dead_tasks ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resubmit_dead_tasks_requeues_and_clears_dlq(redis, state_manager):
    """All tasks are enqueued in Redis and removed from the DLQ."""
    task1 = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    task2 = Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e2"])
    await state_manager.task_state.save_task(task1)
    await state_manager.task_state.save_task(task2)
    await add_to_dlq(state_manager, task1, FROZEN_TIME)
    await add_to_dlq(state_manager, task2, FROZEN_TIME)

    await state_manager.resubmit_dead_tasks([task1, task2])

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert bytes(ULID2) in queue_members
    assert await state_manager.dead_queue.get_by_ids([str(ULID1), str(ULID2)]) == []


@pytest.mark.asyncio
async def test_resubmit_dead_tasks_is_idempotent(redis, state_manager):
    """Re-running resubmit for a task already in Redis does not raise and clears the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    await state_manager.task_state.save_task(task)
    await add_to_dlq(state_manager, task, FROZEN_TIME)

    await state_manager.resubmit_dead_tasks([task])

    task2 = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e1"])
    await add_to_dlq(state_manager, task2, FROZEN_TIME)
    await state_manager.resubmit_dead_tasks([task2])

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert await state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


# ── dispatch_scheduled_task ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_scheduled_task(redis, state_manager):
    """dispatch_scheduled_task moves a due task from the scheduler into its Redis queue."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SUBMITTED, retry_attempt=1)
    run_at = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
    await state_manager.task_state.save_task(task)
    await schedule(state_manager, task, run_at)

    due = await state_manager.task_scheduler.next_due(["default"])
    assert due is not None
    await state_manager.dispatch_scheduled_task(due)

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert await state_manager.task_scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_dispatch_scheduled_task_skips_cancelled(redis, state_manager):
    """dispatch_scheduled_task does not re-enqueue a task that was cancelled after scheduler acquisition."""
    cancelled = Task(
        id=ULID1, name="retry_task", queue="default", status=TaskStatus.CANCELLED, retry_attempt=1
    )
    await state_manager.task_state.save_task(cancelled)

    stale = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SCHEDULED, retry_attempt=1)
    await state_manager.dispatch_scheduled_task(stale)

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) not in queue_members
    saved = await state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_dispatch_scheduled_task_skips_cancelled_task(redis, state_manager):
    """dispatch_scheduled_task silently skips tasks that are already CANCELLED."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.CANCELLED)
    await state_manager.task_state.save_task(task)

    result = await state_manager.dispatch_scheduled_task(task)

    assert result is task
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) not in members


# ── schedule_retry_task / dispatch recovery ───────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_retry_task_self_heals_via_dispatch(redis, state_manager):
    """If save_task was missed (crash), the scheduler dispatch recovers by re-enqueueing."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.STARTED, retry_attempt=1)
    run_at = FROZEN_TIME

    await state_manager.task_state.save_task(task)
    await schedule(state_manager, task, run_at)

    due = await state_manager.task_scheduler.next_due(["default"])
    assert due is not None
    await state_manager.dispatch_scheduled_task(due)

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in queue_members
    assert await state_manager.task_scheduler.next_due(["default"]) is None


@pytest.mark.asyncio
async def test_dispatch_acquired_record_not_requeued(redis, state_manager):
    """A task removed from the schedule by next_due is not returned on a subsequent call."""
    task = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SUBMITTED, retry_attempt=1)
    run_at = FROZEN_TIME

    await state_manager.task_state.save_task(task)
    await schedule(state_manager, task, run_at)
    await state_manager.task_scheduler.next_due(["default"])

    assert await state_manager.task_scheduler.next_due(["default"]) is None


# ── request_task_cancellation ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_scheduled_task(redis, state_manager):
    """Cancelling a SCHEDULED task removes it from the scheduler and marks it CANCELLED in Redis."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.SCHEDULED, retry_attempt=1)
    run_at = FROZEN_TIME + dt.timedelta(hours=1)
    await schedule(state_manager, task, run_at)
    await state_manager.save_task(task)

    result = await state_manager.request_task_cancellation(ULID1)

    assert result is not None
    assert result.status == TaskStatus.CANCELLED
    assert await state_manager.task_scheduler.next_due(["default"]) is None
    saved = await state_manager.task_state.get_task(ULID1)
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_request_task_cancellation_returns_none_for_missing_task(state_manager):
    """request_task_cancellation returns None when the task does not exist."""
    result = await state_manager.request_task_cancellation(ULID1)
    assert result is None


@pytest.mark.asyncio
async def test_cancel_submitted_task(redis, state_manager):
    """Cancelling a SUBMITTED task removes it from the queue and marks it CANCELLED."""
    task = Task(
        id=ULID1, name="my_task", queue="default", status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME
    )
    pipe = state_manager.task_state.pipeline()
    state_manager.task_state.stage_requeue(pipe, task)
    await pipe.execute()
    await state_manager.task_state.save_task(task)

    result = await state_manager.request_task_cancellation(ULID1)

    assert result is not None
    assert result.status == TaskStatus.CANCELLED
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) not in members
    saved = await state_manager.task_state.get_task(ULID1)
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_started_task_publishes_message(state_manager):
    """Cancelling a STARTED task signals the cancel event via the shared pubsub channel."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)
    await state_manager.task_state.save_task(task)

    with state_manager.cancel_event(ULID1):
        cancel_listener = asyncio.create_task(state_manager.run_cancel_listener())
        monitor = asyncio.create_task(state_manager.monitor_task_cancellation(ULID1))
        await asyncio.sleep(0.05)  # let the listener subscribe
        result = await state_manager.request_task_cancellation(ULID1)

        assert result is not None
        assert result.status == TaskStatus.STARTED  # status unchanged — worker handles it
        with pytest.raises(UserCancellationError):
            await asyncio.wait_for(monitor, timeout=1.0)

        cancel_listener.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cancel_listener


@pytest.mark.asyncio
async def test_cancel_terminal_task_raises(state_manager):
    """Cancelling a task in a terminal status raises TaskException."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.COMPLETED)
    await state_manager.task_state.save_task(task)

    with pytest.raises(TaskException, match="cannot be cancelled"):
        await state_manager.request_task_cancellation(ULID1)


# ── request_dag_cancellation ───────────────────────────────────────────────────
#
# Uses state_manager_real_ta (real RedisTaskState/RedisTaskSubmit via FakeRedis)
# rather than the Dummy-backed `state_manager` fixture: get_dag_run is not
# implemented on DummyTaskState (see tests/conftest.py), and per CLAUDE.md this
# dispatch-adjacent, race-prone path needs a real-backend test regardless.


@pytest.mark.asyncio
async def test_request_dag_cancellation_returns_none_for_unknown_run(state_manager_real_ta):
    """request_dag_cancellation returns None when the DAG run has never been registered."""
    result = await state_manager_real_ta.request_dag_cancellation(ULID())
    assert result is None


@pytest.mark.asyncio
async def test_request_dag_cancellation_sweeps_every_status_bucket(state_manager_real_ta):
    """
    A single sweep correctly buckets SCHEDULED/SUBMITTED/STARTED/already-terminal tasks.

    SCHEDULED and SUBMITTED tasks are cancelled immediately; STARTED is left for the
    trailing broadcast; already-terminal (COMPLETED) tasks are counted but untouched.
    """
    dag_run_id = ULID()
    scheduled_id, submitted_id, started_id, completed_id = ULID(), ULID(), ULID(), ULID()
    run_at = FROZEN_TIME + dt.timedelta(hours=1)

    scheduled_task = Task(
        id=scheduled_id, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED
    )
    await state_manager_real_ta.submit_task(scheduled_task)
    scheduled_task.set_status(TaskStatus.SCHEDULED)
    await state_manager_real_ta.task_scheduler.add(scheduled_task, run_at)
    await state_manager_real_ta.task_state.save_task(scheduled_task)

    submitted_task = Task(
        id=submitted_id, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED
    )
    await state_manager_real_ta.submit_task(submitted_task)

    started_task = Task(
        id=started_id, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED
    )
    await state_manager_real_ta.submit_task(started_task)
    started_task.set_status(TaskStatus.STARTED)
    await state_manager_real_ta.task_state.save_task(started_task)

    completed_task = Task(
        id=completed_id, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED
    )
    await state_manager_real_ta.submit_task(completed_task)
    completed_task.set_status(TaskStatus.COMPLETED)
    await state_manager_real_ta.task_state.save_task(completed_task)

    result = await state_manager_real_ta.request_dag_cancellation(dag_run_id)

    assert result is not None
    assert result.already_terminal == 1
    assert result.cancelled_immediately == 2
    assert result.signalled_running == 1
    assert {(t.task_id, t.status) for t in result.tasks} == {
        (scheduled_id, "cancelled"),
        (submitted_id, "cancelled"),
        (started_id, "signalled"),
        (completed_id, "already_terminal"),
    }

    assert (await state_manager_real_ta.task_state.get_task(scheduled_id)).status == TaskStatus.CANCELLED
    assert (await state_manager_real_ta.task_state.get_task(submitted_id)).status == TaskStatus.CANCELLED
    # STARTED tasks are only signalled here, not directly transitioned.
    assert (await state_manager_real_ta.task_state.get_task(started_id)).status == TaskStatus.STARTED
    assert (await state_manager_real_ta.task_state.get_task(completed_id)).status == TaskStatus.COMPLETED
    assert await state_manager_real_ta.task_scheduler.next_due(["default"]) is None
    assert await state_manager_real_ta.is_dag_run_cancelling(dag_run_id) is True
    # started_id is still STARTED (unresolved) -- the run isn't fully settled yet.
    run_after = await state_manager_real_ta.get_dag_run(dag_run_id)
    assert run_after.status == DagRunStatus.CANCELLING


@pytest.mark.asyncio
async def test_request_dag_cancellation_settles_run_status_to_cancelled(state_manager_real_ta):
    """
    get_dag_run reports CANCELLED (not stuck at CANCELLING) once the sweep is the only work left.

    Regression test: the SCHEDULED/SUBMITTED/UNSUBMITTED branch cancels tasks
    directly, bypassing TaskProcessor -- which is what normally calls
    finalize_dag_run_task -> record_dag_run_task_terminal on a terminal
    transition. Without an equivalent call here, the run's completed/failed
    counters would never reach len(task_ids), and get_dag_run's CANCELLING-vs-
    CANCELLED derivation (state_manager.py's request_dag_cancellation docstring)
    would report CANCELLING forever even after every task has actually stopped.
    """
    dag_run_id = ULID()
    task_a = Task(
        id=ULID1, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED
    )
    task_b = Task(
        id=ULID2, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED
    )
    await state_manager_real_ta.submit_task(task_a)
    await state_manager_real_ta.submit_task(task_b)

    await state_manager_real_ta.request_dag_cancellation(dag_run_id)

    run = await state_manager_real_ta.get_dag_run(dag_run_id)
    assert run is not None
    assert run.status == DagRunStatus.CANCELLED


@pytest.mark.asyncio
async def test_request_dag_cancellation_signals_all_started_tasks_via_one_broadcast(state_manager_real_ta):
    """
    N concurrently-STARTED tasks in one DAG run are all cancelled via a single broadcast.

    This is the core noise-avoidance property of DAG cancellation (see
    docs/dag-cancellation-design.md): publish_dag_cancellation is called exactly
    once regardless of how many STARTED tasks the run has, and every worker-local
    cancel_event for that run fires off that one message.
    """
    dag_run_id = ULID()
    task_ids = [ULID() for _ in range(5)]
    for tid in task_ids:
        task = Task(
            id=tid, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED
        )
        await state_manager_real_ta.submit_task(task)
        task.set_status(TaskStatus.STARTED)
        await state_manager_real_ta.task_state.save_task(task)

    with contextlib.ExitStack() as stack:
        for tid in task_ids:
            stack.enter_context(state_manager_real_ta.cancel_event(tid, dag_run_id))

        cancel_listener = asyncio.create_task(state_manager_real_ta.run_cancel_listener())
        await asyncio.sleep(0.05)  # let the listener subscribe

        with patch.object(
            state_manager_real_ta.cancellation_bus,
            "publish_dag_cancellation",
            wraps=state_manager_real_ta.cancellation_bus.publish_dag_cancellation,
        ) as mock_publish:
            result = await state_manager_real_ta.request_dag_cancellation(dag_run_id)
            await asyncio.sleep(0.1)  # let the listener process the one broadcast

        assert result is not None
        assert result.signalled_running == 5
        mock_publish.assert_awaited_once_with(dag_run_id)
        assert all(state_manager_real_ta._cancel_events[tid].event.is_set() for tid in task_ids)

        cancel_listener.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cancel_listener


@pytest.mark.asyncio
async def test_request_dag_cancellation_no_broadcast_when_nothing_started(state_manager_real_ta):
    """No pub/sub message is published when the run has no STARTED tasks to signal."""
    dag_run_id = ULID()
    task = Task(id=ULID1, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED)
    await state_manager_real_ta.submit_task(task)

    with patch.object(
        state_manager_real_ta.cancellation_bus, "publish_dag_cancellation", new_callable=AsyncMock
    ) as mock_publish:
        result = await state_manager_real_ta.request_dag_cancellation(dag_run_id)

    assert result is not None
    assert result.signalled_running == 0
    mock_publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_mark_and_is_dag_run_cancelling_proxy(state_manager_real_ta):
    """mark_dag_run_cancelling/is_dag_run_cancelling proxy through to the task_state adapter."""
    dag_run_id = ULID()
    task = Task(id=ULID1, name="my_task", queue="default", dag_run_id=dag_run_id, status=TaskStatus.SUBMITTED)
    await state_manager_real_ta.submit_task(task)

    assert await state_manager_real_ta.is_dag_run_cancelling(dag_run_id) is False
    await state_manager_real_ta.mark_dag_run_cancelling(dag_run_id)
    assert await state_manager_real_ta.is_dag_run_cancelling(dag_run_id) is True


# ── submit_task (rate-limited branch) ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_task_rate_limited_branch(redis, state_manager_real_ta):
    """submit_task routes through submit_rate_limited_task when queue has rate config."""
    await state_manager_real_ta.routing.save_queue_config(
        QueueConfig(
            name="default",
            rate_numerator=5,
            rate_denominator=1,
            rate_period=RatePeriod.MINUTE,
        )
    )
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.UNSUBMITTED)

    await state_manager_real_ta.submit_task(task)

    assert task.status == TaskStatus.SUBMITTED
    count = await redis.zcard(state_manager_real_ta.task_state.QUEUE_RATE_LIMITER(queue="default"))
    assert count == 1
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_submit_task_raises_and_reverts_status_when_rate_limited(redis, state_manager_real_ta):
    """submit_task raises TaskRateLimitedError and reverts task.status when the limit is exceeded."""
    await state_manager_real_ta.routing.save_queue_config(
        QueueConfig(
            name="default",
            rate_numerator=1,
            rate_denominator=1,
            rate_period=RatePeriod.MINUTE,
        )
    )
    occupier = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.UNSUBMITTED)
    await state_manager_real_ta.submit_task(occupier)

    rejected = Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.UNSUBMITTED)

    with pytest.raises(TaskRateLimitedError):
        await state_manager_real_ta.submit_task(rejected)

    assert rejected.status == TaskStatus.UNSUBMITTED
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID2) not in members


# ── task_in_registry ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_task_in_registry(state_manager):
    """Test that a task is correctly identified as being in the active tasks registry."""
    task = Task(
        id=ULID2,
        name="No Submitted At",
        status=TaskStatus.SUBMITTED,
        queue="default",
        submitted_at=None,
    )

    assert task.id not in state_manager.current_tasks_by_queue[task.queue]

    with state_manager.task_in_registry(task):
        assert task.id in state_manager.current_tasks_by_queue[task.queue]

    assert task.id not in state_manager.current_tasks_by_queue[task.queue]


# ── cancel_event ──────────────────────────────────────────────────────────────


def test_cancel_event_registers_and_deregisters(state_manager):
    """cancel_event adds the event on enter and removes it on exit."""
    assert ULID1 not in state_manager._cancel_events

    with state_manager.cancel_event(ULID1):
        assert ULID1 in state_manager._cancel_events

    assert ULID1 not in state_manager._cancel_events


def test_cancel_event_stores_dag_run_id(state_manager):
    """cancel_event registers dag_run_id alongside the event, for signal_cancel_dag to match on."""
    dag_run_id = ULID2
    with state_manager.cancel_event(ULID1, dag_run_id):
        assert state_manager._cancel_events[ULID1].dag_run_id == dag_run_id


def test_cancel_event_defaults_dag_run_id_to_none(state_manager):
    """cancel_event's dag_run_id parameter is optional, for callers with no DAG run."""
    with state_manager.cancel_event(ULID1):
        assert state_manager._cancel_events[ULID1].dag_run_id is None


# ── signal_cancel_dag ─────────────────────────────────────────────────────────


def test_signal_cancel_dag_fires_matching_events_only(state_manager):
    """signal_cancel_dag sets the event for every in-flight task with a matching dag_run_id, and no others."""
    dag_run_id = ULID2
    other_dag_run_id = ULID()
    with (
        state_manager.cancel_event(ULID1, dag_run_id),
        state_manager.cancel_event(other_dag_run_id, other_dag_run_id),
    ):
        count = state_manager.signal_cancel_dag(dag_run_id)

        assert count == 1
        assert state_manager._cancel_events[ULID1].event.is_set()
        assert not state_manager._cancel_events[other_dag_run_id].event.is_set()


def test_signal_cancel_dag_fires_all_matching_events(state_manager):
    """signal_cancel_dag fires every in-flight task belonging to the run, not just one."""
    dag_run_id = ULID2
    task_ids = [ULID(), ULID(), ULID()]
    with contextlib.ExitStack() as stack:
        for tid in task_ids:
            stack.enter_context(state_manager.cancel_event(tid, dag_run_id))

        count = state_manager.signal_cancel_dag(dag_run_id)

        assert count == 3
        assert all(state_manager._cancel_events[tid].event.is_set() for tid in task_ids)


def test_signal_cancel_dag_returns_zero_when_no_matching_tasks(state_manager):
    """signal_cancel_dag returns 0 and is a no-op when no in-flight task belongs to the run."""
    assert state_manager.signal_cancel_dag(ULID()) == 0


# ── monitor_task_cancellation ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_monitor_task_cancellation_raises_on_cancel_message(state_manager):
    """monitor_task_cancellation raises UserCancellationError when signal_cancel is called."""
    task_id = ULID1
    with state_manager.cancel_event(task_id):
        monitor = asyncio.create_task(state_manager.monitor_task_cancellation(task_id))
        state_manager.signal_cancel(task_id)
        with pytest.raises(UserCancellationError):
            await monitor


@pytest.mark.asyncio
async def test_monitor_task_cancellation_does_not_exit_without_message(state_manager):
    """monitor_task_cancellation keeps running when no cancel signal is sent."""
    task_id = ULID1
    with state_manager.cancel_event(task_id):
        monitor = asyncio.create_task(state_manager.monitor_task_cancellation(task_id))
        await asyncio.sleep(0.1)
        assert not monitor.done(), "monitor should still be running when no cancel signal is sent"
        monitor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor


# ── schedule_new_task ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_new_task_sets_status_and_submitted_at(state_manager):
    """schedule_new_task sets status=SCHEDULED and saves the task."""
    task = Task(id=ULID1, name="t", version=1, queue="default")
    run_at = FROZEN_TIME + dt.timedelta(hours=1)

    result = await state_manager.schedule_new_task(task, run_at)

    assert result is task
    assert task.status == TaskStatus.SCHEDULED
    assert task.submitted_at is None


@pytest.mark.asyncio
async def test_schedule_new_task_appears_in_scheduler(state_manager):
    """schedule_new_task registers the task in the scheduler sorted set."""
    task = Task(id=ULID1, name="t", version=1, queue="default")
    run_at = FROZEN_TIME + dt.timedelta(hours=1)

    await state_manager.schedule_new_task(task, run_at)

    scheduled = await state_manager.task_scheduler.get_by_filter(queue="default")
    assert len(scheduled) == 1
    assert scheduled[0][0].id == ULID1
    assert scheduled[0][0].status == TaskStatus.SCHEDULED


@pytest.mark.asyncio
async def test_schedule_new_task_does_not_increment_retry_attempt(state_manager):
    """schedule_new_task leaves retry_attempt=0 for a fresh task."""
    task = Task(id=ULID1, name="t", version=1, queue="default", retry_attempt=0)
    run_at = FROZEN_TIME + dt.timedelta(hours=1)

    await state_manager.schedule_new_task(task, run_at)

    assert task.retry_attempt == 0


# ── schedule_retry_task ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_retry_task_adds_to_scheduler(redis, state_manager):
    """schedule_retry_task saves the task and adds it to the scheduled queue."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SCHEDULED)
    run_at = FROZEN_TIME + dt.timedelta(minutes=5)

    result = await state_manager.schedule_retry_task(task, run_at)

    assert result is task
    due = await state_manager.task_scheduler.next_due(["default"])
    assert due is not None
    assert due.id == ULID1


# ── update_task_heartbeat / remove_task_heartbeat / get_active_tasks ─────────


@pytest.mark.asyncio
async def test_update_task_heartbeat_sets_timestamp(redis, state_manager_real_ta):
    """update_task_heartbeat stamps heartbeat_at on the task and persists it."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)
    await state_manager_real_ta.task_state.save_task(task)
    assert task.heartbeat_at is None

    await state_manager_real_ta.update_task_heartbeat(task)

    assert task.heartbeat_at is not None
    score = await redis.zscore(
        state_manager_real_ta.task_state.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)
    )
    assert score is not None


@pytest.mark.asyncio
async def test_remove_task_heartbeat_clears_entry(redis, state_manager_real_ta):
    """remove_task_heartbeat removes the task from the heartbeat sorted set."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)
    await state_manager_real_ta.task_state.save_task(task)
    await state_manager_real_ta.update_task_heartbeat(task)

    await state_manager_real_ta.remove_task_heartbeat(task)

    score = await redis.zscore(
        state_manager_real_ta.task_state.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)
    )
    assert score is None


@pytest.mark.asyncio
async def test_get_active_tasks_returns_heartbeating_tasks(state_manager_real_ta):
    """get_active_tasks returns tasks currently registered in any heartbeat sorted set."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)
    await state_manager_real_ta.task_state.save_task(task)
    await state_manager_real_ta.update_task_heartbeat(task)

    active = await state_manager_real_ta.get_active_tasks({"default"})

    assert len(active) == 1
    assert active[0].id == ULID1


# ── queue_retry_task ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_queue_retry_task_requeues_immediately(redis, state_manager):
    """queue_retry_task sets status to SUBMITTED and enqueues the task."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)

    result = await state_manager.queue_retry_task(task)

    assert result is task
    assert result.status == TaskStatus.SUBMITTED
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in members


@pytest.mark.asyncio
async def test_queue_retry_task_bypasses_rate_limit(redis, state_manager_real_ta):
    """
    queue_retry_task succeeds and does not touch the rate-limiter bucket even when the queue is at capacity.

    Retries must never be gated by rate limits: blocking them would require manual
    intervention (e.g. moving to the DLQ) rather than allowing automatic recovery.
    """
    await state_manager_real_ta.routing.save_queue_config(
        QueueConfig(name="default", rate_numerator=1, rate_denominator=1, rate_period=RatePeriod.MINUTE)
    )
    rate_key = state_manager_real_ta.task_state.QUEUE_RATE_LIMITER(queue="default")
    await redis.zadd(rate_key, {ULID2.bytes: FROZEN_TIME.timestamp()})

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    result = await state_manager_real_ta.queue_retry_task(task)

    assert result.status == TaskStatus.SUBMITTED
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in members
    count = await redis.zcard(rate_key)
    assert count == 1


@pytest.mark.asyncio
async def test_schedule_retry_task_bypasses_rate_limit(redis, state_manager_real_ta):
    """
    schedule_retry_task succeeds and does not touch the rate-limiter bucket even when the queue is at capacity.

    Retries must never be gated by rate limits: blocking them would require manual
    intervention (e.g. moving to the DLQ) rather than allowing automatic recovery.
    """
    await state_manager_real_ta.routing.save_queue_config(
        QueueConfig(name="default", rate_numerator=1, rate_denominator=1, rate_period=RatePeriod.MINUTE)
    )
    rate_key = state_manager_real_ta.task_state.QUEUE_RATE_LIMITER(queue="default")
    await redis.zadd(rate_key, {ULID2.bytes: FROZEN_TIME.timestamp()})

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SCHEDULED)
    run_at = FROZEN_TIME + dt.timedelta(minutes=5)
    result = await state_manager_real_ta.schedule_retry_task(task, run_at)

    assert result is task
    due = await state_manager_real_ta.task_scheduler.next_due(["default"])
    assert due is not None
    assert due.id == ULID1
    count = await redis.zcard(rate_key)
    assert count == 1


# ── active_tasks_per_queue ────────────────────────────────────────────────────


def test_active_tasks_per_queue_reflects_registry(state_manager):
    """
    active_tasks_per_queue mirrors the tasks currently tracked in task_in_registry.

    this is only true while there is one task consumer
    """
    task1 = Task(id=ULID1, name="t1", version=1, queue="q1")
    task2 = Task(id=ULID2, name="t2", version=1, queue="q1")

    assert state_manager.active_tasks_per_queue == {}

    with state_manager.task_in_registry(task1):
        assert state_manager.active_tasks_per_queue == {"q1": 1}
        with state_manager.task_in_registry(task2):
            assert state_manager.active_tasks_per_queue == {"q1": 2}
        assert state_manager.active_tasks_per_queue == {"q1": 1}

    assert state_manager.active_tasks_per_queue == {"q1": 0}


# ── submit_dag ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_dag_simple_chain(state_manager):
    """submit_dag submits root tasks and returns Task objects."""
    root = DAGNode("fetch_data")
    child = DAGNode("process_data")
    root.then(child)

    dag_run_id, submitted = await state_manager.submit_dag(root, name="my-run")

    assert dag_run_id is not None
    assert len(submitted) == 1
    assert submitted[0].id == root.id
    assert submitted[0].status == TaskStatus.SUBMITTED
    assert submitted[0].dag_run_id is not None
    assert submitted[0].dag_run_name == "my-run"


@pytest.mark.asyncio
async def test_submit_dag_defaults_dag_run_id_when_no_name_and_no_override(state_manager):
    """submit_dag self-generates a dag_run_id when neither name nor dag_run_id is supplied."""
    root = DAGNode("fetch_data")

    dag_run_id, submitted = await state_manager.submit_dag(root, name="unnamed")

    assert submitted[0].dag_run_id == dag_run_id


@pytest.mark.asyncio
async def test_submit_dag_honors_explicit_dag_run_id_override(state_manager):
    """submit_dag uses a caller-supplied dag_run_id instead of self-generating one."""
    root = DAGNode("fetch_data")
    forced_id = ULID()

    dag_run_id, submitted = await state_manager.submit_dag(root, name="forced", dag_run_id=forced_id)

    assert dag_run_id == forced_id
    assert submitted[0].dag_run_id == forced_id


@pytest.mark.asyncio
async def test_submit_dag_multi_root_shares_dag_run_id(state_manager):
    """All roots submitted in the same submit_dag call share a single dag_run_id."""
    branch_a = DAGNode("branch_a")
    branch_b = DAGNode("branch_b")
    collector = DAGNode("collect")
    DAGNode.merge(branch_a, branch_b, into=collector)

    state_manager.init_fan_in = AsyncMock()

    dag_run_id, submitted = await state_manager.submit_dag(branch_a, branch_b, name="multi-root-run")

    assert dag_run_id is not None
    assert len(submitted) == 2
    assert submitted[0].dag_run_id is not None
    assert submitted[0].dag_run_id == submitted[1].dag_run_id
    assert submitted[0].dag_run_id == dag_run_id
    assert submitted[0].dag_run_name == "multi-root-run"
    assert submitted[1].dag_run_name == "multi-root-run"


@pytest.mark.asyncio
async def test_submit_dag_fan_in_initialises_fan_in_sets(state_manager):
    """submit_dag pre-populates Redis fan-in sets before submitting tasks."""
    branch_a = DAGNode("branch_a")
    branch_b = DAGNode("branch_b")
    collector = DAGNode("collect")
    DAGNode.merge(branch_a, branch_b, into=collector)

    state_manager.init_fan_in = AsyncMock()

    dag_run_id, submitted = await state_manager.submit_dag(branch_a, branch_b, name="fan-in-run")

    # init_fan_in must be called with the run's dag_run_id and the collector's fan-in key
    fan_in_key = f"dag:fan-in:{collector.id}"
    state_manager.init_fan_in.assert_awaited_once_with(dag_run_id, fan_in_key, {branch_a.id, branch_b.id})
    assert len(submitted) == 2


# ── dispatch_cron_dag ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_cron_dag_submits_task_and_reschedules(redis, state_manager):
    """dispatch_cron_dag submits the root task and reschedules the entry."""
    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(
        name="daily_job",
        cron_expr="0 0 * * *",
        dag_spec=spec,
        concurrency_policy=ConcurrencyPolicy.ALWAYS,
    )
    run_at = FROZEN_TIME

    await state_manager.dispatch_cron_dag(entry, run_at)

    # A task should now exist in the DummyTaskAdapter store
    stored = state_manager.task_state._store
    assert len(stored) == 1
    task = next(iter(stored.values()))
    assert task.name == "my_job"
    assert task.status == TaskStatus.SUBMITTED

    # The entry should be rescheduled in the cron sorted set
    members = await redis.zrange("cron-schedule", 0, -1)
    assert bytes(entry.id) in members


@pytest.mark.asyncio
async def test_dispatch_cron_dag_skip_if_running_skips_when_active(redis, state_manager):
    """dispatch_cron_dag skips dispatch but reschedules when concurrency_policy=SKIP_IF_RUNNING and previous run is active."""
    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(
        name="guarded_job",
        cron_expr="0 0 * * *",
        dag_spec=spec,
        concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING,
    )

    # Plant an active task that the skip-guard will find
    active_task = Task(id=ULID1, name="my_job", queue="default", status=TaskStatus.STARTED)
    await state_manager.task_state.save_task(active_task)
    await redis.set(f"cron-active:{entry.id}", str(ULID1))

    run_at = FROZEN_TIME
    await state_manager.dispatch_cron_dag(entry, run_at)

    # No new tasks should have been submitted (only the pre-planted active one)
    stored = state_manager.task_state._store
    assert len(stored) == 1
    assert ULID1 in stored

    # Entry must still be rescheduled
    members = await redis.zrange("cron-schedule", 0, -1)
    assert bytes(entry.id) in members


@pytest.mark.asyncio
async def test_dispatch_cron_dag_skips_when_dispatch_lock_lost(redis, state_manager):
    """
    dispatch_cron_dag does nothing at all if another dispatcher already holds the lock.

    Distinct from the SKIP_IF_RUNNING "previous run still active" skip: this is the
    dispatch-lock guard against two dispatchers racing to fire the same due occurrence
    (e.g. during a scheduler restart), and applies regardless of concurrency_policy.
    """
    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(
        name="unguarded_job",
        cron_expr="0 0 * * *",
        dag_spec=spec,
        concurrency_policy=ConcurrencyPolicy.ALWAYS,
    )

    with patch.object(
        state_manager.cron_dag_scheduler, "try_acquire_dispatch_lock", AsyncMock(return_value=False)
    ) as mock_acquire:
        await state_manager.dispatch_cron_dag(entry, FROZEN_TIME)

    mock_acquire.assert_awaited_once_with(entry.id)
    # Nothing should have been submitted or rescheduled -- the method returned immediately.
    stored = state_manager.task_state._store
    assert len(stored) == 0
    members = await redis.zrange("cron-schedule", 0, -1)
    assert bytes(entry.id) not in members


@pytest.mark.asyncio
async def test_dispatch_cron_dag_skip_if_running_records_active_task_when_not_skipping(redis, state_manager):
    """dispatch_cron_dag records the new root task when concurrency_policy=SKIP_IF_RUNNING and no prior run is active."""
    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(
        name="guarded_job",
        cron_expr="0 0 * * *",
        dag_spec=spec,
        concurrency_policy=ConcurrencyPolicy.SKIP_IF_RUNNING,
    )
    # No active run planted — guard should not fire

    run_at = FROZEN_TIME
    await state_manager.dispatch_cron_dag(entry, run_at)

    # The new root task should have been submitted
    stored = state_manager.task_state._store
    assert len(stored) == 1

    # The active-run key should now record the new task ID
    active_key = f"cron-active:{entry.id}"
    active_val = await redis.get(active_key)
    assert active_val is not None


@pytest.mark.asyncio
async def test_dispatch_cron_dag_fan_in_dag_initialises_sets(state_manager):
    """dispatch_cron_dag pre-populates fan-in sets when the DAG contains fan-in nodes."""
    # Build a diamond DAG: root → (a, b) → collector (fan-in)
    root_node = DAGNode("root_job")
    branch_a = DAGNode("branch_a")
    branch_b = DAGNode("branch_b")
    collector = DAGNode("collector")
    root_node.then(branch_a, branch_b)
    DAGNode.merge(branch_a, branch_b, into=collector)

    root_spec = root_node.to_spec()
    entry = CronDAGEntry(
        name="fan_in_job",
        cron_expr="0 0 * * *",
        dag_spec=root_spec,
        concurrency_policy=ConcurrencyPolicy.ALWAYS,
    )

    run_at = FROZEN_TIME
    with patch.object(state_manager.task_state, "stage_init_fan_in") as mock_stage_init:
        await state_manager.dispatch_cron_dag(entry, run_at)

    # stage_init_fan_in must have been called for the collector's fan-in key
    assert mock_stage_init.call_count >= 1


@pytest.mark.asyncio
async def test_dispatch_cron_dag_stages_submission_in_pipeline_for_non_rate_limited_queue(state_manager):
    """For non-rate-limited queues, submission is staged in the same pipeline (no separate submit_task call)."""
    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(
        name="daily_job",
        cron_expr="0 0 * * *",
        dag_spec=spec,
        concurrency_policy=ConcurrencyPolicy.ALWAYS,
    )

    with (
        patch.object(
            state_manager.task_state, "stage_submit_task", wraps=state_manager.task_state.stage_submit_task
        ) as mock_stage,
        patch.object(state_manager, "submit_task", new_callable=AsyncMock) as mock_submit,
    ):
        await state_manager.dispatch_cron_dag(entry, FROZEN_TIME)

    mock_stage.assert_called_once()
    mock_submit.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_cron_dag_falls_back_to_submit_task_for_rate_limited_queue(state_manager):
    """For rate-limited queues, dispatch_cron_dag falls back to submit_task() to enforce the rate limit."""
    await state_manager.routing.save_queue_config(
        QueueConfig(name="default", rate_numerator=5, rate_denominator=1, rate_period=RatePeriod.MINUTE)
    )

    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(
        name="daily_job",
        cron_expr="0 0 * * *",
        dag_spec=spec,
        concurrency_policy=ConcurrencyPolicy.ALWAYS,
    )

    with (
        patch.object(state_manager.task_state, "stage_submit_task") as mock_stage,
        patch.object(state_manager, "submit_task", new_callable=AsyncMock) as mock_submit,
    ):
        await state_manager.dispatch_cron_dag(entry, FROZEN_TIME)

    mock_stage.assert_not_called()
    mock_submit.assert_called_once()


@pytest.mark.asyncio
async def test_dispatch_cron_dag_swallows_rate_limit_rejection(state_manager, caplog):
    """A rate-limited root task doesn't crash dispatch_cron_dag -- it's logged and skipped."""
    await state_manager.routing.save_queue_config(
        QueueConfig(name="default", rate_numerator=5, rate_denominator=1, rate_period=RatePeriod.MINUTE)
    )

    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(
        name="daily_job",
        cron_expr="0 0 * * *",
        dag_spec=spec,
        concurrency_policy=ConcurrencyPolicy.ALWAYS,
    )

    with (
        patch.object(
            state_manager, "submit_task", side_effect=TaskRateLimitedError("queue is full")
        ) as mock_submit,
        caplog.at_level(logging.WARNING),
    ):
        await state_manager.dispatch_cron_dag(entry, FROZEN_TIME)  # must not raise

    mock_submit.assert_called_once()
    assert any("rejected by queue" in record.message for record in caplog.records)


# ── resolve_queue ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def testresolve_queue_no_routing_returns_task_queue(state_manager):
    """When no routing config exists, resolve_queue returns the task's own queue."""
    task = Task(id=ULID1, name="my_task", version=1, queue="original")
    result = await state_manager.resolve_queue(task)
    assert result == "original"


@pytest.mark.asyncio
async def testresolve_queue_single_strategy(state_manager):
    """SINGLE routing always returns the configured queue."""
    config = RoutingConfig(
        task_name="my_task", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["target"]
    )
    await state_manager.routing.save_routing_config(config)

    task = Task(id=ULID1, name="my_task", version=1, queue="ignored")
    result = await state_manager.resolve_queue(task)
    assert result == "target"


@pytest.mark.asyncio
async def testresolve_queue_weighted_strategy_returns_one_of_configured_queues(state_manager):
    """WEIGHTED routing returns one of the configured queues."""
    config = RoutingConfig(
        task_name="my_task",
        task_version=1,
        strategy=RoutingStrategy.WEIGHTED,
        queues=["fast", "slow"],
        weights=[1.0, 1.0],
    )
    await state_manager.routing.save_routing_config(config)

    task = Task(id=ULID1, name="my_task", version=1, queue="ignored")
    results = {await state_manager.resolve_queue(task) for _ in range(20)}
    assert results <= {"fast", "slow"}
    assert len(results) > 0


@pytest.mark.asyncio
async def testresolve_queue_routing_is_version_specific(state_manager):
    """Routing config is looked up by (name, version); a different version falls through."""
    config = RoutingConfig(
        task_name="my_task", task_version=2, strategy=RoutingStrategy.SINGLE, queues=["routed"]
    )
    await state_manager.routing.save_routing_config(config)

    task_v1 = Task(id=ULID1, name="my_task", version=1, queue="original")
    assert await state_manager.resolve_queue(task_v1) == "original"

    task_v2 = Task(id=ULID2, name="my_task", version=2, queue="original")
    assert await state_manager.resolve_queue(task_v2) == "routed"


# ── cache behaviour ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_queue_config_caches_result(redis, session_factory, dummy_task_adapter):
    """get_queue_config returns a cached value on the second call."""
    routing_backend = SQLRoutingBackend(session_factory)
    sm = StateManager(
        routing_backend,
        task_state=dummy_task_adapter,
        task_submit=DummyTaskSubmit(dummy_task_adapter._store),
        dead_queue=RedisDeadQueue(redis, dummy_task_adapter),
        task_scheduler=RedisTaskScheduler(redis, dummy_task_adapter, routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )
    await sm.routing.save_queue_config(QueueConfig(name="q1", max_concurrent=5))
    r1 = await sm.get_queue_config("q1")
    assert r1 is not None
    assert r1.max_concurrent == 5
    # Replace the adapter method so any second SQL call would return a different value
    sm.routing.get_queue_config = AsyncMock(return_value=QueueConfig(name="q1", max_concurrent=99))
    r2 = await sm.get_queue_config("q1")
    assert r2 is not None
    assert r2.max_concurrent == 5, "cache should serve the original value"


@pytest.mark.asyncio
async def test_get_routing_config_caches_result(redis, session_factory, dummy_task_adapter):
    """get_routing_config returns a cached value on the second call."""
    routing_backend = SQLRoutingBackend(session_factory)
    sm = StateManager(
        routing_backend,
        task_state=dummy_task_adapter,
        task_submit=DummyTaskSubmit(dummy_task_adapter._store),
        dead_queue=RedisDeadQueue(redis, dummy_task_adapter),
        task_scheduler=RedisTaskScheduler(redis, dummy_task_adapter, routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )
    config = RoutingConfig(task_name="t", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["routed"])
    await sm.routing.save_routing_config(config)
    r1 = await sm.get_routing_config("t", 1)
    assert r1 is not None
    assert r1.queues == ["routed"]

    sm.routing.get_routing_config = AsyncMock(
        return_value=RoutingConfig(
            task_name="t", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["other"]
        )
    )
    r2 = await sm.get_routing_config("t", 1)
    assert r2 is not None, "cache should serve the original value"
    assert r2.queues == ["routed"], "cache should serve the original value"


# ── explicit invalidation ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_queue_config_invalidates_cache_and_bumps_refresh_tag(
    redis, session_factory, dummy_task_adapter
):
    """save_queue_config writes to SQL, clears the cache entry, and bumps refresh_tag for containing roles."""
    routing_backend = SQLRoutingBackend(session_factory)
    sm = StateManager(
        routing_backend,
        task_state=dummy_task_adapter,
        task_submit=DummyTaskSubmit(dummy_task_adapter._store),
        dead_queue=RedisDeadQueue(redis, dummy_task_adapter),
        task_scheduler=RedisTaskScheduler(redis, dummy_task_adapter, routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )
    qca = SQLQueueConfigAdapter(session_factory)
    await qca.save_queue_config(QueueConfig(name="q1", max_concurrent=5))
    await qca.save_role("role_a", {"q1"})
    tag_before = await sm.poll_refresh_signal("role_a")

    # Warm the cache
    r1 = await sm.get_queue_config("q1")
    assert r1 is not None
    assert r1.max_concurrent == 5

    # Update via the StateManager wrapper
    await sm.save_queue_config(QueueConfig(name="q1", max_concurrent=20))

    # Cache should be cleared — next call hits SQL and returns the new value
    r2 = await sm.get_queue_config("q1")
    assert r2 is not None
    assert r2.max_concurrent == 20

    # refresh_tag should have been bumped for role_a (contains q1)
    tag_after = await sm.get_refresh_tag("role_a")
    assert tag_after != tag_before


@pytest.mark.asyncio
async def test_save_routing_config_invalidates_cache_and_bumps_version(
    redis, session_factory, dummy_task_adapter
):
    """save_routing_config writes to SQL, clears the cache entry, and updates routing:version to a new ULID."""
    routing_backend = SQLRoutingBackend(session_factory)
    sm = StateManager(
        routing_backend,
        task_state=dummy_task_adapter,
        task_submit=DummyTaskSubmit(dummy_task_adapter._store),
        dead_queue=RedisDeadQueue(redis, dummy_task_adapter),
        task_scheduler=RedisTaskScheduler(redis, dummy_task_adapter, routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )

    config_v1 = RoutingConfig(
        task_name="t", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["q_old"]
    )
    await sm.routing.save_routing_config(config_v1)

    # Warm the cache
    r1 = await sm.get_routing_config("t", 1)
    assert r1 is not None
    assert r1.queues == ["q_old"]

    version_before = await redis.get("routing:version")

    config_v2 = RoutingConfig(
        task_name="t", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["q_new"]
    )
    await sm.save_routing_config(config_v2)

    # Cache should be cleared — next call hits SQL and returns the new value
    r2 = await sm.get_routing_config("t", 1)
    assert r2 is not None
    assert r2.queues == ["q_new"]

    version_after = await redis.get("routing:version")
    assert version_after is not None
    assert version_before != version_after


@pytest.mark.asyncio
async def test_delete_routing_config_invalidates_cache_and_bumps_version(
    redis, session_factory, dummy_task_adapter
):
    """delete_routing_config removes from SQL, clears the cache entry, and updates routing:version to a new ULID."""
    routing_backend = SQLRoutingBackend(session_factory)
    sm = StateManager(
        routing_backend,
        task_state=dummy_task_adapter,
        task_submit=DummyTaskSubmit(dummy_task_adapter._store),
        dead_queue=RedisDeadQueue(redis, dummy_task_adapter),
        task_scheduler=RedisTaskScheduler(redis, dummy_task_adapter, routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )
    config = RoutingConfig(task_name="t", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["q"])
    await sm.routing.save_routing_config(config)

    # Warm the cache
    await sm.get_routing_config("t", 1)

    version_before = await redis.get("routing:version")
    deleted = await sm.delete_routing_config("t", 1)

    assert deleted is True
    # Cache entry cleared — returns None (SQL has no config now)
    r = await sm.get_routing_config("t", 1)
    assert r is None

    version_after = await redis.get("routing:version")
    assert version_after is not None
    assert version_before != version_after


@pytest.mark.asyncio
async def test_invalidate_all_routing_config_clears_entire_cache(redis, session_factory, dummy_task_adapter):
    """invalidate_all_routing_config clears all entries from the routing cache dict."""
    routing_backend = SQLRoutingBackend(session_factory)
    sm = StateManager(
        routing_backend,
        task_state=dummy_task_adapter,
        task_submit=DummyTaskSubmit(dummy_task_adapter._store),
        dead_queue=RedisDeadQueue(redis, dummy_task_adapter),
        task_scheduler=RedisTaskScheduler(redis, dummy_task_adapter, routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )
    for i in range(3):
        cfg = RoutingConfig(task_name=f"t{i}", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["q"])
        await sm.routing.save_routing_config(cfg)
        await sm.get_routing_config(f"t{i}", 1)

    assert len(sm._routing_config_cache) == 3

    sm.invalidate_all_routing_config()

    assert len(sm._routing_config_cache) == 0


# ── saga-mode state-manager paths ────────────────────────────────────────────
# These tests use saga_state_manager (DummyTaskState, _atomic_state=None) or
# cron_saga_state_manager (AtomicDummyTaskState + DummyCronDAGScheduler) to
# exercise the else/elif branches that are dead code when all adapters are atomic.


async def _dlq_saga(sm: StateManager, task: Task, failed_at: dt.datetime) -> None:
    """Seed a task into the DLQ via the direct add_to_dlq method (saga-mode DLQ)."""
    await sm.dead_queue.add_to_dlq(task, failed_at)


async def _schedule_saga(sm: StateManager, task: Task, run_at: dt.datetime) -> None:
    """Seed a task into the scheduler via the direct add method (saga-mode scheduler)."""
    await sm.task_scheduler.add(task, run_at)


@pytest.mark.asyncio
async def test_fail_task_with_dlq_saga_mode(saga_state_manager):
    """fail_task calls dead_queue.add_to_dlq directly when adapters are non-atomic."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["oops"])
    task.task_config = make_task_config(DeadLetterPolicy.SAVE)

    await saga_state_manager.fail_task(task)

    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.FAILED
    dlq = await saga_state_manager.dead_queue.get_by_ids([str(ULID1)])
    assert len(dlq) == 1


@pytest.mark.asyncio
async def test_fail_task_no_dlq_saga_mode(saga_state_manager):
    """fail_task with NONE policy saves the task without touching the DLQ in saga mode."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED)
    task.task_config = make_task_config(DeadLetterPolicy.NONE)

    await saga_state_manager.fail_task(task)

    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.FAILED
    assert await saga_state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


@pytest.mark.asyncio
async def test_resubmit_dead_tasks_saga_mode(saga_state_manager):
    """resubmit_dead_tasks saves the blob, actually enqueues it, then removes from DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["e"])
    await saga_state_manager.task_state.save_task(task)
    await _dlq_saga(saga_state_manager, task, FROZEN_TIME)

    await saga_state_manager.resubmit_dead_tasks([task])

    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.SUBMITTED
    assert ULID1 in saga_state_manager.task_submit.queued
    assert await saga_state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


@pytest.mark.asyncio
async def test_submit_tasks_batch_saga_mode(saga_state_manager):
    """submit_tasks_batch saves each blob and actually enqueues each task in saga mode."""
    tasks = [
        Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.UNSUBMITTED),
        Task(id=ULID2, name="my_task", queue="default", status=TaskStatus.UNSUBMITTED),
    ]

    await saga_state_manager.submit_tasks_batch(tasks)

    for task_id in (ULID1, ULID2):
        saved = await saga_state_manager.task_state.get_task(task_id)
        assert saved is not None
        assert saved.status == TaskStatus.SUBMITTED
        assert task_id in saga_state_manager.task_submit.queued


@pytest.mark.asyncio
async def test_requeue_task_saga_mode(saga_state_manager):
    """requeue_task saves the blob and actually enqueues the task in saga mode."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)

    await saga_state_manager.requeue_task(task)

    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.SUBMITTED
    assert ULID1 in saga_state_manager.task_submit.queued


@pytest.mark.asyncio
async def test_cancel_submitted_task_saga_mode(saga_state_manager):
    """Cancelling a SUBMITTED task in saga mode saves CANCELLED state without a pipeline."""
    task = Task(
        id=ULID1, name="my_task", queue="default", status=TaskStatus.SUBMITTED, submitted_at=FROZEN_TIME
    )
    await saga_state_manager.task_state.save_task(task)

    result = await saga_state_manager.request_task_cancellation(ULID1)

    assert result is not None
    assert result.status == TaskStatus.CANCELLED
    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_scheduled_task_saga_mode(saga_state_manager):
    """Cancelling a SCHEDULED task in saga mode removes it from the scheduler sequentially."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.SCHEDULED)
    run_at = FROZEN_TIME + dt.timedelta(hours=1)
    await saga_state_manager.task_state.save_task(task)
    await _schedule_saga(saga_state_manager, task, run_at)

    result = await saga_state_manager.request_task_cancellation(ULID1)

    assert result is not None
    assert result.status == TaskStatus.CANCELLED
    assert await saga_state_manager.task_scheduler.get_run_at(ULID1) is None
    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_schedule_new_task_saga_mode(saga_state_manager):
    """schedule_new_task saves the task and calls scheduler.add() sequentially in saga mode."""
    task = Task(id=ULID1, name="t", version=1, queue="default")
    run_at = FROZEN_TIME + dt.timedelta(hours=1)

    result = await saga_state_manager.schedule_new_task(task, run_at)

    assert result is task
    assert task.status == TaskStatus.SCHEDULED
    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.SCHEDULED
    scheduled_at = await saga_state_manager.task_scheduler.get_run_at(ULID1)
    assert scheduled_at is not None


@pytest.mark.asyncio
async def test_clean_stale_task_saga_mode(saga_state_manager):
    """clean() with stale_time saves STALLED status and removes heartbeat sequentially in saga mode."""
    two_hours_ago = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    started = Task(
        id=ULID1,
        name="my_task",
        queue="default",
        status=TaskStatus.STARTED,
        started_at=two_hours_ago,
        heartbeat_at=two_hours_ago,
    )
    await saga_state_manager.task_state.save_task(started)
    await saga_state_manager.task_state.update_task_heartbeat(started)
    await saga_state_manager.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task", function=dummy_fn, max_heartbeat_interval=dt.timedelta(minutes=5)
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await saga_state_manager.clean(stale_time=dt.timedelta(minutes=30))

    saved = await saga_state_manager.task_state.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STALLED
    assert ULID1 not in saga_state_manager.task_state._heartbeats


# ── recover_orphaned_scheduled ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_recover_orphan_skipped_when_flag_false(redis, state_manager):
    """clean() without recover_orphaned_scheduled=True does not re-add orphans."""
    task = Task(id=ULID1, name="my_task", version=1, queue="default", status=TaskStatus.SCHEDULED)
    await state_manager.task_state.save_task(task)
    await redis.hset(state_manager.task_scheduler.SCHEDULE_TASK_QUEUE, str(ULID1), "default")

    with patch("datetime.datetime") as mock_dt:
        mock_dt.now.return_value = FROZEN_TIME
        await state_manager.clean()

    score = await redis.zscore(state_manager.task_scheduler.SCHEDULE_QUEUE(queue="default"), bytes(ULID1))
    assert score is None


# ── drop_stale_indexes ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_drop_stale_indexes_skipped_when_flag_false(state_manager):
    """clean() without drop_stale_indexes=True does not call drop_stale_indexes on any adapter."""
    with (
        patch.object(state_manager.routing, "drop_stale_indexes", AsyncMock(return_value=[])) as routing_drop,
        patch.object(state_manager.task_state, "drop_stale_indexes", AsyncMock(return_value=[])) as task_drop,
        patch.object(state_manager.dead_queue, "drop_stale_indexes", AsyncMock(return_value=[])) as dlq_drop,
    ):
        await state_manager.clean()

    routing_drop.assert_not_called()
    task_drop.assert_not_called()
    dlq_drop.assert_not_called()


@pytest.mark.asyncio
async def test_clean_drop_stale_indexes_calls_all_three_adapters(state_manager):
    """clean(drop_stale_indexes=True) calls drop_stale_indexes on routing, task_state, and dead_queue."""
    with (
        patch.object(
            state_manager.routing, "drop_stale_indexes", AsyncMock(return_value=["routing-idx-v1"])
        ) as routing_drop,
        patch.object(
            state_manager.task_state, "drop_stale_indexes", AsyncMock(return_value=["task-idx-v1"])
        ) as task_drop,
        patch.object(
            state_manager.dead_queue, "drop_stale_indexes", AsyncMock(return_value=["dlq-idx-v1"])
        ) as dlq_drop,
    ):
        await state_manager.clean(drop_stale_indexes=True)

    routing_drop.assert_called_once()
    task_drop.assert_called_once()
    dlq_drop.assert_called_once()


# ── clean_orphaned_dlq ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_orphaned_dlq_skipped_when_flag_false(state_manager):
    """clean() without clean_orphaned_dlq=True does not call clean_orphaned_entries."""
    with patch.object(
        state_manager.dead_queue, "clean_orphaned_entries", AsyncMock(return_value=0)
    ) as dlq_clean:
        await state_manager.clean()

    dlq_clean.assert_not_called()


@pytest.mark.asyncio
async def test_clean_orphaned_dlq_calls_dead_queue(state_manager):
    """clean(clean_orphaned_dlq=True) calls clean_orphaned_entries on the dead queue."""
    with patch.object(
        state_manager.dead_queue, "clean_orphaned_entries", AsyncMock(return_value=2)
    ) as dlq_clean:
        await state_manager.clean(clean_orphaned_dlq=True)

    dlq_clean.assert_called_once()


# ── cron no-pipeline paths ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_cron_dag_no_pipeline(cron_saga_state_manager):
    """dispatch_cron_dag calls reschedule/set_active_run/submit_task sequentially when cron is non-atomic."""
    spec_queue = "default"
    await cron_saga_state_manager.routing.save_queue_config(QueueConfig(name=spec_queue))

    from jobbers.models.dag import DAGTaskSpec

    spec = DAGTaskSpec(name="my_job", queue=spec_queue)
    entry = CronDAGEntry(
        name="nightly",
        cron_expr="0 0 * * *",
        dag_spec=spec,
    )
    cron_sched = cron_saga_state_manager.cron_dag_scheduler
    assert isinstance(cron_sched, DummyCronDAGScheduler)
    await cron_sched.add(entry, FROZEN_TIME - dt.timedelta(hours=1))

    await cron_saga_state_manager.dispatch_cron_dag(entry, FROZEN_TIME)

    # Entry should have been rescheduled to a future time
    next_run_at = await cron_sched.get_next_run_at(entry.id)
    assert next_run_at is not None
    assert next_run_at > FROZEN_TIME

    # A task should have been submitted
    submitted = list(cron_saga_state_manager.task_state._store.values())
    assert len(submitted) == 1
    assert submitted[0].status == TaskStatus.SUBMITTED


@pytest.mark.asyncio
async def test_complete_cron_task_atomic_state_separate_cron_backend(cron_saga_state_manager):
    """complete_cron_task saves via pipeline and calls clear_active_run separately on a non-atomic cron."""
    cron_sched = cron_saga_state_manager.cron_dag_scheduler
    assert isinstance(cron_sched, DummyCronDAGScheduler)

    from jobbers.models.dag import DAGTaskSpec

    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(name="nightly", cron_expr="0 0 * * *", dag_spec=spec)
    await cron_sched.add(entry, FROZEN_TIME)
    task_id = ULID()
    await cron_sched.set_active_run(entry.id, task_id)

    task = Task(id=task_id, name="my_job", queue="default", status=TaskStatus.COMPLETED, cron_id=entry.id)

    await cron_saga_state_manager.complete_cron_task(task)

    saved = await cron_saga_state_manager.task_state.get_task(task_id)
    assert saved is not None
    assert saved.status == TaskStatus.COMPLETED
    assert await cron_sched.get_active_run(entry.id) is None


@pytest.mark.asyncio
async def test_complete_cron_task_full_saga_mode(saga_state_manager):
    """complete_cron_task uses save_task + clear_active_run sequentially when state is also non-atomic."""
    from jobbers.models.dag import DAGTaskSpec

    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(name="nightly", cron_expr="0 0 * * *", dag_spec=spec)
    cron_sched = saga_state_manager.cron_dag_scheduler
    await cron_sched.add(entry, FROZEN_TIME)
    task_id = ULID()
    await cron_sched.set_active_run(entry.id, task_id, ttl=3600)

    task = Task(id=task_id, name="my_job", queue="default", status=TaskStatus.COMPLETED, cron_id=entry.id)

    await saga_state_manager.complete_cron_task(task)

    saved = await saga_state_manager.task_state.get_task(task_id)
    assert saved is not None
    assert saved.status == TaskStatus.COMPLETED
    assert await cron_sched.get_active_run(entry.id) is None


@pytest.mark.asyncio
async def test_reschedule_cron_entries_bulk_no_pipeline(cron_saga_state_manager):
    """reschedule_cron_entries_bulk calls reschedule() sequentially when cron scheduler is non-atomic."""
    from jobbers.models.dag import DAGTaskSpec

    spec = DAGTaskSpec(name="my_job", queue="default")
    entry = CronDAGEntry(name="nightly", cron_expr="0 0 * * *", dag_spec=spec)
    cron_sched = cron_saga_state_manager.cron_dag_scheduler
    assert isinstance(cron_sched, DummyCronDAGScheduler)
    await cron_sched.add(entry, FROZEN_TIME)

    run_at = FROZEN_TIME
    await cron_saga_state_manager.reschedule_cron_entries_bulk([(entry, run_at)])

    next_run_at = await cron_sched.get_next_run_at(entry.id)
    assert next_run_at is not None
    assert next_run_at > run_at
