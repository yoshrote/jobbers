import asyncio
import contextlib
import datetime as dt
from collections import defaultdict
from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers import registry
from jobbers.adapters.sql import SQLQueueConfigAdapter, SQLRoutingBackend
from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGNode, DAGTaskSpec
from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task
from jobbers.models.task_config import DeadLetterPolicy, TaskConfig
from jobbers.models.task_routing import RoutingConfig, RoutingStrategy
from jobbers.models.task_status import TaskStatus
from jobbers.state_manager import StateManager, TaskException, UserCancellationError

FROZEN_TIME = dt.datetime.fromisoformat("2021-01-01T00:00:00+00:00")
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")


async def schedule(sm: StateManager, task: Task, run_at: dt.datetime) -> None:
    pipe = sm.job_store.pipeline(transaction=True)
    sm.task_scheduler.stage_add(pipe, task, run_at)
    await pipe.execute()


async def add_to_dlq(sm: StateManager, task: Task, failed_at: dt.datetime) -> None:
    pipe = sm.job_store.pipeline(transaction=True)
    sm.dead_queue.stage_add(pipe, task, failed_at)
    await pipe.execute()


@pytest.mark.asyncio
async def test_get_refresh_tag(state_manager):
    """Test that get_refresh_tag creates and returns a consistent refresh tag for a role."""
    tag1 = await state_manager.get_refresh_tag("test_role")
    assert tag1 is not None

    tag2 = await state_manager.get_refresh_tag("test_role")
    assert tag1 == tag2


@pytest.mark.asyncio
async def test_bump_refresh_tag_publishes_pubsub(state_manager):
    """bump_refresh_tag publishes the new tag value to queue-config-refresh:{role}."""
    with patch.object(state_manager.job_store, "publish", new_callable=AsyncMock) as mock_publish:
        new_tag = await state_manager.bump_refresh_tag("myrole")

    mock_publish.assert_called_once_with("queue-config-refresh:myrole", new_tag)


@pytest.mark.asyncio
async def test_save_role_publishes_pubsub(state_manager):
    """save_role publishes the new refresh tag to queue-config-refresh:{role}."""
    with patch.object(state_manager.job_store, "publish", new_callable=AsyncMock) as mock_publish:
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
    await state_manager_real_ta.ta.submit_task(valid_task)

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
async def test_concurrency_limits_empty_queues(state_manager, rate_limiter):
    await state_manager.routing.save_queue_config(QueueConfig(name="queue1", max_concurrent=1))
    await state_manager.routing.save_queue_config(QueueConfig(name="queue2", max_concurrent=1))

    task_queues = ["queue1", "queue2"]
    current_tasks_by_queue = defaultdict(set)

    result = await rate_limiter.concurrency_limits(task_queues, current_tasks_by_queue)
    assert result == {"queue1", "queue2"}


# ── clean ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clean_rate_limit_age(redis, state_manager):
    """Test cleaning tasks from the rate limiter based on rate_limit_age."""
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
    await state_manager_real_ta.ta.save_task(completed)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager_real_ta.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task", function=dummy_fn, max_heartbeat_interval=dt.timedelta(minutes=5)
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager_real_ta.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager_real_ta.ta.get_task(ULID1)
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
    await state_manager_real_ta.ta.save_task(started)
    await redis.zadd("task-heartbeats:default", {ULID1.bytes: two_hours_ago.timestamp()})
    await state_manager_real_ta.routing.save_queue_config(QueueConfig(name="default"))

    stale_config = TaskConfig(
        name="my_task", function=dummy_fn, max_heartbeat_interval=dt.timedelta(minutes=5)
    )
    with patch.object(registry, "get_task_config", return_value=stale_config):
        await state_manager_real_ta.clean(stale_time=dt.timedelta(minutes=30))

    saved = await state_manager_real_ta.ta.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.STALLED
    assert await redis.zscore("task-heartbeats:default", ULID1.bytes) is None


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

    saved = await state_manager.ta.get_task(ULID1)
    assert saved.status == TaskStatus.FAILED
    assert await state_manager.dead_queue.get_by_ids([str(ULID1)]) == []


@pytest.mark.asyncio
async def test_fail_task_with_dlq_writes_both_stores(redis, state_manager):
    """fail_task with SAVE policy updates Redis and writes to the DLQ."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.FAILED, errors=["oops"])
    task.task_config = make_task_config(DeadLetterPolicy.SAVE)

    await state_manager.fail_task(task)

    saved = await state_manager.ta.get_task(ULID1)
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
    await state_manager.ta.save_task(task1)
    await state_manager.ta.save_task(task2)
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
    await state_manager.ta.save_task(task)
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
    await state_manager.ta.save_task(task)
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
    await state_manager.ta.save_task(cancelled)

    stale = Task(id=ULID1, name="retry_task", queue="default", status=TaskStatus.SCHEDULED, retry_attempt=1)
    await state_manager.dispatch_scheduled_task(stale)

    queue_members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) not in queue_members
    saved = await state_manager.ta.get_task(ULID1)
    assert saved is not None
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_dispatch_scheduled_task_skips_cancelled_task(redis, state_manager):
    """dispatch_scheduled_task silently skips tasks that are already CANCELLED."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.CANCELLED)
    await state_manager.ta.save_task(task)

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

    await state_manager.ta.save_task(task)
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

    await state_manager.ta.save_task(task)
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
    saved = await state_manager.ta.get_task(ULID1)
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
    pipe = state_manager.job_store.pipeline()
    state_manager.ta.stage_requeue(pipe, task)
    await pipe.execute()
    await state_manager.ta.save_task(task)

    result = await state_manager.request_task_cancellation(ULID1)

    assert result is not None
    assert result.status == TaskStatus.CANCELLED
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) not in members
    saved = await state_manager.ta.get_task(ULID1)
    assert saved.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_started_task_publishes_message(state_manager):
    """Cancelling a STARTED task publishes a cancel message on the task's pubsub channel."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)
    await state_manager.ta.save_task(task)

    monitor = asyncio.create_task(state_manager.monitor_task_cancellation(ULID1))
    await asyncio.sleep(0.05)
    result = await state_manager.request_task_cancellation(ULID1)

    assert result is not None
    assert result.status == TaskStatus.STARTED  # status unchanged — worker handles it
    with pytest.raises(UserCancellationError):
        await asyncio.wait_for(monitor, timeout=1.0)


@pytest.mark.asyncio
async def test_cancel_terminal_task_raises(state_manager):
    """Cancelling a task in a terminal status raises TaskException."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.COMPLETED)
    await state_manager.ta.save_task(task)

    with pytest.raises(TaskException, match="cannot be cancelled"):
        await state_manager.request_task_cancellation(ULID1)


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
    count = await redis.zcard(state_manager_real_ta.ta.QUEUE_RATE_LIMITER(queue="default"))
    assert count == 1
    members = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in members


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


# ── monitor_task_cancellation ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_monitor_task_cancellation_raises_on_cancel_message(state_manager, redis):
    """monitor_task_cancellation raises UserCancellationError when a cancel message is published."""
    task_id = ULID1
    monitor = asyncio.create_task(state_manager.monitor_task_cancellation(task_id))
    await asyncio.sleep(0.05)
    await redis.publish(f"task_cancel_{task_id}", "cancel")
    with pytest.raises(UserCancellationError):
        await monitor


@pytest.mark.asyncio
async def test_monitor_task_cancellation_does_not_exit_without_message(state_manager):
    """monitor_task_cancellation keeps running when no cancel message is sent."""
    task_id = ULID1
    monitor = asyncio.create_task(state_manager.monitor_task_cancellation(task_id))
    await asyncio.sleep(0.1)
    assert not monitor.done(), "monitor should still be running when no cancel message is sent"
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
async def test_update_task_heartbeat_sets_timestamp(state_manager_real_ta):
    """update_task_heartbeat stamps heartbeat_at on the task and persists it."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)
    await state_manager_real_ta.ta.save_task(task)
    assert task.heartbeat_at is None

    await state_manager_real_ta.update_task_heartbeat(task)

    assert task.heartbeat_at is not None
    score = await state_manager_real_ta.job_store.zscore(
        state_manager_real_ta.ta.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)
    )
    assert score is not None


@pytest.mark.asyncio
async def test_remove_task_heartbeat_clears_entry(state_manager_real_ta):
    """remove_task_heartbeat removes the task from the heartbeat sorted set."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)
    await state_manager_real_ta.ta.save_task(task)
    await state_manager_real_ta.update_task_heartbeat(task)

    await state_manager_real_ta.remove_task_heartbeat(task)

    score = await state_manager_real_ta.job_store.zscore(
        state_manager_real_ta.ta.HEARTBEAT_SCORES(queue="default"), bytes(ULID1)
    )
    assert score is None


@pytest.mark.asyncio
async def test_get_active_tasks_returns_heartbeating_tasks(state_manager_real_ta):
    """get_active_tasks returns tasks currently registered in any heartbeat sorted set."""
    task = Task(id=ULID1, name="my_task", queue="default", status=TaskStatus.STARTED)
    await state_manager_real_ta.ta.save_task(task)
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
    rate_key = state_manager_real_ta.ta.QUEUE_RATE_LIMITER(queue="default")
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
    rate_key = state_manager_real_ta.ta.QUEUE_RATE_LIMITER(queue="default")
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

    dag_run_id, submitted = await state_manager.submit_dag(root)

    assert dag_run_id is not None
    assert len(submitted) == 1
    assert submitted[0].id == root.id
    assert submitted[0].status == TaskStatus.SUBMITTED
    assert submitted[0].dag_run_id is not None


@pytest.mark.asyncio
async def test_submit_dag_multi_root_shares_dag_run_id(state_manager):
    """All roots submitted in the same submit_dag call share a single dag_run_id."""
    branch_a = DAGNode("branch_a")
    branch_b = DAGNode("branch_b")
    collector = DAGNode("collect")
    DAGNode.merge(branch_a, branch_b, into=collector)

    state_manager.init_fan_in = AsyncMock()

    dag_run_id, submitted = await state_manager.submit_dag(branch_a, branch_b)

    assert dag_run_id is not None
    assert len(submitted) == 2
    assert submitted[0].dag_run_id is not None
    assert submitted[0].dag_run_id == submitted[1].dag_run_id
    assert submitted[0].dag_run_id == dag_run_id


@pytest.mark.asyncio
async def test_submit_dag_fan_in_initialises_fan_in_sets(state_manager):
    """submit_dag pre-populates Redis fan-in sets before submitting tasks."""
    branch_a = DAGNode("branch_a")
    branch_b = DAGNode("branch_b")
    collector = DAGNode("collect")
    DAGNode.merge(branch_a, branch_b, into=collector)

    state_manager.init_fan_in = AsyncMock()

    _, submitted = await state_manager.submit_dag(branch_a, branch_b)

    # init_fan_in must be called with the collector's fan-in key
    fan_in_key = f"dag:fan-in:{collector.id}"
    state_manager.init_fan_in.assert_awaited_once_with(fan_in_key, {branch_a.id, branch_b.id})
    assert len(submitted) == 2


# ── dispatch_cron_dag ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_cron_dag_submits_task_and_reschedules(state_manager):
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
    stored = state_manager.ta._store
    assert len(stored) == 1
    task = next(iter(stored.values()))
    assert task.name == "my_job"
    assert task.status == TaskStatus.SUBMITTED

    # The entry should be rescheduled in the cron sorted set
    members = await state_manager.job_store.zrange("cron-schedule", 0, -1)
    assert bytes(entry.id) in members


@pytest.mark.asyncio
async def test_dispatch_cron_dag_skip_if_running_skips_when_active(state_manager):
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
    await state_manager.ta.save_task(active_task)
    await state_manager.job_store.set(f"cron-active:{entry.id}", str(ULID1))

    run_at = FROZEN_TIME
    await state_manager.dispatch_cron_dag(entry, run_at)

    # No new tasks should have been submitted (only the pre-planted active one)
    stored = state_manager.ta._store
    assert len(stored) == 1
    assert ULID1 in stored

    # Entry must still be rescheduled
    members = await state_manager.job_store.zrange("cron-schedule", 0, -1)
    assert bytes(entry.id) in members


@pytest.mark.asyncio
async def test_dispatch_cron_dag_skip_if_running_records_active_task_when_not_skipping(state_manager):
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
    stored = state_manager.ta._store
    assert len(stored) == 1

    # The active-run key should now record the new task ID
    active_key = f"cron-active:{entry.id}"
    active_val = await state_manager.job_store.get(active_key)
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

    root_spec = root_node._to_spec()
    entry = CronDAGEntry(
        name="fan_in_job",
        cron_expr="0 0 * * *",
        dag_spec=root_spec,
        concurrency_policy=ConcurrencyPolicy.ALWAYS,
    )

    run_at = FROZEN_TIME
    with patch.object(state_manager.ta, "stage_init_fan_in") as mock_stage_init:
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
            state_manager.ta, "stage_submit_task", wraps=state_manager.ta.stage_submit_task
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
        patch.object(state_manager.ta, "stage_submit_task") as mock_stage,
        patch.object(state_manager, "submit_task", new_callable=AsyncMock) as mock_submit,
    ):
        await state_manager.dispatch_cron_dag(entry, FROZEN_TIME)

    mock_stage.assert_not_called()
    mock_submit.assert_called_once()


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
    sm = StateManager(redis, SQLRoutingBackend(session_factory), task_adapter=dummy_task_adapter)
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
    sm = StateManager(redis, SQLRoutingBackend(session_factory), task_adapter=dummy_task_adapter)
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
    sm = StateManager(redis, SQLRoutingBackend(session_factory), task_adapter=dummy_task_adapter)
    qca = SQLQueueConfigAdapter(session_factory)
    await qca.save_queue_config(QueueConfig(name="q1", max_concurrent=5))
    await qca.save_role("role_a", {"q1"})
    tag_before = await sm.get_refresh_tag("role_a")

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
    sm = StateManager(redis, SQLRoutingBackend(session_factory), task_adapter=dummy_task_adapter)

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
    sm = StateManager(redis, SQLRoutingBackend(session_factory), task_adapter=dummy_task_adapter)
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
    sm = StateManager(redis, SQLRoutingBackend(session_factory), task_adapter=dummy_task_adapter)
    for i in range(3):
        cfg = RoutingConfig(task_name=f"t{i}", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["q"])
        await sm.routing.save_routing_config(cfg)
        await sm.get_routing_config(f"t{i}", 1)

    assert len(sm._routing_config_cache) == 3

    sm.invalidate_all_routing_config()

    assert len(sm._routing_config_cache) == 0
