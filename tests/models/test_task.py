import asyncio
import datetime as dt
from unittest.mock import patch

import pytest
from ulid import ULID

from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.models.task import Task, TaskPagination, TaskStatus
from jobbers.models.task_config import TaskConfig

FROZEN_TIME = dt.datetime.fromisoformat("2021-01-01T00:00:00+00:00")
ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")
ULID2 = ULID.from_str("01JQC31BHQ5AXV0JK23ZWSS5NA")


@pytest.fixture
def sample_task():
    """Create a sample task for testing."""
    return Task(
        id=ULID(),
        name="test_task",
        version=1,
        queue="default",
        parameters={},
        status=TaskStatus.STARTED,
        started_at=dt.datetime.now(dt.UTC)
    )


def test_task_serialization_and_deserialization():
    """Test that a Task can be packed and unpacked correctly."""
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={"key": "value"},
        results={"result_key": "result_value"},
        errors=[],
        status=TaskStatus.STARTED,
        submitted_at=dt.datetime(2025, 4, 4, 12, 0, 0, tzinfo=dt.UTC),
        started_at=dt.datetime(2025, 4, 4, 12, 5, 0, tzinfo=dt.UTC),
        heartbeat_at=None,
        completed_at=None,
    )

    packed = task.pack()
    deserialized_task = Task.unpack(task_id, packed)

    assert task == deserialized_task

def test_valid_params():
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={},
        results={},
        errors=[],
        status=TaskStatus.UNSUBMITTED,
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
    )

    def task_function(foo: str, bar: int|None=5) -> None: # pragma: no cover
        pass

    task.task_config = TaskConfig(
        name="test_task",
        version=1,
        function=task_function,
        timeout=10,
        max_retries=3,
    )
    task.parameters = {"foo": "spam", "bar": 5}
    assert task.valid_task_params()

    task.parameters = {"foo": "spam", "bar": None}
    assert task.valid_task_params()

    task.parameters = {"foo": "spam", "bar": "baz"}
    assert not task.valid_task_params()


def test_task_serialization_with_none_values():
    """Test that Task pack/unpack roundtrip handles None values properly."""
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={},
        results={},
        status=TaskStatus.UNSUBMITTED,
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        completed_at=None,
    )

    packed = task.pack()
    deserialized_task = Task.unpack(task_id, packed)

    assert task == deserialized_task


def test_task_serialization_with_non_none_values():
    """Test that Task pack/unpack roundtrip handles non-None values properly."""
    task_id = ULID()
    task = Task(
        id=task_id,
        name="Test Task",
        version=1,
        parameters={"key": "value"},
        results={"result_key": "result_value"},
        errors=["Some error occurred"],
        status=TaskStatus.COMPLETED,
        submitted_at=dt.datetime(2025, 4, 4, 12, 0, 0, tzinfo=dt.UTC),
        started_at=dt.datetime(2025, 4, 4, 12, 5, 0, tzinfo=dt.UTC),
        heartbeat_at=dt.datetime(2025, 4, 4, 12, 10, 0, tzinfo=dt.UTC),
        completed_at=dt.datetime(2025, 4, 4, 12, 15, 0, tzinfo=dt.UTC),
    )

    packed = task.pack()
    deserialized_task = Task.unpack(task_id, packed)

    assert task == deserialized_task


@pytest.mark.asyncio
async def test_submit_task(redis, task_adapter):
    """Test submitting a task to Redis."""
    task = Task(id=ULID1, name="Test Task", status=TaskStatus.UNSUBMITTED, queue="default")
    task.set_status(TaskStatus.SUBMITTED)  # SM sets this before calling the adapter
    await task_adapter.submit_task(task)
    # Verify the task was added to Redis
    task_list = await redis.zrange("task-queues:default", 0, -1)
    assert bytes(ULID1) in task_list
    saved_task = await task_adapter.get_task(ULID1)
    assert saved_task.name == "Test Task"
    assert saved_task.status == TaskStatus.SUBMITTED
    assert saved_task.submitted_at == task.submitted_at

@pytest.mark.asyncio
async def test_submit_task_twice_updates_only(redis, task_adapter):
    """Test that submitting a task twice updates the task but does not add it to the task-list again."""
    # Submit the task for the first time
    task = Task(id=ULID1, name="Initial Task", status="unsubmitted")
    task.set_status(TaskStatus.SUBMITTED)  # SM sets this before calling the adapter
    await task_adapter.submit_task(task)

    # Submit the task again with updated details
    updated_task = Task(id=ULID1, name="Updated Task", status="completed", submitted_at=task.submitted_at)
    await task_adapter.submit_task(updated_task)

    # Verify the task ID is only added once to the task-list
    task_list = await redis.zrange(f"task-queues:{task.queue}", 0, -1)
    assert task_list == [bytes(ULID1)]

    # Verify the task details were updated
    # submitted_at should not change
    saved_task = await task_adapter.get_task(ULID1)
    assert saved_task.name == "Updated Task"
    assert saved_task.status == TaskStatus.COMPLETED
    assert saved_task.submitted_at == task.submitted_at

@pytest.mark.asyncio
async def test_get_task(task_adapter):
    """Test retrieving a task from Redis."""
    task_to_save = Task(id=ULID1, name="Test Task", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME)
    await task_adapter.save_task(task_to_save)
    task = await task_adapter.get_task(ULID1)
    assert task is not None
    assert task.id == ULID1
    assert task.name == "Test Task"
    assert task.status == TaskStatus.STARTED
    assert task.submitted_at == FROZEN_TIME

@pytest.mark.asyncio
async def test_get_task_not_found(task_adapter):
    """Test retrieving a non-existent task."""
    task = await task_adapter.get_task(ULID1)
    assert task is None

@pytest.mark.asyncio
async def test_task_exists(task_adapter):
    """Test checking if a task exists in Redis."""
    await task_adapter.save_task(Task(id=ULID1, name="Test Task", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME))
    exists = await task_adapter.task_exists(ULID1)
    assert exists
    exists = await task_adapter.task_exists(999)
    assert not exists

@pytest.mark.asyncio
async def test_get_all_tasks(real_task_adapter):
    """Test retrieving all tasks from Redis."""
    t1 = Task(id=ULID1, name="Task 1", status=TaskStatus.STARTED, submitted_at=FROZEN_TIME)
    t2 = Task(id=ULID2, name="Task 2", status=TaskStatus.COMPLETED, submitted_at=FROZEN_TIME)
    await real_task_adapter.submit_task(t1)
    await real_task_adapter.submit_task(t2)
    tasks = await real_task_adapter.get_all_tasks(TaskPagination(queue="default"))
    assert len(tasks) == 2
    assert {t.id for t in tasks} == {ULID1, ULID2}


@pytest.mark.asyncio
async def test_get_all_tasks_empty(real_task_adapter):
    """Test retrieving tasks when no tasks exist."""
    tasks = await real_task_adapter.get_all_tasks(TaskPagination(queue="default"))
    assert tasks == []


class TestUpdateTaskHeartbeat:
    """Tests for TaskAdapter.update_task_heartbeat."""

    @pytest.mark.asyncio
    async def test_update_task_heartbeat_sets_timestamp(self, task_adapter, sample_task):
        """Test that heartbeat timestamp is updated in task details."""
        # Arrange
        await task_adapter.save_task(sample_task)
        sample_task.heartbeat_at = dt.datetime.now(dt.UTC)

        # Act
        await task_adapter.update_task_heartbeat(sample_task)

        # Assert
        updated_task = await task_adapter.get_task(sample_task.id)
        assert updated_task.heartbeat_at == sample_task.heartbeat_at

    @pytest.mark.asyncio
    async def test_update_task_heartbeat_adds_to_scores(self, task_adapter, sample_task):
        """Test that heartbeat score is added to the sorted set."""
        # Arrange
        await task_adapter.save_task(sample_task)
        sample_task.heartbeat_at = dt.datetime.now(dt.UTC)

        # Act
        await task_adapter.update_task_heartbeat(sample_task)

        # Assert
        scores = await task_adapter.data_store.zrange(
            task_adapter.HEARTBEAT_SCORES(queue=sample_task.queue),
            0, -1, withscores=True
        )
        assert any(bytes(sample_task.id) == task_id for task_id, _ in scores)

    @pytest.mark.asyncio
    async def test_update_task_heartbeat_updates_existing_score(self, task_adapter, sample_task):
        """Test that updating heartbeat overwrites the previous score."""
        # Arrange
        await task_adapter.save_task(sample_task)
        first_time = dt.datetime.now(dt.UTC)
        sample_task.heartbeat_at = first_time
        await task_adapter.update_task_heartbeat(sample_task)

        # Act
        second_time = first_time + dt.timedelta(seconds=10)
        sample_task.heartbeat_at = second_time
        await task_adapter.update_task_heartbeat(sample_task)

        # Assert
        scores = await task_adapter.data_store.zrange(
            task_adapter.HEARTBEAT_SCORES(queue=sample_task.queue),
            0, -1, withscores=True
        )
        score = next(score for task_id, score in scores if bytes(sample_task.id) == task_id)
        assert score == second_time.timestamp()


class TestGetStaleTasks:
    """Tests for TaskAdapter.get_stale_tasks."""

    @pytest.mark.asyncio
    async def test_get_stale_tasks_returns_stale_tasks(self, task_adapter):
        """Test that stale tasks are returned."""
        # Arrange
        now = dt.datetime.now(dt.UTC)
        stale_time = dt.timedelta(minutes=5)

        # Create a stale task (heartbeat 10 minutes ago)
        stale_task = Task(
            id=ULID(),
            name="stale_task",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=10)
        )
        await task_adapter.save_task(stale_task)
        await task_adapter.update_task_heartbeat(stale_task)

        # Act
        stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, stale_time)]

        # Assert
        assert len(stale_tasks) == 1
        assert stale_tasks[0].id == stale_task.id

    @pytest.mark.asyncio
    async def test_get_stale_tasks_excludes_recent_tasks(self, task_adapter):
        """Test that recent tasks are not returned."""
        # Arrange
        now = dt.datetime.now(dt.UTC)
        stale_time = dt.timedelta(minutes=5)

        # Create a recent task (heartbeat 1 minute ago)
        recent_task = Task(
            id=ULID(),
            name="recent_task",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=1)
        )
        await task_adapter.save_task(recent_task)
        await task_adapter.update_task_heartbeat(recent_task)

        # Act
        stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, stale_time)]

        # Assert
        assert len(stale_tasks) == 0

    @pytest.mark.asyncio
    async def test_get_stale_tasks_handles_multiple_queues(self, task_adapter):
        """Test that stale tasks from multiple queues are returned."""
        # Arrange
        now = dt.datetime.now(dt.UTC)
        stale_time = dt.timedelta(minutes=5)

        # Create stale tasks in different queues
        stale_task_1 = Task(
            id=ULID(),
            name="task_1",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=10)
        )
        stale_task_2 = Task(
            id=ULID(),
            name="task_2",
            queue="high_priority",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=10)
        )
        await task_adapter.save_task(stale_task_1)
        await task_adapter.save_task(stale_task_2)
        await task_adapter.update_task_heartbeat(stale_task_1)
        await task_adapter.update_task_heartbeat(stale_task_2)

        # Act
        stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default", "high_priority"}, stale_time)]

        # Assert
        assert len(stale_tasks) == 2
        stale_task_ids = {task.id for task in stale_tasks}
        assert stale_task_1.id in stale_task_ids
        assert stale_task_2.id in stale_task_ids

    @pytest.mark.asyncio
    async def test_get_stale_tasks_returns_none_for_missing_tasks(self, task_adapter, monkeypatch):
        """Test that None results are filtered out."""
        # Arrange
        now = dt.datetime.now(dt.UTC)
        stale_time = dt.timedelta(minutes=5)

        stale_task = Task(
            id=ULID(),
            name="stale_task",
            queue="default",
            status=TaskStatus.STARTED,
            started_at=now,
            heartbeat_at=now - dt.timedelta(minutes=10)
        )
        await task_adapter.save_task(stale_task)
        await task_adapter.update_task_heartbeat(stale_task)

        # Delete the task from storage to simulate a missing task
        await task_adapter.data_store.delete(task_adapter.TASK_DETAILS(task_id=stale_task.id))

        # Act
        stale_tasks = [task async for task in task_adapter.get_stale_tasks({"default"}, stale_time)]

        # Assert
        assert len(stale_tasks) == 0


class TestCleanTerminalTasks:
    """Tests for TaskAdapter.clean_terminal_tasks."""

    @pytest.mark.asyncio
    async def test_deletes_old_completed_task_blob(self, task_adapter, redis):
        """A completed task blob older than max_age is deleted."""
        task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
                    completed_at=FROZEN_TIME - dt.timedelta(days=8))
        await task_adapter.save_task(task)

        await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))

        assert not await redis.exists(f"task:{ULID1}")

    @pytest.mark.asyncio
    async def test_removes_heartbeat_entry(self, task_adapter, redis):
        """The heartbeat sorted-set entry is removed along with the task blob."""
        task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
                    completed_at=FROZEN_TIME - dt.timedelta(days=8))
        await task_adapter.save_task(task)
        await redis.zadd(task_adapter.HEARTBEAT_SCORES(queue="default"),
                         {ULID1.bytes: (FROZEN_TIME - dt.timedelta(days=8)).timestamp()})

        await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))

        score = await redis.zscore(task_adapter.HEARTBEAT_SCORES(queue="default"), ULID1.bytes)
        assert score is None

    @pytest.mark.asyncio
    async def test_removes_type_index_entry(self, task_adapter, redis):
        """Orphaned task-type-idx entries are removed alongside the task blob."""
        task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
                    completed_at=FROZEN_TIME - dt.timedelta(days=8))
        await task_adapter.save_task(task)
        await redis.sadd(task_adapter.TASK_BY_TYPE_IDX(name="test_task"), ULID1.bytes)

        await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))

        members = await redis.smembers(task_adapter.TASK_BY_TYPE_IDX(name="test_task"))
        assert ULID1.bytes not in members

    @pytest.mark.asyncio
    async def test_skips_active_task(self, task_adapter, redis):
        """Tasks in active statuses are never deleted regardless of age."""
        task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.STARTED,
                    started_at=FROZEN_TIME - dt.timedelta(days=100))
        await task_adapter.save_task(task)

        await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))

        assert await redis.exists(f"task:{ULID1}")

    @pytest.mark.asyncio
    async def test_skips_task_within_age(self, task_adapter, redis):
        """A completed task whose completed_at is within max_age is not deleted."""
        task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
                    completed_at=FROZEN_TIME - dt.timedelta(hours=1))
        await task_adapter.save_task(task)

        await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))

        assert await redis.exists(f"task:{ULID1}")

    @pytest.mark.asyncio
    async def test_skips_task_without_completed_at(self, task_adapter, redis):
        """A terminal task with no completed_at is not deleted."""
        task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.FAILED,
                    completed_at=None)
        await task_adapter.save_task(task)

        await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(seconds=0))

        assert await redis.exists(f"task:{ULID1}")

    @pytest.mark.parametrize("status", [
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.STALLED,
        TaskStatus.DROPPED,
    ])
    @pytest.mark.asyncio
    async def test_cleans_all_terminal_statuses(self, task_adapter, redis, status):
        """All five terminal statuses are eligible for cleanup when old enough."""
        task = Task(id=ULID1, name="test_task", queue="default", status=status,
                    completed_at=FROZEN_TIME - dt.timedelta(days=8))
        await task_adapter.save_task(task)

        await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))

        assert not await redis.exists(f"task:{ULID1}")

    @pytest.mark.asyncio
    async def test_leaves_other_tasks_untouched(self, task_adapter, redis):
        """Only old terminal tasks are deleted; active and recent tasks remain."""
        old_task = Task(id=ULID1, name="test_task", queue="default", status=TaskStatus.COMPLETED,
                        completed_at=FROZEN_TIME - dt.timedelta(days=8))
        recent_task = Task(id=ULID2, name="test_task", queue="default", status=TaskStatus.COMPLETED,
                           completed_at=FROZEN_TIME - dt.timedelta(hours=1))
        await task_adapter.save_task(old_task)
        await task_adapter.save_task(recent_task)

        await task_adapter.clean_terminal_tasks(FROZEN_TIME, dt.timedelta(days=7))

        assert not await redis.exists(f"task:{ULID1}")
        assert await redis.exists(f"task:{ULID2}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_rate_task(task_id: ULID, submitted_at: dt.datetime) -> Task:
    return Task(id=task_id, name="test", queue="default", status=TaskStatus.SUBMITTED, submitted_at=submitted_at)

def _default_queue_config(rate_numerator: int = 2) -> QueueConfig:
    return QueueConfig(name="default", rate_numerator=rate_numerator, rate_denominator=1, rate_period=RatePeriod.MINUTE)


# ── submit_rate_limited_task ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_submit_rate_limited_enqueues_when_empty(redis, task_adapter, task_adapter_dt_module):
    """Atomically enqueues the task and records it in the rate-limiter when the set is empty."""
    task = _make_rate_task(ULID1, FROZEN_TIME)
    with patch(task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert bytes(ULID1) in await redis.zrange("rate-limiter:default", 0, -1)
    assert bytes(ULID1) in await redis.zrange("task-queues:default", 0, -1)
    assert await redis.exists(f"task:{ULID1}")


@pytest.mark.asyncio
async def test_submit_rate_limited_enqueues_with_room(redis, task_adapter, task_adapter_dt_module):
    """Enqueues when one slot is already used out of two."""
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp() - 1})
    task = _make_rate_task(ULID2, FROZEN_TIME)
    with patch(task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert bytes(ULID2) in await redis.zrange("task-queues:default", 0, -1)


@pytest.mark.asyncio
async def test_submit_rate_limited_prunes_expired_entries(redis, task_adapter, task_adapter_dt_module):
    """Expired rate-limiter entries are pruned; the new task is accepted."""
    # Both entries are > 60 s old — outside the 1-minute window
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp() - 60})
    await redis.zadd("rate-limiter:default", {ULID2.bytes: FROZEN_TIME.timestamp() - 61})
    new_id = ULID()
    task = _make_rate_task(new_id, FROZEN_TIME)
    with patch(task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is True
    assert new_id.bytes in await redis.zrange("task-queues:default", 0, -1)


@pytest.mark.asyncio
async def test_submit_rate_limited_rejects_when_full(redis, task_adapter, task_adapter_dt_module):
    """Task is not enqueued and task details are not written when rate limit is reached."""
    await redis.zadd("rate-limiter:default", {ULID1.bytes: FROZEN_TIME.timestamp() - 1})
    await redis.zadd("rate-limiter:default", {ULID2.bytes: FROZEN_TIME.timestamp() - 2})
    new_id = ULID()
    task = _make_rate_task(new_id, FROZEN_TIME)
    with patch(task_adapter_dt_module) as mock_dt:
        mock_dt.datetime.now.return_value = FROZEN_TIME
        mock_dt.timedelta = dt.timedelta
        result = await task_adapter.submit_rate_limited_task(task, _default_queue_config())
    assert result is False
    assert new_id.bytes not in await redis.zrange("task-queues:default", 0, -1)
    assert not await redis.exists(f"task:{new_id}")


@pytest.mark.asyncio
async def test_submit_rate_limited_concurrent_respects_limit(redis, task_adapter):
    """Concurrent submissions must not collectively exceed the rate limit."""
    now = dt.datetime.now(dt.UTC)
    limit = 5
    queue_config = _default_queue_config(rate_numerator=limit)
    tasks = [_make_rate_task(ULID(), now) for _ in range(10)]

    results = await asyncio.gather(
        *[task_adapter.submit_rate_limited_task(t, queue_config) for t in tasks]
    )

    accepted = sum(1 for r in results if r)
    assert accepted == limit
    assert await redis.zcard("rate-limiter:default") == limit
    assert await redis.zcard("task-queues:default") == limit


# ── valid_task_params: no task_config ─────────────────────────────────────────

def test_valid_task_params_no_task_config():
    """valid_task_params returns True immediately when task_config is None."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    assert task.task_config is None
    assert task.valid_task_params() is True


# ── shutdown ───────────────────────────────────────────────────────────────────

def test_shutdown_no_task_config_is_noop():
    """shutdown() returns immediately when task_config is None."""
    from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    task.shutdown()  # should not raise
    assert task.status == TaskStatus.STARTED


def test_shutdown_continue_policy_is_noop():
    """shutdown() with CONTINUE policy leaves the task status unchanged."""
    from jobbers.models.task_config import TaskConfig
    from jobbers.models.task_shutdown_policy import TaskShutdownPolicy

    async def noop() -> None: ...
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    task.task_config = TaskConfig(name="t", function=noop, on_shutdown=TaskShutdownPolicy.CONTINUE)
    task.shutdown()
    assert task.status == TaskStatus.STARTED


# ── should_retry / should_schedule ───────────────────────────────────────────

def test_should_retry_true_when_retries_remain():
    from jobbers.models.task_config import TaskConfig
    async def noop() -> None: ...
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, max_retries=3)
    task.retry_attempt = 1
    assert task.should_retry() is True


def test_should_retry_false_when_exhausted():
    from jobbers.models.task_config import TaskConfig
    async def noop() -> None: ...
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, max_retries=3)
    task.retry_attempt = 3
    assert task.should_retry() is False


def test_should_schedule_true_when_retry_delay_set():
    from jobbers.models.task_config import TaskConfig
    async def noop() -> None: ...
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, retry_delay=10)
    assert task.should_schedule() is True


def test_should_schedule_false_when_no_retry_delay():
    from jobbers.models.task_config import TaskConfig
    async def noop() -> None: ...
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, retry_delay=None)
    assert task.should_schedule() is False


# ── summarized: with errors ───────────────────────────────────────────────────

def test_summarized_includes_last_error_when_errors_present():
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED,
                errors=["first", "last error"])
    summary = task.summarized()
    assert summary["last_error"] == "last error"


def test_summarized_omits_last_error_when_no_errors():
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    summary = task.summarized()
    assert "last_error" not in summary


# ── set_status: retried_at and SCHEDULED/UNSUBMITTED branches ────────────────

def test_set_status_started_sets_retried_at_when_already_started():
    """Second STARTED transition sets retried_at instead of started_at."""
    task = Task(id=ULID1, name="t", version=1, queue="default",
                status=TaskStatus.SUBMITTED,
                started_at=dt.datetime(2024, 1, 1, tzinfo=dt.UTC))
    task.set_status(TaskStatus.STARTED)
    assert task.retried_at is not None


def test_set_status_scheduled_increments_retry_attempt():
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    before = task.retry_attempt
    task.set_status(TaskStatus.SCHEDULED)
    assert task.retry_attempt == before + 1
    assert task.status == TaskStatus.SCHEDULED


def test_set_status_unsubmitted_increments_retry_attempt():
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    before = task.retry_attempt
    task.set_status(TaskStatus.UNSUBMITTED)
    assert task.retry_attempt == before + 1
