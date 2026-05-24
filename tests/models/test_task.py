import datetime as dt
from unittest.mock import AsyncMock

import pytest
from ulid import ULID

from jobbers.models.dag import DAGNode, DAGTaskSpec, DynamicFanOut, FanInCallback, SimpleCallback, TaskResult
from jobbers.models.task import Task, TaskPagination
from jobbers.models.task_config import TaskConfig
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.models.task_status import TaskStatus

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")


def test_valid_params():
    """valid_task_params validates parameter types against the function signature."""
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

    def task_function(foo: str, bar: int | None = 5) -> None: ...

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


# ── valid_task_params: no task_config ─────────────────────────────────────────


def test_valid_task_params_no_task_config():
    """valid_task_params returns True immediately when task_config is None."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    assert task.task_config is None
    assert task.valid_task_params() is True


# ── shutdown ───────────────────────────────────────────────────────────────────


def test_shutdown_no_task_config_is_noop():
    """shutdown() returns immediately when task_config is None."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    task.shutdown()  # should not raise
    assert task.status == TaskStatus.STARTED


def test_shutdown_continue_policy_is_noop():
    """shutdown() with CONTINUE policy leaves the task status unchanged."""

    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    task.task_config = TaskConfig(name="t", function=noop, on_shutdown=TaskShutdownPolicy.CONTINUE)
    task.shutdown()
    assert task.status == TaskStatus.STARTED


def test_shutdown_resubmit_policy_sets_status_unsubmitted():
    """shutdown() with RESUBMIT policy sets status to UNSUBMITTED without incrementing retry_attempt."""

    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    task.task_config = TaskConfig(name="t", function=noop, on_shutdown=TaskShutdownPolicy.RESUBMIT)
    before = task.retry_attempt
    task.shutdown()
    assert task.status == TaskStatus.UNSUBMITTED
    assert task.retry_attempt == before  # direct assignment, not set_status


# ── should_retry / should_schedule ───────────────────────────────────────────


def test_should_retry_true_when_retries_remain():
    """should_retry() is True while retry_attempt is still below max_retries."""
    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, max_retries=3)
    task.retry_attempt = 1
    assert task.should_retry() is True


def test_should_retry_false_when_exhausted():
    """should_retry() is False once retry_attempt equals max_retries."""
    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, max_retries=3)
    task.retry_attempt = 3
    assert task.should_retry() is False


def test_should_schedule_true_when_retry_delay_set():
    """should_schedule() is True when the task config has a non-None retry_delay."""
    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, retry_delay=10)
    assert task.should_schedule() is True


def test_should_schedule_false_when_no_retry_delay():
    """should_schedule() is False when retry_delay is None."""
    async def noop() -> None: ...

    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    task.task_config = TaskConfig(name="t", function=noop, retry_delay=None)
    assert task.should_schedule() is False


# ── summarized ────────────────────────────────────────────────────────────────


def test_summarized_includes_last_error_when_errors_present():
    """summarized() includes last_error when the errors list is non-empty."""
    task = Task(
        id=ULID1,
        name="t",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        errors=["first", "last error"],
    )
    summary = task.summarized()
    assert summary["last_error"] == "last error"


def test_summarized_omits_last_error_when_no_errors():
    """summarized() omits last_error when the errors list is empty."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)
    summary = task.summarized()
    assert "last_error" not in summary


# ── heartbeat ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_heartbeat_sets_heartbeat_at_and_calls_adapter():
    """heartbeat() timestamps heartbeat_at and delegates to the injected adapter."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    mock_adapter = AsyncMock()
    task._adapter = mock_adapter
    await task.heartbeat()
    assert task.heartbeat_at is not None
    mock_adapter.update_task_heartbeat.assert_awaited_once_with(task)


# ── set_status ────────────────────────────────────────────────────────────────


def test_set_status_started_sets_retried_at_when_already_started():
    """Second STARTED transition sets retried_at instead of started_at."""
    task = Task(
        id=ULID1,
        name="t",
        version=1,
        queue="default",
        status=TaskStatus.SUBMITTED,
        started_at=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
    )
    task.set_status(TaskStatus.STARTED)
    assert task.retried_at is not None


def test_set_status_scheduled_increments_retry_attempt():
    """set_status(SCHEDULED) bumps retry_attempt and records the new status."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    before = task.retry_attempt
    task.set_status(TaskStatus.SCHEDULED)
    assert task.retry_attempt == before + 1
    assert task.status == TaskStatus.SCHEDULED


def test_set_status_unsubmitted_increments_retry_attempt():
    """set_status(UNSUBMITTED) bumps retry_attempt (immediate retry path)."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.FAILED)
    before = task.retry_attempt
    task.set_status(TaskStatus.UNSUBMITTED)
    assert task.retry_attempt == before + 1


# ── TaskPagination ─────────────────────────────────────────────────────────────


def test_serialize_start_with_ulid():
    """serialize_start returns the ULID as a string when start is not None."""
    pagination = TaskPagination(queue="default", start=ULID1)
    result = pagination.model_dump(mode="json")
    assert result["start"] == str(ULID1)


def test_serialize_start_with_none():
    """serialize_start returns None when start is None."""
    pagination = TaskPagination(queue="default")
    result = pagination.model_dump(mode="json")
    assert result["start"] is None


# ── to_dict / from_dict ───────────────────────────────────────────────────────


def _make_full_task(**overrides) -> Task:
    base = dict(
        id=ULID1,
        name="t",
        version=1,
        queue="default",
        status=TaskStatus.SUBMITTED,
        parameters={"x": 1},
        results={"y": 2},
        errors=["err"],
        retry_attempt=2,
    )
    base.update(overrides)
    return Task(**base)


def test_to_dict_round_trip():
    """from_dict(to_dict()) reproduces the original task."""
    task = _make_full_task()
    d = task.model_dump(context={"mode": "msgpack"}, exclude={"id"})
    restored = Task.model_validate({"id": task.id, **d})
    assert restored.id == task.id
    assert restored.name == task.name
    assert restored.parameters == task.parameters
    assert restored.results == task.results
    assert restored.errors == task.errors
    assert restored.retry_attempt == task.retry_attempt


def test_from_dict_missing_parent_ids_defaults_to_empty():
    """from_dict handles a dict with no parent_ids gracefully."""
    minimal = {"id": ULID1, "name": "t", "queue": "default", "version": 1, "status": "submitted"}
    task = Task.model_validate(minimal)
    assert task.parent_ids == []


def test_to_dict_preserves_dag_callbacks():
    """dag_callbacks are round-tripped through to_dict/from_dict."""
    child_spec = DAGTaskSpec(name="child")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.SUBMITTED,
        dag_callbacks=[SimpleCallback(task=child_spec)],
    )
    d = task.model_dump(context={"mode": "msgpack"}, exclude={"id"})
    restored = Task.model_validate({"id": ULID1, **d})
    assert len(restored.dag_callbacks) == 1
    assert isinstance(restored.dag_callbacks[0], SimpleCallback)
    assert restored.dag_callbacks[0].task.name == "child"


# ── generate_callbacks ────────────────────────────────────────────────────────


def _make_adapter(fan_in_complete_return=0, fan_in_members=None):
    """Return a mock task adapter for generate_callbacks tests."""
    mock_adapter = AsyncMock()
    mock_adapter.fan_in_complete.return_value = fan_in_complete_return
    mock_adapter.get_fan_in_members.return_value = fan_in_members or []
    return mock_adapter


@pytest.mark.asyncio
async def test_generate_callbacks_simple_creates_child_with_correct_ids():
    """SimpleCallback creates a child Task with parent_ids set to the parent's ID."""
    child_spec = DAGTaskSpec(name="child_task")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        dag_callbacks=[SimpleCallback(task=child_spec)],
    )
    children = await task.generate_callbacks(_make_adapter())

    assert len(children) == 1
    assert children[0].id == child_spec.id


@pytest.mark.asyncio
async def test_generate_callbacks_simple_child_has_parent_ids():
    """Simple callback child has parent_ids=[parent.id]."""
    child_spec = DAGTaskSpec(name="child_task")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        dag_callbacks=[SimpleCallback(task=child_spec)],
    )
    children = await task.generate_callbacks(_make_adapter())

    assert children[0].parent_ids == [ULID1]


@pytest.mark.asyncio
async def test_generate_callbacks_fan_in_collector_has_member_parent_ids():
    """Fan-in collector gets parent_ids from the members set returned by the adapter."""
    p1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
    p2 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG0A")
    collector_spec = DAGTaskSpec(name="collector")
    fan_in_key = f"dag:fan-in:{collector_spec.id}"
    task = Task(
        id=ULID1,
        name="last_branch",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        dag_callbacks=[FanInCallback(task=collector_spec, fan_in_key=fan_in_key)],
    )
    children = await task.generate_callbacks(_make_adapter(fan_in_complete_return=0, fan_in_members=[p1, p2]))

    assert set(children[0].parent_ids) == {p1, p2}


@pytest.mark.asyncio
async def test_generate_callbacks_fan_in_not_last_returns_nothing():
    """FanInCallback with remaining > 0 does not create a child task."""
    collector_spec = DAGTaskSpec(name="collector")
    fan_in_key = f"dag:fan-in:{collector_spec.id}"
    task = Task(
        id=ULID1,
        name="branch",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        dag_callbacks=[FanInCallback(task=collector_spec, fan_in_key=fan_in_key)],
    )
    children = await task.generate_callbacks(_make_adapter(fan_in_complete_return=1))

    assert children == []


@pytest.mark.asyncio
async def test_generate_callbacks_fan_in_last_creates_collector():
    """FanInCallback with remaining == 0 creates the collector task."""
    collector_spec = DAGTaskSpec(name="collector")
    fan_in_key = f"dag:fan-in:{collector_spec.id}"
    task = Task(
        id=ULID1,
        name="last_branch",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        dag_callbacks=[FanInCallback(task=collector_spec, fan_in_key=fan_in_key)],
    )
    children = await task.generate_callbacks(_make_adapter(fan_in_complete_return=0))

    assert len(children) == 1
    assert children[0].id == collector_spec.id


# ── generate_error_callbacks ─────────────────────────────────────────────────


def test_generate_error_callbacks_no_error_callbacks_returns_empty():
    """generate_error_callbacks() returns [] when no callbacks have error_callback set."""
    child_spec = DAGTaskSpec(name="child_task")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        dag_callbacks=[SimpleCallback(task=child_spec)],
    )
    assert task.generate_error_callbacks() == []


def test_generate_error_callbacks_simple_callback_returns_error_task():
    """generate_error_callbacks() returns the error task when SimpleCallback has error_callback."""
    error_spec = DAGTaskSpec(name="error_handler")
    child_spec = DAGTaskSpec(name="child_task")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        dag_callbacks=[SimpleCallback(task=child_spec, error_callback=error_spec)],
    )
    errors = task.generate_error_callbacks()

    assert len(errors) == 1
    assert errors[0].id == error_spec.id
    assert errors[0].name == "error_handler"
    assert errors[0].parent_ids == [ULID1]


def test_generate_error_callbacks_fan_in_callback_returns_error_task():
    """generate_error_callbacks() returns the error task when FanInCallback has error_callback."""
    error_spec = DAGTaskSpec(name="fan_error")
    collector_spec = DAGTaskSpec(name="collector")
    fan_in_key = f"dag:fan-in:{collector_spec.id}"
    task = Task(
        id=ULID1,
        name="branch",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        dag_callbacks=[FanInCallback(task=collector_spec, fan_in_key=fan_in_key, error_callback=error_spec)],
    )
    errors = task.generate_error_callbacks()

    assert len(errors) == 1
    assert errors[0].id == error_spec.id
    assert errors[0].parent_ids == [ULID1]


def test_generate_error_callbacks_multiple_callbacks_only_those_with_error():
    """Only callbacks that have error_callback set contribute an error task."""
    error_spec = DAGTaskSpec(name="error_handler")
    child_no_err = DAGTaskSpec(name="no_error")
    child_with_err = DAGTaskSpec(name="with_error")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        dag_callbacks=[
            SimpleCallback(task=child_no_err),
            SimpleCallback(task=child_with_err, error_callback=error_spec),
        ],
    )
    errors = task.generate_error_callbacks()

    assert len(errors) == 1
    assert errors[0].id == error_spec.id


def test_generate_error_callbacks_preserves_error_spec_dag_callbacks():
    """The error task inherits dag_callbacks from the error spec (enabling chaining)."""
    grandchild_spec = DAGTaskSpec(name="grandchild")
    error_spec = DAGTaskSpec(name="error_handler", dag_callbacks=[SimpleCallback(task=grandchild_spec)])
    child_spec = DAGTaskSpec(name="child")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.FAILED,
        dag_callbacks=[SimpleCallback(task=child_spec, error_callback=error_spec)],
    )
    errors = task.generate_error_callbacks()

    assert len(errors[0].dag_callbacks) == 1
    assert errors[0].dag_callbacks[0].task.id == grandchild_spec.id


# ── parent_results ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_parent_results_with_parent_ids_returns_parent_results():
    """parent_results() fetches and returns the single parent's results dict via parent_ids."""
    parent_id = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
    parent_task = Task(
        id=parent_id,
        name="parent",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        results={"val": 42},
    )
    task = Task(
        id=ULID1,
        name="child",
        version=1,
        queue="default",
        status=TaskStatus.STARTED,
        parent_ids=[parent_id],
    )
    mock_adapter = AsyncMock()
    mock_adapter.get_tasks_bulk.return_value = [parent_task]
    task._adapter = mock_adapter
    result = await task.parent_results()

    assert result == {"val": 42}
    mock_adapter.get_tasks_bulk.assert_awaited_once_with([parent_id])


@pytest.mark.asyncio
async def test_parent_results_parent_not_found_returns_empty():
    """parent_results() returns {} when parent_ids is empty (root task)."""
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.STARTED,
    )
    mock_adapter = AsyncMock()
    task._adapter = mock_adapter
    result = await task.parent_results()

    assert result == {}
    mock_adapter.get_tasks_bulk.assert_not_called()


@pytest.mark.asyncio
async def test_parent_results_fan_in_returns_list_of_results():
    """parent_results() returns a list of results for fan-in collectors (via parent_ids)."""
    p1_id = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
    p2_id = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG0A")
    p1 = Task(id=p1_id, name="p1", version=1, queue="default", status=TaskStatus.COMPLETED, results={"a": 1})
    p2 = Task(id=p2_id, name="p2", version=1, queue="default", status=TaskStatus.COMPLETED, results={"b": 2})

    # collector has parent_ids set to both predecessors
    collector = Task(
        id=ULID1,
        name="collector",
        version=1,
        queue="default",
        status=TaskStatus.STARTED,
        parent_ids=[p1_id, p2_id],
    )

    mock_adapter = AsyncMock()
    mock_adapter.get_tasks_bulk.return_value = [p1, p2]
    collector._adapter = mock_adapter
    result = await collector.parent_results()

    assert isinstance(result, list)
    assert {"a": 1} in result
    assert {"b": 2} in result


@pytest.mark.asyncio
async def test_parent_results_no_parent_info_returns_empty():
    """parent_results() returns {} when there is no parent information at all."""
    task = Task(id=ULID1, name="root", version=1, queue="default", status=TaskStatus.STARTED)
    result = await task.parent_results()

    assert result == {}


# ── make_result ───────────────────────────────────────────────────────────────


def test_make_result_populates_parent_ids():
    """make_result() returns a TaskResult with parent_ids copied from the task."""
    parent_id = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
    task = Task(
        id=ULID1, name="child", version=1, queue="default", status=TaskStatus.STARTED, parent_ids=[parent_id]
    )
    result = task.make_result(results={"x": 1})

    assert isinstance(result, TaskResult)
    assert result.results == {"x": 1}
    assert result.parent_ids == [parent_id]
    assert result.fanout is None


def test_make_result_empty_parent_ids_for_root_task():
    """make_result() returns TaskResult with empty parent_ids for root tasks."""
    task = Task(id=ULID1, name="root", version=1, queue="default", status=TaskStatus.STARTED)
    result = task.make_result(results={"y": 2})

    assert isinstance(result, TaskResult)
    assert result.parent_ids == []


def test_make_result_passes_fanout_through():
    """make_result() includes a fanout when provided."""
    task = Task(id=ULID1, name="dispatcher", version=1, queue="default", status=TaskStatus.STARTED)
    fanout = DynamicFanOut(children=[DAGNode("child")], collector=DAGNode("collect"))
    result = task.make_result(results={}, fanout=fanout)

    assert isinstance(result, TaskResult)
    assert result.fanout is fanout


def test_make_result_defaults_to_empty_results():
    """make_result() with no arguments returns TaskResult with empty results."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.STARTED)
    result = task.make_result()

    assert isinstance(result, TaskResult)
    assert result.results == {}


# ── inject_parent_results propagation ────────────────────────────────────────


@pytest.mark.asyncio
async def test_generate_callbacks_simple_propagates_inject_flag():
    """SimpleCallback with inject_parent_results=True produces a child Task with the flag set."""
    child_spec = DAGTaskSpec(name="child_task")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        dag_callbacks=[SimpleCallback(task=child_spec, inject_parent_results=True)],
    )
    children = await task.generate_callbacks(_make_adapter())

    assert len(children) == 1
    assert children[0].inject_parent_results is True


@pytest.mark.asyncio
async def test_generate_callbacks_simple_no_injection_by_default():
    """SimpleCallback without inject_parent_results produces a child with the flag False."""
    child_spec = DAGTaskSpec(name="child_task")
    task = Task(
        id=ULID1,
        name="root",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        dag_callbacks=[SimpleCallback(task=child_spec)],
    )
    children = await task.generate_callbacks(_make_adapter())

    assert children[0].inject_parent_results is False


@pytest.mark.asyncio
async def test_generate_callbacks_fan_in_propagates_inject_flag():
    """FanInCallback with inject_parent_results=True produces a collector Task with the flag set."""
    p1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG09")
    collector_spec = DAGTaskSpec(name="collector")
    fan_in_key = f"dag:fan-in:{collector_spec.id}"
    task = Task(
        id=ULID1,
        name="last_branch",
        version=1,
        queue="default",
        status=TaskStatus.COMPLETED,
        dag_callbacks=[FanInCallback(task=collector_spec, fan_in_key=fan_in_key, inject_parent_results=True)],
    )
    children = await task.generate_callbacks(_make_adapter(fan_in_complete_return=0, fan_in_members=[p1]))

    assert len(children) == 1
    assert children[0].inject_parent_results is True


# ── valid_task_params: missing annotated parameters ───────────────────────────


def test_valid_task_params_skips_annotated_missing_param():
    """valid_task_params passes when an annotated parameter is absent from parameters (e.g. to-be-injected)."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)

    def fn(parent_results: dict | None = None) -> None: ...

    task.task_config = TaskConfig(name="t", version=1, function=fn, timeout=10)
    task.parameters = {}  # parent_results not yet present
    assert task.valid_task_params() is True


def test_valid_task_params_still_checks_provided_params():
    """valid_task_params still type-checks parameters that are present."""
    task = Task(id=ULID1, name="t", version=1, queue="default", status=TaskStatus.SUBMITTED)

    def fn(x: int) -> None: ...

    task.task_config = TaskConfig(name="t", version=1, function=fn, timeout=10)
    task.parameters = {"x": "not_an_int"}
    assert task.valid_task_params() is False


def test_to_dict_round_trip_preserves_inject_flag():
    """inject_parent_results survives a to_dict/from_dict round-trip."""
    task = _make_full_task(inject_parent_results=True)
    d = task.model_dump(context={"mode": "msgpack"}, exclude={"id"})
    restored = Task.model_validate({"id": task.id, **d})
    assert restored.inject_parent_results is True


def test_from_dict_missing_inject_flag_defaults_false():
    """from_dict defaults inject_parent_results to False for records that predate the field."""
    minimal = {"id": ULID1, "name": "t", "queue": "default", "version": 1, "status": "submitted"}
    task = Task.model_validate(minimal)
    assert task.inject_parent_results is False
