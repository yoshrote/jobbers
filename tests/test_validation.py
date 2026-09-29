from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task
from jobbers.models.task_config import TaskConfig
from jobbers.models.task_routing import RoutingConfig, RoutingRule, RoutingStrategy
from jobbers.validation import ValidationError, validate_task

ULID1 = ULID.from_str("01JQC31AJP7TSA9X8AEP64XG08")


def _mock_sm(routing=None, queue_config=None):
    sm = AsyncMock()
    sm.get_routing_config = AsyncMock(return_value=routing)
    sm.get_queue_config = AsyncMock(return_value=queue_config)
    return sm


@pytest.mark.asyncio
async def test_validate_task_unregistered():
    """Unregistered task raises ValidationError without any Redis calls."""
    task = Task(id=ULID1, name="unknown_task", parameters={})
    with patch("jobbers.registry.get_task_config", return_value=None):
        with pytest.raises(ValidationError, match="Unknown task"):
            await validate_task(task, _mock_sm())


@pytest.mark.asyncio
async def test_validate_task_invalid_params():
    """Task with wrong parameter type raises ValidationError."""

    async def task_function(foo: int) -> None: ...

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", parameters={"foo": "bar"})
    with patch("jobbers.registry.get_task_config", return_value=task_config):
        with pytest.raises(ValidationError, match="Invalid parameters"):
            await validate_task(task, _mock_sm())


@pytest.mark.asyncio
async def test_validate_task_valid_sets_task_config():
    """Valid task passes validation and task_config is set on the task."""

    async def task_function(foo: int) -> None: ...

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", parameters={"foo": 42})

    with patch("jobbers.registry.get_task_config", return_value=task_config):
        await validate_task(task, _mock_sm(routing=None, queue_config=QueueConfig(name="default")))

    assert task.task_config is task_config


@pytest.mark.asyncio
async def test_validate_task_unmapped_lane_without_matching_queue():
    """A lane with no routing rule and no same-named queue raises ValidationError."""

    async def task_function(foo: int) -> None: ...

    task_config = TaskConfig(name="Test Task", function=task_function)
    task = Task(id=ULID1, name="Test Task", lane="unknown-lane", parameters={"foo": 42})

    with patch("jobbers.registry.get_task_config", return_value=task_config):
        with pytest.raises(ValidationError, match="Unknown lane unknown-lane"):
            await validate_task(task, _mock_sm(routing=None, queue_config=None))


# ── lane validity ─────────────────────────────────────────────────────────────


def _task_config():
    async def task_function(foo: int) -> None: ...

    return TaskConfig(name="Test Task", function=task_function)


@pytest.mark.asyncio
async def test_validate_task_lane_valid_via_same_named_queue():
    """With no routing rule, a lane is valid when a queue of that name exists."""
    task = Task(id=ULID1, name="Test Task", lane="heavy", parameters={"foo": 42})
    sm = _mock_sm(routing=None, queue_config=QueueConfig(name="heavy"))
    with patch("jobbers.registry.get_task_config", return_value=_task_config()):
        await validate_task(task, sm)
    sm.get_queue_config.assert_awaited_once_with("heavy")


@pytest.mark.asyncio
async def test_validate_task_lane_valid_via_routing_rule():
    """A lane with a matching rule is valid even with no queue of that name."""
    routing = RoutingConfig(
        task_name="Test Task",
        task_version=0,
        rules=[RoutingRule(from_lane="gold", strategy=RoutingStrategy.SINGLE, queues=["shard-a"])],
    )
    task = Task(id=ULID1, name="Test Task", lane="gold", parameters={"foo": 42})
    sm = _mock_sm(routing=routing, queue_config=QueueConfig(name="shard-a"))
    with patch("jobbers.registry.get_task_config", return_value=_task_config()):
        await validate_task(task, sm)
    # The rule's *target* queue is what gets checked, not the lane name.
    sm.get_queue_config.assert_awaited_once_with("shard-a")


@pytest.mark.asyncio
async def test_validate_task_routing_rule_targeting_unknown_queue():
    """A matching rule pointing at a queue that does not exist is a validation error."""
    routing = RoutingConfig(
        task_name="Test Task",
        task_version=0,
        rules=[RoutingRule(from_lane="gold", strategy=RoutingStrategy.SINGLE, queues=["ghost"])],
    )
    task = Task(id=ULID1, name="Test Task", lane="gold", parameters={"foo": 42})
    with patch("jobbers.registry.get_task_config", return_value=_task_config()):
        with pytest.raises(ValidationError, match="targets unknown queue ghost"):
            await validate_task(task, _mock_sm(routing=routing, queue_config=None))


@pytest.mark.asyncio
async def test_validate_task_unmatched_lane_falls_back_to_queue_check():
    """A lane no rule matches still validates against a same-named queue."""
    routing = RoutingConfig(
        task_name="Test Task",
        task_version=0,
        rules=[RoutingRule(from_lane="gold", strategy=RoutingStrategy.SINGLE, queues=["shard-a"])],
    )
    task = Task(id=ULID1, name="Test Task", lane="silver", parameters={"foo": 42})
    sm = _mock_sm(routing=routing, queue_config=QueueConfig(name="silver"))
    with patch("jobbers.registry.get_task_config", return_value=_task_config()):
        await validate_task(task, sm)
    sm.get_queue_config.assert_awaited_once_with("silver")
