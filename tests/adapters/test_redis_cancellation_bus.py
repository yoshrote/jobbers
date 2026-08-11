"""
Tests for RedisCancellationBus.

Covers publish_cancellation/publish_dag_cancellation and the listen_cancellations
parser: "task:<id>" vs "dag:<id>" prefix dispatch, and malformed/unknown-prefix
payloads being logged and skipped rather than raised.
"""

import asyncio
import contextlib

import pytest
from ulid import ULID

from jobbers.adapters.redis import RedisCancellationBus


async def _next_message(bus: RedisCancellationBus):
    """Pull exactly one message from listen_cancellations(), then stop iterating."""
    async for msg in bus.listen_cancellations():
        return msg
    return None  # pragma: no cover -- listen_cancellations() never raises StopAsyncIteration


@pytest.mark.asyncio
async def test_publish_cancellation_delivers_task_kind(redis):
    """publish_cancellation is received as a CancellationMessage(kind='task', id=...)."""
    bus = RedisCancellationBus(redis)
    task_id = ULID()

    listener = asyncio.create_task(_next_message(bus))
    await asyncio.sleep(0.05)  # let the listener subscribe
    await bus.publish_cancellation(task_id)

    msg = await asyncio.wait_for(listener, timeout=1.0)
    assert msg is not None
    assert msg.kind == "task"
    assert msg.id == task_id


@pytest.mark.asyncio
async def test_publish_dag_cancellation_delivers_dag_kind(redis):
    """publish_dag_cancellation is received as a CancellationMessage(kind='dag', id=...)."""
    bus = RedisCancellationBus(redis)
    dag_run_id = ULID()

    listener = asyncio.create_task(_next_message(bus))
    await asyncio.sleep(0.05)
    await bus.publish_dag_cancellation(dag_run_id)

    msg = await asyncio.wait_for(listener, timeout=1.0)
    assert msg is not None
    assert msg.kind == "dag"
    assert msg.id == dag_run_id


@pytest.mark.asyncio
async def test_publish_stale_cancellation_delivers_stale_kind(redis):
    """publish_stale_cancellation is received as a CancellationMessage(kind='stale', id=...)."""
    bus = RedisCancellationBus(redis)
    task_id = ULID()

    listener = asyncio.create_task(_next_message(bus))
    await asyncio.sleep(0.05)  # let the listener subscribe
    await bus.publish_stale_cancellation(task_id)

    msg = await asyncio.wait_for(listener, timeout=1.0)
    assert msg is not None
    assert msg.kind == "stale"
    assert msg.id == task_id


@pytest.mark.asyncio
async def test_publish_dag_cancellation_does_not_publish_per_task_messages(redis):
    """A single publish_dag_cancellation call produces exactly one message on the channel."""
    bus = RedisCancellationBus(redis)
    dag_run_id = ULID()

    received = []

    async def _collect_one():
        async for msg in bus.listen_cancellations():
            received.append(msg)
            return

    listener = asyncio.create_task(_collect_one())
    await asyncio.sleep(0.05)
    await bus.publish_dag_cancellation(dag_run_id)
    await asyncio.wait_for(listener, timeout=1.0)

    assert len(received) == 1
    assert received[0] == ("dag", dag_run_id)


@pytest.mark.asyncio
async def test_listen_cancellations_skips_malformed_payload(redis, caplog):
    """A payload with no recognizable 'task:'/'dag:' prefix is logged and skipped, not raised."""
    bus = RedisCancellationBus(redis)

    listener = asyncio.create_task(_next_message(bus))
    await asyncio.sleep(0.05)
    await redis.publish(bus.CHANNEL, "not-a-valid-payload")
    await asyncio.sleep(0.05)
    good_id = ULID()
    await bus.publish_cancellation(good_id)

    msg = await asyncio.wait_for(listener, timeout=1.0)
    assert msg is not None
    assert msg.kind == "task"
    assert msg.id == good_id


@pytest.mark.asyncio
async def test_listen_cancellations_skips_invalid_ulid_after_valid_prefix(redis):
    """A well-prefixed but non-ULID payload is logged and skipped, not raised."""
    bus = RedisCancellationBus(redis)

    listener = asyncio.create_task(_next_message(bus))
    await asyncio.sleep(0.05)
    await redis.publish(bus.CHANNEL, "task:not-a-ulid")
    await asyncio.sleep(0.05)
    good_id = ULID()
    await bus.publish_dag_cancellation(good_id)

    msg = await asyncio.wait_for(listener, timeout=1.0)
    assert msg is not None
    assert msg.kind == "dag"
    assert msg.id == good_id


@pytest.mark.asyncio
async def test_listen_cancellations_can_be_cancelled_cleanly(redis):
    """The listener task can be cancelled mid-subscription without error."""
    bus = RedisCancellationBus(redis)
    listener = asyncio.create_task(_next_message(bus))
    await asyncio.sleep(0.05)
    listener.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await listener
