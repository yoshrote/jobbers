"""Tests specific to JsonDeadQueue (Redis Stack: RedisJSON + RediSearch)."""

import datetime as dt

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from jobbers.adapters.json_redis import JsonDeadQueue, JsonTaskAdapter
from jobbers.db import DEFAULT_REDIS_URL
from jobbers.models.task import Task
from jobbers.models.task_status import TaskStatus

EARLIER = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
LATER = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)


def make_task(
    task_id: str = "01JQC31AJP7TSA9X8AEP64XG08",
    name: str = "my_task",
    version: int = 1,
    queue: str = "default",
) -> Task:
    from ulid import ULID

    return Task(
        id=ULID.from_str(task_id),
        name=name,
        version=version,
        queue=queue,
        status=TaskStatus.FAILED,
        errors=["something went wrong"],
    )


@pytest_asyncio.fixture
async def redis():
    """Real Redis instance on db=0 for Redis Stack tests."""
    client = aioredis.from_url(DEFAULT_REDIS_URL, db=0)
    try:
        await client.flushdb()
    except RedisConnectionError as exc:
        await client.aclose()
        pytest.skip(f"Redis not available: {exc}")
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def task_adapter(redis):
    """JsonTaskAdapter backed by real Redis Stack instance."""
    adapter = JsonTaskAdapter(redis)
    try:
        await adapter.ensure_index()
    except ResponseError as exc:
        pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
    return adapter


@pytest_asyncio.fixture
async def dq(redis, task_adapter):
    """JsonDeadQueue backed by real Redis Stack instance."""
    jdq = JsonDeadQueue(redis, task_adapter)
    try:
        await jdq.ensure_index()
    except ResponseError as exc:
        pytest.skip(f"Redis Stack (RediSearch) not available: {exc}")
    yield jdq


async def add_to_dlq(dq: JsonDeadQueue, task: Task, failed_at: dt.datetime) -> None:
    pipe = dq.data_store.pipeline(transaction=True)
    dq.stage_add(pipe, task, failed_at)
    await pipe.execute()


# ── json-specific ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_by_filter_sorted_by_failed_at_desc(dq, task_adapter):
    """Results should be sorted by failed_at descending (most recent first)."""
    t1 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG01")
    t2 = make_task(task_id="01JQC31AJP7TSA9X8AEP64XG02")
    await task_adapter.save_task(t1)
    await task_adapter.save_task(t2)
    await add_to_dlq(dq, t1, EARLIER)
    await add_to_dlq(dq, t2, LATER)

    results = await dq.get_by_filter()
    assert len(results) == 2
    assert results[0].id == t2.id  # LATER first
