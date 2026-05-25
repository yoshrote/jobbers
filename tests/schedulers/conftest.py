import pytest_asyncio

from jobbers.adapters.sql import SQLQueueConfigAdapter
from jobbers.schedulers.sql_task_scheduler import SQLTaskScheduler
from jobbers.schedulers.task_scheduler import TaskScheduler


@pytest_asyncio.fixture
async def qca(session_factory):
    yield SQLQueueConfigAdapter(session_factory)


@pytest_asyncio.fixture
async def redis_scheduler(redis, dummy_task_adapter, session_factory):
    """TaskScheduler backed by FakeAsyncRedis + DummyTaskAdapter."""
    qca = SQLQueueConfigAdapter(session_factory)
    yield TaskScheduler(redis, dummy_task_adapter, qca.get_all_queues)


@pytest_asyncio.fixture(params=["redis", "sql"], ids=["redis", "sql"])
async def scheduler(request, redis, dummy_task_adapter, session_factory):
    """
    Parametrized fixture yielding a scheduler implementation for each backend.

    - ``"redis"``: TaskScheduler backed by FakeAsyncRedis + DummyTaskAdapter
    - ``"sql"``: SQLTaskScheduler backed by in-memory SQLite

    Both implementations expose ``pipeline()`` / ``stage_add()`` / ``stage_remove()`` /
    ``next_due()`` / ``next_due_bulk()`` / ``get_by_filter()`` / ``get_run_at()``.
    SQLTaskScheduler embeds full task data in the schedule table, so tests that call
    ``dummy_task_adapter.save_task()`` before scheduling are fine — those writes go to
    the dummy in-memory store and are ignored by the SQL scheduler.
    """
    qca = SQLQueueConfigAdapter(session_factory)
    if request.param == "redis":
        yield TaskScheduler(redis, dummy_task_adapter, qca.get_all_queues)
    else:
        yield SQLTaskScheduler(session_factory, qca.get_all_queues)
