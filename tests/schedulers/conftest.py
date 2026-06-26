import datetime as dt

import pytest_asyncio

from jobbers.adapters.redis import RedisCronDAGScheduler, RedisTaskScheduler
from jobbers.adapters.sql import SQLCronDAGScheduler, SQLQueueConfigAdapter, SQLTaskScheduler
from jobbers.adapters.static import StaticCronDAGScheduler
from jobbers.models.cron_dag import CronDAGEntry
from jobbers.models.dag import DAGTaskSpec

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)


def _make_static_entry() -> CronDAGEntry:
    return CronDAGEntry(
        name="static_test",
        cron_expr="0 * * * *",
        dag_spec=DAGTaskSpec(name="root_task"),
    )


@pytest_asyncio.fixture
async def qca(session_factory):
    yield SQLQueueConfigAdapter(session_factory)


@pytest_asyncio.fixture
async def redis_scheduler(redis, dummy_task_adapter, session_factory):
    """RedisTaskScheduler backed by the shared real Redis connection + DummyTaskAdapter."""
    qca = SQLQueueConfigAdapter(session_factory)
    yield RedisTaskScheduler(redis, dummy_task_adapter, qca.get_all_queues)


@pytest_asyncio.fixture(params=["redis", "sql"], ids=["redis", "sql"])
async def scheduler(request, redis, dummy_task_adapter, session_factory):
    """
    Parametrized fixture yielding a scheduler implementation for each backend.

    - ``"redis"``: RedisTaskScheduler backed by the shared real Redis connection + DummyTaskAdapter
    - ``"sql"``: SQLTaskScheduler backed by in-memory SQLite

    Both implementations expose ``pipeline()`` / ``stage_add()`` / ``stage_remove()`` /
    ``next_due()`` / ``next_due_bulk()`` / ``get_by_filter()`` / ``get_run_at()``.
    SQLTaskScheduler embeds full task data in the schedule table, so tests that call
    ``dummy_task_adapter.save_task()`` before scheduling are fine — those writes go to
    the dummy in-memory store and are ignored by the SQL scheduler.
    """
    qca = SQLQueueConfigAdapter(session_factory)
    if request.param == "redis":
        yield RedisTaskScheduler(redis, dummy_task_adapter, qca.get_all_queues)
    else:
        yield SQLTaskScheduler(session_factory, qca.get_all_queues)


@pytest_asyncio.fixture(params=["redis", "sql", "static"], ids=["redis", "sql", "static"])
async def cron_dag_scheduler(request, redis, session_factory):
    """
    Parametrized fixture yielding a CronDAGSchedulerProtocol implementation.

    - ``"redis"``: RedisCronDAGScheduler backed by the shared real Redis connection.
    - ``"sql"``: SQLCronDAGScheduler backed by in-memory SQLite.
    - ``"static"``: StaticCronDAGScheduler pre-seeded with one past-due entry
      (for tests that only need to read or list; add/remove raise ReadOnly).
    """
    if request.param == "redis":
        yield RedisCronDAGScheduler(redis)
    elif request.param == "sql":
        yield SQLCronDAGScheduler(session_factory)
    else:
        entry = _make_static_entry()
        yield StaticCronDAGScheduler(entries=[entry], initial_next_run_at={entry.id: PAST})


@pytest_asyncio.fixture(params=["redis", "sql"], ids=["redis", "sql"])
async def mutable_cron_dag_scheduler(request, redis, session_factory):
    """
    Parametrized fixture yielding a mutable CronDAGSchedulerProtocol implementation.

    Use this fixture for tests that call ``add()`` or ``remove()`` — the static backend
    raises ``RoutingBackendReadOnlyError`` on write operations and is excluded here.
    """
    if request.param == "redis":
        yield RedisCronDAGScheduler(redis)
    else:
        yield SQLCronDAGScheduler(session_factory)
