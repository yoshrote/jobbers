import pytest

from jobbers.models.queue_config import QueueConfig, QueueConfigAdapter, RatePeriod


@pytest.fixture
def queue_config_adapter(sqlite_conn):
    return QueueConfigAdapter(sqlite_conn)


# ---------------------------------------------------------------------------
# RatePeriod
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rate_period", "rate_denominator", "expected_seconds"),
    [
        (None, 1, None),
        (RatePeriod.SECOND, None, None),
        (RatePeriod.SECOND, 1, 1),
        (RatePeriod.MINUTE, 1, 60),
        (RatePeriod.HOUR, 1, 3600),
        (RatePeriod.DAY, 1, 86400),
    ],
)
def test_period_in_seconds(rate_period, rate_denominator, expected_seconds):
    config = QueueConfig(name="test_queue", rate_denominator=rate_denominator, rate_period=rate_period)
    assert config.period_in_seconds() == expected_seconds


# ---------------------------------------------------------------------------
# get_queues / save_role
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_queues(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_role("role1", {"queue1", "queue2"})
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == {"queue1", "queue2"}


@pytest.mark.asyncio
async def test_get_queues_empty(queue_config_adapter):
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == set()


@pytest.mark.asyncio
async def test_save_role(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_role("role1", {"queue1", "queue2"})
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == {"queue1", "queue2"}


@pytest.mark.asyncio
async def test_save_role_replaces_existing(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue3"))
    await queue_config_adapter.save_role("role1", {"queue1", "queue2"})
    await queue_config_adapter.save_role("role1", {"queue3"})
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == {"queue3"}


# ---------------------------------------------------------------------------
# get_all_queues
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_queues(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue3"))
    queues = await queue_config_adapter.get_all_queues()
    assert set(queues) == {"queue1", "queue2", "queue3"}


@pytest.mark.asyncio
async def test_get_all_queues_empty(queue_config_adapter):
    queues = await queue_config_adapter.get_all_queues()
    assert queues == []


# ---------------------------------------------------------------------------
# get_all_roles
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_roles(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1"))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2"))
    await queue_config_adapter.save_role("role1", {"queue1"})
    await queue_config_adapter.save_role("role2", {"queue2"})
    roles = await queue_config_adapter.get_all_roles()
    assert set(roles) == {"role1", "role2"}


@pytest.mark.asyncio
async def test_get_all_roles_empty(queue_config_adapter):
    roles = await queue_config_adapter.get_all_roles()
    assert roles == []


# ---------------------------------------------------------------------------
# get_queue_limits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_queue_limits_empty_set(queue_config_adapter):
    result = await queue_config_adapter.get_queue_limits(set())
    assert result == {}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_with_limit(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="test_queue", max_concurrent=5))
    result = await queue_config_adapter.get_queue_limits({"test_queue"})
    assert result == {"test_queue": 5}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_no_limit(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="unlimited_queue", max_concurrent=None))
    result = await queue_config_adapter.get_queue_limits({"unlimited_queue"})
    assert result == {"unlimited_queue": None}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_zero_limit(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="zero_limit_queue", max_concurrent=0))
    result = await queue_config_adapter.get_queue_limits({"zero_limit_queue"})
    assert result == {"zero_limit_queue": 0}


@pytest.mark.asyncio
async def test_get_queue_limits_multiple_queues(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue1", max_concurrent=3))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue2", max_concurrent=10))
    await queue_config_adapter.save_queue_config(QueueConfig(name="queue3", max_concurrent=1))
    result = await queue_config_adapter.get_queue_limits({"queue1", "queue2", "queue3"})
    assert result == {"queue1": 3, "queue2": 10, "queue3": 1}


@pytest.mark.asyncio
async def test_get_queue_limits_with_nonexistent_queue(queue_config_adapter):
    result = await queue_config_adapter.get_queue_limits({"nonexistent_queue"})
    assert result == {"nonexistent_queue": None}


@pytest.mark.asyncio
async def test_get_queue_limits_mixed_existing_and_nonexistent(queue_config_adapter):
    await queue_config_adapter.save_queue_config(QueueConfig(name="existing_queue", max_concurrent=7))
    result = await queue_config_adapter.get_queue_limits({"existing_queue", "nonexistent_queue"})
    assert result == {"existing_queue": 7, "nonexistent_queue": None}


@pytest.mark.asyncio
async def test_get_queue_limits_with_rate_limiting_config(queue_config_adapter):
    await queue_config_adapter.save_queue_config(
        QueueConfig(
            name="rate_limited_queue",
            max_concurrent=15,
            rate_numerator=5,
            rate_denominator=2,
            rate_period=RatePeriod.MINUTE,
        )
    )
    result = await queue_config_adapter.get_queue_limits({"rate_limited_queue"})
    assert result == {"rate_limited_queue": 15}


@pytest.mark.asyncio
async def test_get_queue_limits_large_number_of_queues(queue_config_adapter):
    queue_names = [f"queue_{i}" for i in range(20)]
    for i, name in enumerate(queue_names):
        await queue_config_adapter.save_queue_config(QueueConfig(name=name, max_concurrent=i + 1))
    result = await queue_config_adapter.get_queue_limits(set(queue_names))
    assert len(result) == 20
    for i, name in enumerate(queue_names):
        assert result[name] == i + 1
