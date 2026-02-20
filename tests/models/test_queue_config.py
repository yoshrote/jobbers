import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from jobbers.models.queue_config import QueueConfig, RatePeriod
from jobbers.state_manager import QueueConfigAdapter
from jobbers.utils.serialization import serialize


@pytest_asyncio.fixture(autouse=True)
async def redis():
    """Fixture to reset the tasks in the mocked Redis before each test."""
    fake_store = fakeredis.FakeRedis()
    yield fake_store
    await fake_store.close()

@pytest.fixture
def queue_config_adapter(redis):
    """Fixture to provide a QueueConfigAdapter instance with a fake Redis data store."""
    return QueueConfigAdapter(redis)


@pytest.mark.parametrize(("rate_period", "rate_denominator", "expected_seconds"), [
    # bad configurations
    (None, 1, None),
    (RatePeriod.SECOND, None, None),
    # real configurations
    (RatePeriod.SECOND, 1, 1),
    (RatePeriod.MINUTE, 1, 60),
    (RatePeriod.HOUR, 1, 3600),
    (RatePeriod.DAY, 1, 86400),
])
def test_period_in_seconds(rate_period, rate_denominator, expected_seconds):
    config = QueueConfig(
        name="test_queue",
        rate_denominator=rate_denominator,
        rate_period=rate_period
    )
    assert config.period_in_seconds() == expected_seconds

@pytest.mark.parametrize(("input_value", "expected_result"), [
    (None, None),  # Test with None input
    (b"second", RatePeriod.SECOND),  # Valid input for SECOND
    (b"minute", RatePeriod.MINUTE),  # Valid input for MINUTE
    (b"hour", RatePeriod.HOUR),  # Valid input for HOUR
    (b"day", RatePeriod.DAY),  # Valid input for DAY
    (b"invalid", None),  # Invalid input
])
def test_from_bytes(input_value, expected_result):
    assert RatePeriod.from_bytes(input_value) == expected_result

@pytest.mark.parametrize(("rate_period", "expected_bytes"), [
    (RatePeriod.SECOND, b"second"),
    (RatePeriod.MINUTE, b"minute"),
    (RatePeriod.HOUR, b"hour"),
    (RatePeriod.DAY, b"day"),
])
def test_to_bytes(rate_period, expected_bytes):
    assert rate_period.to_bytes() == expected_bytes


@pytest.mark.asyncio
async def test_get_queues(redis, queue_config_adapter):
    """Test retrieving queues for a specific role."""
    # Add queues for a role
    await redis.sadd("worker-queues:role1", "queue1", "queue2")
    # Retrieve queues
    queues = await queue_config_adapter.get_queues("role1")
    assert set(queues) == {"queue1", "queue2"}

@pytest.mark.asyncio
async def test_get_queues_empty(redis, queue_config_adapter):
    """Test retrieving queues for a role with no queues."""
    queues = await queue_config_adapter.get_queues("role1")
    assert queues == set()

@pytest.mark.asyncio
async def test_set_queues(redis, queue_config_adapter):
    """Test setting queues for a specific role."""
    # Set queues for a role
    await queue_config_adapter.set_queues("role1", {"queue1", "queue2"})
    # Verify the queues were set
    queues = await redis.smembers("worker-queues:role1")
    assert set(queues) == {b"queue1", b"queue2"}
    all_queues = await redis.smembers("all-queues")
    assert set(all_queues) == {b"queue1", b"queue2"}

@pytest.mark.asyncio
async def test_get_all_queues(redis, queue_config_adapter):
    """Test retrieving all queues across roles."""
    # Add queues for multiple roles
    await redis.sadd("worker-queues:role1", "queue1", "queue2")
    await redis.sadd("worker-queues:role2", "queue3")
    # Retrieve all queues
    queues = await queue_config_adapter.get_all_queues()
    assert set(queues) == {"queue1", "queue2", "queue3"}

@pytest.mark.asyncio
async def test_get_all_queues_empty(redis, queue_config_adapter):
    """Test retrieving all queues when no queues exist."""
    queues = await queue_config_adapter.get_all_queues()
    assert queues == []

@pytest.mark.asyncio
async def test_get_all_roles(redis, queue_config_adapter):
    """Test retrieving all roles."""
    # Add roles with queues
    await redis.sadd("worker-queues:role1", "queue1")
    await redis.sadd("worker-queues:role2", "queue2")
    # Retrieve all roles
    roles = await queue_config_adapter.get_all_roles()
    assert set(roles) == {"role1", "role2"}

@pytest.mark.asyncio
async def test_get_all_roles_empty(queue_config_adapter):
    """Test retrieving all roles when no roles exist."""
    roles = await queue_config_adapter.get_all_roles()
    assert roles == []

@pytest.mark.asyncio
async def test_get_queue_limits_empty_set(queue_config_adapter):
    """Test get_queue_limits with an empty set of queues."""
    result = await queue_config_adapter.get_queue_limits(set())
    assert result == {}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_with_limit(redis, queue_config_adapter):
    """Test get_queue_limits with a single queue that has max_concurrent set."""
    # Set up queue config in Redis
    await redis.hset("queue-config:test_queue", mapping={
        "max_concurrent": "5",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"test_queue"})
    assert result == {"test_queue": 5}


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_no_limit(redis, queue_config_adapter):
    """Test get_queue_limits with a queue that has no max_concurrent limit."""
    # Set up queue config in Redis with no max_concurrent (defaults to 10)
    await redis.hset("queue-config:unlimited_queue", mapping={
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"unlimited_queue"})
    assert result == {"unlimited_queue": 10}  # Default value


@pytest.mark.asyncio
async def test_get_queue_limits_single_queue_zero_limit(redis, queue_config_adapter):
    """Test get_queue_limits with a queue that has max_concurrent set to 0."""
    # Set up queue config in Redis
    await redis.hset("queue-config:zero_limit_queue", mapping={
        "max_concurrent": "0",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"zero_limit_queue"})
    assert result == {"zero_limit_queue": 0}


@pytest.mark.asyncio
async def test_get_queue_limits_multiple_queues(redis, queue_config_adapter):
    """Test get_queue_limits with multiple queues having different limits."""
    # Set up multiple queue configs in Redis
    await redis.hset("queue-config:queue1", mapping={
        "max_concurrent": "3",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    await redis.hset("queue-config:queue2", mapping={
        "max_concurrent": "10",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    await redis.hset("queue-config:queue3", mapping={
        "max_concurrent": "1",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"queue1", "queue2", "queue3"})
    expected = {"queue1": 3, "queue2": 10, "queue3": 1}
    assert result == expected


@pytest.mark.asyncio
async def test_get_queue_limits_with_nonexistent_queue(queue_config_adapter):
    """Test get_queue_limits with a queue that doesn't exist in Redis."""
    # Don't set up any queue config - this should still work with defaults
    result = await queue_config_adapter.get_queue_limits({"nonexistent_queue"})
    assert result == {"nonexistent_queue": 10}  # Default max_concurrent


@pytest.mark.asyncio
async def test_get_queue_limits_mixed_existing_and_nonexistent(redis, queue_config_adapter):
    """Test get_queue_limits with a mix of existing and non-existing queues."""
    # Set up one queue config
    await redis.hset("queue-config:existing_queue", mapping={
        "max_concurrent": "7",
        "rate_numerator": serialize(None),
        "rate_denominator": serialize(None),
        "rate_period": ""
    })

    result = await queue_config_adapter.get_queue_limits({"existing_queue", "nonexistent_queue"})
    expected = {"existing_queue": 7, "nonexistent_queue": 10}
    assert result == expected


@pytest.mark.asyncio
async def test_get_queue_limits_with_rate_limiting_config(redis, queue_config_adapter):
    """Test get_queue_limits with queues that have rate limiting configured (should still return max_concurrent)."""
    # Set up queue config with rate limiting
    await redis.hset("queue-config:rate_limited_queue", mapping={
        "max_concurrent": "15",
        "rate_numerator": serialize(5),
        "rate_denominator": serialize(2),
        "rate_period": "minute"
    })

    result = await queue_config_adapter.get_queue_limits({"rate_limited_queue"})
    assert result == {"rate_limited_queue": 15}


@pytest.mark.asyncio
async def test_get_queue_limits_large_number_of_queues(redis, queue_config_adapter):
    """Test get_queue_limits with a large number of queues to ensure it handles concurrency well."""
    queue_names = [f"queue_{i}" for i in range(20)]

    # Set up configs for all queues
    for i, queue_name in enumerate(queue_names):
        await redis.hset(f"queue-config:{queue_name}", mapping={
            "max_concurrent": str(i + 1),  # Different limit for each queue
            "rate_numerator": serialize(None),
            "rate_denominator": serialize(None),
            "rate_period": ""
        })

    result = await queue_config_adapter.get_queue_limits(set(queue_names))

    # Verify all queues are present with correct limits
    assert len(result) == 20
    for i, queue_name in enumerate(queue_names):
        expected_limit = i + 1
        assert result[queue_name] == expected_limit
