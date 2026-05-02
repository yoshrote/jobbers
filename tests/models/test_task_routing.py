import pytest
from pydantic import ValidationError

from jobbers.models.task_routing import RoutingConfig, RoutingStrategy, TaskRoutingConfigAdapter


@pytest.fixture
def adapter(session_factory):
    return TaskRoutingConfigAdapter(session_factory)


# ---------------------------------------------------------------------------
# RoutingConfig validators
# ---------------------------------------------------------------------------


def test_single_valid():
    config = RoutingConfig(strategy=RoutingStrategy.SINGLE, queues=["fast"])
    assert config.queues == ["fast"]
    assert config.weights is None


def test_single_requires_exactly_one_queue():
    with pytest.raises(ValidationError):
        RoutingConfig(strategy=RoutingStrategy.SINGLE, queues=["fast", "slow"])


def test_single_rejects_empty_queues():
    with pytest.raises(ValidationError):
        RoutingConfig(strategy=RoutingStrategy.SINGLE, queues=[])


def test_single_rejects_weights():
    with pytest.raises(ValidationError):
        RoutingConfig(strategy=RoutingStrategy.SINGLE, queues=["fast"], weights=[1.0])


def test_weighted_valid():
    config = RoutingConfig(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[0.7, 0.3])
    assert config.queues == ["fast", "slow"]
    assert config.weights == [0.7, 0.3]


def test_weighted_equal_weights():
    config = RoutingConfig(strategy=RoutingStrategy.WEIGHTED, queues=["a", "b", "c"], weights=[1.0, 1.0, 1.0])
    assert len(config.weights) == 3  # type: ignore[arg-type]


def test_weighted_requires_at_least_two_queues():
    with pytest.raises(ValidationError):
        RoutingConfig(strategy=RoutingStrategy.WEIGHTED, queues=["only"], weights=[1.0])


def test_weighted_requires_weights():
    with pytest.raises(ValidationError):
        RoutingConfig(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"])


def test_weighted_weights_length_must_match_queues():
    with pytest.raises(ValidationError):
        RoutingConfig(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[1.0])


# ---------------------------------------------------------------------------
# RoutingConfig.from_row
# ---------------------------------------------------------------------------


def test_from_row_single():
    row = ("my_task", 1, "single", '["fast"]', None)
    config = RoutingConfig.from_row(row)
    assert config.task_name == "my_task"
    assert config.task_version == 1
    assert config.strategy == RoutingStrategy.SINGLE
    assert config.queues == ["fast"]
    assert config.weights is None


def test_from_row_weighted():
    row = ("my_task", 2, "weighted", '["fast", "slow"]', '[0.8, 0.2]')
    config = RoutingConfig.from_row(row)
    assert config.strategy == RoutingStrategy.WEIGHTED
    assert config.queues == ["fast", "slow"]
    assert config.weights == [0.8, 0.2]


# ---------------------------------------------------------------------------
# TaskRoutingConfigAdapter CRUD
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_routing_config_returns_none_when_absent(adapter):
    result = await adapter.get_routing_config("unknown_task", 1)
    assert result is None


@pytest.mark.asyncio
async def test_save_and_get_single(adapter):
    config = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    await adapter.save_routing_config(config)
    result = await adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.strategy == RoutingStrategy.SINGLE
    assert result.queues == ["fast"]
    assert result.weights is None


@pytest.mark.asyncio
async def test_save_and_get_weighted(adapter):
    config = RoutingConfig(
        task_name="echo", task_version=1, strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[2.0, 1.0]
    )
    await adapter.save_routing_config(config)
    result = await adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.strategy == RoutingStrategy.WEIGHTED
    assert result.queues == ["fast", "slow"]
    assert result.weights == [2.0, 1.0]


@pytest.mark.asyncio
async def test_save_upserts_existing(adapter):
    config1 = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    await adapter.save_routing_config(config1)

    config2 = RoutingConfig(
        task_name="echo", task_version=1, strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[1.0, 1.0]
    )
    await adapter.save_routing_config(config2)

    result = await adapter.get_routing_config("echo", 1)
    assert result is not None
    assert result.strategy == RoutingStrategy.WEIGHTED


@pytest.mark.asyncio
async def test_configs_are_isolated_by_version(adapter):
    v1 = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    v2 = RoutingConfig(
        task_name="echo", task_version=2, strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[1.0, 1.0]
    )
    await adapter.save_routing_config(v1)
    await adapter.save_routing_config(v2)

    r1 = await adapter.get_routing_config("echo", 1)
    r2 = await adapter.get_routing_config("echo", 2)
    assert r1 is not None
    assert r1.strategy == RoutingStrategy.SINGLE
    assert r2 is not None
    assert r2.strategy == RoutingStrategy.WEIGHTED


@pytest.mark.asyncio
async def test_delete_returns_false_when_absent(adapter):
    deleted = await adapter.delete_routing_config("ghost", 99)
    assert deleted is False


@pytest.mark.asyncio
async def test_delete_removes_config(adapter):
    config = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    await adapter.save_routing_config(config)
    deleted = await adapter.delete_routing_config("echo", 1)
    assert deleted is True
    assert await adapter.get_routing_config("echo", 1) is None


@pytest.mark.asyncio
async def test_delete_second_call_returns_false(adapter):
    config = RoutingConfig(task_name="echo", task_version=1, strategy=RoutingStrategy.SINGLE, queues=["fast"])
    await adapter.save_routing_config(config)
    await adapter.delete_routing_config("echo", 1)
    assert await adapter.delete_routing_config("echo", 1) is False
