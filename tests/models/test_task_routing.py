import pytest
from pydantic import ValidationError

from jobbers.models.task_routing import RoutingConfig, RoutingStrategy

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
    row = ("my_task", 2, "weighted", '["fast", "slow"]', "[0.8, 0.2]")
    config = RoutingConfig.from_row(row)
    assert config.strategy == RoutingStrategy.WEIGHTED
    assert config.queues == ["fast", "slow"]
    assert config.weights == [0.8, 0.2]
