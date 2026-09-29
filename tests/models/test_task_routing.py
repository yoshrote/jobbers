import pytest
from pydantic import ValidationError

from jobbers.models.task_routing import WILDCARD_LANE, RoutingConfig, RoutingRule, RoutingStrategy

# ---------------------------------------------------------------------------
# RoutingRule validators
# ---------------------------------------------------------------------------


def test_single_valid():
    rule = RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["fast"])
    assert rule.queues == ["fast"]
    assert rule.weights is None
    assert rule.from_lane is None


def test_single_requires_exactly_one_queue():
    with pytest.raises(ValidationError):
        RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["fast", "slow"])


def test_single_rejects_empty_queues():
    with pytest.raises(ValidationError):
        RoutingRule(strategy=RoutingStrategy.SINGLE, queues=[])


def test_single_rejects_weights():
    with pytest.raises(ValidationError):
        RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["fast"], weights=[1.0])


def test_weighted_valid():
    rule = RoutingRule(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[0.7, 0.3])
    assert rule.queues == ["fast", "slow"]
    assert rule.weights == [0.7, 0.3]


def test_weighted_equal_weights():
    rule = RoutingRule(strategy=RoutingStrategy.WEIGHTED, queues=["a", "b", "c"], weights=[1.0, 1.0, 1.0])
    assert len(rule.weights) == 3  # type: ignore[arg-type]


def test_weighted_requires_at_least_two_queues():
    with pytest.raises(ValidationError):
        RoutingRule(strategy=RoutingStrategy.WEIGHTED, queues=["only"], weights=[1.0])


def test_weighted_requires_weights():
    with pytest.raises(ValidationError):
        RoutingRule(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"])


def test_weighted_weights_length_must_match_queues():
    with pytest.raises(ValidationError):
        RoutingRule(strategy=RoutingStrategy.WEIGHTED, queues=["fast", "slow"], weights=[1.0])


def test_rule_validation_propagates_through_config():
    with pytest.raises(ValidationError):
        RoutingConfig(rules=[RoutingRule.model_construct(strategy=RoutingStrategy.SINGLE, queues=[])])


# ---------------------------------------------------------------------------
# RoutingConfig validators
# ---------------------------------------------------------------------------


def test_config_requires_at_least_one_rule():
    with pytest.raises(ValidationError):
        RoutingConfig(task_name="t", task_version=1, rules=[])


def test_config_rejects_duplicate_lane_rules():
    with pytest.raises(ValidationError):
        RoutingConfig(
            rules=[
                RoutingRule(from_lane="priority", strategy=RoutingStrategy.SINGLE, queues=["a"]),
                RoutingRule(from_lane="priority", strategy=RoutingStrategy.SINGLE, queues=["b"]),
            ]
        )


def test_config_rejects_two_wildcard_rules():
    with pytest.raises(ValidationError):
        RoutingConfig(
            rules=[
                RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["a"]),
                RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["b"]),
            ]
        )


# ---------------------------------------------------------------------------
# RoutingConfig.rule_for
# ---------------------------------------------------------------------------


def test_rule_for_prefers_lane_scoped_rule_over_wildcard():
    config = RoutingConfig(
        rules=[
            RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["bulk"]),
            RoutingRule(from_lane="priority", strategy=RoutingStrategy.SINGLE, queues=["fast"]),
        ]
    )
    assert config.rule_for("priority").queues == ["fast"]  # type: ignore[union-attr]


def test_rule_for_falls_back_to_wildcard():
    config = RoutingConfig(
        rules=[
            RoutingRule(from_lane="priority", strategy=RoutingStrategy.SINGLE, queues=["fast"]),
            RoutingRule(strategy=RoutingStrategy.SINGLE, queues=["bulk"]),
        ]
    )
    assert config.rule_for("standard").queues == ["bulk"]  # type: ignore[union-attr]


def test_rule_for_returns_none_when_no_rule_applies():
    config = RoutingConfig(
        rules=[RoutingRule(from_lane="priority", strategy=RoutingStrategy.SINGLE, queues=["fast"])]
    )
    assert config.rule_for("standard") is None


def test_target_queues_spans_every_rule():
    config = RoutingConfig(
        rules=[
            RoutingRule(from_lane="priority", strategy=RoutingStrategy.SINGLE, queues=["fast"]),
            RoutingRule(
                from_lane="standard",
                strategy=RoutingStrategy.WEIGHTED,
                queues=["bulk-a", "bulk-b"],
                weights=[1.0, 1.0],
            ),
        ]
    )
    assert config.target_queues() == {"fast", "bulk-a", "bulk-b"}


# ---------------------------------------------------------------------------
# RoutingConfig.from_rows
# ---------------------------------------------------------------------------


def test_from_rows_single():
    rows = [(WILDCARD_LANE, "single", '["fast"]', None)]
    config = RoutingConfig.from_rows("my_task", 1, rows)
    assert config.task_name == "my_task"
    assert config.task_version == 1
    assert len(config.rules) == 1
    assert config.rules[0].from_lane is None
    assert config.rules[0].strategy == RoutingStrategy.SINGLE
    assert config.rules[0].queues == ["fast"]
    assert config.rules[0].weights is None


def test_from_rows_weighted():
    rows = [(WILDCARD_LANE, "weighted", '["fast", "slow"]', "[0.8, 0.2]")]
    config = RoutingConfig.from_rows("my_task", 2, rows)
    assert config.rules[0].strategy == RoutingStrategy.WEIGHTED
    assert config.rules[0].queues == ["fast", "slow"]
    assert config.rules[0].weights == [0.8, 0.2]


def test_from_rows_lane_scoped():
    rows = [
        ("priority", "single", '["fast"]', None),
        (WILDCARD_LANE, "single", '["bulk"]', None),
    ]
    config = RoutingConfig.from_rows("my_task", 1, rows)
    assert config.rule_for("priority").queues == ["fast"]  # type: ignore[union-attr]
    assert config.rule_for("anything-else").queues == ["bulk"]  # type: ignore[union-attr]
