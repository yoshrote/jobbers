from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, model_validator

# Sentinel stored in the ``from_lane`` column for the wildcard rule. SQL primary
# keys can't contain NULL portably, so the empty string stands in for "any lane"
# on the storage side only -- the model itself uses ``None``.
WILDCARD_LANE = ""


class RoutingStrategy(StrEnum):
    """Queue routing strategy for a task type."""

    SINGLE = "single"
    WEIGHTED = "weighted"


class RoutingRule(BaseModel):
    """
    One lane → queue(s) mapping within a task type's routing config.

    ``from_lane`` scopes the rule to work that *asked for* that lane. ``None``
    is the wildcard: it matches any lane, which is the blanket-override
    behaviour routing configs had before lanes existed.
    """

    from_lane: str | None = None
    strategy: RoutingStrategy
    queues: list[str]
    weights: list[float] | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.strategy == RoutingStrategy.SINGLE:
            if len(self.queues) != 1:
                raise ValueError("SINGLE routing requires exactly one queue")
            if self.weights is not None:
                raise ValueError("SINGLE routing does not accept weights")
        elif self.strategy == RoutingStrategy.WEIGHTED:
            if len(self.queues) < 2:
                raise ValueError("WEIGHTED routing requires at least two queues")
            if self.weights is None or len(self.weights) != len(self.queues):
                raise ValueError("WEIGHTED routing requires weights with the same length as queues")
        return self


class RoutingConfig(BaseModel):
    """
    Lane → queue routing configuration for a task type.

    A config is a list of rules. ``rule_for(lane)`` picks the rule scoped to
    that lane, falling back to the wildcard rule. When neither exists the caller
    applies the identity default: the lane resolves to the queue of the same
    name (see ``docs/lanes-and-queues.md``).
    """

    task_name: str = ""
    task_version: int = 0
    rules: list[RoutingRule]

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.rules:
            raise ValueError("A routing config requires at least one rule")
        seen: set[str | None] = set()
        for rule in self.rules:
            if rule.from_lane in seen:
                label = "wildcard" if rule.from_lane is None else repr(rule.from_lane)
                raise ValueError(f"Duplicate routing rule for {label} lane")
            seen.add(rule.from_lane)
        return self

    def rule_for(self, lane: str) -> RoutingRule | None:
        """Return the rule matching *lane*, falling back to the wildcard rule, else None."""
        wildcard: RoutingRule | None = None
        for rule in self.rules:
            if rule.from_lane == lane:
                return rule
            if rule.from_lane is None:
                wildcard = rule
        return wildcard

    def target_queues(self) -> set[str]:
        """Every physical queue any rule in this config can resolve to."""
        return {queue for rule in self.rules for queue in rule.queues}

    @classmethod
    def from_rows(cls, task_name: str, task_version: int, rows: list[Any]) -> Self:
        """
        Construct from ``task_routing_rules`` rows.

        Each row is ``(from_lane, strategy, queues_json, weights_json)``, where
        ``from_lane`` is ``WILDCARD_LANE`` for the wildcard rule.
        """
        return cls(
            task_name=task_name,
            task_version=task_version,
            rules=[
                RoutingRule(
                    from_lane=None if from_lane == WILDCARD_LANE else from_lane,
                    strategy=RoutingStrategy(strategy),
                    queues=json.loads(queues_json),
                    weights=json.loads(weights_json) if weights_json is not None else None,
                )
                for from_lane, strategy, queues_json, weights_json in rows
            ],
        )
