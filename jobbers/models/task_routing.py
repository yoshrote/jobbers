from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, model_validator


class RoutingStrategy(StrEnum):
    """Queue routing strategy for a task type."""

    SINGLE = "single"
    WEIGHTED = "weighted"


class RoutingConfig(BaseModel):
    """Queue routing configuration for a task type."""

    task_name: str = ""
    task_version: int = 0
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

    @classmethod
    def from_row(cls, row: Any) -> Self:
        """Construct from a DB row (task_name, task_version, strategy, queues_json, weights_json)."""
        task_name, task_version, strategy, queues_json, weights_json = row
        return cls(
            task_name=task_name,
            task_version=task_version,
            strategy=RoutingStrategy(strategy),
            queues=json.loads(queues_json),
            weights=json.loads(weights_json) if weights_json is not None else None,
        )
