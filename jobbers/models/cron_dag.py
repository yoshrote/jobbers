"""Model for recurring cron-scheduled DAG entries."""

from __future__ import annotations

import datetime as dt
from enum import Enum

from pydantic import BaseModel, Field
from pydantic import field_serializer
from ulid import ULID

from jobbers.models.dag import DAGTaskSpec


class ConcurrencyPolicy(str, Enum):
    """Controls what happens when a cron fires and the previous run is still active."""

    ALWAYS = "always"
    SKIP_IF_RUNNING = "skip_if_running"


class CronDAGEntry(BaseModel):
    """
    A recurring scheduled DAG definition.

    Stored in Redis as a hash at `cron-dag:{id}` with the entry ID as a member
    of the `cron-schedule` sorted set (score = next_run_at Unix timestamp).
    """

    id: ULID = Field(default_factory=ULID)
    name: str
    cron_expr: str
    dag_spec: DAGTaskSpec
    enabled: bool = True
    concurrency_policy: ConcurrencyPolicy = ConcurrencyPolicy.ALWAYS
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))

    @field_serializer("id", when_used="json")
    def serialize_id(self, value: ULID) -> str:
        """Serialize ULID to string for JSON output."""
        return str(value)
