"""
Pluggable adapters for task storage, dead letter queue, and routing config.

Task storage:
- `TaskStateProtocol` — task blob persistence interface.
- `TaskSubmitProtocol` — composite submit/pop interface (requires co-located state + queue).
- `JsonTaskAdapter` — Redis Stack (RedisJSON + RediSearch) backend.
- `MsgpackTaskAdapter` — plain Redis (msgpack binary strings) backend.
- `SQLTaskAdapter` — SQLAlchemy backend (tasks / task_fan_in / dag_runs tables).

Dead letter queue:
- `DeadQueueProtocol` — interface all DLQ implementations must satisfy.
- `DeadQueue` — plain Redis backend (sorted sets + hash indexes).
- `JsonDeadQueue` — Redis Stack backend (RedisJSON + RediSearch).
- `SQLDeadQueue` — SQLAlchemy backend (dead_letter_queue table).

Routing backends (queues, roles, task routing config):
- `RoutingBackendProtocol` — interface all routing backends must satisfy.
- `RoutingBackendReadOnlyError` — raised by read-only backends on write ops.
- `SQLRoutingBackend` — SQLAlchemy backend (default; requires SQL_PATH).
- `RedisRoutingBackend` — plain Redis backend (no SQL required).
- `RedisJSONRoutingBackend` — Redis Stack backend (RedisJSON + RediSearch; no SQL required).
- `StaticRoutingBackend` — read-only hardcoded config (no database required).
"""

from jobbers.adapters.redis import DeadQueue, MsgpackTaskAdapter, RedisRoutingBackend
from jobbers.adapters.redis_json import JsonDeadQueue, JsonTaskAdapter, RedisJSONRoutingBackend
from jobbers.adapters.sql import SQLDeadQueue, SQLRoutingBackend, SQLTaskAdapter
from jobbers.adapters.static import StaticRoutingBackend
from jobbers.protocols import (
    DeadQueueProtocol,
    RoutingBackendProtocol,
    RoutingBackendReadOnlyError,
    TaskStateProtocol,
    TaskSubmitProtocol,
)

__all__ = [
    "TaskStateProtocol",
    "TaskSubmitProtocol",
    "DeadQueueProtocol",
    "JsonTaskAdapter",
    "MsgpackTaskAdapter",
    "SQLTaskAdapter",
    "DeadQueue",
    "JsonDeadQueue",
    "SQLDeadQueue",
    "RoutingBackendProtocol",
    "RoutingBackendReadOnlyError",
    "SQLRoutingBackend",
    "RedisRoutingBackend",
    "RedisJSONRoutingBackend",
    "StaticRoutingBackend",
]
