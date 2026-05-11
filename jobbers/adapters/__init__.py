"""
Pluggable adapters for task storage, dead letter queue, and routing config.

Task storage:
- `TaskAdapterProtocol` — interface all task adapters must satisfy.
- `JsonTaskAdapter` — Redis Stack (RedisJSON + RediSearch) backend.
- `MsgpackTaskAdapter` — plain Redis (msgpack binary strings) backend.

Dead letter queue:
- `DeadQueueProtocol` — interface all DLQ implementations must satisfy.
- `DeadQueue` — plain Redis backend (sorted sets + hash indexes).
- `JsonDeadQueue` — Redis Stack backend (RedisJSON + RediSearch).

Routing backends (queues, roles, task routing config):
- `RoutingBackendProtocol` — interface all routing backends must satisfy.
- `RoutingBackendReadOnlyError` — raised by read-only backends on write ops.
- `SQLRoutingBackend` — SQLAlchemy backend (default; requires SQL_PATH).
- `RedisRoutingBackend` — plain Redis backend (no SQL required).
- `RedisJSONRoutingBackend` — Redis Stack backend (RedisJSON + RediSearch; no SQL required).
- `StaticRoutingBackend` — read-only hardcoded config (no database required).
"""

from jobbers.adapters.protocols import (
    DeadQueueProtocol,
    RoutingBackendProtocol,
    RoutingBackendReadOnlyError,
    TaskAdapterProtocol,
)
from jobbers.adapters.redis import DeadQueue, MsgpackTaskAdapter, RedisRoutingBackend
from jobbers.adapters.redis_json import JsonDeadQueue, JsonTaskAdapter, RedisJSONRoutingBackend
from jobbers.adapters.sql import SQLRoutingBackend
from jobbers.adapters.static import StaticRoutingBackend

__all__ = [
    "TaskAdapterProtocol",
    "DeadQueueProtocol",
    "JsonTaskAdapter",
    "MsgpackTaskAdapter",
    "DeadQueue",
    "JsonDeadQueue",
    "RoutingBackendProtocol",
    "RoutingBackendReadOnlyError",
    "SQLRoutingBackend",
    "RedisRoutingBackend",
    "RedisJSONRoutingBackend",
    "StaticRoutingBackend",
]
