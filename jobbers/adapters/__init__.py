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
- `StaticRoutingBackend` — read-only hardcoded config (no database required).
"""

from jobbers.adapters.json_redis import JsonDeadQueue, JsonTaskAdapter
from jobbers.adapters.raw_redis import DeadQueue, MsgpackTaskAdapter
from jobbers.adapters.redis_routing import RedisRoutingBackend
from jobbers.adapters.routing_backend import (
    RoutingBackendProtocol,
    RoutingBackendReadOnlyError,
    SQLRoutingBackend,
)
from jobbers.adapters.static_routing import StaticRoutingBackend
from jobbers.adapters.task_adapter import DeadQueueProtocol, TaskAdapterProtocol

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
    "StaticRoutingBackend",
]
