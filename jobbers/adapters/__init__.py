"""
Pluggable adapters for task storage, dead letter queue, routing config, and cron scheduling.

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

Cron DAG schedulers:
- `CronDAGSchedulerProtocol` — interface all cron DAG scheduler implementations must satisfy.
- `RedisCronDAGScheduler` — Redis backend (sorted set + hash per entry).
- `SQLCronDAGScheduler` — SQLAlchemy backend (shares SQL_PATH session factory).
- `StaticCronDAGScheduler` — read-only hardcoded entries (in-memory runtime state).
"""

from jobbers.adapters.redis import DeadQueue, MsgpackTaskAdapter, RedisRoutingBackend
from jobbers.adapters.redis_json import JsonDeadQueue, JsonTaskAdapter, RedisJSONRoutingBackend
from jobbers.adapters.sql import SQLCronDAGScheduler, SQLRoutingBackend
from jobbers.adapters.static import StaticCronDAGScheduler, StaticRoutingBackend
from jobbers.protocols import (
    CronDAGSchedulerProtocol,
    DeadQueueProtocol,
    RoutingBackendProtocol,
    RoutingBackendReadOnlyError,
    TaskAdapterProtocol,
)
from jobbers.schedulers.cron_dag_scheduler import RedisCronDAGScheduler

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
    "CronDAGSchedulerProtocol",
    "RedisCronDAGScheduler",
    "SQLCronDAGScheduler",
    "StaticCronDAGScheduler",
]
