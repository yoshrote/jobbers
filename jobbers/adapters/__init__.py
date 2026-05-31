"""
Pluggable adapters for task storage, dead letter queue, routing config, and cron DAG scheduling.

Task storage:
- `TaskStateProtocol` — task blob persistence interface.
- `TaskSubmitProtocol` — composite submit/pop interface (requires co-located state + queue).
- `RedisTaskState` — plain Redis backend (msgpack); implements TaskStateProtocol + AtomicTaskStateProtocol.
- `RedisTaskSubmit` — plain Redis backend (msgpack); implements TaskSubmitProtocol.
- `RedisJSONTaskState` — Redis Stack backend (RedisJSON + RediSearch); implements TaskStateProtocol + AtomicTaskStateProtocol.
- `RedisJSONTaskSubmit` — Redis Stack backend (RedisJSON + RediSearch); implements TaskSubmitProtocol.
- `SQLTaskState` — SQLAlchemy backend (tasks / task_fan_in / dag_runs tables); implements TaskStateProtocol + AtomicTaskStateProtocol.
- `SQLTaskSubmit` — SQLAlchemy backend (tasks / task_queue tables); implements TaskSubmitProtocol.

Dead letter queue:
- `DeadQueueProtocol` — interface all DLQ implementations must satisfy.
- `RedisDeadQueue` — plain Redis backend (sorted sets + hash indexes).
- `RedisJSONDeadQueue` — Redis Stack backend (RedisJSON + RediSearch).
- `SQLDeadQueue` — SQLAlchemy backend (dead_letter_queue table).

Routing backends (queues, roles, task routing config):
- `RoutingBackendProtocol` — interface all routing backends must satisfy.
- `RoutingBackendReadOnlyError` — raised by read-only backends on write ops.
- `SQLRoutingBackend` — SQLAlchemy backend (default; requires SQL_PATH).
- `RedisRoutingBackend` — plain Redis backend (no SQL required).
- `RedisJSONRoutingBackend` — Redis Stack backend (RedisJSON + RediSearch; no SQL required).
- `StaticRoutingBackend` — read-only hardcoded config (no database required).

Task schedulers:
- `RedisTaskScheduler` — plain Redis backend; implements ``AtomicTaskSchedulerProtocol``.
- `SQLTaskScheduler` — SQLAlchemy backend (task_schedule table).

Cron DAG schedulers:
- `CronDAGSchedulerProtocol` — interface all cron DAG scheduler implementations must satisfy.
- `RedisCronDAGScheduler` — plain Redis backend.
- `SQLCronDAGScheduler` — SQLAlchemy backend (cron_dag_entries / cron_dag_active_runs tables).
- `StaticCronDAGScheduler` — read-only in-memory backend.
"""

from jobbers.adapters.redis import (
    RedisCronDAGScheduler,
    RedisDeadQueue,
    RedisRoutingBackend,
    RedisTaskScheduler,
    RedisTaskState,
    RedisTaskSubmit,
)
from jobbers.adapters.redis_json import (
    RedisJSONDeadQueue,
    RedisJSONRoutingBackend,
    RedisJSONTaskState,
    RedisJSONTaskSubmit,
)
from jobbers.adapters.sql import (
    SQLCronDAGScheduler,
    SQLDeadQueue,
    SQLRoutingBackend,
    SQLTaskScheduler,
    SQLTaskState,
    SQLTaskSubmit,
)
from jobbers.adapters.static import StaticCronDAGScheduler, StaticRoutingBackend
from jobbers.protocols import (
    CronDAGSchedulerProtocol,
    DeadQueueProtocol,
    RoutingBackendProtocol,
    RoutingBackendReadOnlyError,
    TaskStateProtocol,
    TaskSubmitProtocol,
)

__all__ = [
    "TaskStateProtocol",
    "TaskSubmitProtocol",
    "RedisTaskState",
    "RedisTaskSubmit",
    "RedisJSONTaskState",
    "RedisJSONTaskSubmit",
    "SQLTaskState",
    "SQLTaskSubmit",
    "DeadQueueProtocol",
    "RedisDeadQueue",
    "RedisJSONDeadQueue",
    "SQLDeadQueue",
    "RoutingBackendProtocol",
    "RoutingBackendReadOnlyError",
    "SQLRoutingBackend",
    "RedisRoutingBackend",
    "RedisJSONRoutingBackend",
    "StaticRoutingBackend",
    "RedisTaskScheduler",
    "SQLTaskScheduler",
    "CronDAGSchedulerProtocol",
    "RedisCronDAGScheduler",
    "SQLCronDAGScheduler",
    "StaticCronDAGScheduler",
]
