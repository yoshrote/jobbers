"""
Pluggable adapters for task storage and dead letter queue operations.

Task storage:
  ``TaskAdapterProtocol``  – interface all task adapters must satisfy.
  ``JsonTaskAdapter``      – Redis Stack (RedisJSON + RediSearch) backend.
  ``MsgpackTaskAdapter``   – plain Redis (msgpack binary strings) backend.
  ``SqlTaskAdapter``       – SQLite (aiosqlite) backend.

Dead letter queue:
  ``DeadQueueProtocol``  – interface all DLQ implementations must satisfy.
  ``DeadQueue``          – plain Redis backend (sorted sets + hash indexes).
  ``JsonDeadQueue``      – Redis Stack backend (RedisJSON + RediSearch).
  ``SqlDeadQueue``       – SQLite backend.

Pipeline types:
  ``SqlPipeline``  – SQL command accumulator used by SqlTaskAdapter stage methods.
"""

from jobbers.adapters.json_redis import JsonDeadQueue, JsonTaskAdapter
from jobbers.adapters.pipeline import SqlPipeline
from jobbers.adapters.raw_redis import DeadQueue, MsgpackTaskAdapter
from jobbers.adapters.sql import SqlDeadQueue, SqlTaskAdapter
from jobbers.adapters.task_adapter import DeadQueueProtocol, TaskAdapterProtocol

__all__ = [
    "TaskAdapterProtocol",
    "DeadQueueProtocol",
    "JsonTaskAdapter",
    "MsgpackTaskAdapter",
    "SqlTaskAdapter",
    "DeadQueue",
    "JsonDeadQueue",
    "SqlDeadQueue",
    "SqlPipeline",
]
