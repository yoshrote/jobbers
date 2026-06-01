"""
Redis Stack (RedisJSON + RediSearch) task submit adapter.

- `RedisJSONTaskSubmit` — TaskSubmitProtocol backed by Redis Stack Lua scripts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from jobbers.adapters._shared import _SharedRedisTaskSubmitBase
from jobbers.adapters.redis_json.task_state import RedisJSONTaskState, _JSON_SUBMIT_RATE_LIMITED_SCRIPT, _JSON_SUBMIT_SCRIPT

if TYPE_CHECKING:
    from redis.asyncio.client import Redis


class RedisJSONTaskSubmit(_SharedRedisTaskSubmitBase):
    """
    TaskSubmitProtocol backed by Redis Stack (JSON encoding).

    Lua scripts atomically enqueue the JSON task blob and queue membership.
    Requires the same Redis client as the paired ``RedisJSONTaskState``.
    """

    # Atomically enqueue a new task (no rate limiting).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # KEYS[4] = dag-runs
    # ARGV[1] = submitted_at timestamp
    # ARGV[2] = task_id bytes
    # ARGV[3] = '1' to SADD type index, '0' to SREM
    # ARGV[4] = JSON-encoded task blob
    # ARGV[5] = dag_run_id bytes (empty string if task is not part of a DAG run)
    SUBMIT_SCRIPT = _JSON_SUBMIT_SCRIPT

    # Atomically check rate limit and enqueue.
    # KEYS[1] = rate-limiter:{queue}
    # KEYS[2] = task-queues:{queue}
    # KEYS[3] = task:{task_id}
    # KEYS[4] = task-type-idx:{name}
    # KEYS[5] = dag-runs
    # ARGV[1] = earliest_time
    # ARGV[2] = rate_numerator
    # ARGV[3] = submitted_at timestamp
    # ARGV[4] = task_id bytes
    # ARGV[5] = '1'/'0' for type index
    # ARGV[6] = JSON-encoded task blob
    # ARGV[7] = dag_run_id bytes (empty string if task is not part of a DAG run)
    # Returns: 1 if enqueued, 0 if rate-limited
    SUBMIT_RATE_LIMITED_SCRIPT = _JSON_SUBMIT_RATE_LIMITED_SCRIPT

    def __init__(self, data_store: Redis, state: RedisJSONTaskState) -> None:
        super().__init__(data_store, state.pack, state.get_task)
