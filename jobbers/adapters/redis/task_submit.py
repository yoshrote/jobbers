"""
Plain Redis task submit adapter.

- `RedisTaskSubmit` — TaskSubmitProtocol backed by plain Redis Lua scripts (msgpack encoding).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from jobbers.adapters._shared import _SharedRedisTaskSubmitBase

if TYPE_CHECKING:
    from redis.asyncio.client import Redis

    from jobbers.adapters.redis.task_state import RedisTaskState
    from jobbers.models.task import Task


_MSGPACK_SUBMIT_SCRIPT = """
    local exists = redis.call('EXISTS', KEYS[2])
    if exists == 0 then
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
    end
    redis.call('SET', KEYS[2], ARGV[4])
    if ARGV[3] == '1' then
        redis.call('SADD', KEYS[3], ARGV[2])
    else
        redis.call('SREM', KEYS[3], ARGV[2])
    end
    if ARGV[5] ~= '' then
        redis.call('ZADD', KEYS[4], 'NX', ARGV[1], ARGV[5])
        redis.call('ZADD', KEYS[5], ARGV[1], ARGV[2])
    end
    return 1
"""

_MSGPACK_SUBMIT_RATE_LIMITED_SCRIPT = """
    local enqueued = 0
    local exists = redis.call('EXISTS', KEYS[3])
    local numerator = tonumber(ARGV[2])
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
    if exists == 0 then
        local count = redis.call('ZCARD', KEYS[1])
        if numerator ~= 0 and count < numerator then
            redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
            redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
            redis.call('SET', KEYS[3], ARGV[6])
            enqueued = 1
        end
    else
        redis.call('SET', KEYS[3], ARGV[6])
        enqueued = 1
    end
    if enqueued == 1 and ARGV[5] == '1' then
        redis.call('SADD', KEYS[4], ARGV[4])
    else
        redis.call('SREM', KEYS[4], ARGV[4])
    end
    if enqueued == 1 and ARGV[7] ~= '' then
        redis.call('ZADD', KEYS[5], 'NX', ARGV[3], ARGV[7])
        redis.call('ZADD', KEYS[6], ARGV[3], ARGV[4])
    end
    return enqueued
"""


class RedisTaskSubmit(_SharedRedisTaskSubmitBase):
    """
    TaskSubmitProtocol backed by plain Redis (msgpack encoding).

    Lua scripts atomically enqueue the task blob and queue membership in one round-trip.
    Requires the same Redis client as the paired ``RedisTaskState``.
    """

    # Atomically enqueue a new task (no rate limiting).
    # KEYS[1] = task-queues:{queue}
    # KEYS[2] = task:{task_id}
    # KEYS[3] = task-type-idx:{name}
    # KEYS[4] = dag-runs
    # KEYS[5] = dag-run:{dag_run_id}:tasks (placeholder key when task has no DAG run)
    # ARGV[1] = submitted_at timestamp
    # ARGV[2] = task_id bytes
    # ARGV[3] = '1'/'0' for type index
    # ARGV[4] = msgpack-encoded task blob
    # ARGV[5] = dag_run_id bytes (empty string if task is not part of a DAG run)
    SUBMIT_SCRIPT = _MSGPACK_SUBMIT_SCRIPT

    # Atomically check rate limit and enqueue.
    # KEYS[1] = rate-limiter:{queue}
    # KEYS[2] = task-queues:{queue}
    # KEYS[3] = task:{task_id}
    # KEYS[4] = task-type-idx:{name}
    # KEYS[5] = dag-runs
    # KEYS[6] = dag-run:{dag_run_id}:tasks (placeholder key when task has no DAG run)
    # ARGV[1] = earliest_time
    # ARGV[2] = rate_numerator
    # ARGV[3] = submitted_at timestamp
    # ARGV[4] = task_id bytes
    # ARGV[5] = '1'/'0' for type index
    # ARGV[6] = msgpack-encoded task blob
    # ARGV[7] = dag_run_id bytes (empty string if task is not part of a DAG run)
    # Returns: 1 if enqueued, 0 if rate-limited
    SUBMIT_RATE_LIMITED_SCRIPT = _MSGPACK_SUBMIT_RATE_LIMITED_SCRIPT

    def __init__(self, data_store: Redis, state: RedisTaskState) -> None:
        super().__init__(data_store, state.pack, state.get_task)

    def _extra_submit_keys(self, task: Task) -> list[str]:
        return [
            self.DAG_RUN_TASKS(dag_run_id=task.dag_run_id)
            if task.dag_run_id is not None
            else self.DAG_RUN_TASKS(dag_run_id="")
        ]

    def _extra_rate_limited_keys(self, task: Task) -> list[str]:
        return [
            self.DAG_RUN_TASKS(dag_run_id=task.dag_run_id)
            if task.dag_run_id is not None
            else self.DAG_RUN_TASKS(dag_run_id="")
        ]
