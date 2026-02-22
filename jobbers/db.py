import os

import redis.asyncio as redis

from jobbers.models.dead_queue import DeadQueue
from jobbers.models.task_scheduler import TaskScheduler
from jobbers.state_manager import StateManager

_client: redis.Redis | None = None
_state_manager: StateManager | None = None

def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))  # type: ignore
    return _client

def set_client(new_client: redis.Redis) -> redis.Redis:
    global _client
    if _client is not None:
        _client.close()

    _client = new_client
    return _client

async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None

def get_state_manager() -> StateManager:
    global _state_manager
    if _state_manager is None:
        scheduler_db_path = os.environ.get("SCHEDULER_DB_PATH", "task_schedule.db")
        dead_queue_db_path = os.environ.get("DEAD_QUEUE_DB_PATH", "dead_queue.db")
        _state_manager = StateManager(
            get_client(),
            task_scheduler=TaskScheduler(scheduler_db_path),
            dead_queue=DeadQueue(dead_queue_db_path)
        )
    return _state_manager
