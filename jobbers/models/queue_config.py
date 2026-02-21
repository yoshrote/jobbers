import asyncio
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self, cast

from pydantic import BaseModel
from redis.asyncio import Redis

from jobbers.utils.serialization import NONE, deserialize

if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterator

    from redis.asyncio.client import Pipeline


class RatePeriod(StrEnum):
    """Enumeration of rate limiting periods."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"

    @classmethod
    def from_bytes(cls, value: bytes | None) -> Self | None:
        if value is None:
            return None
        period_str = value.decode()
        try:
            return cls(period_str)
        except ValueError:
            # TODO: Swallowing bad data may not be the best approach
            return None

    def to_bytes(self) -> bytes:
        return self.value.encode()

class QueueConfig(BaseModel):
    """Configuration for a task queue."""

    name: str  # Name of the queue
    max_concurrent: int | None = 10  # Maximum number of concurrent tasks that can be processed from this queue
    # max_tasks_per_worker: int | None = None  # Maximum number of tasks a worker can process before shutting down
    # task_timeout: int | None = None  # Timeout for tasks in this queue in seconds, if applicable
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying failed tasks in this queue
    # backoff_strategy: str = "exponential"  # Backoff strategy for retries (e.g., "exponential", "linear")
    # retry_delay: dt.timedelta = dt.timedelta(seconds=5)  # Delay before retrying the task
    # Rate limiting is {task_number} tasks every {rate_number} {rate_period}
    #  e.g. 5 tasks every 2 minutea
    rate_numerator: int | None = None  # Number of tasks to process from this queue
    rate_denominator: int | None = None  # Number of tasks to rate limit
    rate_period: RatePeriod | None = None  # Period for rate limiting in seconds

    def period_in_seconds(self) -> int | None:
        """Convert the rate period to seconds."""
        if self.rate_period is None or self.rate_denominator is None:
            return None
        match self.rate_period:
            case RatePeriod.SECOND:
                return 1 * self.rate_denominator
            case RatePeriod.MINUTE:
                return 60 * self.rate_denominator
            case RatePeriod.HOUR:
                return 3600 * self.rate_denominator
            case RatePeriod.DAY:
                return 86400 * self.rate_denominator

    @classmethod
    def from_redis(cls, name: str, raw_task_data: dict[bytes, bytes]) -> Self:
        return cls(
            name=name,
            max_concurrent=int(raw_task_data.get(b"max_concurrent") or b"10"),
            rate_numerator=deserialize(raw_task_data.get(b"rate_numerator") or NONE),
            rate_denominator=deserialize(raw_task_data.get(b"rate_denominator") or NONE),
            rate_period=RatePeriod.from_bytes(raw_task_data.get(b"rate_period")),
            # max_tasks_per_worker=int(raw_task_data.get(b"max_tasks_per_worker", b"0")),
            # task_timeout=int(raw_task_data.get(b"task_timeout", b"0")),
            # retry_delay=dt.timedelta(seconds=int(raw_task_data.get(b"retry_delay", b"5"))),
            # backoff_strategy=raw_task_data.get(b"backoff_strategy", b"exponential").decode(),
        )

class QueueConfigAdapter:
    """
    Manages queue configuration in a Redis data store.

    - `worker-queues:<role>`: Set of queues for a given role, used to manage which queues are available for task submission.
    - `queue-config:<queue>`: Hash of queue configuration data which is shared for by all roles using this queue
    - `all_queues`: a list of all queues used across all roles.
    """

    QUEUES_BY_ROLE = "worker-queues:{role}".format
    QUEUE_CONFIG = "queue-config:{queue}".format
    ALL_QUEUES = "all-queues"

    def __init__(self, data_store: Redis) -> None:
        self.data_store: Redis = data_store

    async def get_queues(self, role: str) -> set[str]:
        return {role.decode() for role in await cast("Awaitable[set[bytes]]", self.data_store.smembers(self.QUEUES_BY_ROLE(role=role)))}

    async def set_queues(self, role: str, queues: set[str]) -> None:
        pipe: Pipeline = self.data_store.pipeline(transaction=True)
        pipe.delete(self.QUEUES_BY_ROLE(role=role))
        pipe.sadd(self.ALL_QUEUES, *queues)
        pipe.sadd(self.QUEUES_BY_ROLE(role=role), *queues)
        await pipe.execute()

    async def get_all_queues(self) -> list[str]:
        # find the union of the queues for all roles
        # this query approach is not ideal for large numbers of roles or queues
        roles = await self.get_all_roles()
        if not roles:
            return []

        return [
            queue.decode()
            for queue in await cast("Awaitable[list[bytes]]", self.data_store.sunion(
                [self.QUEUES_BY_ROLE(role=role) for role in roles]
            ))
        ]

    async def get_all_roles(self) -> list[str]:
        roles = []
        async for key in self.data_store.scan_iter(match=self.QUEUES_BY_ROLE(role="*").encode()):
            roles.append(key.decode().split(":")[1])
        return roles

    async def get_queue_config(self, queue: str) -> QueueConfig:
        raw_data: dict[bytes, Any] = await cast(
            "Awaitable[dict[bytes, Any]]",
            self.data_store.hgetall(self.QUEUE_CONFIG(queue=queue))
        )  # Ensure the queue config exists in the store
        return QueueConfig.from_redis(queue, raw_data)

    async def get_queue_limits(self, queues: set[str]) -> dict[str, int | None]:
        # TODO: replace with a redis query
        result_gen: Iterator[Awaitable[QueueConfig | None]] = (
            self.get_queue_config(q)
            for q in queues
        )

        return {
            conf.name: conf.max_concurrent
            for conf in await asyncio.gather(*result_gen)
            if conf is not None
        }

