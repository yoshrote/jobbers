import datetime as dt
import logging
from asyncio import TaskGroup

from pydantic import BaseModel
from ulid import ULID

from .serialization import (
    EMPTY_DICT,
    NONE,
    decode_optional_datetime,
    decode_optional_string,
    deserialize,
    serialize,
)

logger = logging.getLogger(__name__)

class Task(BaseModel):
    """A task to be executed."""

    id: ULID
    # task mapping fields
    name: str
    queue: str = "default"
    version: int = 0
    parameters: dict = {}
    results: dict = {}
    error: str | None = None
    # status fields
    status: str = "unsubmitted"
    submitted_at: dt.datetime | None = None
    retried_at: dt.datetime | None = None
    started_at: dt.datetime | None = None
    heartbeat_at: dt.datetime | None = None
    completed_at: dt.datetime | None = None

    expected_exceptions = tuple[Exception]

    def should_retry(self) -> bool:
        return False

    def has_callbacks(self) -> bool:
        return False

    def generate_callbacks(self) -> list["Task"]:
        return []

    def summarized(self):
        summary = self.model_dump(include={"id", "name", "parameters", "status", "submitted_at"})
        summary["id"] = str(self.id)
        return summary

    @classmethod
    def from_redis(cls, task_id: ULID, raw_task_data: dict) -> "Task":
        # Try to set good defaults for missing fields so when new fields are added to the task model, we don't break
        return cls(
            id=task_id,
            name=raw_task_data.get(b"name", b"").decode("utf-8"),
            version=int(raw_task_data.get(b"version", b"0")),
            parameters=deserialize(raw_task_data.get(b"parameters") or EMPTY_DICT),
            results=deserialize(raw_task_data.get(b"results") or EMPTY_DICT),
            error=decode_optional_string(raw_task_data.get(b"error")),
            status=raw_task_data.get(b"status", b"").decode("utf-8"),
            submitted_at=dt.datetime.fromisoformat(raw_task_data.get(b"submitted_at", b"").decode("utf-8")),
            started_at=decode_optional_datetime(raw_task_data.get(b"started_at")),
            heartbeat_at=decode_optional_datetime(raw_task_data.get(b"heartbeat_at")),
            completed_at=decode_optional_datetime(raw_task_data.get(b"completed_at")),
        )

    def to_redis(self):
        return {
            "name": self.name,
            "version": self.version,
            "parameters": serialize(self.parameters or {}),
            "results": serialize(self.results or {}),
            "error": self.error or NONE,
            "status": self.status,
            "submitted_at": self.submitted_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else NONE,
            "heartbeat_at": self.heartbeat_at.isoformat() if self.heartbeat_at else NONE,
            "completed_at": self.completed_at.isoformat() if self.completed_at else NONE,
        }

class StateManager:
    """Manages tasks in a Redis data store."""

    def __init__(self, data_store):
        self.data_store = data_store

    async def submit_task(self, task: Task):
        pipe = self.data_store.pipeline(transaction=True)
        # Avoid pushing a task onto the queue multiple times
        if not await self.task_exists(task.id):
            pipe.lpush(f"task-list:{task.queue}", bytes(task.id))
            task.submitted_at = dt.datetime.now(dt.timezone.utc)
            task.status = "submitted"

        pipe.hset(f"task:{task.id}", mapping=task.to_redis())
        await pipe.execute()

    async def get_task(self, task_id: ULID) -> Task | None:
        raw_task_data: dict = await self.data_store.hgetall(f"task:{task_id}".encode())

        if raw_task_data:
            return Task.from_redis(task_id, raw_task_data)

        return None

    async def task_exists(self, task_id: ULID) -> bool:
        return await self.data_store.exists(f"task:{task_id}")

    async def get_all_tasks(self) -> list[ULID]:
        task_ids = await self.data_store.lrange("task-list:default", 0, -1)
        if not task_ids:
            return []
        results = []
        async with TaskGroup() as group:
            for task_id in task_ids:
                group.create_task(self._add_task_to_results(ULID(task_id), results))
        return results

    async def _add_task_to_results(self, task_id: ULID, results: list[Task]):
        task = await self.get_task(task_id)
        if task:
            results.append(task)
        return results

def build_sm() -> StateManager:
    from jobbers import db
    return StateManager(db.get_client())
