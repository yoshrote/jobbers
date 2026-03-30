import datetime as dt
import inspect
import logging
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

from opentelemetry import metrics
from pydantic import BaseModel, Field, TypeAdapter, field_serializer
from ulid import ULID

from jobbers.models.task_shutdown_policy import TaskShutdownPolicy

from .dag import DAGCallback
from .task_config import TaskConfig
from .task_status import TaskStatus

if TYPE_CHECKING:
    from jobbers.adapters.task_adapter import TaskAdapterProtocol
    from jobbers.models.dag import DynamicFanOut, TaskResult

_dag_callback_adapter: TypeAdapter[list[DAGCallback]] = TypeAdapter(list[DAGCallback])

logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)


class Task(BaseModel):
    """A task to be executed."""

    id: ULID
    # task mapping fields
    name: str
    queue: str = "default"
    version: int = 0
    parameters: dict[Any, Any] = {}
    results: dict[Any, Any] = {}
    errors: list[str] = []
    # status fields
    retry_attempt: int = 0  # Number of times this task has been retried
    status: TaskStatus = Field(default=TaskStatus.UNSUBMITTED)
    submitted_at: dt.datetime | None = None
    retried_at: dt.datetime | None = None
    started_at: dt.datetime | None = None
    heartbeat_at: dt.datetime | None = None
    completed_at: dt.datetime | None = None

    task_config: TaskConfig | None = Field(default=None, exclude=True)
    dag_callbacks: list[DAGCallback] = Field(default_factory=list)
    # Immediate parent task IDs — empty for root tasks, one entry for simple-chain
    # and dynamic fan-out children, many entries for fan-in collectors.
    parent_ids: list[ULID] = Field(default_factory=list)
    # Set on cron-dispatched root tasks so the worker can clear the active-run key on completion.
    cron_id: ULID | None = Field(default=None)

    @field_serializer("id", when_used="json")
    def serialize_id(self, value: ULID) -> str:
        """Serialize ULID to string for JSON output."""
        return str(value)

    def valid_task_params(self) -> bool:
        if not self.task_config:
            # Safer to fail here than chance something funky downstream
            return True
        signature = inspect.get_annotations(self.task_config.function)
        for param, psig in signature.items():
            # Skip the return type annotation
            if param != "return" and not isinstance(self.parameters[param], psig):
                return False
        return True

    def shutdown(self) -> None:
        if self.task_config is None:
            return
        match self.task_config.on_shutdown:
            case TaskShutdownPolicy.CONTINUE:
                # NOOP: The execution of the task function needs to be wrapped
                # in `shield()` already.
                # TODO: maybe warn or panic since this should be unreachable
                pass
            case TaskShutdownPolicy.STOP:
                self.set_status(TaskStatus.STALLED)
            case TaskShutdownPolicy.RESUBMIT:
                # Direct assignment: shutdown-triggered resubmit should not increment retry_attempt
                self.status = TaskStatus.UNSUBMITTED

    def should_retry(self) -> bool:
        if not self.task_config:
            # safer to fail here than chance something funky downstream
            return False
        return self.retry_attempt < self.task_config.max_retries

    def should_schedule(self) -> bool:
        """Return True if the retry should be delayed (SCHEDULED) rather than immediate (UNSUBMITTED)."""
        if not self.task_config:
            return False
        return self.task_config.retry_delay is not None

    def has_callbacks(self) -> bool:
        return bool(self.dag_callbacks)

    def generate_error_callbacks(self) -> list[Self]:
        """
        Return tasks to submit when this task fails permanently.

        For each `dag_callback` that has an `error_callback` spec, a task is
        created with `parent_ids=[self.id]`.  No Redis I/O is required.
        """
        results: list[Self] = []
        for cb in self.dag_callbacks:
            if cb.error_callback is not None:
                spec = cb.error_callback
                results.append(
                    self.__class__(
                        id=spec.id,
                        name=spec.name,
                        queue=spec.queue,
                        version=spec.version,
                        parameters=spec.parameters,
                        dag_callbacks=spec.dag_callbacks,
                        parent_ids=[self.id],
                    )
                )
        return results

    async def generate_callbacks(self, ta: "TaskAdapterProtocol") -> list[Self]:
        """
        Generate tasks to submit after this task completes.

        For `SimpleCallback` entries the next task is created immediately with
        `parent_ids=[self.id]`.  For `FanInCallback` entries the task's ID is
        removed from the shared Redis set; the collector task is only returned
        once the set is empty (all predecessors have completed), and its
        `parent_ids` is populated from the permanent fan-in members set.

        Returns -1 from `fan_in_complete` when the ID was not a member (already
        processed or key expired); in that case the collector is not submitted.
        """
        from jobbers.models.dag import FanInCallback, SimpleCallback

        results: list[Self] = []
        for cb in self.dag_callbacks:
            spec = cb.task
            match cb:
                case SimpleCallback():
                    results.append(
                        self.__class__(
                            id=spec.id,
                            name=spec.name,
                            queue=spec.queue,
                            version=spec.version,
                            parameters=spec.parameters,
                            dag_callbacks=spec.dag_callbacks,
                            parent_ids=[self.id],
                        )
                    )
                case FanInCallback():
                    remaining = await ta.fan_in_complete(cb.fan_in_key, self.id)
                    if remaining == 0:
                        member_ids = await ta.get_fan_in_members(cb.fan_in_key)
                        results.append(
                            self.__class__(
                                id=spec.id,
                                name=spec.name,
                                queue=spec.queue,
                                version=spec.version,
                                parameters=spec.parameters,
                                dag_callbacks=spec.dag_callbacks,
                                parent_ids=member_ids,
                            )
                        )
                    elif remaining == -1:
                        logger.warning(
                            "fan_in_complete returned -1 for key %s task %s — "
                            "ID was not a member (already processed or key expired); skipping collector.",
                            cb.fan_in_key,
                            self.id,
                        )
        return results

    def make_result(
        self,
        results: "dict[Any, Any] | None" = None,
        fanout: "DynamicFanOut | None" = None,
    ) -> "TaskResult":
        """
        Create a `TaskResult` with `parent_ids` pre-populated from this task's known parents.

        Use this instead of constructing `TaskResult` directly so that ancestry
        is tracked without requiring task authors to manage it:

        ```python
        task = get_current_task()
        return task.make_result(results={"count": n})
        ```
        """
        from jobbers.models.dag import TaskResult

        return TaskResult(results=results or {}, fanout=fanout, parent_ids=list(self.parent_ids))

    def summarized(self) -> dict[str, Any]:
        summary = self.model_dump(
            include={"id", "name", "parameters", "status", "retry_attempt", "submitted_at"}
        )
        summary["id"] = str(self.id)
        if self.errors:
            summary["last_error"] = self.errors[-1]
        return summary

    @property
    def _ta(self) -> "TaskAdapterProtocol":
        from jobbers.db import create_task_adapter, get_client

        return create_task_adapter(get_client())

    async def heartbeat(self) -> None:
        self.heartbeat_at = dt.datetime.now(dt.UTC)
        await self._ta.update_task_heartbeat(self)

    async def parent_results(self) -> dict[Any, Any] | list[dict[Any, Any]]:
        """
        Fetch the results of this task's parent(s) using `parent_ids`.

        Returns a single dict when there is one parent, a list of dicts when
        there are multiple (fan-in collector), or `{}` for root tasks.
        """
        if not self.parent_ids:
            return {}
        tasks = await self._ta.get_tasks_bulk(self.parent_ids)
        results_list = [t.results if t is not None else {} for t in tasks]
        return results_list[0] if len(results_list) == 1 else results_list

    def to_dict(self) -> dict[str, Any]:
        """Serialize task fields to a dict for RedisJSON storage."""

        def _ts(d: dt.datetime | None) -> float | None:
            return d.timestamp() if d is not None else None

        return {
            "name": self.name,
            "queue": self.queue,
            "version": self.version,
            "parameters": self.parameters or {},
            "results": self.results or {},
            "errors": self.errors,
            "retry_attempt": self.retry_attempt,
            "status": str(self.status),
            "submitted_at": _ts(self.submitted_at),
            "retried_at": _ts(self.retried_at),
            "started_at": _ts(self.started_at),
            "heartbeat_at": _ts(self.heartbeat_at),
            "completed_at": _ts(self.completed_at),
            "dag_callbacks": [cb.model_dump() for cb in self.dag_callbacks],
            "parent_ids": [str(pid) for pid in self.parent_ids],
            "cron_id": str(self.cron_id) if self.cron_id is not None else None,
        }

    @classmethod
    def from_dict(cls, task_id: ULID, raw: dict[str, Any]) -> "Self":
        """Construct a Task from a plain dict (as produced by to_dict())."""

        def _dt(ts: float | None) -> dt.datetime | None:
            return dt.datetime.fromtimestamp(ts, dt.UTC) if ts is not None else None

        return cls(
            id=task_id,
            name=raw.get("name", ""),
            queue=raw.get("queue", "default"),
            version=raw.get("version", 0),
            parameters=raw.get("parameters") or {},
            results=raw.get("results") or {},
            errors=raw.get("errors") or [],
            retry_attempt=raw.get("retry_attempt", 0),
            status=raw.get("status", TaskStatus.UNSUBMITTED),
            submitted_at=_dt(raw.get("submitted_at")),
            retried_at=_dt(raw.get("retried_at")),
            started_at=_dt(raw.get("started_at")),
            heartbeat_at=_dt(raw.get("heartbeat_at")),
            completed_at=_dt(raw.get("completed_at")),
            dag_callbacks=_dag_callback_adapter.validate_python(raw.get("dag_callbacks") or []),
            parent_ids=[ULID.from_str(pid) for pid in raw.get("parent_ids") or []],
            cron_id=ULID.from_str(raw["cron_id"]) if raw.get("cron_id") else None,
        )

    def set_status(self, status: TaskStatus) -> None:
        match status:
            case TaskStatus.STARTED:
                if not self.started_at:
                    self.started_at = dt.datetime.now(dt.UTC)
                else:
                    self.retried_at = dt.datetime.now(dt.UTC)
            case TaskStatus.SUBMITTED:
                self.submitted_at = dt.datetime.now(dt.UTC)
            case (
                TaskStatus.COMPLETED
                | TaskStatus.FAILED
                | TaskStatus.CANCELLED
                | TaskStatus.STALLED
                | TaskStatus.DROPPED
            ):
                self.completed_at = dt.datetime.now(dt.UTC)
            case TaskStatus.SCHEDULED | TaskStatus.UNSUBMITTED:
                self.retry_attempt += 1
        self.status = status


class PaginationOrder(StrEnum):
    "Supported fields to order task list by."

    SUBMITTED_AT = "submitted_at"
    TASK_ID = "task_id"


class TaskPagination(BaseModel):
    "Pagination details."

    queue: str = Field()
    limit: int = Field(default=10, gt=0, le=100)
    offset: int = Field(default=0, ge=0)
    start: ULID | None = Field(default=None)
    order_by: PaginationOrder = Field(default=PaginationOrder.SUBMITTED_AT)
    task_name: str | None = Field(default=None)
    task_version: int | None = Field(default=None)
    status: TaskStatus | None = Field(default=None)

    @field_serializer("start", when_used="json")
    def serialize_start(self, value: ULID | None) -> str | None:
        """Serialize ULID to string for JSON output."""
        return str(value) if value is not None else None
