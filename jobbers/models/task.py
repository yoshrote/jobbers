import datetime as dt
import logging
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, Self, cast, get_args, get_origin, get_type_hints

from pydantic import BaseModel, Field, FieldSerializationInfo, PrivateAttr, TypeAdapter, field_serializer
from pydantic.functional_validators import BeforeValidator
from ulid import ULID

from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.utils.di import get_injected_param_names

from .dag import DAGCallback, DAGTaskSpec, FanInCallback, SimpleCallback, TaskResult
from .task_config import TaskConfig
from .task_status import TaskStatus

if TYPE_CHECKING:
    from jobbers.models.dag import DynamicFanOut
    from jobbers.protocols import TaskAdapterProtocol

_dag_callback_adapter: TypeAdapter[list[DAGCallback]] = TypeAdapter(list[DAGCallback])

logger = logging.getLogger(__name__)


def _parse_timestamp(v: object) -> dt.datetime | None:
    if v is None or isinstance(v, dt.datetime):
        return v
    if isinstance(v, (int, float)):
        return dt.datetime.fromtimestamp(v, dt.UTC)
    if isinstance(v, str):
        return dt.datetime.fromisoformat(v)
    raise ValueError(f"Cannot parse timestamp: {v!r}")


def _parse_ulid(v: object) -> ULID | None:
    if v is None or isinstance(v, ULID):
        return v
    if isinstance(v, str):
        return cast("ULID", ULID.from_str(v))
    if isinstance(v, (bytes, bytearray)):
        return cast("ULID", ULID.from_bytes(bytes(v)))
    raise ValueError(f"Cannot parse ULID: {v!r}")


Timestamp = Annotated[dt.datetime | None, BeforeValidator(_parse_timestamp)]
ULIDField = Annotated[ULID, BeforeValidator(_parse_ulid)]
OptionalULIDField = Annotated[ULID | None, BeforeValidator(_parse_ulid)]


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
    submitted_at: Timestamp = None
    retried_at: Timestamp = None
    started_at: Timestamp = None
    heartbeat_at: Timestamp = None
    completed_at: Timestamp = None

    task_config: TaskConfig | None = Field(default=None, exclude=True)
    dag_callbacks: list[DAGCallback] = Field(default_factory=list)
    # Immediate parent task IDs — empty for root tasks, one entry for simple-chain
    # and dynamic fan-out children, many entries for fan-in collectors.
    parent_ids: list[ULIDField] = Field(default_factory=list)
    # When True, the worker fetches parent results via parent_ids and injects them
    # as a `parent_results` kwarg before calling the task function.
    inject_parent_results: bool = False
    # Set on cron-dispatched root tasks so the worker can clear the active-run key on completion.
    cron_id: OptionalULIDField = Field(default=None)
    # Shared across every task in a DAG run; None for standalone tasks.
    # Generated once at submission time (submit_dag / dispatch_cron_dag) and propagated to all
    # descendants via generate_callbacks, generate_error_callbacks, and _handle_dynamic_fanout.
    dag_run_id: OptionalULIDField = Field(default=None)

    @field_serializer("id", when_used="json")
    def serialize_id(self, value: ULID) -> str:
        return str(value)

    @field_serializer("submitted_at", "retried_at", "started_at", "heartbeat_at", "completed_at")
    def serialize_timestamp(
        self, value: dt.datetime | None, info: FieldSerializationInfo
    ) -> float | str | dt.datetime | None:
        if value is None:
            return None
        mode = (info.context or {}).get("mode")
        if mode == "msgpack":
            return value  # msgpack ExtType(1) handles datetime → binary
        if mode == "redis_json":
            return value.timestamp()  # float for RediSearch NumericField
        return value.isoformat()  # ISO string for SQL columns and API responses

    @field_serializer("cron_id", "dag_run_id")
    def serialize_optional_ulid(self, value: ULID | None, info: FieldSerializationInfo) -> ULID | str | None:
        if value is None:
            return None
        if (info.context or {}).get("mode") == "msgpack":
            return value  # msgpack ExtType(3) handles ULID → binary
        return str(value)

    @field_serializer("parent_ids")
    def serialize_parent_ids(self, value: list[ULID], info: FieldSerializationInfo) -> list[ULID] | list[str]:
        if (info.context or {}).get("mode") == "msgpack":
            return list(value)  # msgpack ExtType(3) handles each ULID → binary
        return [str(v) for v in value]

    @field_serializer("dag_callbacks")
    def serialize_dag_callbacks(self, value: list[DAGCallback]) -> list[dict[str, Any]]:
        return [cb.model_dump(mode="json") for cb in value]

    def valid_task_params(self) -> bool:
        if not self.task_config:
            # Safer to fail here than chance something funky downstream
            return True
        injected = get_injected_param_names(self.task_config.function)
        try:
            hints = get_type_hints(self.task_config.function, include_extras=True)
        except Exception:
            return True  # can't resolve hints — skip validation
        for param, hint in hints.items():
            # Skip the return type annotation
            if param == "return":
                continue
            # Skip parameters that are dependency-injected — never in task.parameters
            if param in injected:
                continue
            # Skip parameters not present in the submitted payload — they may
            # have defaults or be injected at execution time (e.g. parent_results).
            if param not in self.parameters:
                continue
            # Strip Annotated wrapper before isinstance (Annotated is not a valid isinstance target)
            raw_type = get_args(hint)[0] if get_origin(hint) is Annotated else hint
            if not isinstance(self.parameters[param], raw_type):
                return False
        return True

    def shutdown(self) -> None:
        if self.task_config is None:
            return
        match self.task_config.on_shutdown:
            case TaskShutdownPolicy.CONTINUE:
                # NOOP: the task coroutine is wrapped in asyncio.shield(), so even if
                # CancelledError propagates to the outer await, the inner coroutine keeps
                # running. STARTED is the correct state to persist — the task is still
                # in flight.
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

    def _build_callback_task(
        self,
        spec: "DAGTaskSpec",
        parent_ids: list[ULID],
        inject_parent_results: bool = False,
    ) -> "Self":
        return self.__class__(
            id=spec.id,
            name=spec.name,
            queue=spec.queue,
            version=spec.version,
            parameters=spec.parameters,
            dag_callbacks=spec.dag_callbacks,
            parent_ids=parent_ids,
            inject_parent_results=inject_parent_results,
            dag_run_id=self.dag_run_id,
        )

    def generate_error_callbacks(self) -> list[Self]:
        """
        Return tasks to submit when this task fails permanently.

        For each `dag_callback` that has an `error_callback` spec, a task is
        created with `parent_ids=[self.id]`.  No Redis I/O is required.
        """
        return [
            self._build_callback_task(cb.error_callback, [self.id])
            for cb in self.dag_callbacks
            if cb.error_callback is not None
        ]

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
        results: list[Self] = []
        for cb in self.dag_callbacks:
            match cb:
                case SimpleCallback():
                    results.append(self._build_callback_task(cb.task, [self.id], cb.inject_parent_results))
                case FanInCallback():
                    remaining = await ta.fan_in_complete(cb.fan_in_key, self.id)
                    if remaining == 0:
                        member_ids = await ta.get_fan_in_members(cb.fan_in_key)
                        results.append(
                            self._build_callback_task(cb.task, member_ids, cb.inject_parent_results)
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
        return TaskResult(results=results or {}, fanout=fanout, parent_ids=list(self.parent_ids))

    def summarized(self) -> dict[str, Any]:
        summary = self.model_dump(
            mode="json", include={"id", "name", "parameters", "status", "retry_attempt", "submitted_at"}
        )
        if self.errors:
            summary["last_error"] = self.errors[-1]
        return summary

    _adapter: "TaskAdapterProtocol | None" = PrivateAttr(default=None)

    async def heartbeat(self) -> None:
        if self._adapter is None:
            raise RuntimeError("Task adapter not injected — set task._adapter before running")
        self.heartbeat_at = dt.datetime.now(dt.UTC)
        await self._adapter.update_task_heartbeat(self)

    async def parent_results(self) -> dict[Any, Any] | list[dict[Any, Any]]:
        """
        Fetch the results of this task's parent(s) using `parent_ids`.

        Returns a single dict when there is one parent, a list of dicts when
        there are multiple (fan-in collector), or `{}` for root tasks.
        """
        if not self.parent_ids:
            return {}
        if self._adapter is None:
            raise RuntimeError("Task adapter not injected — set task._adapter before running")
        tasks = await self._adapter.get_tasks_bulk(self.parent_ids)
        results_list = [t.results if t is not None else {} for t in tasks]
        return results_list[0] if len(results_list) == 1 else results_list

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
