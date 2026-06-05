"""
Protocol definitions for all pluggable adapters.

Routing:
- `RoutingBackendReadOnlyError` — raised by read-only backends on write operations.
- `QueueConfigProtocol` — interface for queue/role configuration and refresh-tag management.
- `TaskRoutingConfigProtocol` — interface for task routing configuration.
- `RoutingBackendProtocol` — interface all routing backends must implement.
- `CancellationBusProtocol` — pub/sub channel for in-flight task cancellation signals.
- `RoutingNotificationProtocol` — routing version key and per-role queue-config refresh signals.

Task storage / dead-letter queue (split-store protocols):
- `TaskStateProtocol` — task blob persistence, heartbeats, fan-in sets, DAG run index.
- `TaskSubmitProtocol` — composite submit/pop operations requiring co-located state and queue.
- `TaskQueueProtocol` — active queue membership and rate limiting.
- `TaskSchedulerProtocol` — scheduled/delayed task queue.
- `CronDAGSchedulerProtocol` — recurring cron-scheduled DAG entries.
- `TransactionHandle` — opaque write batch accepted by all Atomic protocol stage_* methods.
- `AtomicTaskStateProtocol` — extends TaskStateProtocol with pipeline staging methods.
- `AtomicTaskSchedulerProtocol` — extends TaskSchedulerProtocol with pipeline staging.
- `AtomicDeadQueueProtocol` — extends DeadQueueProtocol with pipeline staging.
- `AtomicCronDAGSchedulerProtocol` — extends CronDAGSchedulerProtocol with pipeline staging.
- `DeadQueueProtocol` — interface all dead-letter queue implementations must implement.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, runtime_checkable

from typing_extensions import Protocol

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import AsyncGenerator, AsyncIterator, Callable
    from typing import Any

    from ulid import ULID

    from jobbers.models.cron_dag import CronDAGEntry
    from jobbers.models.dag import DAGRunPagination
    from jobbers.models.queue_config import QueueConfig
    from jobbers.models.task import Task, TaskPagination
    from jobbers.models.task_routing import RoutingConfig
    from jobbers.models.task_status import TaskStatus


class TransactionHandle(Protocol):  # pragma: no cover
    """
    Opaque write batch: a Redis pipeline or a SQL transaction batch.

    Created by an adapter's ``pipeline()`` method; passed to ``stage_*`` methods;
    committed by calling ``execute()``.  Redis ``Pipeline`` already satisfies this
    protocol structurally — no changes to Redis adapters are needed at the call site.
    """

    async def execute(self) -> list[object]: ...


class RoutingBackendReadOnlyError(Exception):
    """Raised when a write operation is attempted on a read-only routing backend."""


@runtime_checkable
class QueueConfigProtocol(Protocol):
    """Interface for queue/role configuration and refresh-tag management."""

    async def get_queue_config(self, queue: str) -> QueueConfig | None: ...
    async def save_queue_config(self, queue_config: QueueConfig) -> None: ...
    async def delete_queue(self, queue_name: str) -> None: ...
    async def get_all_queues(self) -> list[str]: ...
    async def get_queues(self, role: str) -> set[str]: ...
    async def save_role(self, role: str, queues_set: set[str]) -> str: ...
    async def get_all_roles(self) -> list[str]: ...
    async def delete_role(self, role: str) -> None: ...
    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]: ...
    async def get_refresh_tag(self, role: str) -> ULID: ...
    async def bump_refresh_tag(self, role: str) -> str: ...
    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]: ...


@runtime_checkable
class TaskRoutingConfigProtocol(Protocol):
    """Interface for task routing configuration."""

    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None: ...
    async def save_routing_config(self, routing_config: RoutingConfig) -> None: ...
    async def delete_routing_config(self, task_name: str, task_version: int) -> bool: ...


@runtime_checkable
class RoutingBackendProtocol(Protocol):
    """Interface all routing backends must implement."""

    # Queue CRUD
    async def get_queue_config(self, queue: str) -> QueueConfig | None: ...
    async def save_queue_config(self, queue_config: QueueConfig) -> None: ...
    async def delete_queue(self, queue_name: str) -> None: ...
    async def get_all_queues(self) -> list[str]: ...

    # Role CRUD
    async def get_queues(self, role: str) -> set[str]: ...
    async def save_role(self, role: str, queues_set: set[str]) -> str: ...
    async def get_all_roles(self) -> list[str]: ...
    async def delete_role(self, role: str) -> None: ...

    # Change detection (ULID-stamped refresh tags drive pub/sub worker refresh)
    async def get_refresh_tag(self, role: str) -> ULID: ...
    async def bump_refresh_tag(self, role: str) -> str: ...
    async def bump_refresh_tags_for_queue(self, queue_name: str) -> list[str]: ...

    # Task routing config CRUD
    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None: ...
    async def save_routing_config(self, routing_config: RoutingConfig) -> None: ...
    async def delete_routing_config(self, task_name: str, task_version: int) -> bool: ...


class CancellationBusProtocol(Protocol):  # pragma: no cover
    """Pub/sub channel for in-flight task cancellation signals."""

    async def publish_cancellation(self, task_id: ULID) -> None: ...
    def listen_cancellations(self) -> AsyncIterator[ULID]: ...


class RoutingNotificationProtocol(Protocol):  # pragma: no cover
    """Routing version key and per-role queue-config refresh signals."""

    async def get_routing_version(self) -> ULID | None: ...
    async def bump_routing_version(self) -> None: ...
    async def notify_refresh(self, role: str, tag: str) -> None: ...
    async def poll_refresh_signal(self, role: str) -> bool: ...


class DeadQueueProtocol(Protocol):  # pragma: no cover
    """Interface for dead letter queue operations."""

    async def ensure_index(self) -> None: ...
    async def add_to_dlq(self, task: Task, failed_at: dt.datetime) -> None: ...
    async def remove_from_dlq(self, task_id: ULID, queue: str, name: str) -> None: ...
    def stage_add(self, pipe: TransactionHandle, task: Task, failed_at: dt.datetime) -> None: ...
    def stage_remove(self, pipe: TransactionHandle, task_id: ULID, queue: str, name: str) -> None: ...
    async def get_history(self, task_id: str) -> list[dict[str, Any]]: ...
    async def get_by_ids(self, task_ids: list[str]) -> list[Task]: ...
    async def get_by_filter(
        self,
        queue: str | None,
        task_name: str | None,
        task_version: int | None,
        limit: int,
    ) -> list[Task]: ...
    async def remove_many(self, task_ids: list[str]) -> None: ...
    async def clean(self, earlier_than: dt.datetime) -> None: ...


# ---------------------------------------------------------------------------
# Split-store protocols — use these for new cross-datastore implementations.
# ---------------------------------------------------------------------------


class TaskSubmitProtocol(Protocol):  # pragma: no cover
    """
    Composite submit and pop operations that require co-located task state and queue.

    These three operations span both ``TaskStateProtocol`` (blob persistence) and
    ``TaskQueueProtocol`` (queue membership).  Redis adapters implement them via
    atomic Lua scripts; SQL adapters implement them with a transaction that writes
    to both the tasks and task_queue tables.

    ``StateManager`` uses this protocol for all submission and dequeue operations.
    When state and queue live on the same backend, the same adapter object satisfies
    both ``TaskStateProtocol`` and ``TaskSubmitProtocol``.  In a split-store
    deployment (e.g. SQL state + Redis queue), a separate adapter can be supplied.
    """

    async def submit_task(self, task: Task) -> bool:
        """Persist the task blob and enqueue it atomically.  Returns True on success."""
        ...

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """Check the sliding rate-limit window and enqueue only if under the limit."""
        ...

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """Blocking pop the next task from any of the given queues.  Returns None on timeout."""
        ...

    async def clean_rate_limiter(
        self, queues: set[bytes], now: dt.datetime, rate_limit_age: dt.timedelta
    ) -> None:
        """Remove rate-limiter sorted-set entries older than ``rate_limit_age``."""
        ...


@runtime_checkable
class TaskStateProtocol(Protocol):  # pragma: no cover
    """
    Task blob persistence: stores task state, heartbeats, fan-in sets, and DAG run index.

    Does not manage queue membership — that belongs to TaskQueueProtocol.
    """

    @property
    def backend_key(self) -> str:
        """
        Stable string identifying the backend instance (e.g. Redis URL or SQL DSN).

        Used by StateManager to detect same-backend mode and enable atomic pipelines.
        """
        ...

    # Task blob CRUD
    async def save_task(self, task: Task) -> None: ...
    async def get_task(self, task_id: ULID) -> Task | None: ...
    async def get_tasks_bulk(self, task_ids: list[ULID]) -> list[Task | None]: ...
    async def task_exists(self, task_id: ULID) -> bool: ...
    async def get_active_tasks(self, queues: set[str]) -> list[Task]: ...
    def get_stale_tasks(self, queues: set[str], stale_time: dt.timedelta) -> AsyncGenerator[Task, None]: ...
    async def get_all_tasks(self, pagination: TaskPagination) -> list[Task]: ...

    async def compare_and_set_status(self, task_id: ULID, expected: TaskStatus, new: TaskStatus) -> bool:
        """
        Atomically transition task status only if the current status equals ``expected``.

        Returns True if the transition was applied, False if the current status did not
        match (e.g. concurrent cancellation changed it first).

        Redis: implemented via WATCH/MULTI on the task key.
        SQL: implemented via ``UPDATE ... WHERE status = ? RETURNING status``.
        """
        ...

    # Heartbeat
    async def update_task_heartbeat(self, task: Task) -> None: ...
    async def remove_task_heartbeat(self, task: Task) -> None: ...

    # Fan-in sets (must be co-located with task state for atomic last-writer-wins semantics)
    async def init_fan_in(self, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400) -> None: ...
    async def fan_in_complete(self, fan_in_key: str, task_id: ULID) -> int: ...
    async def get_fan_in_members(self, fan_in_key: str) -> list[ULID]: ...

    # DAG run index
    async def get_dag_runs(
        self, pagination: DAGRunPagination
    ) -> tuple[list[tuple[ULID, dt.datetime]], int]: ...
    async def get_dag_run(self, dag_run_id: ULID) -> tuple[dt.datetime, list[ULID]] | None: ...
    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None: ...

    # Lifecycle
    async def ensure_index(self) -> None: ...
    async def delete_task(self, task: Task) -> None: ...
    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None: ...
    async def clean(
        self,
        queues: set[bytes],
        now: dt.datetime,
        min_queue_age: dt.datetime | None,
        max_queue_age: dt.datetime | None,
    ) -> None: ...


@runtime_checkable
class TaskQueueProtocol(Protocol):  # pragma: no cover
    """
    Active queue membership: enqueue, dequeue, and rate limiting.

    Operates on task IDs and scores, not task blobs — the blob is the task_state's concern.
    """

    @property
    def backend_key(self) -> str: ...

    async def enqueue(self, task_id: ULID, queue: str, score: float) -> bool:
        """Enqueue task_id into queue with the given score.  Returns False if already queued (NX)."""
        ...

    async def get_next_task_id(self, queues: set[str], pop_timeout: int = 0) -> tuple[ULID, str] | None:
        """Blocking pop from the highest-priority queue.  Returns (task_id, queue_name) or None."""
        ...

    async def remove_from_queue(self, task_id: ULID, queue: str) -> None: ...

    async def check_rate_limit_and_enqueue(
        self,
        task_id: ULID,
        queue: str,
        score: float,
        window_start: float,
        max_count: int,
    ) -> bool:
        """Atomically check the sliding window and enqueue if under limit.  Returns False if rate-limited."""
        ...

    async def clean(self, queues: set[bytes], now: dt.datetime, rate_limit_age: dt.timedelta) -> None: ...


@runtime_checkable
class TaskSchedulerProtocol(Protocol):  # pragma: no cover
    """Scheduled/delayed task queue — promotes tasks into active queues at their run_at time."""

    @property
    def backend_key(self) -> str: ...

    async def add(self, task: Task, run_at: dt.datetime) -> None: ...
    async def remove(self, task_id: ULID, queue: str) -> None: ...
    async def get_run_at(self, task_id: ULID) -> dt.datetime | None: ...
    async def next_due_bulk(
        self, n: int, queues: list[str] | None = None
    ) -> list[tuple[Task, dt.datetime]]: ...
    async def get_by_filter(
        self,
        queue: str | None,
        task_name: str | None,
        task_version: int | None,
        limit: int,
        start_after: str | None,
    ) -> list[tuple[Task, dt.datetime]]: ...

    async def recover_orphans(self, now: dt.datetime) -> None: ...


# ---------------------------------------------------------------------------
# Atomic sub-protocols — extend the base protocols with Redis pipeline staging.
# StateManager uses these when all adapters share the same backend, enabling
# MULTI/EXEC atomicity instead of saga-style coordination.
# ---------------------------------------------------------------------------


@runtime_checkable
class AtomicTaskStateProtocol(TaskStateProtocol, Protocol):  # pragma: no cover
    """TaskStateProtocol + pipeline staging methods for same-backend atomic operations."""

    def pipeline(self, transaction: bool = True) -> TransactionHandle: ...

    def stage_save(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_requeue(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_submit_task(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_remove_from_queue(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_remove_heartbeat(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_init_fan_in(
        self, pipe: TransactionHandle, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None: ...
    async def read_for_watch(self, pipe: TransactionHandle, task_id: ULID) -> Task | None: ...

    async def optimistic_dispatch_scheduled(
        self,
        task: Task,
        stage_extra: Callable[[TransactionHandle], None],
    ) -> bool:
        """
        Atomically transition a SCHEDULED task to SUBMITTED.

        Reads the current task state, verifies it is not cancelled or missing, sets
        status to SUBMITTED, stages a requeue, and calls stage_extra(pipe) for any
        additional staged operations before committing.

        Returns True if dispatched, False if the task was not found or already cancelled.
        Implementations choose their locking strategy (WATCH/MULTI for Redis,
        SELECT FOR UPDATE for SQL).
        """
        ...


@runtime_checkable
class AtomicTaskSchedulerProtocol(TaskSchedulerProtocol, Protocol):  # pragma: no cover
    """TaskSchedulerProtocol + pipeline staging for same-backend atomic operations."""

    def pipeline(self, transaction: bool = True) -> TransactionHandle: ...
    def stage_add(self, pipe: TransactionHandle, task: Task, run_at: dt.datetime) -> None: ...
    def stage_remove(self, pipe: TransactionHandle, task_id: ULID, queue: str) -> None: ...


@runtime_checkable
class AtomicDeadQueueProtocol(DeadQueueProtocol, Protocol):  # pragma: no cover
    """DeadQueueProtocol + a backend_key for same-backend detection."""

    @property
    def backend_key(self) -> str: ...

    def pipeline(self, transaction: bool = True) -> TransactionHandle: ...


class CronDAGSchedulerProtocol(Protocol):  # pragma: no cover
    """Interface for recurring cron-scheduled DAG entries."""

    async def add(self, entry: CronDAGEntry, next_run_at: dt.datetime) -> None: ...
    async def remove(self, cron_id: ULID) -> None: ...
    async def get(self, cron_id: ULID) -> CronDAGEntry | None: ...
    async def next_due_bulk(self, n: int) -> list[tuple[CronDAGEntry, dt.datetime]]: ...
    async def reschedule(self, cron_id: ULID, next_run_at: dt.datetime) -> None: ...
    async def get_active_run(self, cron_id: ULID) -> str | None: ...
    async def set_active_run(
        self, cron_id: ULID, task_id: ULID, ttl: int = 86400, nx: bool = False
    ) -> bool: ...
    async def clear_active_run(self, cron_id: ULID) -> None: ...
    async def get_next_run_at(self, cron_id: ULID) -> dt.datetime | None: ...
    async def list(
        self, offset: int = 0, limit: int = 50
    ) -> tuple[list[tuple[CronDAGEntry, dt.datetime]], int]: ...


@runtime_checkable
class AtomicCronDAGSchedulerProtocol(CronDAGSchedulerProtocol, Protocol):  # pragma: no cover
    """
    CronDAGSchedulerProtocol + pipeline staging for same-backend atomic operations.

    ``backend_key`` allows ``StateManager`` to detect when the cron scheduler shares
    a backend with the task-state adapter and fold cron ops into the same pipeline.
    The three ``stage_*`` methods cover the two hot dispatch-path touch points:
    - ``dispatch_cron_dag``: ``stage_reschedule`` + ``stage_set_active_run`` (+ fan-in)
    - ``complete_cron_task``: ``stage_clear_active_run`` (+ task save)
    """

    @property
    def backend_key(self) -> str: ...

    def pipeline(self, transaction: bool = True) -> TransactionHandle: ...

    def stage_reschedule(self, pipe: TransactionHandle, cron_id: ULID, next_run_at: dt.datetime) -> None: ...

    def stage_set_active_run(
        self,
        pipe: TransactionHandle,
        cron_id: ULID,
        task_id: ULID,
        ttl: int = 86400,
        nx: bool = False,
    ) -> None: ...

    def stage_clear_active_run(self, pipe: TransactionHandle, cron_id: ULID) -> None: ...
