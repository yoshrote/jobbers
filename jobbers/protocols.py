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
- `AtomicDagRunProtocol` — optional additive capability: fold DAG-run-terminal
  bookkeeping into the same pipeline as close_dag_run_task (Redis/RedisJSON only).
- `DeadQueueProtocol` — interface all dead-letter queue implementations must implement.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, NamedTuple, runtime_checkable

from typing_extensions import Protocol

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import AsyncGenerator, AsyncIterator, Callable
    from typing import Any

    from ulid import ULID

    from jobbers.models.cron_dag import CronDAGEntry
    from jobbers.models.dag import DAGRunDetail, DagRunOutcome, DAGRunPagination, DAGRunSummary
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
    """Interface for queue/role configuration."""

    async def get_queue_config(self, queue: str) -> QueueConfig | None: ...
    async def save_queue_config(self, queue_config: QueueConfig) -> None: ...
    async def create_queue_config(self, queue_config: QueueConfig) -> bool: ...
    async def delete_queue(self, queue_name: str) -> list[str]: ...
    async def get_all_queues(self) -> list[str]: ...
    async def get_queues(self, role: str) -> set[str]: ...
    async def save_role(self, role: str, queues_set: set[str]) -> None: ...
    async def create_role(self, role: str, queues_set: set[str]) -> bool: ...
    async def get_all_roles(self) -> list[str]: ...
    async def delete_role(self, role: str) -> None: ...
    async def get_queue_limits(self, queues_set: set[str]) -> dict[str, int | None]: ...
    async def get_roles_for_queue(self, queue_name: str) -> list[str]: ...


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
    async def create_queue_config(self, queue_config: QueueConfig) -> bool: ...
    async def delete_queue(self, queue_name: str) -> list[str]: ...
    async def get_all_queues(self) -> list[str]: ...

    # Role CRUD
    async def get_queues(self, role: str) -> set[str]: ...
    async def save_role(self, role: str, queues_set: set[str]) -> None: ...
    async def create_role(self, role: str, queues_set: set[str]) -> bool: ...
    async def get_all_roles(self) -> list[str]: ...
    async def delete_role(self, role: str) -> None: ...

    # Role discovery for change notification
    async def get_roles_for_queue(self, queue_name: str) -> list[str]: ...

    # Lifecycle
    async def drop_stale_indexes(self) -> list[str]: ...

    # Task routing config CRUD
    async def get_routing_config(self, task_name: str, task_version: int) -> RoutingConfig | None: ...
    async def save_routing_config(self, routing_config: RoutingConfig) -> None: ...
    async def delete_routing_config(self, task_name: str, task_version: int) -> bool: ...


CancellationKind = Literal["task", "dag"]


class CancellationMessage(NamedTuple):
    """A parsed cancellation-bus message: cancel one task, or every task in a DAG run."""

    kind: CancellationKind
    id: ULID


class CancellationBusProtocol(Protocol):  # pragma: no cover
    """Pub/sub channel for in-flight task and DAG-run cancellation signals."""

    async def publish_cancellation(self, task_id: ULID) -> None: ...
    async def publish_dag_cancellation(self, dag_run_id: ULID) -> None: ...
    def listen_cancellations(self) -> AsyncIterator[CancellationMessage]: ...


class RoutingNotificationProtocol(Protocol):  # pragma: no cover
    """Routing version key, per-role refresh tags, and pub/sub change signals."""

    async def get_routing_version(self) -> ULID | None: ...
    async def bump_routing_version(self) -> None: ...
    async def get_refresh_tag(self, role: str) -> ULID: ...
    async def bump_refresh_tag(self, role: str) -> str: ...
    async def poll_refresh_signal(self, role: str) -> ULID: ...


class DeadQueueProtocol(Protocol):  # pragma: no cover
    """Interface for dead letter queue operations."""

    async def ensure_index(self) -> None: ...
    async def drop_stale_indexes(self) -> list[str]: ...
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
    async def clean_orphaned_entries(self) -> int: ...


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

    async def enqueue(self, task: Task) -> None:
        """
        Add an already-persisted task's ID to its queue; saga-mode counterpart to submit_task().

        Assumes the caller already saved the task blob via ``TaskStateProtocol.save_task()``
        (the first step of the saga). Unlike ``submit_task()``, this does not (re)persist the
        blob and is not gated on the blob's absence, so it is safe to call both for a
        brand-new submission (immediately after saving) and for re-enqueueing a task that
        was already submitted before (e.g. requeue after cancellation, or DLQ resubmission).
        Used by ``StateManager`` only in saga mode — the atomic pipeline mode instead stages
        the equivalent ZADD via ``AtomicTaskStateProtocol.stage_submit_task``/``stage_requeue``.
        """
        ...

    async def submit_rate_limited_task(self, task: Task, queue_config: QueueConfig) -> bool:
        """Check the sliding rate-limit window and enqueue only if under the limit."""
        ...

    async def get_next_task(self, queues: set[str], pop_timeout: int = 0) -> Task | None:
        """Blocking pop the next task from any of the given queues.  Returns None on timeout."""
        ...

    async def clean_rate_limiter(
        self, queues: set[str], now: dt.datetime, rate_limit_age: dt.timedelta
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

    # Fan-in tracking (scoped per DAG run — consolidated into per-run structures on
    # Redis backends so key count stays flat regardless of collector/nesting count;
    # SQL backends accept dag_run_id for protocol parity but don't need it for routing)
    async def init_fan_in(
        self, dag_run_id: ULID, fan_in_key: str, predecessor_ids: set[ULID], ttl: int = 86400
    ) -> None: ...
    async def fan_in_complete(self, dag_run_id: ULID, fan_in_key: str, task_id: ULID) -> int: ...
    async def get_fan_in_members(self, dag_run_id: ULID, fan_in_key: str) -> list[ULID]: ...
    async def delegate_fan_in(self, dag_run_id: ULID, fan_in_key: str, old_id: ULID, new_id: ULID) -> None:
        """
        Atomically replace *old_id* with *new_id* in the tracking and permanent members data for *fan_in_key*.

        Used when a dynamic fanout arm itself returns a nested ``DynamicFanOut``
        with ``propagate_fan_in=True``: the arm's ID is swapped for the
        grandcollector's ID so the outer fan-in waits for the grandcollector.
        """
        ...

    async def dag_run_fan_in_alive(self, dag_run_id: ULID) -> bool:
        """Whether this run's fan-in tracking is still present (not expired/swept)."""
        ...

    async def refresh_dag_run_fan_in_ttl(self, dag_run_id: ULID, ttl: int = 86400) -> None:
        """Extend (never shrink) this run's fan-in tracking TTL. No-op if it doesn't exist."""
        ...

    # DAG run index
    async def get_dag_runs(self, pagination: DAGRunPagination) -> tuple[list[DAGRunSummary], int]: ...
    async def get_dag_run(self, dag_run_id: ULID) -> DAGRunDetail | None: ...
    async def clean_dag_runs(self, now: dt.datetime, max_age: dt.timedelta) -> None: ...
    async def close_dag_run_task(self, dag_run_id: ULID, task_id: ULID) -> int:
        """
        Mark task_id resolved within dag_run_id's pending set; return the remaining count.

        Returns -1 if task_id was not pending (already closed, or the run's pending
        key/rows expired) — callers must treat -1 as "do not trigger the sweep",
        exactly like fan_in_complete's -1 sentinel.
        """
        ...

    # DAG run aggregate status — wholly additive to the pending/closed mechanism above;
    # neither of these reads or writes DAG_RUN_PENDING/DAG_RUN_CLOSED state.
    async def record_dag_run_task_terminal(self, dag_run_id: ULID, outcome: DagRunOutcome) -> None:
        """Atomically increment the run's completed/failed counters and recompute+persist status."""
        ...

    async def mark_dag_run_complete(self, dag_run_id: ULID) -> None:
        """Set status='complete' iff failed_count == 0. No-op if the run's record is missing."""
        ...

    # DAG run cancellation — a persisted marker, since "cancelling" can't be derived
    # from the completed/failed counters alone (a run can be cancelling with zero
    # failures recorded yet).
    async def mark_dag_run_cancelling(self, dag_run_id: ULID) -> None:
        """Idempotently record that cancellation was requested for this run."""
        ...

    async def is_dag_run_cancelling(self, dag_run_id: ULID) -> bool:
        """Cheap check: has cancellation been requested for this run? False if the run doesn't exist."""
        ...

    async def clear_dag_run_cancellation(self, dag_run_id: ULID) -> None:
        """
        Clear a previously-set cancellation marker (see docs/dag-resume-design.md §4.3).

        Required before resuming a cancelled run: TaskProcessor's is_dag_run_cancelling
        gates in post_process/_handle_retry would otherwise keep suppressing the
        resumed task's own descendants/retries. No-op if the run isn't cancelling.
        """
        ...

    # DAG run resume support (docs/dag-resume-design.md)
    async def reconcile_dag_run_task_retry(self, dag_run_id: ULID, count: int = 1) -> None:
        """
        Undo ``count`` earlier 'failed' terminal-outcome records for tasks about to be retried.

        record_dag_run_task_terminal's 'failed' counter is otherwise monotonic (see
        docs/dag-resume-design.md §2.2) -- without this, a stuck task that's resumed
        and succeeds would leave the run at partial_failure forever, since the
        original failure's increment is never undone by a later success. Takes a
        ``count`` rather than requiring one call per task so resuming N stuck tasks
        in a run costs one round trip, not N. Floored at 0; no-op if the run's
        record is missing.
        """
        ...

    # Lifecycle
    async def ensure_index(self) -> None: ...
    async def drop_stale_indexes(self) -> list[str]: ...
    async def delete_task(self, task: Task) -> None: ...
    async def clean_terminal_tasks(self, now: dt.datetime, max_age: dt.timedelta) -> None: ...
    async def clean(
        self,
        queues: set[str],
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

    async def clean(self, queues: set[str], now: dt.datetime, rate_limit_age: dt.timedelta) -> None: ...


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
    """TaskStateProtocol + pipeline staging methods for same-backend atomic operations (Redis or SQL)."""

    def pipeline(self, transaction: bool = True) -> TransactionHandle: ...

    def stage_save(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_requeue(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_submit_task(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_remove_from_queue(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_remove_heartbeat(self, pipe: TransactionHandle, task: Task) -> None: ...
    def stage_init_fan_in(
        self,
        pipe: TransactionHandle,
        dag_run_id: ULID,
        fan_in_key: str,
        predecessor_ids: set[ULID],
        ttl: int = 86400,
    ) -> None: ...
    async def read_for_watch(self, pipe: TransactionHandle, task_id: ULID) -> Task | None: ...

    async def atomic_dispatch_scheduled(
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
        Locking strategy: WATCH/MULTI (Redis) or SELECT FOR UPDATE (SQL).
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


@runtime_checkable
class AtomicDagRunProtocol(Protocol):  # pragma: no cover
    """
    Optional additive capability, checked independently of AtomicTaskStateProtocol.

    Lets StateManager fold record_dag_run_task_terminal + close_dag_run_task into a
    single pipelined round trip instead of two separate EVALSHA calls.

    Not part of AtomicTaskStateProtocol itself -- an adapter can be atomic-pipeline-
    eligible (stage_save, stage_requeue, etc.) without implementing this. SQL's
    record_dag_run_task_terminal is already a single UPDATE with no separate round
    trip to fold away, so SQLTaskState has no reason to implement it; only
    RedisTaskState/RedisJSONTaskState do.
    """

    def pipeline(self, transaction: bool = True) -> TransactionHandle: ...

    async def stage_record_dag_run_task_terminal(
        self, pipe: TransactionHandle, dag_run_id: ULID, outcome: DagRunOutcome
    ) -> None:
        """
        Stage the counter-increment + status recompute onto pipe.

        Must be awaited even though it only queues a command onto pipe and performs
        no I/O of its own: registered Lua scripts (redis-py's AsyncScript) are
        coroutines regardless of whether `client` is the live connection or a
        pipeline. The actual result is only available in the list returned by
        `await pipe.execute()`.
        """
        ...

    async def stage_close_dag_run_task(
        self, pipe: TransactionHandle, dag_run_id: ULID, task_id: ULID
    ) -> None:
        """Stage close_dag_run_task's SREM/SADD/SCARD move onto pipe (see stage_record_dag_run_task_terminal)."""
        ...


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

    async def try_acquire_dispatch_lock(self, cron_id: ULID, ttl: int = 60) -> bool:
        """
        Atomically claim the right to dispatch this cron entry's current due run.

        Returns True if claimed, False if another dispatcher already holds it.
        Distinct from (and much shorter-lived than) the active-run marker set by
        ``set_active_run`` — this only guards the brief window of the dispatch
        operation itself (reschedule + fan-in init + submit), not how long the
        dispatched DAG run takes to execute. Guards against two scheduler instances
        racing to dispatch the same due occurrence (e.g. during a rolling restart);
        self-heals via TTL if a dispatcher crashes mid-operation without releasing it.
        """
        ...

    async def release_dispatch_lock(self, cron_id: ULID) -> None:
        """Release a dispatch lock held via try_acquire_dispatch_lock, if any."""
        ...


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
