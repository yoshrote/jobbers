import asyncio
import datetime as dt
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from croniter import croniter
from fastapi import FastAPI, HTTPException
from fastapi.params import Query
from opentelemetry import metrics
from pydantic import BaseModel, Field
from ulid import ULID

from jobbers import db, registry
from jobbers.models.cron_dag import ConcurrencyPolicy, CronDAGEntry
from jobbers.models.dag import DAGRunPagination, DAGTaskSpec
from jobbers.models.queue_config import QueueConfig, QueueConfigAdapter
from jobbers.models.task import Task, TaskPagination
from jobbers.state_manager import TaskException
from jobbers.utils.mermaid_dag import MermaidParseError, dag_spec_to_mermaid, parse_mermaid_dag
from jobbers.validation import ValidationError, validate_task


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await db.init_state_manager()
    yield
    await db.close_client()
    await db.close_sqlite_conn()


app = FastAPI(lifespan=lifespan)
logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
hit_counter = meter.create_up_down_counter("hit_counter")
cancellations_requested = meter.create_counter("cancellations_requested", unit="1")


class RootResponse(BaseModel):
    """Response model for the root endpoint."""

    message: str = Field(description="Welcome message.")
    tasks: list[tuple[str, int]] = Field(description="List of registered tasks with their versions.")


@app.get("/")
async def read_root() -> RootResponse:
    """Serve the index page."""
    logger.info("Serving the index page")
    return RootResponse(message="Welcome to Task Manager!", tasks=list(registry.get_tasks()))


@app.post("/submit-task")
async def submit_task(task: Task) -> dict[str, Any]:
    """Handle task submission."""
    logger.info("Submitting a task")
    try:
        await validate_task(task)
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex
    try:
        state_manager = db.get_state_manager()
        await state_manager.submit_task(task)
    except TaskException as ex:
        raise HTTPException(status_code=400, detail=f"Invalid task parameters: {ex}") from ex
    return {
        "message": "Task submitted successfully",
        "task": task.summarized(),
    }


class ScheduleTaskRequest(BaseModel):
    """Request body for scheduling a task at a specific time."""

    task: Task
    run_at: dt.datetime = Field(description="UTC datetime when the task should be executed.")


@app.post("/schedule-task")
async def schedule_task(request: ScheduleTaskRequest) -> dict[str, Any]:
    """Schedule a task to run at a specific UTC datetime."""
    logger.info("Scheduling task %s to run at %s", request.task.name, request.run_at)
    try:
        await validate_task(request.task)
    except ValidationError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex
    sm = db.get_state_manager()
    await sm.schedule_new_task(request.task, request.run_at)
    return {
        "message": "Task scheduled successfully",
        "task": request.task.summarized(),
        "run_at": request.run_at.isoformat(),
    }


@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str) -> dict[str, Any]:
    """
    Retrieve the status of a specific task.

    If the task is the root of a DAG (has ``dag_callbacks``), the response
    includes a ``dag_diagram`` field containing a mermaid flowchart of the
    full DAG structure rooted at this task.
    """
    logger.info("Getting task status for task ID %s", task_id)
    task_uid: ULID = ULID.from_str(task_id)
    task = await db.get_task_adapter().get_task(task_uid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    summary = task.summarized()
    if task.dag_callbacks:
        spec = DAGTaskSpec(
            id=task.id,
            name=task.name,
            queue=task.queue,
            version=task.version,
            parameters=task.parameters,
            dag_callbacks=task.dag_callbacks,
        )
        summary["dag_diagram"] = dag_spec_to_mermaid(spec)
    return summary


@app.post("/task/{task_id}/cancel")
async def cancel_task(task_id: str) -> dict[str, Any]:
    """Cancel a running task by publishing to its cancellation channel."""
    logger.info("Requesting cancellation for task ID %s", task_id)
    task_uid: ULID = ULID.from_str(task_id)
    sm = db.get_state_manager()
    try:
        task = await sm.request_task_cancellation(task_uid)
    except TaskException as ex:
        raise HTTPException(status_code=409, detail=str(ex)) from ex
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    cancellations_requested.add(1, {"queue": task.queue, "task": task.name})
    return {"message": "Cancellation request sent", "task": task.summarized()}


class BulkCancelRequest(BaseModel):
    """Request body for bulk task cancellation."""

    task_ids: list[str] = Field(description="List of task IDs to cancel.")


@app.post("/tasks/cancel")
async def cancel_tasks(request: BulkCancelRequest) -> dict[str, Any]:
    """Cancel multiple tasks by publishing to each task's cancellation channel."""
    logger.info("Requesting cancellation for %d tasks", len(request.task_ids))
    sm = db.get_state_manager()

    async def _cancel_one(task_id_str: str) -> dict[str, Any]:
        try:
            task_uid: ULID = ULID.from_str(task_id_str)
            task = await sm.request_task_cancellation(task_uid)
        except TaskException as ex:
            return {"task_id": task_id_str, "status": "error", "detail": str(ex)}
        if task is None:
            return {"task_id": task_id_str, "status": "not_found"}
        cancellations_requested.add(1, {"queue": task.queue, "task": task.name})
        return {"task_id": task_id_str, "status": "cancellation_requested"}

    results = list(await asyncio.gather(*(_cancel_one(tid) for tid in request.task_ids)))
    return {"results": results}


@app.get("/task-list")
async def get_task_list(filter_query: Annotated[TaskPagination, Query()]) -> dict[str, Any]:
    """Retrieve the list of all tasks."""
    logger.info("Getting all tasks")
    tasks = await db.get_task_adapter().get_all_tasks(filter_query)
    return {"tasks": tasks}


@app.get("/queues/{role}")
async def get_queues(role: str) -> dict[str, Any]:
    """Retrieve the list of all queues for a given role."""
    logger.info("Getting all queues for role %s", role)
    queues = await QueueConfigAdapter(db.get_session_factory()).get_queues(
        role
    )  # Ensure the queues are sorted for consistency
    return {"queues": sorted(queues)}


@app.post("/queues/{role}")
async def set_queues(role: str, queues: list[str]) -> dict[str, Any]:
    """Set the list of all queues for a given role."""
    logger.info("Setting all queues for role %s", role)
    await QueueConfigAdapter(db.get_session_factory()).save_role(role, set(queues))
    return {"message": "Queues set successfully"}


@app.get("/queues")
async def get_all_queues() -> dict[str, Any]:
    """Retrieve the list of all queues."""
    logger.info("Getting all queues")
    queues = await QueueConfigAdapter(db.get_session_factory()).get_all_queues()
    return {"queues": queues}


@app.post("/queues", status_code=201)
async def create_queue(queue_config: QueueConfig) -> dict[str, Any]:
    """Create a new queue with its configuration. Returns 409 if the queue already exists."""
    qca = QueueConfigAdapter(db.get_session_factory())
    existing = await qca.get_queue_config(queue_config.name)
    if existing is not None:
        raise HTTPException(status_code=409, detail=f"Queue '{queue_config.name}' already exists.")
    await qca.save_queue_config(queue_config)
    return {"message": "Queue created successfully", "queue": queue_config.model_dump()}


@app.get("/queues/{queue_name}/config")
async def get_queue_config(queue_name: str) -> dict[str, Any]:
    """Retrieve the configuration for a specific queue."""
    config = await QueueConfigAdapter(db.get_session_factory()).get_queue_config(queue_name)
    if config is None:
        raise HTTPException(status_code=404, detail=f"Queue '{queue_name}' not found.")
    return {"queue": config.model_dump()}


@app.put("/queues/{queue_name}")
async def update_queue(queue_name: str, queue_config: QueueConfig) -> dict[str, Any]:
    """Create or update the configuration for a queue. The name in the body is ignored; the path name is used."""
    queue_config.name = queue_name
    await QueueConfigAdapter(db.get_session_factory()).save_queue_config(queue_config)
    return {"message": "Queue updated successfully", "queue": queue_config.model_dump()}


@app.delete("/queues/{queue_name}", status_code=200)
async def delete_queue(queue_name: str) -> dict[str, Any]:
    """Delete a queue and remove it from all roles. Returns 404 if the queue does not exist."""
    qca = QueueConfigAdapter(db.get_session_factory())
    all_queues = await qca.get_all_queues()
    if queue_name not in all_queues:
        raise HTTPException(status_code=404, detail=f"Queue '{queue_name}' not found.")
    await qca.delete_queue(queue_name)
    return {"message": f"Queue '{queue_name}' deleted successfully."}


class DLQFilter(BaseModel):
    """Filter criteria for searching the dead letter queue."""

    queue: str | None = Field(default=None, description="Filter by queue name.")
    task_name: str | None = Field(default=None, description="Filter by task name.")
    task_version: int | None = Field(default=None, description="Filter by task version.")
    limit: int = Field(default=100, gt=0, le=1000, description="Maximum number of entries to return.")


@app.get("/dead-letter-queue")
async def get_dead_letter_queue(filter_query: Annotated[DLQFilter, Query()]) -> dict[str, Any]:
    """
    Search the dead letter queue.

    Returns tasks that have exhausted retries or been permanently failed, optionally
    filtered by queue, task name, or version. Results include the task summary and
    the most recent error message.
    """
    sm = db.get_state_manager()
    tasks = await sm.dead_queue.get_by_filter(
        queue=filter_query.queue,
        task_name=filter_query.task_name,
        task_version=filter_query.task_version,
        limit=filter_query.limit,
    )
    return {"tasks": [t.summarized() for t in tasks]}


@app.get("/dead-letter-queue/{task_id}/history")
async def get_dlq_task_history(task_id: str) -> dict[str, Any]:
    """
    Retrieve the full failure history for a dead-lettered task.

    Returns every recorded failure event for the task in chronological order,
    including the retry attempt number, timestamp, and error message for each failure.
    """
    sm = db.get_state_manager()
    history = await sm.dead_queue.get_history(task_id)
    return {"task_id": task_id, "history": history}


class DLQResubmitRequest(BaseModel):
    """
    Filter criteria for bulk resubmission from the dead letter queue.

    Provide either `task_ids` for an explicit list, or one or more of `queue`,
    `task_name`, and `task_version` to select by filter. At least one field
    must be set to prevent accidentally resubmitting the entire queue.
    """

    task_ids: list[str] | None = Field(default=None, description="Explicit list of task IDs to resubmit.")
    queue: str | None = Field(default=None, description="Resubmit only tasks from this queue.")
    task_name: str | None = Field(default=None, description="Resubmit only tasks with this name.")
    task_version: int | None = Field(default=None, description="Resubmit only tasks with this version.")
    reset_retry_count: bool = Field(default=True, description="Reset retry_attempt to 0 before resubmitting.")
    limit: int = Field(
        default=100, gt=0, le=1000, description="Maximum number of tasks to resubmit (filter mode only)."
    )


class DLQRemoveRequest(BaseModel):
    """Request body for bulk removal from the dead letter queue."""

    task_ids: list[str] = Field(description="List of task IDs to remove.")


@app.delete("/dead-letter-queue")
async def remove_from_dlq(request: DLQRemoveRequest) -> dict[str, Any]:
    """Bulk remove tasks from the dead letter queue by task ID."""
    sm = db.get_state_manager()
    await sm.dead_queue.remove_many(request.task_ids)
    return {"removed": len(request.task_ids), "task_ids": request.task_ids}


@app.post("/dead-letter-queue/resubmit")
async def resubmit_from_dlq(request: DLQResubmitRequest) -> dict[str, Any]:
    """Bulk resubmit tasks from the dead letter queue back into their active queues."""
    sm = db.get_state_manager()

    if request.task_ids is not None:
        tasks = await sm.dead_queue.get_by_ids(request.task_ids)
    elif request.queue or request.task_name or request.task_version is not None:
        tasks = await sm.dead_queue.get_by_filter(
            queue=request.queue,
            task_name=request.task_name,
            task_version=request.task_version,
            limit=request.limit,
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="Provide task_ids or at least one of: queue, task_name, task_version.",
        )

    resubmitted = await sm.resubmit_dead_tasks(tasks, reset_retry_count=request.reset_retry_count)
    return {
        "resubmitted": len(resubmitted),
        "tasks": [t.summarized() for t in resubmitted],
    }


@app.get("/active-tasks")
async def get_active_tasks(queue: str | None = None) -> dict[str, Any]:
    """Retrieve tasks currently being executed (those with an active heartbeat record)."""
    sm = db.get_state_manager()
    if queue:
        queues: set[str] = {queue}
    else:
        queues = set(await QueueConfigAdapter(db.get_session_factory()).get_all_queues())
    tasks = await sm.get_active_tasks(queues)
    return {"tasks": [t.summarized() for t in tasks]}


@app.get("/scheduled-tasks")
async def get_scheduled_tasks(filter_query: Annotated[TaskPagination, Query()]) -> dict[str, Any]:
    """Retrieve scheduled tasks filtered by queue, and optionally by task name or version."""
    sm = db.get_state_manager()
    tasks = await sm.task_scheduler.get_by_filter(
        queue=filter_query.queue,
        task_name=filter_query.task_name,
        task_version=filter_query.task_version,
        limit=filter_query.limit,
        start_after=str(filter_query.start) if filter_query.start else None,
    )
    return {"tasks": [t.summarized() for t in tasks]}


@app.get("/roles")
async def get_all_roles() -> dict[str, Any]:
    """Retrieve the list of all roles."""
    logger.info("Getting all roles")
    roles = await QueueConfigAdapter(db.get_session_factory()).get_all_roles()
    return {"roles": roles}


class RoleRequest(BaseModel):
    """Request body for creating a role."""

    name: str = Field(description="Role name.")
    queues: list[str] = Field(description="Queues assigned to this role.")


@app.post("/roles", status_code=201)
async def create_role(role: RoleRequest) -> dict[str, Any]:
    """Create a new role with an initial set of queues. Returns 409 if the role already exists."""
    qca = QueueConfigAdapter(db.get_session_factory())
    existing = await qca.get_queues(role.name)
    if existing:
        raise HTTPException(status_code=409, detail=f"Role '{role.name}' already exists.")
    await qca.save_role(role.name, set(role.queues))
    return {"message": "Role created successfully", "role": role.name, "queues": sorted(role.queues)}


@app.get("/roles/{role_name}")
async def get_role(role_name: str) -> dict[str, Any]:
    """Retrieve the queues assigned to a specific role."""
    qca = QueueConfigAdapter(db.get_session_factory())
    all_roles = await qca.get_all_roles()
    if role_name not in all_roles:
        raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
    queues = await qca.get_queues(role_name)
    return {"role": role_name, "queues": sorted(queues)}


@app.put("/roles/{role_name}")
async def update_role(role_name: str, queues: list[str]) -> dict[str, Any]:
    """Replace the queue list for a role. Returns 404 if the role does not exist."""
    qca = QueueConfigAdapter(db.get_session_factory())
    all_roles = await qca.get_all_roles()
    if role_name not in all_roles:
        raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
    await qca.save_role(role_name, set(queues))
    return {"message": "Role updated successfully", "role": role_name, "queues": sorted(queues)}


@app.delete("/roles/{role_name}", status_code=200)
async def delete_role(role_name: str) -> dict[str, Any]:
    """Delete a role. Queue configs are preserved. Returns 404 if the role does not exist."""
    qca = QueueConfigAdapter(db.get_session_factory())
    all_roles = await qca.get_all_roles()
    if role_name not in all_roles:
        raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
    await qca.delete_role(role_name)
    return {"message": f"Role '{role_name}' deleted successfully."}


# ── DAG submission ─────────────────────────────────────────────────────────────


class SubmitDAGRequest(BaseModel):
    """Request body for ad-hoc DAG submission from a mermaid diagram."""

    diagram: str = Field(description="Mermaid flowchart text describing the DAG.")


@app.post("/submit-dag")
async def submit_dag(request: SubmitDAGRequest) -> dict[str, Any]:
    """
    Submit an ad-hoc DAG defined as a mermaid flowchart.

    Parses the diagram, assigns ULIDs to each node, initialises fan-in Redis
    sets, and enqueues all root tasks.  Returns the IDs of the submitted root
    tasks.
    """
    try:
        roots = parse_mermaid_dag(request.diagram)
    except MermaidParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    for root in roots:
        name = root._name
        if not registry.get_task_config(name, 0):
            raise HTTPException(
                status_code=400,
                detail=f"Unknown task name '{name}'. Register it with @register_task before submitting.",
            )

    sm = db.get_state_manager()
    submitted = await sm.submit_dag(*roots)
    return {"root_task_ids": [str(t.id) for t in submitted]}


# ── Cron DAG CRUD ──────────────────────────────────────────────────────────────


class CronDAGRequest(BaseModel):
    """Request body for creating or updating a cron-scheduled DAG."""

    name: str = Field(description="Human-readable name for the cron entry.")
    cron_expr: str = Field(description="Standard 5-field cron expression (e.g. '0 6 * * *').")
    diagram: str = Field(description="Mermaid flowchart text describing the DAG.")
    enabled: bool = Field(default=True, description="Whether the entry fires on schedule.")
    concurrency_policy: ConcurrencyPolicy = Field(
        default=ConcurrencyPolicy.ALWAYS,
        description="What to do when a new run fires while the previous one is still active.",
    )


def _cron_dag_to_dict(entry: CronDAGEntry, next_run_at: dt.datetime | None) -> dict[str, Any]:
    return {
        "id": str(entry.id),
        "name": entry.name,
        "cron_expr": entry.cron_expr,
        "diagram": dag_spec_to_mermaid(entry.dag_spec),
        "enabled": entry.enabled,
        "concurrency_policy": entry.concurrency_policy.value,
        "created_at": entry.created_at.isoformat(),
        "next_run_at": next_run_at.isoformat() if next_run_at else None,
    }


@app.post("/cron-dags", status_code=201)
async def create_cron_dag(request: CronDAGRequest) -> dict[str, Any]:
    """Create a new cron-scheduled DAG entry."""
    if not croniter.is_valid(request.cron_expr):
        raise HTTPException(status_code=400, detail=f"Invalid cron expression: {request.cron_expr!r}")
    try:
        roots = parse_mermaid_dag(request.diagram)
    except MermaidParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if len(roots) != 1:
        raise HTTPException(status_code=400, detail="Cron DAGs must have exactly one root node.")

    dag_spec = roots[0]._to_spec()
    entry = CronDAGEntry(
        name=request.name,
        cron_expr=request.cron_expr,
        dag_spec=dag_spec,
        enabled=request.enabled,
        concurrency_policy=request.concurrency_policy,
    )
    sm = db.get_state_manager()
    await sm.add_cron_dag(entry)
    next_run_at = await sm.cron_dag_scheduler.get_next_run_at(entry.id)
    return _cron_dag_to_dict(entry, next_run_at)


@app.get("/cron-dags")
async def list_cron_dags(offset: int = 0, limit: int = 50) -> dict[str, Any]:
    """List all cron-scheduled DAG entries ordered by next run time."""
    sm = db.get_state_manager()
    entries, total = await sm.cron_dag_scheduler.list(offset=offset, limit=limit)
    return {
        "total": total,
        "cron_dags": [_cron_dag_to_dict(entry, next_run_at) for entry, next_run_at in entries],
    }


@app.get("/cron-dags/{cron_id}")
async def get_cron_dag(cron_id: str) -> dict[str, Any]:
    """Retrieve a single cron-scheduled DAG entry by ID."""
    uid = ULID.from_str(cron_id)
    sm = db.get_state_manager()
    entry = await sm.cron_dag_scheduler.get(uid)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Cron DAG '{cron_id}' not found.")
    next_run_at = await sm.cron_dag_scheduler.get_next_run_at(uid)
    return _cron_dag_to_dict(entry, next_run_at)


@app.put("/cron-dags/{cron_id}")
async def update_cron_dag(cron_id: str, request: CronDAGRequest) -> dict[str, Any]:
    """
    Replace the diagram and settings for a cron-scheduled DAG entry.

    The entry ``id`` and ``created_at`` are preserved; the schedule is reset
    to the next occurrence of the (possibly updated) cron expression.
    """
    uid = ULID.from_str(cron_id)
    sm = db.get_state_manager()
    existing = await sm.cron_dag_scheduler.get(uid)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Cron DAG '{cron_id}' not found.")
    if not croniter.is_valid(request.cron_expr):
        raise HTTPException(status_code=400, detail=f"Invalid cron expression: {request.cron_expr!r}")
    try:
        roots = parse_mermaid_dag(request.diagram)
    except MermaidParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if len(roots) != 1:
        raise HTTPException(status_code=400, detail="Cron DAGs must have exactly one root node.")

    dag_spec = roots[0]._to_spec()
    updated = CronDAGEntry(
        id=existing.id,
        name=request.name,
        cron_expr=request.cron_expr,
        dag_spec=dag_spec,
        enabled=request.enabled,
        concurrency_policy=request.concurrency_policy,
        created_at=existing.created_at,
    )
    await sm.add_cron_dag(updated)
    next_run_at = await sm.cron_dag_scheduler.get_next_run_at(uid)
    return _cron_dag_to_dict(updated, next_run_at)


@app.delete("/cron-dags/{cron_id}", status_code=200)
async def delete_cron_dag(cron_id: str) -> dict[str, Any]:
    """Delete a cron-scheduled DAG entry. Returns 404 if not found."""
    uid = ULID.from_str(cron_id)
    sm = db.get_state_manager()
    entry = await sm.cron_dag_scheduler.get(uid)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Cron DAG '{cron_id}' not found.")
    await sm.remove_cron_dag(uid)
    return {"message": f"Cron DAG '{cron_id}' deleted successfully."}


# ── DAG run inspection ─────────────────────────────────────────────────────────


@app.get("/dags")
async def list_dags(pagination: Annotated[DAGRunPagination, Query()]) -> dict[str, Any]:
    """List all DAG runs ordered by submission time (oldest first)."""
    sm = db.get_state_manager()
    dag_runs, total = await sm.list_dag_runs(pagination)
    return {
        "total": total,
        "dags": [
            {"dag_run_id": str(rid), "submitted_at": ts.isoformat()}
            for rid, ts in dag_runs
        ],
    }


@app.get("/dags/{dag_run_id}")
async def get_dag(dag_run_id: str) -> dict[str, Any]:
    """Get details of a single DAG run including the IDs of all tasks within it."""
    uid = ULID.from_str(dag_run_id)
    sm = db.get_state_manager()
    result = await sm.get_dag_run(uid)
    if result is None:
        raise HTTPException(status_code=404, detail=f"DAG run '{dag_run_id}' not found.")
    submitted_at, task_ids = result
    return {
        "dag_run_id": dag_run_id,
        "submitted_at": submitted_at.isoformat(),
        "task_ids": [str(t) for t in task_ids],
    }
