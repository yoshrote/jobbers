import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException
from fastapi.params import Query
from opentelemetry import metrics
from pydantic import BaseModel, Field
from ulid import ULID

from jobbers import db, registry
from jobbers.models.queue_config import QueueConfig, QueueConfigAdapter
from jobbers.models.task import Task, TaskAdapter, TaskPagination
from jobbers.state_manager import TaskException
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

@app.get("/")
async def read_root() -> dict[str, Any]:
    """Serve the index page."""
    logger.info("Serving the index page")
    return {
        "message": "Welcome to Task Manager!",
        "tasks": list(registry.get_tasks())
    }

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

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str) -> dict[str, Any]:
    """Retrieve the status of a specific task."""
    logger.info("Getting task status for task ID %s", task_id)
    task_uid: ULID = ULID.from_str(task_id)
    task = await TaskAdapter(db.get_client()).get_task(task_uid)
    if task:
        return task.summarized()
    raise HTTPException(status_code=404, detail="Task not found")

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
    tasks = await TaskAdapter(db.get_client()).get_all_tasks(filter_query)
    return {"tasks": tasks}

@app.get("/queues/{role}")
async def get_queues(role: str) -> dict[str, Any]:
    """Retrieve the list of all queues for a given role."""
    logger.info("Getting all queues for role %s", role)
    queues = await QueueConfigAdapter(db.get_sqlite_conn()).get_queues(role)  # Ensure the queues are sorted for consistency
    return {"queues": sorted(queues)}

@app.post("/queues/{role}")
async def set_queues(role: str, queues: list[str]) -> dict[str, Any]:
    """Set the list of all queues for a given role."""
    logger.info("Setting all queues for role %s", role)
    await QueueConfigAdapter(db.get_sqlite_conn()).set_queues(role, set(queues))
    return {"message": "Queues set successfully"}

@app.get("/queues")
async def get_all_queues() -> dict[str, Any]:
    """Retrieve the list of all queues."""
    logger.info("Getting all queues")
    queues = await QueueConfigAdapter(db.get_sqlite_conn()).get_all_queues()
    return {"queues": queues}

@app.post("/queues", status_code=201)
async def create_queue(queue_config: QueueConfig) -> dict[str, Any]:
    """Create a new queue with its configuration. Returns 409 if the queue already exists."""
    qca = QueueConfigAdapter(db.get_sqlite_conn())
    existing = await qca.get_queue_config(queue_config.name)
    if existing is not None:
        raise HTTPException(status_code=409, detail=f"Queue '{queue_config.name}' already exists.")
    await qca.save_queue_config(queue_config)
    return {"message": "Queue created successfully", "queue": queue_config.model_dump()}

@app.get("/queues/{queue_name}/config")
async def get_queue_config(queue_name: str) -> dict[str, Any]:
    """Retrieve the configuration for a specific queue."""
    config = await QueueConfigAdapter(db.get_sqlite_conn()).get_queue_config(queue_name)
    if config is None:
        raise HTTPException(status_code=404, detail=f"Queue '{queue_name}' not found.")
    return {"queue": config.model_dump()}

@app.put("/queues/{queue_name}")
async def update_queue(queue_name: str, queue_config: QueueConfig) -> dict[str, Any]:
    """Create or update the configuration for a queue. The name in the body is ignored; the path name is used."""
    queue_config.name = queue_name
    await QueueConfigAdapter(db.get_sqlite_conn()).save_queue_config(queue_config)
    return {"message": "Queue updated successfully", "queue": queue_config.model_dump()}

@app.delete("/queues/{queue_name}", status_code=200)
async def delete_queue(queue_name: str) -> dict[str, Any]:
    """Delete a queue and remove it from all roles. Returns 404 if the queue does not exist."""
    qca = QueueConfigAdapter(db.get_sqlite_conn())
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
    limit: int = Field(default=100, gt=0, le=1000, description="Maximum number of tasks to resubmit (filter mode only).")


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
    roles = await QueueConfigAdapter(db.get_sqlite_conn()).get_all_roles()
    return {"roles": roles}


class RoleRequest(BaseModel):
    """Request body for creating a role."""

    name: str = Field(description="Role name.")
    queues: list[str] = Field(description="Queues assigned to this role.")


@app.post("/roles", status_code=201)
async def create_role(role: RoleRequest) -> dict[str, Any]:
    """Create a new role with an initial set of queues. Returns 409 if the role already exists."""
    qca = QueueConfigAdapter(db.get_sqlite_conn())
    existing = await qca.get_queues(role.name)
    if existing:
        raise HTTPException(status_code=409, detail=f"Role '{role.name}' already exists.")
    await qca.set_queues(role.name, set(role.queues))
    return {"message": "Role created successfully", "role": role.name, "queues": sorted(role.queues)}

@app.get("/roles/{role_name}")
async def get_role(role_name: str) -> dict[str, Any]:
    """Retrieve the queues assigned to a specific role."""
    qca = QueueConfigAdapter(db.get_sqlite_conn())
    all_roles = await qca.get_all_roles()
    if role_name not in all_roles:
        raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
    queues = await qca.get_queues(role_name)
    return {"role": role_name, "queues": sorted(queues)}

@app.put("/roles/{role_name}")
async def update_role(role_name: str, queues: list[str]) -> dict[str, Any]:
    """Replace the queue list for a role. Returns 404 if the role does not exist."""
    qca = QueueConfigAdapter(db.get_sqlite_conn())
    all_roles = await qca.get_all_roles()
    if role_name not in all_roles:
        raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
    await qca.set_queues(role_name, set(queues))
    return {"message": "Role updated successfully", "role": role_name, "queues": sorted(queues)}

@app.delete("/roles/{role_name}", status_code=200)
async def delete_role(role_name: str) -> dict[str, Any]:
    """Delete a role. Queue configs are preserved. Returns 404 if the role does not exist."""
    qca = QueueConfigAdapter(db.get_sqlite_conn())
    all_roles = await qca.get_all_roles()
    if role_name not in all_roles:
        raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
    await qca.delete_role(role_name)
    return {"message": f"Role '{role_name}' deleted successfully."}
