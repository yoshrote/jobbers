import logging
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException
from fastapi.params import Query
from opentelemetry import metrics
from pydantic import BaseModel, Field
from ulid import ULID

from jobbers import db, registry
from jobbers.models.task import Task, TaskAdapter, TaskPagination
from jobbers.state_manager import QueueConfigAdapter, TaskException

app = FastAPI()
logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
hit_counter = meter.create_up_down_counter("hit_counter")

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
        await db.get_state_manager().submit_task(task)
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
    queues = await QueueConfigAdapter(db.get_client()).get_queues(role)  # Ensure the queues are sorted for consistency
    return {"queues": sorted(queues)}

@app.post("/queues/{role}")
async def set_queues(role: str, queues: list[str]) -> dict[str, Any]:
    """Set the list of all queues for a given role."""
    logger.info("Setting all queues for role %s", role)
    await QueueConfigAdapter(db.get_client()).set_queues(role, set(queues))
    return {"message": "Queues set successfully"}

@app.get("/queues")
async def get_all_queues() -> dict[str, Any]:
    """Retrieve the list of all queues."""
    logger.info("Getting all queues")
    queues = await QueueConfigAdapter(db.get_client()).get_all_queues()
    return {"queues": queues}

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
    tasks = sm.dead_queue.get_by_filter(
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
    history = sm.dead_queue.get_history(task_id)
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
        tasks = sm.dead_queue.get_by_ids(request.task_ids)
    elif request.queue or request.task_name or request.task_version is not None:
        tasks = sm.dead_queue.get_by_filter(
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
    tasks = sm.task_scheduler.get_by_filter(
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
    roles = await QueueConfigAdapter(db.get_client()).get_all_roles()
    return {"roles": roles}
