import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from opentelemetry import metrics
from ulid import ULID

import jobbers.state_manager as sm
from jobbers.models import Task

app = FastAPI()
logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
hit_counter = meter.create_up_down_counter("hit_counter")

@app.get("/")
async def read_root() -> dict[str, Any]:
    """Serve the index page."""
    logger.info("Serving the index page")
    return {"message": "Welcome to Task Manager!"}

@app.post("/submit-task")
async def submit_task(task: Task) -> dict[str, Any]:
    """Handle task submission."""
    logger.info("Submitting a task")
    await sm.build_sm().submit_task(task)
    return {
        "message": "Task submitted successfully",
        "task": task.summarized(),
    }

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str) -> dict[str, Any]:
    """Retrieve the status of a specific task."""
    logger.info("Getting task status for task ID %s", task_id)
    task_uid: ULID = ULID.from_str(task_id)
    task = await sm.build_sm().get_task(task_uid)
    if task:
        return {"task_id": str(task.id), "status": task.status}
    raise HTTPException(status_code=404, detail="Task not found")

@app.get("/task-list")
async def get_task_list() -> dict[str, Any]:
    """Retrieve the list of all tasks."""
    logger.info("Getting all tasks")
    tasks = await sm.build_sm().get_all_tasks()
    return {"tasks": tasks}

@app.get("/queues/{role}")
async def get_queues(role: str) -> dict[str, Any]:
    """Retrieve the list of all queues for a given role."""
    logger.info("Getting all queues for role %s", role)
    queues = await sm.build_sm().get_queues(role)  # Ensure the queues are sorted for consistency
    return {"queues": sorted(queues)}

@app.post("/queues/{role}")
async def set_queues(role: str, queues: list[str]) -> dict[str, Any]:
    """Set the list of all queues for a given role."""
    logger.info("Setting all queues for role %s", role)
    await sm.build_sm().set_queues(role, set(queues))
    return {"message": "Queues set successfully"}

@app.get("/queues")
async def get_all_queues() -> dict[str, Any]:
    """Retrieve the list of all queues."""
    logger.info("Getting all queues")
    queues = await sm.build_sm().get_all_queues()
    return {"queues": queues}

@app.get("/roles")
async def get_all_roles() -> dict[str, Any]:
    """Retrieve the list of all roles."""
    logger.info("Getting all roles")
    roles = await sm.build_sm().get_all_roles()
    return {"roles": roles}
