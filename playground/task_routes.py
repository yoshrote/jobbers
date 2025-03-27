import logging

from fastapi import FastAPI, HTTPException
from opentelemetry import metrics
from ulid import ULID

from .state_manager import Task, StateManager

app = FastAPI()
logger = logging.getLogger(__name__)
meter = metrics.get_meter(__name__)
hit_counter = meter.create_up_down_counter("hit_counter")

def build_tm() -> StateManager:
    from playground import db
    return StateManager(db.get_client())

@app.get("/")
async def read_root():
    """Serve the index page."""
    logger.info("Serving the index page")
    # hit_counter.add(1)
    return {"message": "Welcome to Task Manager!"}

@app.post("/submit-task")
async def submit_task(task: Task):
    """Handle task submission."""
    logger.info("Submitting a task")
    await build_tm().submit_task(task)
    return {
        "message": "Task submitted successfully",
        "task": task.summarized(),
    }

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Retrieve the status of a specific task."""
    logger.info("Getting task status for task ID %s", task_id)
    task_id = ULID.from_str(task_id)
    task = await build_tm().get_task(task_id)
    if task:
        return {"task_id": str(task.id), "status": task.status}
    raise HTTPException(status_code=404, detail="Task not found")

@app.get("/task-list")
async def get_task_list():
    """Retrieve the list of all tasks."""
    logger.info("Getting all tasks")
    tasks = await build_tm().get_all_tasks()
    return {"tasks": tasks}
