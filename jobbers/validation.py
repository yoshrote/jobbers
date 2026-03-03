from jobbers import db, registry
from jobbers.models.queue_config import QueueConfigAdapter
from jobbers.models.task import Task


class ValidationError(Exception):
    """Raised when task validation fails before submission."""

    pass


async def validate_task(task: Task) -> None:
    """Validate task before submission. Raises ValidationError if invalid."""
    task_config = registry.get_task_config(task.name, task.version)
    if task_config is None:
        raise ValidationError(f"Unknown task {task.name} v{task.version}")

    task.task_config = task_config

    if not task.valid_task_params():
        raise ValidationError(f"Invalid parameters for {task.name} v{task.version}")

    queue_config = await QueueConfigAdapter(db.get_sqlite_conn()).get_queue_config(task.queue)
    if queue_config is None:
        raise ValidationError(f"Unknown queue {task.queue}")
