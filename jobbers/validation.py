from jobbers import registry
from jobbers.models.task import Task
from jobbers.state_manager import StateManager


class ValidationError(Exception):
    """Raised when task validation fails before submission."""

    pass


async def validate_task(task: Task, state_manager: StateManager) -> None:
    """Validate task before submission. Raises ValidationError if invalid."""
    task_config = registry.get_task_config(task.name, task.version)
    if task_config is None:
        raise ValidationError(f"Unknown task {task.name} v{task.version}")

    task.task_config = task_config

    if not task.valid_task_params():
        raise ValidationError(f"Invalid parameters for {task.name} v{task.version}")

    # A lane is valid if a routing rule maps it somewhere, or -- by the identity
    # default -- a queue of the same name exists. See docs/lanes-and-queues.md.
    routing = await state_manager.get_routing_config(task.name, task.version)
    rule = routing.rule_for(task.lane) if routing is not None else None
    if rule is None:
        queue_config = await state_manager.get_queue_config(task.lane)
        if queue_config is None:
            raise ValidationError(
                f"Unknown lane {task.lane}: no routing rule matches it and no queue named {task.lane} exists"
            )
    else:
        for queue in rule.queues:
            if await state_manager.get_queue_config(queue) is None:
                raise ValidationError(f"Routing rule for lane {task.lane} targets unknown queue {queue}")
