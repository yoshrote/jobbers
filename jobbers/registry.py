import logging
from collections.abc import Callable, Iterator
from typing import Any

from jobbers.models.task_config import TaskConfig

logger = logging.getLogger(__name__)
_task_function_map : dict[tuple[str, int], TaskConfig] = {}

def register_task(
        name: str,
        version: int,
        max_concurrent: int | None=1,
        timeout: int | None=None,
        max_retries: int=3,
        retry_delay: int | None=None,
        expected_exceptions: tuple[Exception] | None=None,
        max_heartbeat_interval: int | None=None
    ) -> Callable[..., Any]:
    """Register a task function with the given name and version."""

    def decorator(func: Callable[..., Any]) -> Any:
        """Decorate a task function and registers it for use with task instances."""
        if not callable(func):
            logger.exception("Task function must be callable")
            raise ValueError("Task function must be callable")
        if (name, version) in _task_function_map:
            if _task_function_map[(name, version)].function != func:
                logger.exception("Task %s version %d is already registered to another function", name, version)
                raise ValueError(f"Task {name} version {version} is already registered to another function")
            else:
                # Allow re-registration of the same function for the same name and version
                logger.warning("Re-registering task %s version %d to the same function", name, version)
        task_conf = TaskConfig(
            name=name,
            version=version,
            function=func,
            max_concurrent=max_concurrent,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            expected_exceptions=expected_exceptions,
            max_heartbeat_interval=max_heartbeat_interval,
        )
        _task_function_map[(name, version)] = task_conf
        return func
    return decorator

def get_task_config(name: str, version: int) -> TaskConfig | None:
    """Retrieve a task function given its name."""
    return _task_function_map.get((name, version))

def get_tasks() -> Iterator[tuple[str, int]]:
    return iter(_task_function_map.keys())

def clear_registry() -> None:
    """Clear all registered tasks (for testing purposes)."""
    _task_function_map.clear()
