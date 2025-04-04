import logging

from .models import TaskConfig

logger = logging.getLogger(__name__)
_task_function_map = {}

def register_task(
        name: str,
        version: int,
        max_concurrent=1,
        timeout=None,
        max_retries=3,
        retry_delay=None,
        expected_exceptions=()
    ):
    """Register a task function with the given name and version."""

    def decorator(func):
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
            task_function=func,
            max_concurrent=max_concurrent,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            function=func,
            expected_exceptions=expected_exceptions,
        )
        _task_function_map[(name, version)] = task_conf
        return func
    return decorator

def get_task_config(name: str, version: int):
    """Retrieve a task function given its name."""
    return _task_function_map.get((name, version))
