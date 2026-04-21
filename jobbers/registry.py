import datetime as dt
import logging
from collections.abc import Awaitable, Callable, Iterator
from typing import TYPE_CHECKING, Any

from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy, TaskConfig

if TYPE_CHECKING:
    from jobbers.models.dag import DAGNode
    from jobbers.models.task import Task

logger = logging.getLogger(__name__)
_task_function_map: dict[tuple[str, int], TaskConfig] = {}


class TaskWrapper:
    """
    Wraps a registered task function with helpers for submission and DAG construction.

    Instances are callable — calling them invokes the underlying async task function.
    Three additional methods are provided:

    - ``submit(queue, **params)`` — create and submit a task to a queue.
    - ``schedule(run_at, queue, **params)`` — create and schedule a task for future execution.
    - ``node(queue, **params)`` — return a :class:`~jobbers.models.dag.DAGNode` for
      programmatic DAG construction.
    """

    def __init__(self, func: Callable[..., Awaitable[Any]], name: str, version: int) -> None:
        self._func = func
        self._name = name
        self._version = version

    def __call__(self, **kwargs: Any) -> Awaitable[Any]:
        return self._func(**kwargs)

    async def submit(self, queue: str = "default", **params: Any) -> "Task":
        """Create a Task and submit it to *queue*."""
        from ulid import ULID

        from jobbers import db
        from jobbers.models.task import Task

        task = Task(id=ULID(), name=self._name, version=self._version, queue=queue, parameters=params)
        await db.get_state_manager().submit_task(task)
        return task

    async def schedule(self, run_at: dt.datetime, queue: str = "default", **params: Any) -> "Task":
        """Create a Task and schedule it to run at *run_at*."""
        from ulid import ULID

        from jobbers import db
        from jobbers.models.task import Task

        task = Task(id=ULID(), name=self._name, version=self._version, queue=queue, parameters=params)
        await db.get_state_manager().schedule_new_task(task, run_at)
        return task

    def node(self, queue: str = "default", **params: Any) -> "DAGNode":
        """Return a :class:`~jobbers.models.dag.DAGNode` for this task."""
        from jobbers.models.dag import DAGNode

        return DAGNode(self._name, queue=queue, version=self._version, parameters=params)


def register_task(
    name: str,
    version: int,
    max_concurrent: int | None = 1,
    timeout: int | None = None,
    max_retries: int = 3,
    retry_delay: int | None = None,
    max_retry_delay: int | None = None,
    expected_exceptions: tuple[type[Exception]] | None = None,
    max_heartbeat_interval: dt.timedelta | None = None,
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    dead_letter_policy: DeadLetterPolicy = DeadLetterPolicy.NONE,
) -> Callable[..., Any]:
    """Register a task function with the given name and version."""

    def decorator(func: Callable[..., Any]) -> TaskWrapper:
        """Decorate a task function and registers it for use with task instances."""
        if not callable(func):
            logger.exception("Task function must be callable")
            raise ValueError("Task function must be callable")
        # Unwrap a TaskWrapper so double-decoration stores the raw function.
        raw_func: Callable[..., Any] = func._func if isinstance(func, TaskWrapper) else func
        if (name, version) in _task_function_map:
            if _task_function_map[(name, version)].function != raw_func:
                logger.exception(
                    "Task %s version %d is already registered to another function", name, version
                )
                raise ValueError(f"Task {name} version {version} is already registered to another function")
            else:
                # Allow re-registration of the same function for the same name and version
                logger.warning("Re-registering task %s version %d to the same function", name, version)
        task_conf = TaskConfig(
            name=name,
            version=version,
            function=raw_func,
            max_concurrent=max_concurrent,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            max_retry_delay=max_retry_delay or 3600,
            expected_exceptions=expected_exceptions,
            max_heartbeat_interval=max_heartbeat_interval,
            backoff_strategy=backoff_strategy,
            dead_letter_policy=dead_letter_policy,
        )
        _task_function_map[(name, version)] = task_conf
        return TaskWrapper(raw_func, name, version)

    return decorator


def get_task_config(name: str, version: int) -> TaskConfig | None:
    """Retrieve a task function given its name."""
    return _task_function_map.get((name, version))


def get_tasks() -> Iterator[tuple[str, int]]:
    return iter(_task_function_map.keys())


def clear_registry() -> None:
    """Clear all registered tasks (for testing purposes)."""
    _task_function_map.clear()
