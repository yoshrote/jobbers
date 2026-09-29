import datetime as dt
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

from ulid import ULID

from jobbers import db
from jobbers.models.dag import DAGNode
from jobbers.models.router import RouterConfig
from jobbers.models.task import Task
from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy, TaskConfig
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.utils.di import inspect_task_dependencies

logger = logging.getLogger(__name__)
_task_function_map: dict[tuple[str, int], TaskConfig] = {}
_router_function_map: dict[tuple[str, int], RouterConfig] = {}


class TaskWrapper:
    """
    Wraps a registered task function with helpers for submission and DAG construction.

    Instances are callable — calling them invokes the underlying async task function.
    Three additional methods are provided:

    - ``submit(lane, **params)`` — create and submit a task to a lane.
    - ``schedule(run_at, lane, **params)`` — create and schedule a task for future execution.
    - ``node(lane, **params)`` — return a :class:`~jobbers.models.dag.DAGNode` for
      programmatic DAG construction.
    """

    def __init__(self, func: Callable[..., Awaitable[Any]], name: str, version: int) -> None:
        self._func = func
        self._name = name
        self._version = version

    def __call__(self, **kwargs: Any) -> Awaitable[Any]:
        return self._func(**kwargs)

    async def submit(self, lane: str = "default", **params: Any) -> "Task":
        """Create a Task and submit it to *lane*. Raises TaskRateLimitedError if the queue is at capacity."""
        task = Task(id=ULID(), name=self._name, version=self._version, lane=lane, parameters=params)
        await db.get_state_manager().submit_task(task)
        return task

    async def schedule(self, run_at: dt.datetime, lane: str = "default", **params: Any) -> "Task":
        """Create a Task and schedule it to run at *run_at*."""
        task = Task(id=ULID(), name=self._name, version=self._version, lane=lane, parameters=params)
        await db.get_state_manager().schedule_new_task(task, run_at)
        return task

    def node(self, lane: str = "default", **params: Any) -> "DAGNode":
        """Return a :class:`~jobbers.models.dag.DAGNode` for this task."""
        return DAGNode(self._name, lane=lane, version=self._version, parameters=params)


def register_task(
    name: str,
    version: int,
    max_concurrent: int | None = 1,
    timeout: int | None = None,
    max_retries: int = 3,
    retry_delay: int | None = None,
    max_retry_delay: int | None = None,
    expected_exceptions: tuple[type[Exception], ...] | None = None,
    max_heartbeat_interval: dt.timedelta | None = None,
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    dead_letter_policy: DeadLetterPolicy = DeadLetterPolicy.NONE,
    on_shutdown: TaskShutdownPolicy = TaskShutdownPolicy.STOP,
) -> Callable[..., Any]:
    """Register a task function with the given name and version."""

    def decorator(func: Callable[..., Any]) -> TaskWrapper:
        """Decorate a task function and registers it for use with task instances."""
        if not callable(func):
            logger.exception("Task function must be callable")
            raise ValueError("Task function must be callable")
        # Unwrap a TaskWrapper so double-decoration stores the raw function.
        raw_func: Callable[..., Any] = func._func if isinstance(func, TaskWrapper) else func
        dep_graph = inspect_task_dependencies(raw_func)
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
            dependency_graph=dep_graph,
            max_concurrent=max_concurrent,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            max_retry_delay=max_retry_delay or 3600,
            expected_exceptions=expected_exceptions,
            max_heartbeat_interval=max_heartbeat_interval,
            backoff_strategy=backoff_strategy,
            dead_letter_policy=dead_letter_policy,
            on_shutdown=on_shutdown,
        )
        _task_function_map[(name, version)] = task_conf
        return TaskWrapper(raw_func, name, version)

    return decorator


def register_router(name: str, version: int) -> Callable[..., Any]:
    """
    Register a router function under *name* and *version*.

    A router is a **pure, synchronous** function that decides which of a router
    node's candidate tasks should handle a payload:

    ```python
    @register_router(name="route_by_tier", version=1)
    def route_by_tier(results, *, threshold=100) -> str | RouteTo | None:
        return RouteTo("fulfil_order", lane="priority" if results["vip"] else "standard")
    ```

    It receives the parent task's result dict (or, in per-item fan-out mode, one
    item from it) plus any parameters from the node label, and returns a task
    name, a :class:`~jobbers.models.router.RouteTo` selector, or ``None`` to end
    that path.

    Routers run inline on the worker's event loop during callback handling, so
    ``async def`` is rejected at registration and the function must do no I/O.
    Routers are picked up by the same ``task_module`` import that loads tasks --
    define them alongside your ``@register_task`` functions.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if not callable(func):
            raise ValueError("Router function must be callable")
        if inspect.iscoroutinefunction(func):
            raise ValueError(
                f"Router {name} must be a plain 'def' -- routers run synchronously "
                "during callback handling and must not perform I/O"
            )
        if (name, version) in _router_function_map:
            if _router_function_map[(name, version)].function != func:
                raise ValueError(f"Router {name} version {version} is already registered to another function")
            logger.warning("Re-registering router %s version %d to the same function", name, version)
        _router_function_map[(name, version)] = RouterConfig(name=name, version=version, function=func)
        return func

    return decorator


def get_task_config(name: str, version: int) -> TaskConfig | None:
    """Retrieve a task function given its name."""
    return _task_function_map.get((name, version))


def get_router_config(name: str, version: int) -> RouterConfig | None:
    """Retrieve a registered router by name and version."""
    return _router_function_map.get((name, version))


def get_tasks() -> Iterator[tuple[str, int]]:
    return iter(_task_function_map.keys())


def get_routers() -> Iterator[tuple[str, int]]:
    return iter(_router_function_map.keys())


def clear_registry() -> None:
    """Clear all registered tasks and routers (for testing purposes)."""
    _task_function_map.clear()
    _router_function_map.clear()
