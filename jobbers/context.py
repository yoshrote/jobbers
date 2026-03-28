"""
Per-task execution context.

``current_task`` is a :class:`~contextvars.ContextVar` set by the worker
immediately before invoking a registered task function.  Use
:func:`get_current_task` inside a task to retrieve the running
:class:`~jobbers.models.task.Task` instance.

Example::

    from jobbers.context import get_current_task

    @register_task(name="my_task")
    async def my_task(**kwargs):
        task = get_current_task()
        upstream = await task.parent_results()
        task.heartbeat()
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jobbers.models.task import Task

_current_task: ContextVar[Task | None] = ContextVar("_current_task", default=None)


def get_current_task() -> Task:
    """Return the :class:`~jobbers.models.task.Task` running in this worker coroutine.

    Raises :exc:`RuntimeError` if called outside a task function.
    """
    task = _current_task.get()
    if task is None:
        raise RuntimeError("get_current_task() called outside of a running task")
    return task
