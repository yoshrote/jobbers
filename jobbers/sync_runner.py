"""
Entry point and calling convention for synchronous (CPU-bound) tasks.

Functions registered with `@register_task` that aren't `async def` run in a
dedicated `multiprocessing.Process` (see `jobbers/task_processor.py`) rather
than being awaited in-process. `run_sync_task` is the picklable target passed
to that process; it loads the task module fresh (the function/dependency
graph are never pickled across the process boundary — see CLAUDE.md) and
resolves DI locally before calling the task function with a `SyncTaskContext`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from jobbers import registry
from jobbers.utils.di import DependencyResolver, merge_resolved_kwargs
from jobbers.utils.module_loading import load_task_module

if TYPE_CHECKING:
    import multiprocessing
    from collections.abc import Callable
    from multiprocessing.synchronize import Event as MpEvent

    from jobbers.models.task import Task
    from jobbers.utils.di import DependencyNode


@dataclass
class SyncTaskContext:
    """Passed as the first positional argument to every sync task function."""

    task: Task
    _heartbeat_queue: multiprocessing.Queue[Any]
    _cancel_event: MpEvent

    def heartbeat(self) -> None:
        """Explicit, caller-driven heartbeat — call this periodically during long-running work."""
        self._heartbeat_queue.put(self.task.id)

    @property
    def cancelled(self) -> bool:
        """True once cancellation has been requested; cooperative — the task must check this itself."""
        return self._cancel_event.is_set()


async def _resolve_and_call(
    func: Callable[..., Any],
    dependency_graph: list[DependencyNode],
    kwargs: dict[str, Any],
    ctx: SyncTaskContext,
) -> Any:
    resolver = DependencyResolver(dependency_graph)
    async with resolver:
        dep_cache = await resolver.resolve_all()
        full_kwargs = merge_resolved_kwargs(func, dep_cache, kwargs)
        return func(ctx, **full_kwargs)


def run_sync_task(
    task_module: str,
    task: Task,
    kwargs: dict[str, Any],
    heartbeat_q: multiprocessing.Queue[Any],
    cancel_evt: MpEvent,
    result_q: multiprocessing.Queue[Any],
) -> None:
    """Subprocess entry point. Puts ("ok", result) or ("error", exc) on result_q."""
    try:
        load_task_module(task_module)
        cfg = registry.get_task_config(task.name, task.version)
        if cfg is None:
            raise RuntimeError(
                f"Task {task.name} version {task.version} not found after loading {task_module}"
            )
        ctx = SyncTaskContext(task=task, _heartbeat_queue=heartbeat_q, _cancel_event=cancel_evt)
        result = asyncio.run(_resolve_and_call(cfg.function, cfg.dependency_graph, kwargs, ctx))
        result_q.put(("ok", result))
    except BaseException as exc:
        result_q.put(("error", exc))
