"""
Per-subworker-task execution context, available only inside a sync_subworker child process.

``_current_heartbeat_sender`` is set by the subworker bootstrap loop immediately before
invoking a registered sync task function's body, and reset right after -- the same
set()/reset() pattern ``jobbers.context._current_task`` uses for async tasks. Call
``heartbeat()`` from inside a sync task to send a heartbeat back to the parent worker,
which forwards it to ``StateManager.update_task_heartbeat()`` via ``SubworkerPool``'s
``on_heartbeat`` plumbing (see ``jobbers.subworker.pool`` and
``TaskProcessor.process()``), extending the task's heartbeat deadline the same way an
async task's ``get_current_task().heartbeat()`` does.

Example::

    from jobbers.subworker.context import heartbeat


    @register_task(name="my_sync_task", version=1)
    def my_sync_task(**kwargs):
        for item in big_batch:
            process(item)
            heartbeat()
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

_current_heartbeat_sender: ContextVar[Callable[[], None] | None] = ContextVar(
    "_current_heartbeat_sender", default=None
)


def heartbeat() -> None:
    """
    Send a heartbeat for the currently-running sync_subworker task.

    Raises ``RuntimeError`` if called outside a running sync_subworker task -- mirrors
    ``jobbers.context.get_current_task()``'s fail-loudly-if-misused contract.
    """
    sender = _current_heartbeat_sender.get()
    if sender is None:
        raise RuntimeError("heartbeat() called outside of a running sync_subworker task")
    sender()
