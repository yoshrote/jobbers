from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jobbers.subworker.protocols import SubworkerTaskError


class TaskCancelledError(BaseException):
    """
    Raised inside a subworker child by its soft-cancel signal handler to unwind the in-flight task.

    A ``BaseException``, not an ``Exception``, so it isn't accidentally swallowed by a
    task's own ``except Exception`` handling. Never crosses the process boundary — the
    child marshals it into a ``SubworkerTaskError`` before replying.
    """


class SubworkerTaskFailure(Exception):
    """Raised by ``SubworkerPool.dispatch()`` when the task itself failed (``ResultMsg.ok`` is False)."""

    def __init__(self, error: SubworkerTaskError) -> None:
        super().__init__(f"{error.error_type}: {error.message}")
        self.error = error


class SubworkerCrashedError(Exception):
    """
    Raised by ``SubworkerPool.dispatch()`` when the subworker exits before finishing the request.

    Covers both an unexpected crash and a forced ``kill()`` after a cancel/shutdown grace
    period expires — either way, no ``ResultMsg`` was ever delivered for the request.
    """

    def __init__(self, request_id: str, exit_code: int | None) -> None:
        super().__init__(f"subworker exited (exit_code={exit_code}) before completing request {request_id}")
        self.request_id = request_id
        self.exit_code = exit_code
