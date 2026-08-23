class TaskCancelledError(BaseException):
    """
    Raised inside a subworker child by its soft-cancel signal handler to unwind the in-flight task.

    A ``BaseException``, not an ``Exception``, so it isn't accidentally swallowed by a
    task's own ``except Exception`` handling. Never crosses the process boundary — the
    child marshals it into a ``SubworkerTaskError`` before replying.
    """
