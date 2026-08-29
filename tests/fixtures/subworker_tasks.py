"""
Task fixtures for subworker handle tests.

Imported fresh inside a spawned child process (multiprocessing) or a stdio child
process, so keep this module free of anything that only works inside the test process
itself (no test-only imports, no monkeypatching).
"""

import time

from jobbers.registry import register_task
from jobbers.subworker.context import heartbeat


@register_task(name="subworker_add", version=1)
def subworker_add(a: int, b: int) -> int:
    return a + b


@register_task(name="subworker_fail", version=1)
def subworker_fail(message: str) -> None:
    raise ValueError(message)


@register_task(name="subworker_sleep", version=1)
def subworker_sleep(seconds: float) -> str:
    time.sleep(seconds)
    return "done"


@register_task(name="subworker_heartbeat_then_done", version=1)
def subworker_heartbeat_then_done(count: int) -> str:
    for _ in range(count):
        heartbeat()
    return "done"
