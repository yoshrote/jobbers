"""
Real, separately-importable sync task module used by tests/test_sync_tasks.py.

Must be a real module (not defined inline in the test file) because the subprocess
spawned by TaskProcessor._execute_sync loads it fresh via `_load_task_module` and looks
the function up via the registry — see jobbers/sync_runner.py.
"""

import time

from jobbers.registry import register_task
from jobbers.sync_runner import SyncTaskContext


@register_task(name="sync_happy", version=1)
def sync_happy(ctx: SyncTaskContext, x: int) -> dict:
    return {"doubled": x * 2}


@register_task(name="sync_heartbeat", version=1)
def sync_heartbeat(ctx: SyncTaskContext) -> dict:
    ctx.heartbeat()
    time.sleep(0.2)
    return {}


@register_task(name="sync_cooperative_cancel", version=1)
def sync_cooperative_cancel(ctx: SyncTaskContext) -> dict:
    for _ in range(200):
        if ctx.cancelled:
            return {"cancelled_seen": True}
        time.sleep(0.05)
    return {"cancelled_seen": False}


@register_task(name="sync_ignores_cancel", version=1)
def sync_ignores_cancel(ctx: SyncTaskContext) -> dict:
    while True:
        pass
