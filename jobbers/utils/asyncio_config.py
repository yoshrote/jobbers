"""
Shared asyncio event-loop configuration for runner entry points.

Centralizes two process-wide knobs so `worker_proc.py`/`scheduler_proc.py`/`cleaner_proc.py`
don't each reimplement them: whether asyncio debug mode is on, and whether uvloop should
replace the default event loop policy.
"""

from __future__ import annotations

import os


def asyncio_debug_enabled() -> bool:
    """
    Read ASYNCIO_DEBUG (default off).

    Debug mode adds real overhead (slow-callback logging, extra coroutine-origin
    tracking) and should stay off outside of local debugging.
    """
    return os.environ.get("ASYNCIO_DEBUG", "").strip().lower() in ("1", "true", "yes")


def install_uvloop() -> bool:
    """
    Install uvloop as the event loop policy if it's available; return whether it was.

    uvloop is an optional dependency (`pip install -e ".[uvloop]"`) and POSIX-only.
    Falls back silently to the standard asyncio loop when it isn't installed
    (e.g. on Windows, or if the extra wasn't installed).
    """
    try:
        import uvloop  # type: ignore[import-not-found]
    except ImportError:
        return False
    uvloop.install()
    return True
