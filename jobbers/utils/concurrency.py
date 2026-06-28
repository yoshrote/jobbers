import asyncio


def has_capacity(sem: asyncio.Semaphore) -> bool:
    """Return True if `sem` currently has a free slot (i.e. acquire() would not block)."""
    return not sem.locked()
